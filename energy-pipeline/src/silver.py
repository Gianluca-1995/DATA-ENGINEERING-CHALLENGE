from pathlib import Path
from zoneinfo import ZoneInfo
import operator

import pandas as pd

from config import ensure_dir, write_parquet


class Silver:
    def __init__(self, cfg_silver: dict, run_id: str):
        self.cfg_silver = cfg_silver
        self.run_id = run_id
        self.lookups = cfg_silver.get("lookups", {})

    def run(self) -> dict:
        results = {}
        for src in self.cfg_silver.get("sources", []):
            out_path = self._process_source(src)
            results[src["id"]] = str(out_path)
        return results

    def _process_source(self, src: dict) -> Path:
        in_dir = ensure_dir(Path(src["input"]["dir"]))
        in_path = in_dir / src["input"]["name"]
        out_dir = ensure_dir(Path(src["output"]["dir"]))
        out_path = out_dir / src["output"]["name"]

        df = pd.read_parquet(in_path)

        df = self._apply_mappings(df, src.get("mappings", []))
        df = self._apply_aggregation(df, src.get("aggregation", {}))
        df = self._apply_filtering(df, src.get("filter", []))

        write_parquet(df, out_path, src, "Silver")
        return out_path

    def _write_parquet(self, df: pd.DataFrame, out_path: Path, src: dict) -> None:
        mode = src.get("output", {}).get("mode", "overwrite")
        merge_keys = src.get("output", {}).get("merge_keys", [])
        print(
            f"Writing Silver data to {out_path} with mode={mode} and merge_keys={merge_keys}"
        )

        if mode == "overwrite" or not out_path.exists():
            df.to_parquet(out_path, index=False)
            return

        if mode == "append":
            old = pd.read_parquet(out_path)
            df_all = pd.concat([old, df], ignore_index=True)
            if merge_keys:
                df_all = df_all.drop_duplicates(subset=merge_keys, keep="last")
            else:
                df_all = df_all.drop_duplicates(keep="last")
            df_all.to_parquet(out_path, index=False)
            return

        raise Exception(f"Unknown output mode: {mode}")

    def _apply_mappings(self, df: pd.DataFrame, mappings: list[dict]) -> pd.DataFrame:
        output_df = pd.DataFrame()

        for column_mapping_info in mappings:
            if not column_mapping_info.get("active", True):
                continue

            output_column = column_mapping_info["output_column"]
            input_column = column_mapping_info["input_column"]
            output_type = column_mapping_info.get("output_type", "string")

            series = df[input_column]

            if output_type == "datetime_utc":
                ts_config = column_mapping_info.get("timestamp", {})
                series = self._parse_to_utc(series, ts_config)
                output_df[output_column] = series
                continue

            output_df[output_column] = self._cast_series(series, output_type)

        return output_df

    def _cast_series(self, series: pd.Series, out_type: str) -> pd.Series:
        if out_type == "string":
            return series.where(pd.notnull(series), None).astype(str)

        if out_type == "float" or out_type == "double":
            return pd.to_numeric(series, errors="coerce")

        if out_type == "int":
            return pd.to_numeric(series, errors="coerce").astype("Int64")

        raise Exception(f"Unknown output_type: {out_type}")

    def _parse_to_utc(self, series: pd.Series, ts_cfg: dict) -> pd.Series:
        input_format = ts_cfg.get("input_format")
        input_timezone = ts_cfg.get("input_timezone", "UTC")
        output_timezone = ts_cfg.get("output_timezone", "UTC")

        if not input_format:
            raise Exception("timestamp.input_format is required for datetime_utc")

        dt = pd.to_datetime(series, format=input_format, errors="coerce")

        dt = dt.dt.tz_localize(
            ZoneInfo(input_timezone), ambiguous="NaT", nonexistent="NaT"
        )

        dt_utc = dt.dt.tz_convert(ZoneInfo(output_timezone))

        return dt_utc

    def _apply_aggregation(self, df: pd.DataFrame, agg_cfg: dict) -> pd.DataFrame:
        if not agg_cfg or not agg_cfg.get("enabled", False):
            return df

        grain = agg_cfg.get("grain")
        timestamp_column = agg_cfg.get("timestamp_column")
        group_by = agg_cfg.get("group_by", [])
        metrics = agg_cfg.get("metrics", [])

        df = df.copy()
        df[timestamp_column] = df[timestamp_column].dt.floor(grain)

        agg_dict = {}
        for metric in metrics:
            column_to_agg = metric["column"]
            function_to_agg = metric["agg"]
            agg_dict[column_to_agg] = function_to_agg

        grouped = df.groupby(group_by, dropna=False).agg(agg_dict).reset_index()
        return grouped

    def _apply_filtering(
        self, df: pd.DataFrame, filter_cfg: list[dict]
    ) -> pd.DataFrame:
        if not filter_cfg:
            return df

        df_filtered = df.copy()
        filter_operators = {
            "==": operator.eq,
            "!=": operator.ne,
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le,
            "is Null": lambda s, _: s.isnull(),
            "is not Null": lambda s, _: s.notnull(),
        }

        for filter_rule in filter_cfg:
            column = filter_rule["column"]
            operatoration = filter_rule["operator"]
            value = filter_rule["value"]

            if operatoration not in filter_operators:
                raise ValueError(f"Unknown filter operator: {operatoration}")
            df_filtered = df_filtered[
                filter_operators[operatoration](df_filtered[column], value)
            ]

        return df_filtered
