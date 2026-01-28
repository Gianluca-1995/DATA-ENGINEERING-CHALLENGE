from pathlib import Path
import pandas as pd

from config import ensure_dir, write_parquet


class Gold:
    def __init__(self, cfg_gold: dict, run_id: str):
        self.cfg_gold = cfg_gold
        self.run_id = run_id

    def run(self) -> dict:
        results = {}
        for src in self.cfg_gold.get("sources", []):
            out_path = self._process_job(src)
            results[src["id"]] = str(out_path)
        return results

    def _process_job(self, src: dict) -> Path:
        inputs = src.get("input", [])
        out_dir = ensure_dir(Path(src["output"]["dir"]))
        out_path = out_dir / src["output"]["name"]

        dfs: dict[str, pd.DataFrame] = {}
        dfs = self._load_inputs(inputs)

        df = self._apply_joins(dfs, src.get("joins", []))
        df = self._apply_aggregation(df, src.get("aggregation", {}))
        df = self._apply_post_calculations(df, src.get("post_calculations", []))

        write_parquet(df, out_path, src, "Gold")
        return out_path

    def _load_inputs(self, inputs: list[dict]) -> dict[str, pd.DataFrame]:
        dfs: dict[str, pd.DataFrame] = {}
        for inp in inputs:
            in_dir = ensure_dir(Path(inp["dir"]))
            in_path = in_dir / inp["name"]
            df_id = Path(inp["name"]).stem  # get filename without extension
            dfs[df_id] = pd.read_parquet(in_path)
        return dfs

    def _apply_joins(
        self, dfs: dict[str, pd.DataFrame], joins: list[dict]
    ) -> pd.DataFrame:
        if not joins:
            if len(dfs) != 1:
                raise Exception("No joins defined but multiple inputs provided")
            return next(iter(dfs.values()))

        base = None  # keep track of the previous join on this dataframe if multiple joins are defined
        for j in joins:
            left_df = j["left"]
            right_df = j["right"]
            how = j.get("how", "left")
            on_map = j.get("based_on", {})

            if base is None:
                left_df = dfs[left_df]
            else:
                left_df = base

            right_df = dfs[right_df]

            left_on = []
            right_on = []
            for k, v in on_map.items():
                left_on.append(k)
                right_on.append(v)

            base = left_df.merge(right_df, how=how, left_on=left_on, right_on=right_on)

        return base

    def _apply_aggregation(self, df: pd.DataFrame, agg_cfg: dict) -> pd.DataFrame:
        if not agg_cfg or not agg_cfg.get("enabled", False):
            return df

        group_by = agg_cfg.get("group_by", [])
        metrics = agg_cfg.get("metrics", [])

        if not group_by:
            raise Exception(
                "aggregation.group_by is required when aggregation is enabled"
            )
        if not metrics:
            raise Exception(
                "aggregation.metrics is required when aggregation is enabled"
            )

        partial_df_aggregations = []

        for metric in metrics:
            metric_name = metric.get("name")  # optional
            aggregation_column = metric["column"]
            aggragation_funcion = metric["agg"]
            aggregation_filter = metric.get("filter")

            df_metric = df

            if aggregation_filter:
                df_metric = self._apply_metric_filter(df_metric, aggregation_filter)

            grouped = (
                df_metric.groupby(group_by, dropna=False)[aggregation_column]
                .agg(aggragation_funcion)
                .reset_index()
            )

            if metric_name:
                grouped = grouped.rename(columns={aggregation_column: metric_name})

            partial_df_aggregations.append(grouped)

        # Merge all metric results on group_by
        result = partial_df_aggregations[0]
        for aggregation in partial_df_aggregations[1:]:
            result = result.merge(aggregation, how="outer", on=group_by)

        # Fill NaN metrics with 0
        for metric in metrics:
            metric_name = metric.get("name") or metric["column"]
            if metric_name in result.columns:
                result[metric_name] = result[metric_name].fillna(0)

        return result

    def _apply_metric_filter(self, df: pd.DataFrame, filt: dict) -> pd.DataFrame:
        out = df
        for k, v in filt.items():
            out = out[out[k] == v]
        return out

    def _apply_post_calculations(
        self, df: pd.DataFrame, calcs: list[dict]
    ) -> pd.DataFrame:
        if not calcs:
            return df

        df = df.copy()
        for c in calcs:
            name = c["name"]
            formula = c["formula"]

            df[name] = pd.eval(
                formula,
                engine="python",
                parser="pandas",
                local_dict=df.to_dict("series"),
            )

        return df

    def _write_parquet(self, df: pd.DataFrame, out_path: Path, job: dict) -> None:
        mode = job.get("output", {}).get("mode", "overwrite")
        merge_keys = job.get("output", {}).get("merge_keys", [])

        print(
            f"Writing Gold data to {out_path} with mode={mode} and merge_keys={merge_keys}"
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
