import json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

from config import ensure_dir, write_parquet


class Bronze:
    def __init__(self, cfg_bronze: dict, cfg_raw: dict, run_id: str):
        self.cfg_bronze = cfg_bronze
        self.cfg_raw = cfg_raw
        self.run_id = run_id

    def run(self) -> dict:
        results = {}
        for src in self.cfg_bronze.get("sources", []):
            out_path = self._process_source(src)
            results[src["id"]] = str(out_path)
        return results

    def _process_source(self, src: dict) -> Path:
        in_dir = ensure_dir(Path(src["input"]["dir"]) / self.run_id)
        in_path = in_dir / src["input"]["name"]
        out_dir = ensure_dir(Path(src["output"]["dir"]))
        out_path = out_dir / src["output"]["name"]

        input_df = self._read_input(src, in_path)

        output_df = self._organize_input(input_df, src)

        write_parquet(output_df, out_path, src, "Bronze")

        return out_path

    def _read_input(self, src: dict, in_path: Path) -> pd.DataFrame:
        print("Reading input from:", in_path)
        if src.get("type") == "csv":
            df = self._read_csv(in_path)
        elif src.get("type") == "json":
            df = self._read_json(in_path)
        else:
            raise Exception(f"Unknown source type: {src.get('type')}")
        return df

    # ---------- CSV ----------
    def _read_csv(self, in_path: Path) -> Path:
        df = pd.read_csv(in_path, dtype=str)
        return df

    # ---------- JSON ----------
    def _read_json(self, in_path: Path):
        with open(in_path, "r", encoding="utf-8") as f:
            records = json.load(f)
        df = pd.DataFrame(records).fillna("").astype(str)
        return df

    # ---------- Common functions ----------
    def _organize_input(self, df: pd.DataFrame, src: dict) -> pd.DataFrame:
        df = self._select_and_rename(df, src)
        df = self._add_ingestion_timestamp(df)
        return df

    def _select_and_rename(self, df: pd.DataFrame, src: dict) -> pd.DataFrame:
        mappings = [c for c in src.get("columns", []) if c.get("active", True)]
        in_cols = [m["input"] for m in mappings]
        out_cols = [m["output"] for m in mappings]

        df = df[in_cols].copy()
        rename_map = dict(zip(in_cols, out_cols))
        return df.rename(columns=rename_map)

    def _add_ingestion_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        ts = datetime.now(timezone.utc).isoformat()
        df["ingestion_timestamp"] = ts
        return df

    def _write_parquet(self, df: pd.DataFrame, out_path: Path, src: dict) -> None:
        mode = src.get("output", {}).get("mode", "overwrite")
        merge_keys = src.get("output", {}).get("merge_keys", [])
        print(
            f"Writing Bronze data to {out_path} with mode={mode} and merge_keys={merge_keys}"
        )

        if mode == "append" and out_path.exists():
            old_parquet = pd.read_parquet(out_path)
            output_df = pd.concat([old_parquet, df], ignore_index=True)

            if merge_keys:
                output_df = output_df.drop_duplicates(subset=merge_keys, keep="last")
            else:
                output_df = output_df.drop_duplicates(keep="last")
        else:
            output_df = df

        output_df.to_parquet(out_path, index=False)
