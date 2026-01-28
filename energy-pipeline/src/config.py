from pathlib import Path
from typing import Any, Dict
import yaml
import pandas as pd


def load_yaml(path: str | Path) -> Dict[str, Any]:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def ensure_dir(path: str | Path) -> Path:
    try:
        p = Path(path)
        p.mkdir(parents=True, exist_ok=True)
        return p
    except Exception as e:
        print(f"Error creating directory {path}: {e}")
        raise


def write_parquet(df: pd.DataFrame, out_path: Path, job: dict, level: str) -> None:
    mode = job.get("output", {}).get("mode", "overwrite")
    merge_keys = job.get("output", {}).get("merge_keys", [])

    print(
        f"Writing {level} data to {out_path} with mode={mode} and merge_keys={merge_keys}"
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


def require_keys(d: Dict[str, Any], keys: list[str], ctx: str) -> None:
    missing = [k for k in keys if k not in d]
    if missing:
        raise KeyError(f"Missing keys in {ctx}: {missing}")


def validate_raw_config(cfg: Dict[str, Any]) -> None:
    require_keys(cfg, ["raw", "sources"], "root")
    require_keys(cfg["raw"], ["base_dir", "run_id_format"], "raw")

    sources = cfg["sources"]
    require_keys(sources, ["csv", "api"], "sources")

    csv = sources["csv"]
    require_keys(csv, ["input_path", "output_subdir", "output_filename"], "sources.csv")

    api = sources["api"]
    require_keys(
        api,
        [
            "base_url",
            "api_key_env",
            "regions",
            "params",
            "output_subdir",
            "output_filename",
        ],
        "sources.api",
    )
    if not isinstance(api["regions"], list) or not api["regions"]:
        raise ConfigError("sources.api.regions must be a non-empty list")
