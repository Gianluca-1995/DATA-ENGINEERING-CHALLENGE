import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict
from datetime import datetime, timedelta, timezone

import requests

from config import ensure_dir


class RawExtractor:
    def __init__(self, run_id: str, cfg: Dict[str, Any]) -> None:
        self.cfg = cfg
        self.run_id = run_id

    def run(self) -> dict[str, dict]:
        raw_cfg = self.cfg["raw"]
        base_dir = ensure_dir(Path(raw_cfg["base_dir"]))
        sources = self.cfg["sources"]

        unknown_source_name: list = []
        output_paths: dict[Path] = {}

        for source_type, source_cfg_ls in sources.items():
            if source_type == "csv":
                for csv_cfg in source_cfg_ls:
                    output_dir = ensure_dir(
                        Path(base_dir) / csv_cfg["output_subdir"] / self.run_id
                    )
                    output_path = self._ingest_csv(output_dir, csv_cfg)
            elif source_type == "api":
                for api_cfg in source_cfg_ls:
                    output_dir = ensure_dir(
                        Path(base_dir) / api_cfg["output_subdir"] / self.run_id
                    )
                    output_path = self._ingest_api(output_dir, api_cfg)
            else:
                unknown_source_name.append(source_type)
                continue
            print(f"Ingested raw source '{source_type}' to: {output_path}")
            output_paths[source_type] = output_path

        if unknown_source_name != []:
            raise Exception(
                f"Unknown raw source: {unknown_source_name}. All known sources where processed."
            )

        return {
            "run_id": self.run_id,
            "output_path": output_paths,
        }

    def _ingest_csv(self, output_dir: Path, csv_cfg: Dict[str, Any]) -> Path:
        in_path = Path(csv_cfg["input_path"])
        if not in_path.exists():
            raise FileNotFoundError(f"CSV input not found: {in_path}")

        output_path = output_dir / csv_cfg["output_filename"]

        shutil.copyfile(in_path, output_path)
        return output_path

    def _ingest_api(self, output_dir: Path, api_cfg: Dict[str, Any]) -> Path:
        api_key_env = api_cfg["api_key_env"]
        api_key = os.getenv(api_key_env)
        if not api_key:
            raise KeyError(f"Missing API key env var: {api_key_env}")

        output_path = output_dir / api_cfg["output_filename"]

        now = datetime.now(timezone.utc)
        start = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H")
        end = now.strftime("%Y-%m-%dT%H")

        params = dict(api_cfg.get("params", {}))
        params["api_key"] = api_key
        params["start"] = start
        params["end"] = end

        facets = api_cfg.get("facets", {})
        for facet_name, facet_values in facets.items():
            for i, v in enumerate(facet_values):
                params[f"facets[{facet_name}][{i}]"] = v

        data_columns = api_cfg.get("data_columns")
        for i, col in enumerate(data_columns):
            params[f"data[{i}]"] = col

        try:
            session = requests.Session()
            response = session.get(api_cfg["base_url"], params=params, timeout=60)
            response.raise_for_status()
        except requests.RequestException as e:
            raise requests.RequestException(f"API request failed: {e}") from e

        try:
            response_json = response.json()
        except ValueError as e:
            raise ValueError("API did not return valid JSON") from e

        with output_path.open("w", encoding="utf-8") as f:
            json.dump(response_json["response"]["data"], f, ensure_ascii=False)
        return output_path
