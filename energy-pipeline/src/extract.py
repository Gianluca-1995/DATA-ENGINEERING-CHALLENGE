from raw import RawExtractor
from config import load_yaml


def run_extract(
    run_id: str, raw_config_path: str = "energy-pipeline/configs/raw.yml"
) -> dict:
    cfg = load_yaml(raw_config_path)
    outputsRaw = RawExtractor(run_id=run_id, cfg=cfg).run()
    return
