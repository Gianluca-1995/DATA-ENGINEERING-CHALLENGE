from bronze import Bronze
from silver import Silver
from gold import Gold
from config import load_yaml


def run_transform(run_id: str):
    run_transform_bronze(run_id)
    run_transform_silver(run_id)
    run_transform_gold(run_id)
    return


def run_transform_bronze(
    run_id: str,
    bronze_config_path: str = "energy-pipeline/configs/bronze.yml",
    raw_config_path: str = "energy-pipeline/configs/raw.yml",
) -> dict:
    cfg_bronze = load_yaml(bronze_config_path)
    cfg_raw = load_yaml(raw_config_path)
    print("Starting Bronze Transformation")
    bronze_run = Bronze(cfg_bronze=cfg_bronze, cfg_raw=cfg_raw, run_id=run_id)
    outputsBronze = bronze_run.run()
    return outputsBronze


def run_transform_silver(
    run_id: str,
    silver_config_path: str = "energy-pipeline/configs/silver.yml",
) -> dict:
    cfg_silver = load_yaml(silver_config_path)
    print("Starting Silver Transformation")
    silver_run = Silver(cfg_silver=cfg_silver, run_id=run_id)
    outputsSilver = silver_run.run()
    return outputsSilver


def run_transform_gold(
    run_id: str, gold_config_path: str = "energy-pipeline/configs/gold.yml"
) -> dict:
    cfg_gold = load_yaml(gold_config_path)
    print("Starting Gold Transformation")
    gold_run = Gold(cfg_gold=cfg_gold, run_id=run_id)
    outputsGold = gold_run.run()
    return outputsGold
