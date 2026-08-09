# Databricks notebook source
# config_loader.py
# Loads pipeline_config.yaml and resolves the environment-specific block.
# Imported by bronze_notebook.py / silver_notebook.py / silver_plus_notebook.py
# (e.g. via `%run ./config/config_loader`) instead of hardcoding S3 paths,
# catalog names, or job defaults inline.

import yaml


def load_pipeline_config(env: str, config_path: str = "./config/pipeline_config.yaml") -> dict:
    """
    Load pipeline_config.yaml and return the merged config for one environment.

    Args:
        env: "dev" | "stg" | "prod"
        config_path: path to pipeline_config.yaml (workspace file path or DBFS/volume path)

    Returns:
        dict with keys: aws, databricks, regions, tables, job_defaults
    """
    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    if env not in raw.get("environments", {}):
        raise ValueError(f"Unknown environment '{env}'. Valid options: {list(raw.get('environments', {}).keys())}")

    env_cfg = raw["environments"][env]

    return {
        "aws": env_cfg["aws"],
        "databricks": env_cfg["databricks"],
        "regions": raw["regions"],
        "tables": raw["tables"],
        "job_defaults": raw["job_defaults"],
    }


def resolve_raw_landing_root(cfg: dict) -> str:
    """s3://<bucket>/<raw_prefix> — root that bronze_notebook.py walks per region."""
    return f"s3://{cfg['aws']['s3_bucket']}/{cfg['aws']['s3_raw_prefix']}"


def resolve_bronze_target(cfg: dict) -> str:
    """<catalog>.<bronze_schema> — fully qualified Bronze schema."""
    return f"{cfg['databricks']['catalog']}.{cfg['databricks']['bronze_schema']}"


def resolve_silver_target(cfg: dict, table_key: str = "silver_summary") -> str:
    """<catalog>.<silver_schema>.<table> — fully qualified Silver table."""
    return f"{cfg['databricks']['catalog']}.{cfg['databricks']['silver_schema']}.{cfg['tables'][table_key]}"


def resolve_silver_plus_target(cfg: dict, table_key: str = "silver_lines") -> str:
    """<catalog>.<silver_plus_schema>.<table> — fully qualified Silver Plus table."""
    return f"{cfg['databricks']['catalog']}.{cfg['databricks']['silver_plus_schema']}.{cfg['tables'][table_key]}"


def resolve_cdf_state_table(cfg: dict) -> str:
    """<catalog>.control_schema.cdf_state — fully qualified control table used by CdfVersionTracker."""
    return f"{cfg['databricks']['catalog']}.{cfg['databricks']['control_schema']}.{cfg['tables']['cdf_state']}"
