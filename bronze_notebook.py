# Databricks notebook source
"""Call engagement voice-analysis (Contact Lens) Bronze ingestion.

Reads redacted Contact Lens voice-analysis JSON files from regional Amazon
Connect S3 export locations, serializes complex (struct/array/map) columns to
JSON strings to avoid cross-region schema conflicts, derives region and
partition metadata, and appends the result to a centralized Bronze Delta
table.
"""

# COMMAND ----------

# Imports
from datetime import datetime, timedelta
from functools import reduce

from pyspark.sql.functions import (
    col,
    dayofmonth,
    lit,
    month,
    regexp_extract,
    regexp_replace,
    to_date,
    to_json,
    year,
)
from pyspark.sql.types import ArrayType, MapType, StructType

# COMMAND ----------

# Spark execution configs
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")
spark.conf.set("spark.sql.files.maxPartitionBytes", "536870912")
spark.conf.set("spark.sql.files.openCostInBytes", "4194304")
spark.conf.set("spark.sql.sources.parallelPartitionDiscovery.parallelism", "50")

# COMMAND ----------

# MAGIC %run ./config/config_loader

# COMMAND ----------

# Runtime parameters
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("lookback_days", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("regions", "")

env = dbutils.widgets.get("env") or "dev"
lookback_days = int(dbutils.widgets.get("lookback_days") or 2)
end_date_str = dbutils.widgets.get("end_date")
end_date = (
    datetime.today().date()
    if end_date_str == ""
    else datetime.strptime(end_date_str, "%Y-%m-%d").date()
)
regions = [r.strip() for r in dbutils.widgets.get("regions").split(",") if r.strip()]

print("CALL TRANSCRIPT BRONZE INGEST STARTED")
print(f"Environment          : {env}")
print(f"Lookback days       : {lookback_days}")
print(f"End date            : {end_date}")
print(f"Regions parameter    : {regions if regions else 'ALL'}")

# COMMAND ----------

# Storage locations - resolved from config/pipeline_config.yaml (see setup_guide.md)
cfg = load_pipeline_config(env=env, config_path="./config/pipeline_config.yaml")

BRONZE_VOICE_PATH = f"s3://{cfg['aws']['s3_curated_bucket']}/{cfg['aws']['s3_bronze_prefix']}"
RAW_LANDING_ROOT = resolve_raw_landing_root(cfg)

# COMMAND ----------

# Source voice-analysis buckets per region - one sub-path per entry in
# config/pipeline_config.yaml -> regions, rooted at RAW_LANDING_ROOT
REGION_SOURCE_PATHS = {region: f"{RAW_LANDING_ROOT}/{region}/" for region in cfg["regions"]}

if regions:
    REGION_SOURCE_PATHS = {r: REGION_SOURCE_PATHS[r] for r in regions if r in REGION_SOURCE_PATHS}
print(f"Regions to ingest : {list(REGION_SOURCE_PATHS.keys())}")

# COMMAND ----------

# Utility: check path existence
def _path_exists(path: str) -> bool:
    """Return True if the given DBFS/volume path exists."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


# COMMAND ----------

# Utility: generate paths for available voice data, tagged per region.
#
# NOTE: region is returned explicitly per path here - it is NOT parsed back
# out of the file path later. An earlier version tried to regex the region
# out of src_file_path (assuming a "redacted-<region>" folder naming
# convention), but our actual layout is
# <raw_landing_root>/<REGION>/<yyyy>/<mm>/<dd>/analysis_redacted-<uuid>.json
# - no such "redacted-<region>" segment exists, so that regex accidentally
# matched the "redacted-" prefix INSIDE THE FILENAME instead, capturing the
# contact_id/UUID + ".json" as the "region" value. Tagging region
# deterministically at discovery time (where we already know it for certain)
# avoids this class of bug entirely.
def discover_source_paths():
    """Generate day-level paths for available voice data within the lookback
    window, grouped by region: {region: [path, path, ...]}."""
    start_date = end_date - timedelta(days=lookback_days)
    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    print(f"Scanning date range: {start_date} - {end_date}")

    paths_by_region = {}
    empty_regions = []
    for region, base in REGION_SOURCE_PATHS.items():
        region_paths = []
        for d in dates:
            p = f"{base}{d.year}/{d.month:02d}/{d.day:02d}/"
            if _path_exists(p):
                region_paths.append(p)
        if region_paths:
            paths_by_region[region] = region_paths
        else:
            print(f"Region {region}: no data found")
            empty_regions.append(region)
    return paths_by_region, empty_regions


paths_by_region, empty_regions = discover_source_paths()
if not paths_by_region:
    raise Exception("No voice file paths found for the given parameters")

# COMMAND ----------

# Utility: serialize complex columns to JSON
def serialize_complex_columns(df):
    """Serialize complex (struct / array / map) columns to JSON strings to avoid schema conflicts across regions."""
    schema = df.schema  # compute once - avoids repeated Analyze RPCs
    return df.select(
        [
            to_json(col(f.name)).alias(f.name)
            if isinstance(f.dataType, (StructType, ArrayType, MapType))
            else col(f.name)
            for f in schema.fields
        ]
    )


# COMMAND ----------

# Read raw JSON files per region (so each region's rows can be tagged with
# a literal, known-correct region value) and union the results.
def _read_region(region: str, paths: list):
    return (
        spark.read.option("pathGlobFilter", "*analysis_redacted*.json")
        .option("recursiveFileLookup", "true")
        .option("ignoreMissingFiles", "true")
        .json(paths)
        .withColumn("src_file_path", col("_metadata.file_path"))
        .withColumn("region", lit(region))
    )


region_dataframes = [_read_region(region, paths) for region, paths in paths_by_region.items()]
df_raw = reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), region_dataframes)

# COMMAND ----------

# Serialize complex columns to JSON to avoid schema conflicts across regions
df_parsed = serialize_complex_columns(df_raw)

# COMMAND ----------

# Add metadata columns for partitioning and auditing.
# NOTE: "region" is NOT derived here - it was already tagged as a literal,
# known-correct value per-row in _read_region() above, from the same
# REGION_SOURCE_PATHS entry that was used to discover the file in the first
# place. This avoids parsing region back out of the file path.
df_bronze = (
    df_parsed.withColumn(
        "extraction_date",
        to_date(
            regexp_replace(
                regexp_extract(col("src_file_path"), r"(\d{4}/\d{2}/\d{2})", 1),
                "/",
                "-",
            ),
        ),
    )
    .withColumn("year", year("extraction_date"))
    .withColumn("month", month("extraction_date"))
    .withColumn("day", dayofmonth("extraction_date"))
    .withColumn("pl_created_on", lit(datetime.now()).cast("timestamp"))
    .withColumn("pl_created_by", lit("CALL_TXN_BRONZE_INGEST"))
)

# COMMAND ----------

final_df = df_bronze.repartition(sc.defaultParallelism)

# COMMAND ----------

# Append to partitioned Bronze Delta table
print("Launching Call Transcript BRONZE pipeline (partitioned by year/month)")
(
    final_df.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .partitionBy("year", "month")
    .save(BRONZE_VOICE_PATH)
)
print("CALL TRANSCRIPT BRONZE INGEST COMPLETE")

if empty_regions:
    print(f"No data found for regions: {empty_regions}")
else:
    print("All regions processed successfully")

# COMMAND ----------
