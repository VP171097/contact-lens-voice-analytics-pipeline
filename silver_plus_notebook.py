# Databricks notebook source
"""Call Transcript Lines - Silver Plus ingestion (Streaming, CDF).

Reads incremental changes from the Silver call-transcript summary table via
Delta Change Data Feed, explodes the transcript array into individual
line-level rows, and streaming-merges the result into the Silver Plus
call-transcript-lines table.

Source: `call_transcript_summary`
Target: `call_transcript_lines`
"""

# COMMAND ----------

# MAGIC %run ./common_utils

# COMMAND ----------

# MAGIC %run ./partition_window_utils

# COMMAND ----------

# MAGIC %run ./config/config_loader

# COMMAND ----------

# Imports
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment (dev/stg/prod)")
env = dbutils.widgets.get("env") or "dev"

# Source & target locations - resolved from config/pipeline_config.yaml
cfg = load_pipeline_config(env=env, config_path="./config/pipeline_config.yaml")

SILVER_SUMMARY_TABLE = resolve_silver_target(cfg, table_key="silver_summary")
SILVER_LINES_TABLE = resolve_silver_plus_target(cfg, table_key="silver_lines")
CHECKPOINT_PATH = f"s3://{cfg['aws']['s3_curated_bucket']}/checkpoints/call_engagement/transcript_lines"
CONTROL_TABLE = resolve_cdf_state_table(cfg)

natural_key_cols = ["contact_id", "id"]

# SILVER_LINES_TABLE has GENERATED columns (year, month, day - computed from
# transcript_creation_date). Referenced by both _cast_to_target_schema()
# (to exclude them from the aligned DataFrame entirely) and the merge step
# (to exclude them from the explicit SET/INSERT column list).
GENERATED_COLUMNS = {"year", "month", "day"}

# COMMAND ----------

# MAGIC %run ./cdf_version_tracker

# COMMAND ----------

# CDF version bounds for the source table
current_version = get_current_delta_version(SILVER_SUMMARY_TABLE)
if current_version is None:
    dbutils.notebook.exit(f"[INFO]: Could not determine current version for {SILVER_SUMMARY_TABLE}")

starting_version = current_version
latest_state = CdfVersionTracker.get_latest_state(SILVER_SUMMARY_TABLE, SILVER_LINES_TABLE)
source_version = latest_state.get("last_processed_version") if latest_state else None

if source_version is None:
    starting_version = current_version
elif int(source_version) >= current_version:
    dbutils.notebook.exit("[INFO]: No new data to process")
else:
    starting_version = int(source_version) + 1

print(f"[INFO] current_version: {current_version}, starting_version: {starting_version}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Merge-column helpers now shared via common_utils (build_merge_predicate /
# table_column_names) instead of being duplicated locally in each notebook.
# ---------------------------------------------------------------------------

merge_condition = build_merge_predicate(natural_key_cols)

# COMMAND ----------

# Per-contact sequence: running count of non-null transcript content rows
def add_sequence(df):
    """Add a per-contact sequence for non-null transcript content only."""
    order_window = Window.partitionBy("contact_id").orderBy("begin_offset_millis")
    running_window = order_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    running_non_null = F.sum(F.when(F.col("content").isNotNull(), F.lit(1)).otherwise(F.lit(0))).over(
        running_window
    )
    return df.withColumn(
        "sequence", F.when(F.col("content").isNotNull(), running_non_null).cast(IntegerType())
    )


# Target column alignment helper (no type casting - schema already matches).
#
# NOTE: excludes GENERATED columns (year, month, day - computed from
# transcript_creation_date) from the target column list entirely. Delta's
# .columns property lists generated columns like any other, so without this
# exclusion the fallback below would create explicit year=null/month=null/
# day=null columns in the aligned DataFrame (since df never produces them) -
# and an explicit NULL for a generated column fails Delta's
# DELTA_VIOLATE_CONSTRAINT_WITH_VALUES check the moment it's written,
# even via a targeted whenMatchedUpdate(set=...)/whenNotMatchedInsert(values=...)
# that doesn't reference it, if anything downstream ever does reference it.
# Omitting generated columns here (same as never producing them at all) lets
# Delta compute them automatically on insert/update, as intended.
def _cast_to_target_schema(df: DataFrame) -> DataFrame:
    target_cols = [c for c in spark.table(SILVER_LINES_TABLE).columns if c not in GENERATED_COLUMNS]
    return df.select(*[col(c) if c in df.columns else F.lit(None).alias(c) for c in target_cols])


# COMMAND ----------

# Schema definitions for the nested transcript / participants JSON columns
REDACTED_TIMESTAMP_SCHEMA = ArrayType(
    StructType(
        [
            StructField("BeginOffsetMillis", LongType()),
            StructField("EndOffsetMillis", LongType()),
        ]
    )
)

TRANSCRIPT_SCHEMA = ArrayType(
    StructType(
        [
            StructField("ActionItemsDetected", StringType()),
            StructField("BeginOffsetMillis", LongType()),
            StructField("Content", StringType()),
            StructField("EndOffsetMillis", LongType()),
            StructField("Id", StringType()),
            StructField("IssuesDetected", StringType()),
            StructField("LoudnessScore", ArrayType(DoubleType())),
            StructField("OutcomesDetected", StringType()),
            StructField("ParticipantId", StringType()),
            StructField("Redaction", StructType([StructField("RedactedTimestamps", REDACTED_TIMESTAMP_SCHEMA)])),
            StructField("Sentiment", StringType()),
        ]
    )
)

PARTICIPANTS_SCHEMA = ArrayType(
    StructType(
        [
            StructField("ParticipantId", StringType()),
            StructField("ParticipantRole", StringType()),
        ]
    )
)


def parse_json_columns_df(df: DataFrame) -> DataFrame:
    """Parse JSON transcript and participants columns into structured types."""
    return df.withColumn("transcript", from_json(col("transcript"), TRANSCRIPT_SCHEMA)).withColumn(
        "participants", from_json(col("participants"), PARTICIPANTS_SCHEMA)
    )


def derive_transcript_columns_df(df: DataFrame) -> DataFrame:
    """Derive individual transcript-line columns from the exploded transcript struct."""
    return (
        df.withColumn("participant_id", col("transcript.ParticipantId"))
        .withColumn("id", col("transcript.Id"))
        .withColumn("content", col("transcript.Content"))
        .withColumn("begin_offset_millis", col("transcript.BeginOffsetMillis").cast(IntegerType()))
        .withColumn("end_offset_millis", col("transcript.EndOffsetMillis").cast(IntegerType()))
        .withColumn("sentiment", col("transcript.Sentiment"))
        .withColumn(
            "participant_role",
            F.when(
                F.col("participants").isNotNull() & F.col("transcript.ParticipantId").isNotNull(),
                F.expr(
                    """
                    element_at(
                        transform(
                            filter(participants, x -> x.ParticipantId = transcript.ParticipantId),
                            x -> x.ParticipantRole
                        ),
                        1
                    )
                    """
                ),
            ),
        )
        .withColumn(
            "loudness_score",
            F.when(
                col("transcript.LoudnessScore").isNotNull() & (F.size(col("transcript.LoudnessScore")) > 0),
                F.concat_ws(",", col("transcript.LoudnessScore")),
            ),
        )
        .withColumn(
            "redacted_timestamps",
            F.when(
                col("transcript.Redaction.RedactedTimestamps").isNotNull(),
                F.array_join(
                    F.transform(
                        col("transcript.Redaction.RedactedTimestamps"),
                        lambda x: F.concat(
                            x["BeginOffsetMillis"].cast("string"),
                            F.lit("-"),
                            x["EndOffsetMillis"].cast("string"),
                        ),
                    ),
                    ",",
                ),
            ),
        )
        .transform(add_sequence)
    )


# COMMAND ----------

# ---------------------------------------------------------------------------
# Streaming merge callback used by foreachBatch
# ---------------------------------------------------------------------------


def merge_batch(micro_batch_df: DataFrame, batch_id: int) -> None:
    df_filtered = micro_batch_df.where("_change_type in ('insert', 'update_postimage')")

    df_selected = df_filtered.select(
        "region",
        "account_id",
        "contact_id",
        "input_s3_uri",
        "instance_id",
        "job_status",
        "language_code",
        "transcript",
        "participants",
        "transcript_creation_date",
    )

    df_parsed = df_selected.transform(parse_json_columns_df)

    df_exploded = df_parsed.withColumn("transcript", F.explode("transcript"))

    df_derived = df_exploded.transform(derive_transcript_columns_df)

    df_with_audit = (
        df_derived.withColumn("pl_created_on", F.lit(datetime.now()))
        .withColumn("pl_modified_on", F.lit(datetime.now()))
        .withColumn("pl_created_by", F.lit("CALL_TXN_SILVER_PLUS_STREAM"))
        .withColumn("pl_updated_by", F.lit("CALL_TXN_SILVER_PLUS_STREAM"))
    )

    # De-duplicate: keep the latest per (contact_id, id)
    window_by_key = Window.partitionBy(*natural_key_cols).orderBy(col("transcript_creation_date").desc())
    df_deduped = (
        df_with_audit.withColumn("_rn", F.row_number().over(window_by_key))
        .where(col("_rn") == 1)
        .drop("_rn")
    )

    df_aligned = _cast_to_target_schema(df_deduped)

    # NOTE: SILVER_LINES_TABLE has GENERATED columns (year, month, day -
    # computed from transcript_creation_date). whenMatchedUpdateAll() /
    # whenNotMatchedInsertAll() try to set every target column by
    # name-matching against the source DataFrame, which fails with
    # DELTA_MERGE_UNRESOLVED_EXPRESSION since df_aligned has no
    # year/month/day columns. Build an explicit column-set excluding the
    # generated columns instead - Delta computes them automatically from
    # transcript_creation_date on both insert and update.
    target_table = DeltaTable.forName(spark, SILVER_LINES_TABLE)
    mergeable_columns = [c for c in target_table.toDF().columns if c not in GENERATED_COLUMNS]
    merge_set = {c: f"source.{c}" for c in mergeable_columns}

    (
        target_table.alias("target")
        .merge(df_aligned.alias("source"), merge_condition)
        .whenMatchedUpdate(set=merge_set)
        .whenNotMatchedInsert(values=merge_set)
        .execute()
    )


# COMMAND ----------

# ---------------------------------------------------------------------------
# Read the Silver table via Change Data Feed and merge continuously
# ---------------------------------------------------------------------------
print("Launching Call Transcript Lines SILVER PLUS streaming pipeline (partitioned by year, month, day)")

silver_cdf_stream = (
    spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", str(starting_version))
    .table(SILVER_SUMMARY_TABLE)
)

query = (
    silver_cdf_stream.writeStream.foreachBatch(merge_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

print("Call Transcript Lines SILVER PLUS streaming pipeline active")

# Sanity check: confirm the processed transcript_creation_date values form a
# continuous window (no gaps) before recording the new checkpoint version.
processed_dates = [
    r["transcript_creation_date"].strftime("%Y-%m-%d")
    for r in spark.table(SILVER_LINES_TABLE)
    .where(f"contact_id IS NOT NULL")
    .select("transcript_creation_date")
    .distinct()
    .orderBy("transcript_creation_date")
    .tail(30)
]
if processed_dates and not is_date_sequence_continuous(processed_dates):
    print("[WARN] Recently processed transcript_creation_date values are not a continuous sequence")

# Persist processed source-version metadata for downstream incremental runs
allocation_run_id = f"transcript_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

CdfVersionTracker.record_processed_version(
    source_table=SILVER_SUMMARY_TABLE,
    target_table=SILVER_LINES_TABLE,
    version=current_version,
    run_id=allocation_run_id,
)

print(f"[INFO] Recorded processed version for {SILVER_SUMMARY_TABLE}: version {current_version}")

# COMMAND ----------
