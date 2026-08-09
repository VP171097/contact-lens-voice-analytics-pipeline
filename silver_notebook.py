# Databricks notebook source
"""Call Transcript Summary - Silver ingestion (Streaming).

Transforms Bronze voice-analysis records into the **Silver** call-transcript
summary table using Structured Streaming, mirroring the pattern used by
Silver Plus: a streaming read off Bronze, transformed and deduped per
micro-batch, then merged into the target via `foreachBatch`.

Flow
1. Read runtime parameters (`regions`) - an optional scope filter applied to
   every micro-batch. No date/lookback window is needed: streaming tracks
   "what's new since last run" via its own checkpoint, not partition filters.
2. Stream Bronze via `spark.readStream.format("delta")`.
3. Per micro-batch: map Bronze JSON payload to Silver columns (schema-drift
   safe), derive per-quarter sentiment when available.
4. Add audit columns, keep the latest record per `contact_id` within the
   micro-batch, and align to the Silver schema.
5. Merge into the partitioned Silver Delta table (`year` and `month`,
   generated columns) via `foreachBatch`.

Source: `bronze/call_engagement/voice_analysis`
Target: `call_transcript_summary`
"""

# COMMAND ----------

# MAGIC %run ./common_utils

# COMMAND ----------

# MAGIC %run ./config/config_loader

# COMMAND ----------

# Imports
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, from_json, lit, row_number, size, when
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
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

BRONZE_PATH = f"s3://{cfg['aws']['s3_curated_bucket']}/{cfg['aws']['s3_bronze_prefix']}"
SILVER_SUMMARY_TABLE = resolve_silver_target(cfg, table_key="silver_summary")
CHECKPOINT_PATH = f"s3://{cfg['aws']['s3_curated_bucket']}/checkpoints/call_engagement/transcript_summary"
natural_key_cols = ["contact_id"]

# COMMAND ----------

dbutils.widgets.text("regions", "", "Comma-separated list of regions (optional scope filter)")

region_input = dbutils.widgets.get("regions")
regions = [r.strip() for r in region_input.split(",") if r.strip()]

print("Call Transcript Summary SILVER streaming pipeline started")
print(f"Environment : {env}")
print(f"Regions     : {regions if regions else 'ALL'}")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Merge-column helpers now shared via common_utils (build_merge_predicate /
# table_column_names) instead of being duplicated locally in each notebook.
# Replaces the proprietary declarative ingestion framework's insert/update
# column mapping with plain functions.
# ---------------------------------------------------------------------------


def _cast_to_target_schema(df: DataFrame) -> DataFrame:
    """Align a DataFrame's columns/types to the target Silver table schema."""
    target_schema = spark.table(SILVER_SUMMARY_TABLE).schema
    common_fields = [f for f in target_schema.fields if f.name in df.columns]
    return df.select([col(f.name).cast(f.dataType).alias(f.name) for f in common_fields])


merge_condition = build_merge_predicate(natural_key_cols)

# COMMAND ----------

# Schema definitions and withColumn-based transforms for the nested Contact
# Lens JSON payloads captured at Bronze
CUSTOMER_METADATA_SCHEMA = StructType(
    [
        StructField("InputS3Uri", StringType()),
        StructField("InstanceId", StringType()),
        StructField("ContactId", StringType()),
    ]
)

CATEGORIES_SCHEMA = StructType([StructField("MatchedCategories", ArrayType(StringType()))])

SENTIMENT_PERIOD_SCHEMA = ArrayType(
    StructType(
        [
            StructField("EndOffsetMillis", LongType()),
            StructField("Score", DoubleType()),
        ]
    )
)

CONVERSATION_CHARACTERISTICS_SCHEMA = StructType(
    [
        StructField("TotalConversationDurationMillis", LongType()),
        StructField(
            "TalkSpeed",
            StructType(
                [
                    StructField(
                        "DetailsByParticipant",
                        StructType(
                            [
                                StructField("AGENT", StructType([StructField("AverageWordsPerMinute", DoubleType())])),
                                StructField("CUSTOMER", StructType([StructField("AverageWordsPerMinute", DoubleType())])),
                            ]
                        ),
                    )
                ]
            ),
        ),
        StructField(
            "TalkTime",
            StructType(
                [
                    StructField("TotalTimeMillis", LongType()),
                    StructField(
                        "DetailsByParticipant",
                        StructType(
                            [
                                StructField("AGENT", StructType([StructField("TotalTimeMillis", LongType())])),
                                StructField("CUSTOMER", StructType([StructField("TotalTimeMillis", LongType())])),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        StructField("NonTalkTime", StructType([StructField("TotalTimeMillis", LongType())])),
        StructField(
            "Sentiment",
            StructType(
                [
                    StructField(
                        "OverallSentiment",
                        StructType([StructField("AGENT", DoubleType()), StructField("CUSTOMER", DoubleType())]),
                    ),
                    StructField(
                        "SentimentByPeriod",
                        StructType(
                            [
                                StructField(
                                    "QUARTER",
                                    StructType(
                                        [
                                            StructField("AGENT", SENTIMENT_PERIOD_SCHEMA),
                                            StructField("CUSTOMER", SENTIMENT_PERIOD_SCHEMA),
                                        ]
                                    ),
                                )
                            ]
                        ),
                    ),
                ]
            ),
        ),
    ]
)


def customer_metadata_df(df: DataFrame) -> DataFrame:
    """Extract customer metadata fields from JSON column."""
    return (
        df.withColumn("_cm", from_json(col("CustomerMetadata"), CUSTOMER_METADATA_SCHEMA))
        .withColumn("input_s3_uri", col("_cm.InputS3Uri"))
        .withColumn("instance_id", col("_cm.InstanceId"))
        .withColumn("contact_id", col("_cm.ContactId"))
        .drop("CustomerMetadata", "_cm")
    )


def categories_df(df: DataFrame) -> DataFrame:
    """Extract matched categories from JSON column."""
    return (
        df.withColumn("_cat", from_json(col("Categories"), CATEGORIES_SCHEMA))
        .withColumn(
            "matched_categories",
            when(size(col("_cat.MatchedCategories")) > 0, concat_ws(",", col("_cat.MatchedCategories"))).otherwise(
                lit(None)
            ),
        )
        .drop("Categories", "_cat")
    )


def _safe_get(arr_col, idx, field):
    return arr_col.getItem(idx).getField(field)


def conversation_characteristics_df(df: DataFrame) -> DataFrame:
    """Extract conversation characteristics fields from JSON column."""
    return (
        df.withColumn("_cc", from_json(col("ConversationCharacteristics"), CONVERSATION_CHARACTERISTICS_SCHEMA))
        .withColumn("agent_wpm", col("_cc.TalkSpeed.DetailsByParticipant.AGENT.AverageWordsPerMinute"))
        .withColumn("customer_wpm", col("_cc.TalkSpeed.DetailsByParticipant.CUSTOMER.AverageWordsPerMinute"))
        .withColumn("agent_talk_time_millis", col("_cc.TalkTime.DetailsByParticipant.AGENT.TotalTimeMillis"))
        .withColumn("customer_talk_time_millis", col("_cc.TalkTime.DetailsByParticipant.CUSTOMER.TotalTimeMillis"))
        .withColumn("total_talk_time_millis", col("_cc.TalkTime.TotalTimeMillis"))
        .withColumn("non_talk_time_total_millis", col("_cc.NonTalkTime.TotalTimeMillis"))
        .withColumn("call_duration_millis", col("_cc.TotalConversationDurationMillis"))
        .withColumn("agent_sentiment_overall", col("_cc.Sentiment.OverallSentiment.AGENT"))
        .withColumn("customer_sentiment_overall", col("_cc.Sentiment.OverallSentiment.CUSTOMER"))
        .withColumn("agent_q1_sentiment", _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.AGENT"), 0, "Score"))
        .withColumn("agent_q2_sentiment", _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.AGENT"), 1, "Score"))
        .withColumn("agent_q3_sentiment", _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.AGENT"), 2, "Score"))
        .withColumn("agent_q4_sentiment", _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.AGENT"), 3, "Score"))
        .withColumn(
            "customer_q1_sentiment", _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.CUSTOMER"), 0, "Score")
        )
        .withColumn(
            "customer_q2_sentiment", _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.CUSTOMER"), 1, "Score")
        )
        .withColumn(
            "customer_q3_sentiment", _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.CUSTOMER"), 2, "Score")
        )
        .withColumn(
            "customer_q4_sentiment", _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.CUSTOMER"), 3, "Score")
        )
        .withColumn(
            "end_offset_millis_q1",
            _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.AGENT"), 0, "EndOffsetMillis"),
        )
        .withColumn(
            "end_offset_millis_q2",
            _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.AGENT"), 1, "EndOffsetMillis"),
        )
        .withColumn(
            "end_offset_millis_q3",
            _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.AGENT"), 2, "EndOffsetMillis"),
        )
        .withColumn(
            "end_offset_millis_q4",
            _safe_get(col("_cc.Sentiment.SentimentByPeriod.QUARTER.AGENT"), 3, "EndOffsetMillis"),
        )
        .drop("ConversationCharacteristics", "_cc")
    )


# COMMAND ----------

# ---------------------------------------------------------------------------
# Streaming merge callback used by foreachBatch. Contains the same
# transform/dedup/merge logic the batch version used, just run once per
# micro-batch instead of once per notebook run.
# ---------------------------------------------------------------------------

GENERATED_COLUMNS = {"year", "month", "day"}


def merge_batch(micro_batch_df: DataFrame, batch_id: int) -> None:
    # Optional region scope filter, applied per micro-batch
    df_scoped = micro_batch_df.where(col("region").isin(regions)) if regions else micro_batch_df

    # Step 1-2: select / rename columns of interest
    df_bronze_selected = df_scoped.select(
        col("AccountId").alias("account_id"),
        col("CustomerMetadata"),
        col("JobStatus").alias("job_status"),
        col("LanguageCode").alias("language_code"),
        col("Transcript").alias("transcript"),
        col("Participants").alias("participants"),
        col("Categories"),
        col("ConversationCharacteristics"),
        col("extraction_date").alias("transcript_creation_date"),
        col("region"),
    )

    # Step 3: parse nested JSON columns into flat / structured Silver columns
    df_bronze_processed = (
        df_bronze_selected.transform(customer_metadata_df)
        .transform(categories_df)
        .transform(conversation_characteristics_df)
    )

    # Step 6-7: cast to target schema, add audit columns
    df_silver_transform = (
        _cast_to_target_schema(df_bronze_processed)
        .withColumn("pl_created_on", lit(datetime.now()))
        .withColumn("pl_modified_on", lit(datetime.now()))
        .withColumn("pl_created_by", lit("CALL_TXN_SILVER_STREAM"))
        .withColumn("pl_updated_by", lit("CALL_TXN_SILVER_STREAM"))
    )

    # Step 8: de-duplicate within this micro-batch - keep the latest record
    # per contact_id (cross-batch dedup is handled by the MERGE itself, since
    # a repeat contact_id in a later batch will match and update in place)
    window_by_key = Window.partitionBy(*natural_key_cols).orderBy(col("transcript_creation_date").desc())
    df_deduped = (
        df_silver_transform.withColumn("_rn", row_number().over(window_by_key))
        .where(col("_rn") == 1)
        .drop("_rn")
    )

    # Step 9: merge into the partitioned Silver Delta table
    #
    # NOTE: SILVER_SUMMARY_TABLE has GENERATED columns (year, month, day -
    # computed from transcript_creation_date). whenMatchedUpdateAll() /
    # whenNotMatchedInsertAll() try to set every target column by
    # name-matching against the source DataFrame; since df_deduped has no
    # year/month/day columns (correctly - those are generated, not produced
    # here), that fails with DELTA_MERGE_UNRESOLVED_EXPRESSION. Instead,
    # build an explicit column-set that excludes the generated columns -
    # Delta then computes them automatically from transcript_creation_date
    # on both insert and update.
    target_table = DeltaTable.forName(spark, SILVER_SUMMARY_TABLE)
    mergeable_columns = [c for c in target_table.toDF().columns if c not in GENERATED_COLUMNS]
    merge_set = {c: f"source.{c}" for c in mergeable_columns}

    (
        target_table.alias("target")
        .merge(df_deduped.alias("source"), merge_condition)
        .whenMatchedUpdate(set=merge_set)
        .whenNotMatchedInsert(values=merge_set)
        .execute()
    )


# COMMAND ----------

# ---------------------------------------------------------------------------
# Read Bronze as a stream and merge continuously. trigger(availableNow=True)
# processes everything currently available then stops - safe to run
# repeatedly (e.g. from a scheduled job) rather than running forever.
# ---------------------------------------------------------------------------
print("Launching Call Transcript Summary SILVER streaming pipeline (partitioned by year, month - generated)")

bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

query = (
    bronze_stream.writeStream.foreachBatch(merge_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .start()
)
query.awaitTermination()

print("Call Transcript Summary SILVER streaming pipeline completed")

# COMMAND ----------
