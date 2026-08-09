# Databricks notebook source
"""CDF version tracker utility.

Replaces the proprietary metadata-manager component (`MetadataManager` /
`lkp_version_manager.py`) used by the original pipeline with a small native
Delta control table that records, per (source_table, target_table) pair, the
last Delta version of the source table that was successfully processed into
the target table — plus the source watermark timestamps at that version.
This lets a downstream streaming/CDF consumer resume from the correct point
on the next run without relying on Structured Streaming's own checkpoint
alone (useful when the checkpoint needs to be rebuilt).

Reconstructed and merged from two original sources (see
`nomenclature_map.md`): the CDF-state control table used by Silver Plus, and
the richer `lkp_version_manager.py` / `MetadataManager` class, whose extra
capabilities (`has_metadata_for_source_target`, source watermark columns) are
folded in here as `has_tracked_state` and the `source_created_on` /
`source_received_at` fields on `record_processed_version`.

Note: the original `update_metadata_for_source_target` /
`MetadataManager.update_state`-style methods are intentionally NOT reused as
names — all "update_*" style functions across this codebase were renamed per
the nomenclature map; this class's write method is `record_processed_version`.
"""

from datetime import datetime
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

# Explicit schema for the control-table upsert row - required because
# source_created_on / source_received_at are Optional and often None when
# record_processed_version() is called without them; spark.createDataFrame()
# cannot infer a type from an all-null column (CANNOT_DETERMINE_TYPE), so we
# always pass this schema instead of relying on inference.
_CONTROL_ROW_SCHEMA = StructType(
    [
        StructField("source_table", StringType(), nullable=False),
        StructField("target_table", StringType(), nullable=False),
        StructField("last_processed_version", StringType(), nullable=True),
        StructField("source_created_on", TimestampType(), nullable=True),
        StructField("source_received_at", TimestampType(), nullable=True),
        StructField("run_id", StringType(), nullable=True),
        StructField("state_modified_on", TimestampType(), nullable=False),
    ]
)

# Normally the calling notebook (e.g. silver_plus_notebook.py) %run's
# config/config_loader.py first and sets CONTROL_TABLE = resolve_cdf_state_table(cfg)
# before %run'ing this file - that's the expected/preferred path.
#
# The fallback below is fully self-contained (reads pipeline_config.yaml
# directly, no dependency on config_loader.py's functions being in scope)
# so this module also works if it's ever %run or imported on its own,
# regardless of %run order in the caller.
try:
    CONTROL_TABLE
except NameError:
    import yaml

    with open("./config/pipeline_config.yaml", "r") as _f:
        _raw_cfg = yaml.safe_load(_f)
    _env_cfg = _raw_cfg["environments"]["dev"]
    CONTROL_TABLE = (
        f"{_env_cfg['databricks']['catalog']}."
        f"{_env_cfg['databricks']['control_schema']}."
        f"{_raw_cfg['tables']['cdf_state']}"
    )
    print(f"[WARN] CONTROL_TABLE was not set by the caller - falling back to dev config: {CONTROL_TABLE}")


def get_current_delta_version(table_name: str):
    """Return the current (latest committed) Delta version of a table, or None."""
    try:
        history = DeltaTable.forName(spark, table_name).history(1).collect()
        return history[0]["version"] if history else None
    except Exception:
        return None


class CdfVersionTracker:
    """Reads/writes the last-processed Delta version (and source watermark
    timestamps) for a source -> target hop, backed by `CONTROL_TABLE`.

    All methods are static — no object state is required.
    """

    @staticmethod
    def get_latest_state(source_table: str, target_table: str):
        """Filter the control table by (source_table, target_table), sort by
        `state_modified_on` descending, and return the latest row as a dict:
        `last_processed_version` (int), `source_created_on`, `source_received_at`,
        `state_modified_on`, `run_id`. Returns None if no row is found."""
        if not spark.catalog.tableExists(CONTROL_TABLE):
            return None
        row = (
            spark.table(CONTROL_TABLE)
            .where(f"source_table = '{source_table}' AND target_table = '{target_table}'")
            .orderBy("state_modified_on", ascending=False)
            .limit(1)
            .collect()
        )
        if not row:
            return None
        result = row[0].asDict()
        if result.get("last_processed_version") is not None:
            result["last_processed_version"] = int(result["last_processed_version"])
        return result

    @staticmethod
    def has_tracked_state(source_table: str, target_table: str) -> bool:
        """Quick existence check (COUNT query) for whether the control table
        already has a row for the given (source_table, target_table) pair —
        used before deciding whether to bootstrap or write over existing
        state. Returns False on error (logs a warning)."""
        try:
            if not spark.catalog.tableExists(CONTROL_TABLE):
                return False
            count = (
                spark.table(CONTROL_TABLE)
                .where(f"source_table = '{source_table}' AND target_table = '{target_table}'")
                .count()
            )
            return count > 0
        except Exception as exc:
            print(f"[WARN] has_tracked_state failed for {source_table} -> {target_table}: {exc}")
            return False

    @staticmethod
    def record_processed_version(
        source_table: str,
        target_table: str,
        version: int,
        run_id: str,
        source_created_on: Optional[datetime] = None,
        source_received_at: Optional[datetime] = None,
    ):
        """Upsert the last-processed version (and optional source watermark
        timestamps) for a source -> target hop into the control table via a
        Delta MERGE keyed on (source_table, target_table). Sets
        `state_modified_on` to the current timestamp at write time."""
        record = spark.createDataFrame(
            [
                Row(
                    source_table=source_table,
                    target_table=target_table,
                    last_processed_version=str(version),
                    source_created_on=source_created_on,
                    source_received_at=source_received_at,
                    run_id=run_id,
                    state_modified_on=datetime.now(),
                )
            ],
            schema=_CONTROL_ROW_SCHEMA,
        )
        if not spark.catalog.tableExists(CONTROL_TABLE):
            record.write.format("delta").saveAsTable(CONTROL_TABLE)
            print(f"[INFO] Recorded processed version {version} for {source_table} -> {target_table} (new state)")
            return

        target = DeltaTable.forName(spark, CONTROL_TABLE)
        (
            target.alias("target")
            .merge(
                record.alias("source"),
                "target.source_table = source.source_table AND target.target_table = source.target_table",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print(f"[INFO] Recorded processed version {version} for {source_table} -> {target_table}")

# COMMAND ----------
