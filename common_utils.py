# Databricks notebook source
"""Shared utility functions used by the Silver and Silver Plus notebooks.

Native PySpark + Delta Lake + boto3 reconstruction of the shared helper
module. No proprietary framework dependencies. Function names below are a
reconstruction with consistent renaming per `nomenclature_map.md` — the
original module's function names (`get_read_options_and_tsfilter`,
`get_s3_subfolders`, `pyspark_udaf`, `path_exists`, `valid_s3_files`,
`get_valid_files_from_s3`, `generate_date_ids`, `filter_available_columns`,
`_create_merge_predicate`, `_create_merge_predicate_null_safe`,
`_create_dict`, `_column_names`, `_create_merge_dict`, `normalize_table_name`,
`is_table_a_view`, `_extract_single_base_table_from_view`,
`get_current_version_for_table`, `get_source_timestamps_for_version`) are not
reused — see the nomenclature map for the old -> new mapping.
"""

from datetime import datetime, timedelta
from typing import Callable, Dict, Iterator, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame

# COMMAND ----------

DEFAULT_GENERATED_FIELDS = ["year", "month", "day"]
DEFAULT_CATALOG = "{{ configs.catalog }}"
DEFAULT_DATABASE = "{{ configs.database }}"

# COMMAND ----------

# ---------------------------------------------------------------------------
# Change Data Feed read-options helper
# (was: get_read_options_and_tsfilter)
# ---------------------------------------------------------------------------


def build_cdf_read_options(table_name: str, starting_ts: datetime, ending_ts: Optional[datetime] = None) -> Dict:
    """Build Spark readStream options for Delta Change Data Feed, clamping the
    starting timestamp to the table's latest commit so a future-dated start
    can't raise `DELTA_TIMESTAMP_GREATER_THAN_COMMIT`.

    Returns a dict with:
      - `read_options`: options to pass to `.options(**read_options)`
      - `commit_tsfilter`: an extra `_commit_timestamp` filter to AND onto the
        read result when the requested start is beyond the latest commit
        (used to force a safe empty result rather than raising).
    """
    history = DeltaTable.forName(spark, table_name).history(1).collect()
    latest_commit_ts = history[0]["timestamp"] if history else None

    effective_start = starting_ts
    commit_tsfilter = None
    if latest_commit_ts is not None and starting_ts > latest_commit_ts:
        effective_start = latest_commit_ts
        commit_tsfilter = f"_commit_timestamp > '{latest_commit_ts}'"

    read_options = {
        "readChangeData": "true",
        "startingTimestamp": effective_start.strftime("%Y-%m-%d %H:%M:%S"),
    }
    if ending_ts is not None:
        read_options["endingTimestamp"] = ending_ts.strftime("%Y-%m-%d %H:%M:%S")

    return {"read_options": read_options, "commit_tsfilter": commit_tsfilter}


# COMMAND ----------

# ---------------------------------------------------------------------------
# S3 discovery helpers
# (was: get_s3_subfolders, path_exists, valid_s3_files, get_valid_files_from_s3)
# ---------------------------------------------------------------------------


def list_s3_subfolders(bucket: str, prefix: str) -> Iterator[str]:
    """List immediate subfolders under an S3 prefix (boto3, Delimiter='/'),
    handling pagination via continuation tokens. Yields folder names relative
    to the prefix."""
    import boto3

    client = boto3.client("s3")
    continuation_token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/"}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token
        response = client.list_objects_v2(**kwargs)
        for common_prefix in response.get("CommonPrefixes", []):
            yield common_prefix["Prefix"][len(prefix):].rstrip("/")
        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break


def fs_path_exists(path: str):
    """Check whether a filesystem/volume path exists via dbutils.fs.ls.
    Returns the list of FileInfo objects if it exists, False otherwise."""
    try:
        return dbutils.fs.ls(path)
    except Exception:
        return False


def list_valid_s3_paths(root_path: str) -> List[str]:
    """Return all child paths under root_path that currently exist, or an
    empty list when root_path itself is missing."""
    listing = fs_path_exists(root_path)
    if not listing:
        return []
    return [f.path for f in listing]


def generate_partition_date_ids(start_date: str, end_date: str, granularity: str = "day") -> List[str]:
    """Generate date IDs between start_date and end_date (YYYY-MM-DD strings).

    - granularity in {"day", "DAY"}: every day, inclusive.
    - granularity in {"month", "MONTH"}: first day of each covered month
      (inclusive range behavior).
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_ids = []
    if granularity.lower() == "day":
        current = start
        while current <= end:
            date_ids.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
    elif granularity.lower() == "month":
        current = start.replace(day=1)
        end_month = end.replace(day=1)
        while current <= end_month:
            date_ids.append(current.strftime("%Y-%m-%d"))
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)
    else:
        raise ValueError(f"Unsupported granularity: {granularity}")
    return date_ids


def build_valid_files_df(root_path: str, date_ids: List[str]) -> DataFrame:
    """Build partition paths from a list of date IDs (day/month patterns),
    check which paths exist, and return a Spark DataFrame of valid file paths
    with column `file_path_w_prefix_s3`."""
    candidate_paths = [f"{root_path.rstrip('/')}/{date_id}/" for date_id in date_ids]
    existing_paths = [p for p in candidate_paths if fs_path_exists(p)]
    return spark.createDataFrame([(p,) for p in existing_paths], ["file_path_w_prefix_s3"])


# COMMAND ----------

# ---------------------------------------------------------------------------
# Spark UDAF-style aggregation helper
# (was: pyspark_udaf)
# ---------------------------------------------------------------------------


def build_custom_agg_udf(
    arg_cols: Dict[str, Column],
    reduce_func: Callable,
    finish_func: Optional[Callable] = None,
):
    """Build a custom aggregate expression for grouped Spark data.

    - Maps argument names to Spark columns/expressions via `collect_list`.
    - Reduces the collected rows using the user-supplied `reduce_func`.
    - Optionally post-processes the reduced value with `finish_func`.

    Returns a Spark Column usable inside `.agg(...)`.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType
    from pyspark.sql.functions import udf

    collected = {name: F.collect_list(col_expr) for name, col_expr in arg_cols.items()}
    struct_col = F.struct(*[c.alias(name) for name, c in collected.items()])

    def _apply(struct_value):
        kwargs = {name: getattr(struct_value, name) for name in arg_cols.keys()}
        reduced = reduce_func(**kwargs)
        return finish_func(reduced) if finish_func else reduced

    reduce_udf = udf(_apply, StringType())
    return reduce_udf(struct_col)


# COMMAND ----------

# ---------------------------------------------------------------------------
# Column selection helper
# (was: filter_available_columns)
# ---------------------------------------------------------------------------


def select_available_columns(df: DataFrame, desired_columns: List[str]) -> DataFrame:
    """Select only the columns from `desired_columns` that actually exist in
    the DataFrame."""
    available = [c for c in desired_columns if c in df.columns]
    return df.select(*available)


# COMMAND ----------

# ---------------------------------------------------------------------------
# Merge-predicate / merge-assignment helpers (shared by Silver and Silver
# Plus merge logic — consolidates what was previously a duplicated local
# `_merge_predicate` / `_column_names` pair in each notebook)
# (was: _create_merge_predicate, _create_merge_predicate_null_safe,
#  _create_dict, _column_names, _create_merge_dict)
# ---------------------------------------------------------------------------


def build_merge_predicate(merge_cols: List[str], target_alias: str = "target", source_alias: str = "source") -> str:
    """Build a SQL merge join condition using plain equality:
    `target.col = source.col` joined by AND."""
    return " AND ".join([f"{target_alias}.{c} = {source_alias}.{c}" for c in merge_cols])


def build_null_safe_merge_predicate(
    merge_cols: List[str], target_alias: str = "target", source_alias: str = "source"
) -> str:
    """Same as `build_merge_predicate`, but null-safe:
    `target.col <=> source.col` joined by AND."""
    return " AND ".join([f"{target_alias}.{c} <=> {source_alias}.{c}" for c in merge_cols])


def table_column_names(database: str, table: str) -> List[str]:
    """Return the column names of a table by reading zero rows."""
    return spark.table(f"{database}.{table}").limit(0).columns


def _build_assignment_map(columns: List[str], target_alias: str, source_alias: str) -> Dict[str, str]:
    """Build a target-to-source column mapping dict for merge update/insert
    assignments: `target_alias.col -> source_alias.col`."""
    return {f"{target_alias}.{c}": f"{source_alias}.{c}" for c in columns}


def build_merge_assignment_dict(
    database: str,
    table: str,
    merge_type: str,
    business_key_cols: List[str],
    generated_fields: Optional[List[str]] = None,
    target_alias: str = "target",
    source_alias: str = "source",
) -> Dict[str, str]:
    """Build a full merge assignment dictionary based on merge type:

    - "insert": all columns except `generated_fields` (default year/month/day).
    - "update": excludes business keys, generated fields, `pl_created_on`, and
      `pl_created_by` (renamed audit columns).
    """
    generated_fields = generated_fields or DEFAULT_GENERATED_FIELDS
    all_columns = table_column_names(database, table)

    if merge_type == "insert":
        columns = [c for c in all_columns if c not in generated_fields]
    elif merge_type == "update":
        excluded = set(business_key_cols) | set(generated_fields) | {"pl_created_on", "pl_created_by"}
        columns = [c for c in all_columns if c not in excluded]
    else:
        raise ValueError(f"Unsupported merge_type: {merge_type}")

    return _build_assignment_map(columns, target_alias, source_alias)


# COMMAND ----------

# ---------------------------------------------------------------------------
# Table-name / view-resolution helpers
# (was: normalize_table_name, is_table_a_view,
#  _extract_single_base_table_from_view, get_current_version_for_table,
#  get_source_timestamps_for_version)
# ---------------------------------------------------------------------------


def resolve_qualified_table_name(
    table_name: str, catalog: str = DEFAULT_CATALOG, database: str = DEFAULT_DATABASE
) -> str:
    """Normalize a table name to a fully qualified `catalog.database.table`
    form: a 3-part name is unchanged, a 2-part name gets `catalog` prepended,
    a 1-part (bare) name gets `catalog.database` prepended."""
    parts = table_name.split(".")
    if len(parts) == 3:
        return table_name
    if len(parts) == 2:
        return f"{catalog}.{table_name}"
    return f"{catalog}.{database}.{table_name}"


def is_view_object(table_name: str) -> bool:
    """Return True if the given object is a view (via DESCRIBE EXTENDED)."""
    rows = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
    for row in rows:
        if str(row["col_name"]).strip().lower() == "type":
            return "view" in str(row["data_type"]).strip().lower()
    return False


def _resolve_view_base_table(view_name: str) -> Optional[str]:
    """Resolve a single base table name referenced in a view's SQL definition
    (via DESCRIBE EXTENDED's `View Text`), using a regex match on the FROM
    clause. Returns the first matched table name, or None."""
    import re

    rows = spark.sql(f"DESCRIBE EXTENDED {view_name}").collect()
    view_text = None
    for row in rows:
        if str(row["col_name"]).strip().lower() in ("view text", "view original text"):
            view_text = row["data_type"]
            break
    if not view_text:
        return None
    match = re.search(r"FROM\s+([A-Za-z0-9_.`]+)", view_text, re.IGNORECASE)
    return match.group(1).replace("`", "") if match else None


def get_current_delta_version(table_name: str) -> Optional[int]:
    """Return the current (latest committed) Delta version for a table, or —
    if `table_name` is a view — for its resolved base table. Returns None
    when the version can't be determined."""
    try:
        target = table_name
        if is_view_object(table_name):
            base_table = _resolve_view_base_table(table_name)
            if base_table is None:
                return None
            target = base_table
        history = DeltaTable.forName(spark, target).history(1).collect()
        return history[0]["version"] if history else None
    except Exception:
        return None


def get_source_watermarks_for_version(source_table: str, version: int) -> Dict:
    """For a given source table/view and Delta version, resolve the base
    table (if source is a view), check for `pl_created_on` / `received_at`
    columns, query `table_changes(table, version)` for inserted rows, and
    return the max `pl_created_on` and max `received_at` as a dict (None
    values when unavailable)."""
    result = {"pl_created_on": None, "received_at": None}
    try:
        target = source_table
        if is_view_object(source_table):
            base_table = _resolve_view_base_table(source_table)
            if base_table is None:
                return result
            target = base_table

        available_cols = set(table_column_names(*target.split(".", 1)) if "." in target else [])
        changes_df = spark.sql(f"SELECT * FROM table_changes('{target}', {version}) WHERE _change_type = 'insert'")

        if "pl_created_on" in available_cols or "pl_created_on" in changes_df.columns:
            max_created = changes_df.agg({"pl_created_on": "max"}).collect()[0][0]
            result["pl_created_on"] = max_created
        if "received_at" in available_cols or "received_at" in changes_df.columns:
            max_received = changes_df.agg({"received_at": "max"}).collect()[0][0]
            result["received_at"] = max_received
    except Exception:
        pass
    return result

# COMMAND ----------
