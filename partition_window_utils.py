# Databricks notebook source
"""Shared helpers for building a year/month/day partition predicate over a
date range, and for validating date-range continuity.

Native PySpark reconstruction, function names renamed consistently per
`nomenclature_map.md` (original module's `get_year`, `get_partition_cond`,
and `is_continuous` are not reused as names).
"""

from datetime import datetime

# COMMAND ----------

# ---------------------------------------------------------------------------
# (was: get_year)
# ---------------------------------------------------------------------------


def partition_year_range(start_date_str: str, end_date_str: str):
    """Return a list of year values (as strings) between start_date_str and
    end_date_str (YYYY-MM-DD), inclusive. If start_date is later than
    end_date, prints an error and exits the notebook via dbutils.notebook.exit."""
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    if start_date > end_date:
        message = f"[ERROR] start_date {start_date_str} is later than end_date {end_date_str}"
        print(message)
        dbutils.notebook.exit(message)

    return [str(y) for y in range(start_date.year, end_date.year + 1)]


# COMMAND ----------

# ---------------------------------------------------------------------------
# (was: get_partition_cond)
# ---------------------------------------------------------------------------


def build_partition_predicate(start_date_str: str, end_date_str: str) -> str:
    """Build and return a SQL-style partition filter string using generated
    partition columns `year`, `month`, `day`, for the given inclusive date
    range (YYYY-MM-DD strings). Meant to be plugged into Spark SQL / DataFrame
    filtering where table partitions are stored as year/month/day.

    Handles:
      - Multi-year ranges: start-year condition, optional full middle-year
        condition, and end-year condition.
      - Same-year multi-month ranges: first-month partial range, optional
        full middle-month range, and last-month partial range.
      - Same-year same-month ranges: a simple day-BETWEEN condition.
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    if start_date > end_date:
        message = f"[ERROR] start_date {start_date_str} is later than end_date {end_date_str}"
        print(message)
        dbutils.notebook.exit(message)

    if start_date.year != end_date.year:
        # Multi-year range
        start_year_cond = (
            f"(year = {start_date.year} AND "
            f"(month > {start_date.month} OR (month = {start_date.month} AND day >= {start_date.day})))"
        )
        end_year_cond = (
            f"(year = {end_date.year} AND "
            f"(month < {end_date.month} OR (month = {end_date.month} AND day <= {end_date.day})))"
        )
        conditions = [start_year_cond]
        if end_date.year - start_date.year > 1:
            conditions.append(f"(year > {start_date.year} AND year < {end_date.year})")
        conditions.append(end_year_cond)
        return "(" + " OR ".join(conditions) + ")"

    if start_date.month != end_date.month:
        # Same-year, multi-month range
        start_month_cond = f"(month = {start_date.month} AND day >= {start_date.day})"
        end_month_cond = f"(month = {end_date.month} AND day <= {end_date.day})"
        conditions = [start_month_cond]
        if end_date.month - start_date.month > 1:
            conditions.append(f"(month > {start_date.month} AND month < {end_date.month})")
        conditions.append(end_month_cond)
        return f"year = {start_date.year} AND (" + " OR ".join(conditions) + ")"

    # Same-year, same-month range
    return f"year = {start_date.year} AND month = {start_date.month} AND day BETWEEN {start_date.day} AND {end_date.day}"


# COMMAND ----------

# ---------------------------------------------------------------------------
# (was: is_continuous)
# ---------------------------------------------------------------------------


def is_date_sequence_continuous(date_strings) -> bool:
    """Check whether a list of date strings (YYYY-MM-DD) represents a
    continuous date sequence with no gaps.

    Parses and sorts the dates, computes the expected day count from min to
    max date, and returns True if the actual count equals the expected
    count, else False. Used for date-range continuity validation."""
    if not date_strings:
        return True

    parsed_dates = sorted(datetime.strptime(d, "%Y-%m-%d") for d in date_strings)
    expected_count = (parsed_dates[-1] - parsed_dates[0]).days + 1
    actual_count = len(set(parsed_dates))
    return actual_count == expected_count

# COMMAND ----------
