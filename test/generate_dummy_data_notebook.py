# Databricks notebook source
"""Dummy Amazon Connect Contact Lens data generator (Databricks notebook).

Generates synthetic "analysis_redacted" JSON records matching the schema
bronze_notebook.py expects, and writes them directly to the S3 raw-landing
path (or the equivalent Unity Catalog volume path, if configured), laid out
per region as:

    <raw_landing_root>/<REGION>/<yyyy>/<mm>/<dd>/analysis_redacted-<contact_id>.json

Runs entirely inside Databricks - no local AWS CLI or credentials needed,
since it uses the same storage credential already wired up for the pipeline.

Parameters (widgets):
    num_records : number of dummy calls to generate PER DATE, split evenly
                   across the given regions
    regions     : comma-separated region codes, e.g. "EU,NAM,APAC"
                  (must match entries in config/pipeline_config.yaml -> regions)
    year        : comma-separated years, e.g. "2026,2025"
    month       : comma-separated months (1-12), e.g. "1,6,12"
    day         : comma-separated days (1-31), e.g. "1,15,28"
    env         : "dev" | "stg" | "prod" (defaults to "dev")

Date handling: year/month/day are combined as a cartesian product (every
year x every month x every day), and invalid calendar dates (e.g. day=30,
month=2) are skipped with a warning. Data is generated and ingested once per
resulting valid date. If year, month, AND day are all left blank, data is
generated for today's date only.
"""

# COMMAND ----------

# MAGIC %run ../config/config_loader

# COMMAND ----------

# Imports
import itertools
import json
import random
import uuid
from datetime import date

# COMMAND ----------

# Runtime parameters
dbutils.widgets.text("env", "dev", "Environment (dev/stg/prod)")
dbutils.widgets.text("num_records", "20", "Number of dummy records to create PER DATE")
dbutils.widgets.text("regions", "EU", "Comma-separated regions (e.g. EU,NAM,APAC)")
dbutils.widgets.text("year", "", "Comma-separated years (e.g. 2026,2025) - blank = today")
dbutils.widgets.text("month", "", "Comma-separated months 1-12 (e.g. 1,6,12) - blank = today")
dbutils.widgets.text("day", "", "Comma-separated days 1-31 (e.g. 1,15,28) - blank = today")

env = dbutils.widgets.get("env") or "dev"
num_records = int(dbutils.widgets.get("num_records") or 20)
regions = [r.strip().upper() for r in dbutils.widgets.get("regions").split(",") if r.strip()]

if not regions:
    dbutils.notebook.exit("[ERROR] No regions provided")


def _parse_int_list(widget_value: str) -> list:
    return [int(v.strip()) for v in widget_value.split(",") if v.strip()]


year_values = _parse_int_list(dbutils.widgets.get("year"))
month_values = _parse_int_list(dbutils.widgets.get("month"))
day_values = _parse_int_list(dbutils.widgets.get("day"))

# COMMAND ----------

# Resolve the target date list: cartesian product of year x month x day if
# any were provided, otherwise just today. Missing individual fields default
# to today's corresponding component so partial input (e.g. only "month")
# still produces sensible dates.
today = date.today()

if not year_values and not month_values and not day_values:
    target_dates = [today]
    print("No year/month/day provided - defaulting to today's date")
else:
    year_values = year_values or [today.year]
    month_values = month_values or [today.month]
    day_values = day_values or [today.day]

    target_dates = []
    skipped = []
    for y, m, d in itertools.product(year_values, month_values, day_values):
        try:
            target_dates.append(date(y, m, d))
        except ValueError:
            skipped.append((y, m, d))

    if skipped:
        print(f"[WARN] Skipped {len(skipped)} invalid calendar date combination(s): {skipped}")

    if not target_dates:
        dbutils.notebook.exit("[ERROR] No valid dates produced from year/month/day parameters")

print("DUMMY DATA GENERATION STARTED")
print(f"Environment       : {env}")
print(f"Records per date  : {num_records}")
print(f"Regions           : {regions}")
print(f"Target dates      : {sorted(set(target_dates))}")

# COMMAND ----------

# Resolve the raw-landing root from config (works whether it's an s3:// URI
# or a /Volumes/... path - dbutils.fs handles both transparently)
cfg = load_pipeline_config(env=env, config_path="../config/pipeline_config.yaml")

valid_regions = set(cfg["regions"])
unknown = [r for r in regions if r not in valid_regions]
if unknown:
    print(f"[WARN] Unknown region(s) not in pipeline_config.yaml: {unknown} (proceeding anyway)")

RAW_LANDING_ROOT = resolve_raw_landing_root(cfg)
print(f"Raw landing root: {RAW_LANDING_ROOT}")

# COMMAND ----------

# Sample transcript content used to build fake conversations - loaded from
# sample_lines.txt (2000 lines) if present alongside this notebook, falling
# back to a small built-in set so the notebook still runs if that file
# hasn't been uploaded yet.
_FALLBACK_SAMPLE_LINES = [
    "Hello, thank you for calling support, how can I help you today?",
    "Hi, I'm having trouble with my recent order, it hasn't arrived yet.",
    "I'm sorry to hear that. Can you provide your order number?",
    "Sure, it's ORD-{n}.",
    "Let me check that for you, one moment please.",
    "I can see the order is currently in transit and should arrive within two days.",
    "Okay, thank you for checking.",
    "Is there anything else I can help you with today?",
    "No, that's all. Thanks for your help!",
    "You're welcome, have a great day!",
]


def _load_sample_lines(path: str = "../sample_lines.txt") -> list:
    """Load dialogue lines from sample_lines.txt (one line per row), sitting
    in the same Workspace folder as this notebook. Falls back to a small
    built-in set if the file isn't found."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [line.rstrip("\n") for line in f if line.strip()]
        if lines:
            print(f"Loaded {len(lines)} sample lines from {path}")
            return lines
    except FileNotFoundError:
        pass
    print(f"[WARN] {path} not found - using built-in fallback ({len(_FALLBACK_SAMPLE_LINES)} lines)")
    return _FALLBACK_SAMPLE_LINES


SAMPLE_LINES = _load_sample_lines()

PARTICIPANT_ROLES = ["AGENT", "CUSTOMER"]


def _build_transcript(contact_id: str, num_lines: int) -> list:
    """Build a fake alternating agent/customer transcript with offsets."""
    transcript = []
    offset = 0
    for i in range(num_lines):
        role = PARTICIPANT_ROLES[i % 2]
        duration = random.randint(2000, 6000)
        line = SAMPLE_LINES[i % len(SAMPLE_LINES)].format(n=random.randint(1000, 9999))
        transcript.append(
            {
                "ParticipantId": f"{role}_1",
                "Id": f"{contact_id}-line-{i+1}",
                "BeginOffsetMillis": offset,
                "EndOffsetMillis": offset + duration,
                "Content": line,
                "Sentiment": random.choice(["POSITIVE", "NEUTRAL", "NEGATIVE"]),
                "LoudnessScore": [round(random.uniform(30, 80), 2) for _ in range(2)],
                "ActionItemsDetected": None,
                "IssuesDetected": None,
                "OutcomesDetected": None,
                "Redaction": {"RedactedTimestamps": []},
            }
        )
        offset += duration
    return transcript


def _build_conversation_characteristics(total_duration_millis: int) -> dict:
    """Build fake sentiment/talk-time/talk-speed metrics for one call."""
    return {
        "TotalConversationDurationMillis": total_duration_millis,
        "TalkSpeed": {
            "DetailsByParticipant": {
                "AGENT": {"AverageWordsPerMinute": round(random.uniform(120, 160), 1)},
                "CUSTOMER": {"AverageWordsPerMinute": round(random.uniform(100, 150), 1)},
            }
        },
        "TalkTime": {
            "TotalTimeMillis": int(total_duration_millis * 0.8),
            "DetailsByParticipant": {
                "AGENT": {"TotalTimeMillis": int(total_duration_millis * 0.45)},
                "CUSTOMER": {"TotalTimeMillis": int(total_duration_millis * 0.35)},
            },
        },
        "NonTalkTime": {"TotalTimeMillis": int(total_duration_millis * 0.2)},
        "Sentiment": {
            "OverallSentiment": {
                "AGENT": round(random.uniform(1, 5), 2),
                "CUSTOMER": round(random.uniform(1, 5), 2),
            },
            "SentimentByPeriod": {
                "QUARTER": {
                    "AGENT": [
                        {"EndOffsetMillis": int(total_duration_millis * q / 4), "Score": round(random.uniform(1, 5), 2)}
                        for q in range(1, 5)
                    ],
                    "CUSTOMER": [
                        {"EndOffsetMillis": int(total_duration_millis * q / 4), "Score": round(random.uniform(1, 5), 2)}
                        for q in range(1, 5)
                    ],
                }
            },
        },
    }


def build_dummy_record(region: str) -> dict:
    """Build one fake Contact Lens 'analysis_redacted' record for the given region."""
    contact_id = str(uuid.uuid4())
    num_lines = random.randint(6, 10)
    transcript = _build_transcript(contact_id, num_lines)
    total_duration = transcript[-1]["EndOffsetMillis"] if transcript else 0

    return {
        "AccountId": "111122223333",
        "CustomerMetadata": {
            "InputS3Uri": f"s3://dummy-connect-recordings/{region}/{contact_id}.wav",
            "InstanceId": f"dummy-instance-{region.lower()}",
            "ContactId": contact_id,
        },
        "JobStatus": "COMPLETED",
        "LanguageCode": "en-US",
        "Transcript": transcript,
        "Participants": [
            {"ParticipantId": "AGENT_1", "ParticipantRole": "AGENT"},
            {"ParticipantId": "CUSTOMER_1", "ParticipantRole": "CUSTOMER"},
        ],
        "Categories": {
            "MatchedCategories": random.sample(
                ["Billing", "TechnicalIssue", "Complaint", "Compliment", "Escalation"], k=random.randint(0, 2)
            )
        },
        "ConversationCharacteristics": _build_conversation_characteristics(total_duration),
    }


# COMMAND ----------

# Split num_records evenly across the given regions (remainder goes to the
# first regions in the list)
base_count, remainder = divmod(num_records, len(regions))
records_per_region = {
    region: base_count + (1 if i < remainder else 0) for i, region in enumerate(regions)
}
print(f"Records per region: {records_per_region}")

# COMMAND ----------

# Generate and write files directly to the raw-landing path via dbutils.fs,
# once per (date x region) combination. num_records is generated PER DATE,
# split across regions - so total files written = num_records * len(target_dates).
total_written = 0
written_paths = []

for target_date in sorted(set(target_dates)):
    date_subpath = f"{target_date.year}/{target_date.month:02d}/{target_date.day:02d}"

    for region, count in records_per_region.items():
        region_path = f"{RAW_LANDING_ROOT}/{region}/{date_subpath}/"
        print(f"Writing {count} dummy records to {region_path}")

        for _ in range(count):
            record = build_dummy_record(region)
            contact_id = record["CustomerMetadata"]["ContactId"]
            file_path = f"{region_path}analysis_redacted-{contact_id}.json"

            # Compact single-line JSON (no indent) - bronze_notebook.py reads
            # via spark.read.json() in default JSON-Lines mode, which
            # requires exactly one complete JSON object per line.
            # Pretty-printed JSON here causes every line to parse as a
            # corrupt fragment (_corrupt_record).
            payload = json.dumps(record)
            dbutils.fs.put(file_path, payload, overwrite=True)
            total_written += 1

        written_paths.append((target_date, region, region_path))

print(
    f"DUMMY DATA GENERATION COMPLETE - {total_written} files written "
    f"across {len(regions)} region(s) and {len(set(target_dates))} date(s)"
)

# COMMAND ----------

# Quick sanity check: list what was just written per (date, region)
for target_date, region, listing_path in written_paths:
    try:
        files = dbutils.fs.ls(listing_path)
        print(f"{target_date} / {region}: {len(files)} file(s) at {listing_path}")
    except Exception as exc:
        print(f"[WARN] Could not list {listing_path}: {exc}")

# COMMAND ----------
