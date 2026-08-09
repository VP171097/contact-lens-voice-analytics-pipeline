"""generate_dummy_data.py

Generates dummy Amazon Connect Contact Lens-style "analysis_redacted" JSON
files, matching the schema bronze_notebook.py expects, and writes them to a
local folder laid out exactly like the S3 regional landing structure:

    <out_dir>/<REGION>/<yyyy>/<mm>/<dd>/analysis_redacted-<contact_id>.json

Use this to produce test fixtures before uploading to S3 (see
test/upload_dummy_data.sh) and running the pipeline end-to-end in a dev
environment.

Usage:
    python generate_dummy_data.py --out ./dummy_data --region EU --days 2 --contacts-per-day 5
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

PARTICIPANT_ROLES = ["AGENT", "CUSTOMER"]

# Dialogue lines used to build fake conversations - loaded from
# sample_lines.txt (2000 lines, see generate_sample_lines.py) sitting next
# to this script, falling back to a small built-in set if that file isn't
# present.
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


def _load_sample_lines(path: Path = None) -> list:
    """Load dialogue lines from sample_lines.txt next to this script (one
    line per row). Falls back to a small built-in set if not found."""
    txt_path = path or (Path(__file__).parent / "sample_lines.txt")
    try:
        lines = [line.rstrip("\n") for line in txt_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if lines:
            print(f"Loaded {len(lines)} sample lines from {txt_path}")
            return lines
    except FileNotFoundError:
        pass
    print(f"[WARN] {txt_path} not found - using built-in fallback ({len(_FALLBACK_SAMPLE_LINES)} lines)")
    return _FALLBACK_SAMPLE_LINES


SAMPLE_LINES = _load_sample_lines()


def _build_transcript(contact_id: str, num_lines: int) -> list:
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


def build_dummy_record(region: str, call_date: datetime) -> dict:
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
        "_call_date": call_date.strftime("%Y-%m-%d"),  # not part of the real payload, dropped below
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="./dummy_data", help="Output root directory")
    parser.add_argument("--region", default="EU", help="Region code (matches pipeline_config.yaml -> regions)")
    parser.add_argument("--days", type=int, default=2, help="How many days back to generate data for")
    parser.add_argument("--contacts-per-day", type=int, default=5, help="Number of dummy calls per day")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD, defaults to today")
    args = parser.parse_args()

    end_date = (
        datetime.today().date() if args.end_date is None else datetime.strptime(args.end_date, "%Y-%m-%d").date()
    )

    out_root = Path(args.out)
    total_files = 0

    for day_offset in range(args.days + 1):
        call_date = end_date - timedelta(days=day_offset)
        day_dir = out_root / args.region / f"{call_date.year}" / f"{call_date.month:02d}" / f"{call_date.day:02d}"
        day_dir.mkdir(parents=True, exist_ok=True)

        for _ in range(args.contacts_per_day):
            record = build_dummy_record(args.region, datetime.combine(call_date, datetime.min.time()))
            contact_id = record["CustomerMetadata"]["ContactId"]
            record.pop("_call_date")
            file_path = day_dir / f"analysis_redacted-{contact_id}.json"
            with open(file_path, "w") as f:
                # Compact single-line JSON (no indent) - bronze_notebook.py
                # reads via spark.read.json() in default JSON-Lines mode,
                # which requires exactly one complete JSON object per line.
                # Pretty-printed/multi-line JSON here causes every line to
                # parse as a corrupt fragment (_corrupt_record).
                json.dump(record, f)
            total_files += 1

    print(f"Generated {total_files} dummy Contact Lens files under {out_root}/{args.region}/")


if __name__ == "__main__":
    main()
