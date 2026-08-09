"""generate_bulk_dummy_dataset.py

Generates a single JSON Lines (.jsonl / .json) file containing many dummy
Contact Lens "analysis_redacted" records - one full record per line - for
volume/load testing bronze_notebook.py against a realistic-sized batch
without creating thousands of tiny individual files.

Spark's spark.read.json(...) (used in bronze_notebook.py) natively reads
JSON Lines files - each line is parsed as one record - so this file can be
uploaded and read exactly like the many-small-files layout used by
generate_dummy_data.py, just consolidated into one object per line.

Usage:
    python generate_bulk_dummy_dataset.py --out ./bulk_dummy.jsonl --lines 2000 --region EU
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timedelta

PARTICIPANT_ROLES = ["AGENT", "CUSTOMER"]

SAMPLE_LINES = [
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
    "I'd like to update my billing address, is that possible here?",
    "Of course, can you confirm the account holder's name for verification?",
    "It's under the name on the account, yes.",
    "Great, I've updated the address on file.",
    "My internet service has been down since this morning.",
    "I understand the frustration, let me run a diagnostic on the line.",
    "The diagnostic shows a fault upstream, a technician will be dispatched.",
    "When can I expect the technician to arrive?",
    "Between 2pm and 5pm tomorrow, you'll get a confirmation text.",
    "Perfect, thank you for resolving this quickly.",
]


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
        "extraction_date": call_date.strftime("%Y/%m/%d"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="./bulk_dummy.jsonl", help="Output file path")
    parser.add_argument("--lines", type=int, default=2000, help="Number of records (lines) to generate")
    parser.add_argument("--region", default="EU", help="Region code")
    parser.add_argument("--days-back", type=int, default=7, help="Spread records randomly over this many past days")
    args = parser.parse_args()

    today = datetime.today().date()

    with open(args.out, "w") as f:
        for _ in range(args.lines):
            call_date = datetime.combine(today - timedelta(days=random.randint(0, args.days_back)), datetime.min.time())
            record = build_dummy_record(args.region, call_date)
            f.write(json.dumps(record) + "\n")

    print(f"Wrote {args.lines} dummy records (one per line) to {args.out}")


if __name__ == "__main__":
    main()
