#!/usr/bin/env bash
# upload_dummy_data_all_regions.sh
# Generates + uploads dummy Contact Lens data for every region declared in
# config/pipeline_config.yaml -> regions, region by region, to the given
# environment's S3 raw-landing path. Wraps generate_dummy_data.py and
# upload_dummy_data.sh so you don't have to invoke them once per region.
#
# Usage:
#   ./upload_dummy_data_all_regions.sh <env> [days] [contacts_per_day] [local_out_dir]
#
# Example:
#   ./upload_dummy_data_all_regions.sh dev 2 5 ./dummy_data
#
# Requires: python3, aws CLI configured with credentials that can write to
# the target bucket, and a PyYAML install (`pip install pyyaml`) to read
# pipeline_config.yaml.

set -euo pipefail

ENV="${1:?environment required (dev|stg|prod)}"
DAYS="${2:-2}"
CONTACTS_PER_DAY="${3:-5}"
OUT_DIR="${4:-./dummy_data}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_PATH="${SCRIPT_DIR}/../config/pipeline_config.yaml"

# Resolve S3 bucket/prefix and the region list for this environment straight
# out of pipeline_config.yaml, so this script never drifts from the config.
read -r S3_BUCKET S3_PREFIX <<< "$(python3 - "$CONFIG_PATH" "$ENV" <<'PYEOF'
import sys, yaml
config_path, env = sys.argv[1], sys.argv[2]
cfg = yaml.safe_load(open(config_path))
aws_cfg = cfg["environments"][env]["aws"]
print(aws_cfg["s3_bucket"], aws_cfg["s3_raw_prefix"])
PYEOF
)"

REGIONS=($(python3 - "$CONFIG_PATH" <<'PYEOF'
import sys, yaml
cfg = yaml.safe_load(open(sys.argv[1]))
print(" ".join(cfg["regions"]))
PYEOF
))

echo "Environment      : ${ENV}"
echo "S3 bucket/prefix  : s3://${S3_BUCKET}/${S3_PREFIX}"
echo "Regions           : ${REGIONS[*]}"
echo "Days / per-day    : ${DAYS} / ${CONTACTS_PER_DAY}"
echo

for REGION in "${REGIONS[@]}"; do
  echo "=== ${REGION} ==="

  echo "-- generating dummy data"
  python3 "${SCRIPT_DIR}/generate_dummy_data.py" \
    --out "${OUT_DIR}" \
    --region "${REGION}" \
    --days "${DAYS}" \
    --contacts-per-day "${CONTACTS_PER_DAY}"

  echo "-- uploading to S3"
  "${SCRIPT_DIR}/upload_dummy_data.sh" "${OUT_DIR}" "${S3_BUCKET}" "${S3_PREFIX}" "${REGION}"

  echo
done

echo "All regions uploaded. Verify with:"
echo "  aws s3 ls s3://${S3_BUCKET}/${S3_PREFIX}/ --recursive | awk '{print \$4}' | cut -d/ -f1-4 | sort | uniq -c"
