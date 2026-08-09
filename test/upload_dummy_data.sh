#!/usr/bin/env bash
# upload_dummy_data.sh
# Uploads locally-generated dummy Contact Lens files (see generate_dummy_data.py)
# to the S3 raw-landing path used by bronze_notebook.py for the given region,
# preserving the <region>/<yyyy>/<mm>/<dd>/ folder structure.
#
# Usage:
#   ./upload_dummy_data.sh <local_dir> <s3_bucket> <s3_raw_prefix> <region>
#
# Example (matches pipeline_config.yaml -> environments.dev.aws):
#   ./upload_dummy_data.sh ./dummy_data callintel-dev-raw contact-lens/voice-analysis/redacted EU

set -euo pipefail

LOCAL_DIR="${1:?local_dir required}"
S3_BUCKET="${2:?s3_bucket required}"
S3_PREFIX="${3:?s3_raw_prefix required}"
REGION="${4:?region required}"

DEST="s3://${S3_BUCKET}/${S3_PREFIX}/${REGION}/"

echo "Uploading ${LOCAL_DIR}/${REGION}/ -> ${DEST}"
aws s3 cp "${LOCAL_DIR}/${REGION}/" "${DEST}" --recursive

echo "Done. Verify with:"
echo "  aws s3 ls ${DEST} --recursive"
