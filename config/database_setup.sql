-- database_setup.sql
-- One-time setup: Unity Catalog storage credential + external location bound
-- to the S3 bucket, then the catalog/schemas the pipeline writes to.
-- Run as a Databricks account/metastore admin. Replace <ACCOUNT_ROLE_ARN> and
-- <ENV> placeholders per environment (dev/stg/prod) using pipeline_config.yaml.

-- 1. Storage credential wrapping the IAM role that has read/write access
--    to the S3 bucket created in the AWS setup step (see setup_guide.md §1).
CREATE STORAGE CREDENTIAL IF NOT EXISTS callintel_<ENV>_storage_cred
  WITH (
    AWS_IAM_ROLE = '<ACCOUNT_ROLE_ARN>'
  )
  COMMENT 'Storage credential for the Engagement Call Intelligence pipeline (<ENV>)';

-- 2. External location pointing Unity Catalog at the S3 bucket/prefix used
--    for curated (Bronze/Silver/Silver Plus) data.
CREATE EXTERNAL LOCATION IF NOT EXISTS callintel_<ENV>_external_loc
  URL 's3://callintel-<ENV>-curated/warehouse'
  WITH (STORAGE CREDENTIAL callintel_<ENV>_storage_cred)
  COMMENT 'External location for Engagement Call Intelligence curated data (<ENV>)';

-- 3. Catalog for this data product / environment.
CREATE CATALOG IF NOT EXISTS callintel_<ENV>
  MANAGED LOCATION 's3://callintel-<ENV>-curated/warehouse/callintel_<ENV>'
  COMMENT 'Engagement Call Intelligence pipeline catalog (<ENV>)';

USE CATALOG callintel_<ENV>;

-- 4. Schemas per pipeline layer, each with its own S3 sub-path so Bronze,
--    Silver, and Silver Plus data physically separate under the same bucket.
CREATE SCHEMA IF NOT EXISTS bronze_call_engagement
  MANAGED LOCATION 's3://callintel-<ENV>-curated/warehouse/callintel_<ENV>/bronze_call_engagement'
  COMMENT 'Bronze layer - normalized raw voice-analysis records';

CREATE SCHEMA IF NOT EXISTS silver_call_engagement
  MANAGED LOCATION 's3://callintel-<ENV>-curated/warehouse/callintel_<ENV>/silver_call_engagement'
  COMMENT 'Silver layer - parsed/enriched transcript metadata (one row per contact)';

CREATE SCHEMA IF NOT EXISTS silver_plus_call_engagement
  MANAGED LOCATION 's3://callintel-<ENV>-curated/warehouse/callintel_<ENV>/silver_plus_call_engagement'
  COMMENT 'Silver Plus layer - exploded line-level conversation records';

CREATE SCHEMA IF NOT EXISTS pipeline_control
  MANAGED LOCATION 's3://callintel-<ENV>-curated/warehouse/callintel_<ENV>/pipeline_control'
  COMMENT 'Pipeline control tables (CDF version-tracking state, etc.)';

-- 5. Control table used by CdfVersionTracker (code/cdf_version_tracker.py)
--    to track the last processed Delta CDF version per source/target pair.
CREATE TABLE IF NOT EXISTS pipeline_control.callintel_pipeline_cdf_state (
    source_table         STRING      COMMENT 'Fully qualified source table name',
    target_table          STRING      COMMENT 'Fully qualified target table name',
    last_processed_version BIGINT    COMMENT 'Last Delta CDF version successfully merged into target',
    source_created_on     TIMESTAMP  COMMENT 'Watermark: source table creation/commit time observed',
    source_received_at    TIMESTAMP  COMMENT 'Watermark: time this version was read from source',
    state_modified_on     TIMESTAMP  COMMENT 'Time this control row was last written'
)
USING DELTA
COMMENT 'CDF incremental-load bookkeeping for the Engagement Call Intelligence pipeline';
