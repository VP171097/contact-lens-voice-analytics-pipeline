-- metadata/tables/silver_plus/call_engagement/call_transcript_lines.sql
-- Silver Plus table: Amazon Connect Contact Lens call-transcript line-level detail

DROP TABLE IF EXISTS `{{ configs.database }}`.`call_transcript_lines`;

CREATE EXTERNAL TABLE `{{ configs.database }}`.`call_transcript_lines` (
  `region` STRING COMMENT 'Region derived from S3 path or ingestion config',
  `account_id` STRING COMMENT 'Cloud account ID',
  `contact_id` STRING COMMENT 'Amazon Connect Contact ID',
  `input_s3_uri` STRING COMMENT 'Source S3 path of recording',
  `instance_id` STRING COMMENT 'Amazon Connect Instance ID',
  `job_status` STRING COMMENT 'Contact Lens job status',
  `language_code` STRING COMMENT 'Conversation language',
  `participant_id` STRING COMMENT 'ParticipantId from transcript segment',
  `participant_role` STRING COMMENT 'Role of the participant in the call',
  `id` STRING COMMENT 'Unique transcript segment identifier',
  `content` STRING COMMENT 'Transcript segment content',
  `begin_offset_millis` INT COMMENT 'Begin offset in milliseconds for segment',
  `end_offset_millis` INT COMMENT 'End offset in milliseconds for segment',
  `redacted_timestamps` STRING COMMENT 'Redacted timestamps for customer segments',
  `sentiment` STRING COMMENT 'Sentiment of the segment',
  `loudness_score` STRING COMMENT 'Loudness score of the segment',
  `sequence` INT COMMENT 'Sequence number per contact_id ordered by begin_offset_millis',
  `transcript_creation_date` DATE COMMENT 'Date of the transcript',
  `pl_created_on` TIMESTAMP COMMENT 'Record creation timestamp',
  `pl_modified_on` TIMESTAMP COMMENT 'Record last modified timestamp',
  `pl_created_by` STRING COMMENT 'Process that created the record',
  `pl_updated_by` STRING COMMENT 'Process that last updated the record',
  `year` INTEGER GENERATED ALWAYS AS (YEAR(transcript_creation_date)) COMMENT 'Year extracted from transcript_creation_date',
  `month` INTEGER GENERATED ALWAYS AS (MONTH(transcript_creation_date)) COMMENT 'Month extracted from transcript_creation_date',
  `day` INTEGER GENERATED ALWAYS AS (DAY(transcript_creation_date)) COMMENT 'Day extracted from transcript_creation_date'
)
USING DELTA
PARTITIONED BY (`year`, `month`, `day`)
LOCATION '{{ configs.silver_plus_data_path }}/call_engagement/call_transcript_lines'
COMMENT 'Silver Plus table for Amazon Connect voice transcript line-level analysis'
TBLPROPERTIES(
  'lakehouse.primary_key'='contact_id, id',
  'delta.enableChangeDataFeed'='false'
)
