-- metadata/tables/silver/call_engagement/call_transcript_summary.sql
-- Silver table: Amazon Connect Contact Lens call-transcript summary

ALTER TABLE `{{ configs.database }}`.`call_transcript_summary` SET TBLPROPERTIES('delta.enableChangeDataFeed' = 'true');

DROP TABLE IF EXISTS `{{ configs.database }}`.`call_transcript_summary`;

CREATE EXTERNAL TABLE `{{ configs.database }}`.`call_transcript_summary` (
  `region` STRING COMMENT 'Region derived from S3 path or ingestion config',
  `account_id` STRING COMMENT 'Cloud account ID',
  `contact_id` STRING COMMENT 'Amazon Connect Contact ID',
  `input_s3_uri` STRING COMMENT 'Source S3 path of recording',
  `instance_id` STRING COMMENT 'Amazon Connect Instance ID',
  `job_status` STRING COMMENT 'Contact Lens job status',
  `language_code` STRING COMMENT 'Conversation language',
  `matched_categories` STRING COMMENT 'Matched categories from categories',
  `transcript` STRING COMMENT 'Structured transcript segments from Contact Lens',
  `participants` STRING COMMENT 'Participants in the call',
  `agent_wpm` DOUBLE COMMENT 'Agent words per minute',
  `customer_wpm` DOUBLE COMMENT 'Customer words per minute',
  `agent_talk_time_millis` BIGINT COMMENT 'Agent talk time in milliseconds',
  `customer_talk_time_millis` BIGINT COMMENT 'Customer talk time in milliseconds',
  `total_talk_time_millis` BIGINT COMMENT 'Total talk time in milliseconds',
  `non_talk_time_total_millis` BIGINT COMMENT 'Total non-talk time in milliseconds',
  `call_duration_millis` BIGINT COMMENT 'Total call duration in milliseconds',
  `agent_sentiment_overall` DOUBLE COMMENT 'Overall agent sentiment',
  `customer_sentiment_overall` DOUBLE COMMENT 'Overall customer sentiment',
  `agent_q1_sentiment` DOUBLE COMMENT 'Agent sentiment in Q1',
  `agent_q2_sentiment` DOUBLE COMMENT 'Agent sentiment in Q2',
  `agent_q3_sentiment` DOUBLE COMMENT 'Agent sentiment in Q3',
  `agent_q4_sentiment` DOUBLE COMMENT 'Agent sentiment in Q4',
  `customer_q1_sentiment` DOUBLE COMMENT 'Customer sentiment in Q1',
  `customer_q2_sentiment` DOUBLE COMMENT 'Customer sentiment in Q2',
  `customer_q3_sentiment` DOUBLE COMMENT 'Customer sentiment in Q3',
  `customer_q4_sentiment` DOUBLE COMMENT 'Customer sentiment in Q4',
  `end_offset_millis_q1` BIGINT COMMENT 'End offset in milliseconds for Q1',
  `end_offset_millis_q2` BIGINT COMMENT 'End offset in milliseconds for Q2',
  `end_offset_millis_q3` BIGINT COMMENT 'End offset in milliseconds for Q3',
  `end_offset_millis_q4` BIGINT COMMENT 'End offset in milliseconds for Q4',
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
PARTITIONED BY (`year`, `month`)
LOCATION '{{ configs.silver_data_path }}/call_engagement/call_transcript_summary'
COMMENT 'Silver table for Amazon Connect voice transcript summary analysis'
TBLPROPERTIES(
  'lakehouse.primary_key'='contact_id',
  'delta.enableChangeDataFeed'='true'
)
