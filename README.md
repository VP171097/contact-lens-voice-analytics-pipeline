# 🎙️ Call Intelligence: End-to-End Transcript Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg?logo=python&logoColor=white)](https://www.python.org/)
[![Databricks](https://img.shields.io/badge/Platform-Databricks%20Serverless-FF3621.svg?logo=databricks&logoColor=white)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake%203.0-00ADD8.svg?logo=delta&logoColor=white)](https://delta.io/)
[![Apache Spark](https://img.shields.io/badge/Engine-PySpark%20Structured%20Streaming-E25A1C.svg?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![AWS](https://img.shields.io/badge/Cloud-AWS%20S3-232F3E.svg?logo=amazon-aws&logoColor=white)](https://aws.amazon.com/)

An enterprise-grade, multi-region batch and streaming data engineering pipeline built with **PySpark**, **Delta Lake**, and **Databricks Workflows**. 

The pipeline ingests, cleanses, enriches, and structures **Amazon Connect Contact Lens** voice analysis and redacted call transcript JSON files across 5 global regions (**EU, NAM, LATAM, APAC, MEA**) into a high-performance **Medallion Lakehouse Architecture** (Bronze ➔ Silver ➔ Silver Plus).

---

## 📑 Table of Contents
1. [Architecture Overview](#-architecture-overview)
2. [Medallion Pipeline Layers](#-medallion-pipeline-layers)
3. [Key Engineering Highlights](#-key-engineering-highlights)
4. [Data Models & Table DDLs](#-data-models--table-ddls)
5. [Repository Structure](#-repository-structure)
6. [Configuration Management](#-configuration-management)
7. [Databricks Orchestration & Deployment](#-databricks-orchestration--deployment)
8. [Testing & Synthetic Data Generator](#-testing--synthetic-data-generator)
9. [Getting Started](#-getting-started)

---

## 🏛️ Architecture Overview

```mermaid
flowchart TD
    subgraph Regional_S3["AWS S3 Raw Landing (Regional)"]
        S3_EU["S3: EU Region"]
        S3_NAM["S3: NAM Region"]
        S3_LATAM["S3: LATAM Region"]
        S3_APAC["S3: APAC Region"]
        S3_MEA["S3: MEA Region"]
    end

    subgraph Bronze_Layer["Bronze Layer (Ingestion & Normalization)"]
        BronzeIngest["bronze_notebook.py\n(Schema Drift Protection & JSON String Serialization)"]
        BronzeDelta[("Delta Table:\nbronze_call_engagement.voice_analysis")]
    end

    subgraph Silver_Layer["Silver Layer (Structured Streaming & Summaries)"]
        SilverStream["silver_notebook.py\n(Spark Structured Streaming + foreachBatch Deduplication)"]
        SilverDelta[("Delta Table (CDF Enabled):\nsilver_call_engagement.call_transcript_summary")]
    end

    subgraph Silver_Plus_Layer["Silver Plus Layer (Line-Level Granularity)"]
        SilverPlusStream["silver_plus_notebook.py\n(Delta Change Data Feed Ingestion)"]
        SilverPlusDelta[("Delta Table:\nsilver_plus_call_engagement.call_transcript_lines")]
    end

    subgraph Control_Plane["Pipeline Metadata & Control"]
        StateTable[("Delta Control Table:\npipeline_control.callintel_pipeline_cdf_state")]
        Tracker["cdf_version_tracker.py\n(Version Watermarking & Resume Engine)"]
    end

    S3_EU & S3_NAM & S3_LATAM & S3_APAC & S3_MEA --> BronzeIngest
    BronzeIngest --> BronzeDelta
    BronzeDelta -->|spark.readStream| SilverStream
    SilverStream --> SilverDelta
    SilverDelta -->|Delta CDF (readStream)| SilverPlusStream
    SilverPlusStream --> SilverPlusDelta

    SilverPlusStream <--> Tracker
    Tracker <--> StateTable
```

---

## 🔄 Medallion Pipeline Layers

### 1. 🥉 Bronze Ingestion (`bronze_notebook.py`)
- **Source**: Raw redacted Contact Lens voice-analysis JSON files from regional S3 buckets (`<s3_raw_prefix>/<REGION>/<YYYY>/<MM>/<DD>/analysis_redacted-<contact_id>.json`).
- **Functionality**:
  - Automatically handles cross-region schema discrepancies by converting complex nested objects (`StructType`, `ArrayType`, `MapType`) into JSON strings (`to_json`).
  - Extracts partition metadata (`year`, `month`, `day`, `region`) from file paths and ingestion timestamps.
  - Appends records into the centralized Bronze Delta table.

### 2. 🥈 Silver Enrichment & Summaries (`silver_notebook.py`)
- **Source**: `bronze_call_engagement.voice_analysis` (Streaming read via Delta).
- **Target**: `silver_call_engagement.call_transcript_summary` (Delta Table with **Change Data Feed (CDF)** enabled).
- **Functionality**:
  - Executes as a **Structured Streaming** pipeline using micro-batches (`foreachBatch`).
  - Unpacks JSON payloads into structured metrics: call duration, agent/customer talk times, non-talk time, words-per-minute (WPM), matched categories, and customer/agent sentiment broken down by call quartiles (Q1 to Q4).
  - Performs micro-batch deduplication using `row_number() OVER (PARTITION BY contact_id ORDER BY transcript_creation_date DESC)`.
  - Merges into the target partitioned table with schema evolution enabled.

### 3. 🥇 Silver Plus Line-Level Detail (`silver_plus_notebook.py`)
- **Source**: `silver_call_engagement.call_transcript_summary` via **Delta Change Data Feed (`readChangeFeed`)**.
- **Target**: `silver_plus_call_engagement.call_transcript_lines`.
- **Functionality**:
  - Consumes change data (inserts/updates) from Silver using CDF.
  - Explodes the structured `transcript` array into individual conversational utterances/lines.
  - Derives per-utterance sentiment, loudness scores, participant roles (`AGENT` / `CUSTOMER`), sequence order, and timestamp offsets (`begin_offset_millis`, `end_offset_millis`).
  - Streaming-merges line-level records into the partitioned Silver Plus Delta table.

---

## ⚡ Key Engineering Highlights

* **Resilient Schema-Drift Protection**: Amazon Connect Contact Lens schemas frequently evolve across AWS regions. The Bronze layer serializes complex struct/array trees into native JSON strings, guaranteeing zero pipeline failures during schema changes.
* **Delta Change Data Feed (CDF)**: Decouples Silver and Silver Plus processing. Silver Plus processes only net-new and modified transcripts incrementally, cutting compute cost and eliminating full-table scans.
* **Custom CDF Version & State Tracking (`cdf_version_tracker.py`)**: A native Delta control table records the exact source Delta commit version and watermark timestamp per `(source_table, target_table)` pair. This allows streaming jobs to safely rebuild checkpoints and resume without duplicates.
* **Optimized Generated Partitions & Window Helpers (`partition_window_utils.py`)**: Tables utilize Delta Lake generated columns:
  ```sql
  `year` INTEGER GENERATED ALWAYS AS (YEAR(transcript_creation_date)),
  `month` INTEGER GENERATED ALWAYS AS (MONTH(transcript_creation_date)),
  `day` INTEGER GENERATED ALWAYS AS (DAY(transcript_creation_date))
  ```
  Dynamic partition predicate builders translate user date ranges into optimized SQL partition pruning clauses.
* **Shared Utilities (`common_utils.py`)**: Reusable helper functions for date formatting, audit timestamping (`pl_created_on`, `pl_modified_on`), partition schema validations, and safe casting.

---

## 📊 Data Models & Table DDLs

### 1. `call_transcript_summary` (Silver)
| Column | Type | Description |
| :--- | :--- | :--- |
| `region` | `STRING` | Ingestion region (`EU`, `NAM`, `LATAM`, `APAC`, `MEA`) |
| `contact_id` | `STRING` | **Primary Key**: Amazon Connect Contact identifier |
| `account_id` | `STRING` | AWS Account ID |
| `instance_id` | `STRING` | Amazon Connect Instance ID |
| `language_code` | `STRING` | Call language (e.g., `en-US`, `de-DE`) |
| `call_duration_millis` | `BIGINT` | Total call duration in milliseconds |
| `agent_talk_time_millis` | `BIGINT` | Total agent speaking duration |
| `customer_talk_time_millis` | `BIGINT` | Total customer speaking duration |
| `non_talk_time_total_millis` | `BIGINT` | Total silence/dead air duration |
| `agent_sentiment_overall` | `DOUBLE` | Aggregate agent sentiment score |
| `customer_sentiment_overall` | `DOUBLE` | Aggregate customer sentiment score |
| `customer_q1_sentiment` ... `customer_q4_sentiment` | `DOUBLE` | Quarterly progression of customer sentiment |
| `matched_categories` | `STRING` | Contact Lens rule match tags |
| `transcript` | `STRING` | Full serialized conversation structure |
| `year`, `month` | `INTEGER` | **Partition Columns** (Generated from `transcript_creation_date`) |

### 2. `call_transcript_lines` (Silver Plus)
| Column | Type | Description |
| :--- | :--- | :--- |
| `contact_id` | `STRING` | **Composite PK**: Amazon Connect Contact identifier |
| `id` | `STRING` | **Composite PK**: Unique segment identifier |
| `participant_role` | `STRING` | `AGENT` or `CUSTOMER` |
| `sequence` | `INTEGER` | Conversation line index ordered chronologically |
| `content` | `STRING` | Redacted text utterance |
| `begin_offset_millis` | `INTEGER` | Offset when speech started |
| `end_offset_millis` | `INTEGER` | Offset when speech ended |
| `sentiment` | `STRING` | Segment sentiment (`POSITIVE`, `NEUTRAL`, `NEGATIVE`) |
| `loudness_score` | `STRING` | Audio loudness score for the utterance |
| `transcript_creation_date` | `DATE` | Date of the call |
| `year`, `month`, `day` | `INTEGER` | **Partition Columns** |

---

## 📂 Repository Structure

```text
├── config/
│   ├── config_loader.py          # Dynamic environment configuration resolver
│   ├── database_setup.sql        # Unity Catalog, schemas, and volume bootstrap SQL
│   └── pipeline_config.yaml      # Multi-environment (dev/stg/prod) settings
├── test/
│   ├── bulk_dummy_transcripts.jsonl  # Pre-generated realistic Contact Lens payload fixtures
│   ├── dummy_data_job.yml            # Databricks workflow definition for test data generation
│   ├── generate_bulk_dummy_dataset.py# High-throughput mock data generator
│   ├── generate_dummy_data.py        # Regional test file generator
│   ├── generate_dummy_data_notebook.ipynb # Interactive notebook for generating synthetic data
│   ├── sample_lines.txt              # 2,000+ realistic call center conversation utterances
│   └── upload_dummy_data_all_regions.sh # Script to upload test sets across S3 regions
├── bronze_notebook.py            # Bronze Layer: Ingests regional S3 JSONs into Bronze Delta
├── silver_notebook.py            # Silver Layer: Streaming aggregation & summary metrics
├── silver_plus_notebook.py       # Silver Plus Layer: Explodes transcript lines via CDF
├── cdf_version_tracker.py        # Metadata manager tracking Delta commit versions & watermarks
├── common_utils.py               # Shared pipeline utilities (timestamps, audit columns, schema)
├── partition_window_utils.py     # Partition pruning and date range predicate generators
├── metadata_ddl.sql              # DDL for Silver summary table
├── transcript_ddl.sql            # DDL for Silver Plus transcript lines table
├── workflow_job.yml              # Databricks Asset Bundle / Workflow definition (YAML)
└── workflow_job.json             # Databricks REST API Workflow specification (JSON)
```

---

## ⚙️ Configuration Management

Environment settings are maintained in `config/pipeline_config.yaml` and resolved cleanly at runtime by `config/config_loader.py`:

```yaml
environments:
  dev:
    aws:
      region: eu-west-1
      s3_bucket: callintel-dev-raw
      s3_raw_prefix: contact-lens/voice-analysis/redacted
      s3_bronze_prefix: bronze/call_engagement/voice_analysis
    databricks:
      catalog: callintel_dev
      bronze_schema: bronze_call_engagement
      silver_schema: silver_call_engagement
      silver_plus_schema: silver_plus_call_engagement
      control_schema: pipeline_control
```

Pass the `env` parameter (`dev`, `stg`, or `prod`) through Databricks job parameters or notebook widgets to automatically bind paths and catalogs.

---

## 🚀 Databricks Orchestration & Deployment

The pipeline is orchestrated as a directed acyclic graph (DAG) via Databricks Workflows (`workflow_job.yml`):

```text
[ call_txn_bronze_ingest ] 
           │
           ▼
[ call_txn_silver_enrich ]
           │
           ▼
[ call_txn_silver_plus_explode ]
```

### Deploying the Workflow via Databricks CLI:
```bash
# Deploy using Databricks Asset Bundles
databricks bundle deploy -t dev

# Or deploy via Databricks Jobs API
databricks jobs create --json-file workflow_job.json
```

---

## 🧪 Testing & Synthetic Data Generator

The repository includes a comprehensive synthetic test generation suite to simulate Amazon Connect Contact Lens outputs without needing live AWS telephony:

1. **Generate Local Test Fixtures**:
   ```bash
   python test/generate_dummy_data.py \
     --out ./dummy_data \
     --region EU \
     --days 3 \
     --contacts-per-day 10
   ```

2. **Upload Test Data to Multi-Region S3 Buckets**:
   ```bash
   # Uploads generated files to EU, NAM, LATAM, APAC, and MEA regional prefixes
   bash test/upload_dummy_data_all_regions.sh dev
   ```

---

## 🛠️ Getting Started

### Prerequisites
- Python `3.10+`
- PySpark `3.4+` & Delta Lake `2.4+` / `3.0+`
- Access to a Databricks Workspace with Unity Catalog enabled
- AWS S3 bucket access with IAM credentials/instance profile

### Step 1: Initialize Database & Schemas
Run `config/database_setup.sql` in your Databricks SQL Warehouse to create required catalogs, schemas, and control tables.

### Step 2: Create Tables
Execute table DDL scripts:
- `metadata_ddl.sql` (Creates `call_transcript_summary`)
- `transcript_ddl.sql` (Creates `call_transcript_lines`)

### Step 3: Run the Pipeline
Trigger `workflow_job.yml` or run `bronze_notebook.py`, `silver_notebook.py`, and `silver_plus_notebook.py` sequentially in Databricks.
