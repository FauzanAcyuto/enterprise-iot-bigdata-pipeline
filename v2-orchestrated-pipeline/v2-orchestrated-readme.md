# V2: Orchestrated Data Pipeline

> Production-grade data pipeline converting IoT telemetry logs to analytics-ready Parquet, orchestrated with Apache Airflow 3.x.

![Airflow](https://img.shields.io/badge/Airflow_3.x-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-FFF000?style=flat-square&logo=duckdb&logoColor=black)
![AWS S3](https://img.shields.io/badge/S3-569A31?style=flat-square&logo=amazons3&logoColor=white)
![Python](https://img.shields.io/badge/Python_3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![SQL Server](https://img.shields.io/badge/SQL_Server-CC2927?style=flat-square&logo=microsoftsqlserver&logoColor=white)

---

## Overview

This pipeline ingests raw JSON telemetry logs from IoT edge devices stored in S3, transforms them using DuckDB's streaming capabilities, and outputs partitioned Parquet files optimized for analytics workloads.

**What it solves:**

- Converts high-volume JSON logs to columnar Parquet format
- Handles variable timestamp formats gracefully
- Partitions data by date and region for efficient querying
- Tracks processing state to prevent duplicates and enable retries

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         APACHE AIRFLOW 3.x                          │
│  ┌───────────────────────────────────────────────────────────────┐  │
│  │                    s3_datalog_processor DAG                    │  │
│  │                      (hourly schedule)                         │  │
│  └───────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        ▼                         ▼                         ▼
┌───────────────┐       ┌─────────────────┐       ┌─────────────────┐
│   get_keys    │       │     compact     │       │  update_status  │
│───────────────│       │─────────────────│       │─────────────────│
│ • Query MSSQL │──────►│ • Read JSON/S3  │──────►│ • Mark SUCCESS  │
│ • Claim batch │       │ • Transform     │       │ • Update MSSQL  │
│ • Return keys │       │ • Write Parquet │       │                 │
└───────────────┘       └─────────────────┘       └─────────────────┘
        │                         │                         │
        ▼                         ▼                         ▼
┌───────────────┐       ┌─────────────────┐       ┌─────────────────┐
│    SQL Server │       │     AWS S3      │       │    SQL Server   │
│   (metadata)  │       │  (data lake)    │       │   (metadata)    │
└───────────────┘       └─────────────────┘       └───────────────────┘

Data Flow:
──────────────────────────────────────────────────────────────────────

  S3 Source                    DuckDB                     S3 Target
┌──────────────┐          ┌──────────────┐          ┌──────────────────┐
│ device1/     │          │              │          │ datalog_v2/      │
│   2025010112/│  ──────► │  Streaming   │ ──────►  │   hiveperiod=    │
│     log.json │          │  Transform   │          │     2025-01-01/  │
│ device2/     │          │              │          │       region=X/  │
│   2025010112/│          │ • Timestamp  │          │         data.pqt │
│     log.json │          │   normalize  │          └──────────────────┘
│ ...          │          │ • Add region │
└──────────────┘          │ • Partition  │
                          └──────────────┘
```

---

## V1 → V2 Evolution

This version addresses all limitations from the previous script-based approach:

| V1 Limitation | V2 Solution |
|---------------|-------------|
| Manual execution (`while True: sleep(3000)`) | Airflow scheduled DAG (hourly) |
| No validation | Graceful handling of malformed files |
| No state tracking — risk of duplicates | `run_id` claim pattern prevents reprocessing |
| Business logic mixed with orchestration | Clean separation — Airflow only orchestrates |
| No retry safety | Idempotent design — safe to retry any task |
| Implicit data contracts | Explicit type hints on all functions |

---

## Design Principles

These principles guide the codebase architecture:

### 1. Airflow Only Orchestrates

Business logic lives in standalone functions, not inside DAG definitions. Airflow's job is scheduling and dependency management — nothing else.

```python
# ✅ Good: Logic in reusable function
def get_pending_keys_sql(engine, distrik: str, run_id: str, file_limit=1000) -> list[str]:
    ...

@task
def get_keys(distrik: str, run_id: str, limit: int) -> list[str]:
    engine = MsSqlHook(...).get_sqlalchemy_engine()
    return get_pending_keys_sql(engine, distrik, run_id, limit)  # Delegates to function
```

### 2. Jinja Templating Over `**context`

Access Airflow runtime values via Jinja templates, not by passing the entire context dictionary. Cleaner, more explicit, easier to test.

```python
# ✅ Good: Explicit parameters via Jinja
keys = get_keys(
    "{{params.distrik}}",
    "{{dag_run.run_id}}",
    "{{params.key_limit_per_run}}",
)

# ❌ Avoid: Opaque context passing
def get_keys(**context):
    distrik = context['params']['distrik']  # Hidden dependencies
```

### 3. Explicit Type Hints

Every function declares its inputs and outputs. Makes the data contract clear and enables IDE support.

```python
def get_pending_keys_sql(
    engine, 
    distrik: str, 
    run_id: str, 
    file_limit: int = 1000
) -> list[str]:
    ...
```

### 4. Functions Split by Units of Work

Each function does one thing. "Get keys" is separate from "transform data" is separate from "update status." Not split by logical steps within a single operation.

### 5. Safe SQL with Parameter Binding

Never interpolate user input into SQL strings. Use parameterized queries to prevent injection and ensure proper escaping.

```python
# ✅ Good: Parameter binding
query = text("UPDATE table SET status = :status WHERE run_id = :run_id")
conn.execute(query, {"status": status, "run_id": run_id})

# ❌ Bad: String interpolation
query = f"UPDATE table SET status = '{status}' WHERE run_id = '{run_id}'"
```

### 6. Idempotency via Claim Pattern

The `run_id` claim pattern ensures:

- Each file is claimed by exactly one run
- Retries don't reprocess already-completed files
- Failed runs can be safely re-triggered

```python
# Step 1: Claim unclaimed rows with this run's ID
UPDATE ... SET compression_run_id = :run_id WHERE compression_run_id IS NULL

# Step 2: Select only rows claimed by this run
SELECT ... WHERE compression_run_id = :run_id

# Step 3: On success, mark as complete
UPDATE ... SET compression_status = 'SUCCESS' WHERE compression_run_id = :run_id
```

---

## Project Structure

```
v2-orchestrated-pipeline/
│
├── dags/
│   └── s3_datalog_processor.py    # Main DAG + task definitions
│
├── README.md                       # This file
└── requirements.txt                # Python dependencies
```

---

## Key Components

### DAG: `s3_datalog_processor`

| Property | Value |
|----------|-------|
| Schedule | Hourly |
| Tasks | `get_keys` → `compact` → `update_status` |
| Params | `distrik`, `key_limit_per_run`, `ram_limit`, `target_path` |

### Task: `get_keys`

Queries the metadata database for unprocessed S3 keys, claims them with the current `run_id`, and returns the list for processing.

### Task: `compact`

- Initializes DuckDB with S3 credentials
- Reads JSON files directly from S3 (no local download)
- Normalizes variable timestamp formats to consistent datetime
- Writes partitioned Parquet to target S3 path
- Streams data — memory-efficient regardless of batch size

### Task: `update_status`

Marks successfully processed keys as `SUCCESS` in the metadata database.

---

## Configuration

### DAG Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `distrik` | `BRCB` | Region code for data filtering |
| `key_limit_per_run` | `1000` | Max files to process per run |
| `ram_limit` | `10GB` | DuckDB memory limit |
| `target_path` | S3 path | Output location for Parquet files |
| `bucket_name` | `smartdbucket` | Source S3 bucket |

### Required Connections (Airflow UI)

| Connection ID | Type | Purpose |
|---------------|------|---------|
| `aws-creds` | Generic | AWS credentials (access key, secret, region) |
| `mssql-pama` | MSSQL | Metadata database connection |

---

## Timestamp Handling

The pipeline normalizes inconsistent timestamp formats from edge devices:

```sql
CASE
    WHEN heartbeat < 10000000000 THEN make_timestamp(heartbeat * 1000000)      -- seconds
    WHEN heartbeat < 10000000000000 THEN make_timestamp(heartbeat * 1000)      -- milliseconds
    WHEN heartbeat < 10000000000000000 THEN make_timestamp(heartbeat)          -- microseconds
    ELSE make_timestamp(heartbeat / 1000)                                       -- nanoseconds
END + INTERVAL 8 HOURS  -- Convert to local timezone
```

---

## Output Schema

Parquet files are partitioned by:

- `hiveperiod` — Date (YYYY-MM-DD)
- `dstrct_code` — Region identifier

Additional columns added during transformation:

- `datetime_wita` — Normalized timestamp (local timezone)
- `source_file` — Original S3 key for lineage tracking

---

## Error Handling

| Scenario | Behavior |
|----------|----------|
| No pending keys | Task skips gracefully (`AirflowSkipException`) |
| Zero rows produced | Task skips (no empty files written) |
| Transform failure | Exception raised, run marked for retry |
| Partial completion | Only completed keys marked `SUCCESS`; uncompleted keys remain claimable |

---

## Author

**Fauzan Acyuto**

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/fauzan-acyuto)

---

*Built as part of my transition to the Australian data market.*
