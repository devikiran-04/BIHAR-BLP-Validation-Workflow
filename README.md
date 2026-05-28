# PostgreSQL BLP Validation Workflow

## Overview

This project automates the validation and comparison of BLP (Block Load Profile) data between:

* HES (Head End System)
* MDM (Meter Data Management)

The workflow performs:

1. Table truncation
2. HES data aggregation
3. MDM data aggregation
4. BLP mismatch identification
5. Parallel execution for faster processing

The application is written in Python using:

* `psycopg2`
* `concurrent.futures`
* PostgreSQL

---

# Features

* Multi-threaded date processing
* Parallel HES + MDM execution
* Automatic rollback on failures
* Dynamic table handling based on day number
* High-performance PostgreSQL operations
* Logging and execution tracking
* Transaction-safe execution

---

# Project Workflow

```text
START
   |
   ├── Process Date
   |      |
   |      ├── Truncate Existing Tables
   |      |
   |      ├── Parallel Execution
   |      |      ├── Insert HES Data
   |      |      └── Insert MDM Data
   |      |
   |      └── Insert BLP Comparison
   |
END
```

---

# Database Tables Used

## Source Tables

| Schema | Table                  |
| ------ | ---------------------- |
| `fep`  | `fep_csv_lp`           |
| `mdms` | `mdm_loadprofile_data` |
| `cdb`  | `meter_master`         |
| `cdb`  | `hes_vendor_m`         |

---

## Target Tables

| Table Pattern        | Description               |
| -------------------- | ------------------------- |
| `bfd.day{n}_hes_blp` | HES aggregated data       |
| `bfd.day{n}_mdm_blp` | MDM aggregated data       |
| `bfd.blp_day{n}`     | Final mismatch comparison |

---

# Requirements

Install dependencies:

```bash
pip install psycopg2-binary
```

---

# Configuration

Update PostgreSQL connection details:

```python
DB_CONFIG = {
    'host': 'YOUR_HOST',
    'port': '5432',
    'database': 'YOUR_DB',
    'user': 'YOUR_USER',
    'password': 'YOUR_PASSWORD'
}
```

---

# Execution

Run the script:

```bash
python blp_workflow.py
```

---

# Date Configuration

Modify execution dates:

```python
START_DATE = "20260517"
END_DATE   = "20260524"
```

---

# Parallel Processing

The application supports:

## Parallel Date Processing

```python
MAX_PARALLEL_DATES = 3
```

## Parallel HES + MDM Processing

```python
ThreadPoolExecutor(max_workers=2)
```

---

# Processing Logic

## HES Validation

* Reads LP data from `fep.fep_csv_lp`
* Filters successful records
* Aggregates meter counts

---

## MDM Validation

* Reads LP data from `mdms.mdm_loadprofile_data`
* Aggregates LP intervals

---

## BLP Comparison

Identifies meters where:

```sql
blp_cnt < 48
OR blp_cnt BETWEEN 49 AND 95
```

---

# Error Handling

The workflow includes:

* Transaction rollback
* Exception handling
* Connection cleanup
* Failure logging

Example:

```python
except Exception as e:

    if conn:
        conn.rollback()

    print(f"ERROR : {e}")
```

---

# Performance Optimizations

## Recommended PostgreSQL Indexes

```sql
CREATE INDEX idx_fep_csv_lp
ON fep.fep_csv_lp
(meter_time, meter_number);

CREATE INDEX idx_mdm_loadprofile
ON mdms.mdm_loadprofile_data
(lp_date, mtr_number, lp_time);

CREATE INDEX idx_meter_master
ON cdb.meter_master
(mtr_number, record_status, mtr_entry_type);
```

---

# Sample Output

```text
STARTING WORKFLOW...

================================================
PROCESSING DATE : 2026-05-17
================================================

Truncating : bfd.day17_hes_blp
Completed Truncate for DAY 17

Started HES Insert : bfd.day17_hes_blp
Started MDM Insert : bfd.day17_mdm_blp

Completed HES Insert : bfd.day17_hes_blp | Rows : 15000
Completed MDM Insert : bfd.day17_mdm_blp | Rows : 14920

Started BLP Insert : bfd.blp_day17
Completed BLP Insert : bfd.blp_day17 | Rows : 120

SUCCESS : 2026-05-17
```

---

# Tech Stack

* Python 3.x
* PostgreSQL
* psycopg2
* ThreadPoolExecutor

---

# Future Improvements

* Connection pooling
* Batch inserts
* Async PostgreSQL execution
* Config file support
* Logging framework integration
* Airflow scheduling
* Docker support

---

# Author

Devikiran Panigrahi

Python Developer | Data Engineer | PostgreSQL Enthusiast
