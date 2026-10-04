# Medallion Architecture Data Pipeline

An end-to-end data engineering portfolio project demonstrating the **Medallion Architecture** (Bronze, Silver, Gold) using **Apache Spark (PySpark)**, **Delta Lake**, and **Apache Airflow**.

This project simulates a real-world corporate environment where operational data (the "Core") is extracted from various sources (CRMs like Salesforce, ERPs, Oracle DBs) and transformed into a highly optimized analytical model for Business Intelligence (Power BI) and Data Science teams.

## 🏗️ Architecture Overview

The pipeline strictly follows the Medallion Architecture paradigm, processing data progressively to improve structure and quality.

### 0. The "Core" (Source Data)
In a real enterprise, the "Core" refers to the transactional databases (OLTP) from different agencies or systems. In this project, the core is simulated using raw Parquet files located in `data/source/tpch/`. If we were to scale this to multiple CRMs or agencies, we would simply have multiple ingestion pipelines feeding into the Bronze layer.

### 1. 🥉 Bronze Layer (Raw Ingestion)
- **Goal:** Ingest raw data exactly as it arrives from the core systems, appending a timestamp and execution ID.
- **Mechanism:** Insert-only incremental loads. It acts as an immutable historical archive.
- **Files:** `jobs/run_bronze.py`, `jobs/bronze_ingestion.py`, `jobs/bronze_config.yml`.

### 2. 🥈 Silver Layer (Cleansing & Enrichment)
- **Goal:** Filter, clean, and conform data to a strict corporate schema.
- **Mechanism:** Performs validations (rejecting nulls, casting types) and handles **Upserts** (Updates and Inserts) using Delta Lake's native `MERGE` operation. This ensures that changes in the source system (e.g., an order status update) are accurately reflected.
- **Files:** `jobs/silver_processor.py`, `jobs/silver_config.yml`.

### 3. 🥇 Gold Layer (Business Aggregations)
- **Goal:** Deliver finalized, business-ready data products.
- **Mechanism:** Transforms the normalized Silver data into a **Star Schema** (Fact and Dimension tables) tailored for downstream BI tools. It also performs high-level monthly aggregations. Uses Delta `MERGE` to efficiently update analytical tables on daily increments.
- **Files:** `jobs/gold_processor.py`, `jobs/gold_config.yml`.

## ⚙️ Orchestration with Apache Airflow

The entire workflow is orchestrated using **Apache Airflow** (located in `dags/tpch_pipeline.py`). 

Airflow simulates a modern Cloud Data Platform behavior (like **Azure Databricks Job Clusters**):
1. The DAG triggers on a daily schedule.
2. It executes the Bronze, Silver, and Gold jobs sequentially as Spark tasks.
3. If a task fails, the pipeline halts, preventing downstream data corruption.
4. Because the codebase uses Delta Lake `MERGE`, running the DAG daily takes only seconds/minutes, as it only processes the specific increment (deltas) of that day rather than recalculating the entire historical dataset.

## 📂 Repository Structure

The project is structured to keep execution code, configurations, and exploratory environments decoupled:

```text
├── dags/
│   └── tpch_pipeline.py         # Airflow DAG for orchestration
├── data/
│   └── source/                  # Simulated "Core" operational data
├── exports/                     # Gold layer CSV exports for BI tools
├── jobs/
│   ├── run_bronze.py            # Bronze execution wrapper
│   ├── bronze_ingestion.py      # Bronze logic class
│   ├── silver_processor.py      # Silver logic class
│   ├── gold_processor.py        # Gold logic class
│   ├── bronze_config.yml        # Bronze configurations & schema
│   ├── silver_config.yml        # Silver configurations & schema
│   └── gold_config.yml          # Gold schema, primary keys & aggregations
├── notebooks/                   # Jupyter notebooks for ad-hoc exploration
└── docker-compose.yml           # Containerized Spark & Airflow infrastructure
```

## 🚀 Key Technologies
- **Apache Spark (PySpark):** Distributed data processing engine.
- **Delta Lake:** Brings ACID transactions, Time Travel, and Upserts (`MERGE`) to Spark workloads.
- **Apache Airflow:** Directed Acyclic Graph (DAG) task scheduling and orchestration.
- **Docker / Podman:** Infrastructure containerization (Spark Master, Workers, Airflow).
