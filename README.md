# Medallion Architecture Data Pipeline

An end-to-end data engineering portfolio project demonstrating the **Medallion Architecture** (Bronze, Silver, Gold) using **Apache Spark (PySpark)**, **Delta Lake**, and **Apache Airflow**.

This project simulates a real-world corporate environment where operational data (the "Core") is extracted from various sources (CRMs like Salesforce, ERPs, Oracle DBs) and transformed into a highly optimized analytical model for Business Intelligence (Power BI) and Data Science teams.

## 🏗️ Spark Cluster Architecture & Execution

The project relies on a containerized **Spark Standalone Cluster** managed via Docker/Podman, closely mimicking the behavior of Cloud Data Platforms (such as Azure Databricks Job Clusters):

- **Cluster Manager (Master):** Monitors the available resources across the cluster.
- **Workers:** Two worker nodes holding the physical memory and CPU cores.
- **The Driver (Airflow):** Acts as the brain of the operation. Airflow executes the Python scripts, which instantiate the `SparkSession`. The Driver negotiates resources with the Master, allocates **Executors** on the Workers, and uses the **Catalyst Optimizer** to build the physical execution plan (DAG of stages) dynamically applying techniques like Broadcast Joins or Adaptive Query Execution (AQE).

## 📊 Volumetrics & Scale
The pipeline was battle-tested by processing a massive **TPC-H dataset** containing nearly **18 million historical records**. The pipeline successfully ingests this volume and transforms it into a highly optimized Star Schema consisting of 9 analytical tables, including:
- `fact_sales`: ~17.9 Million rows
- `fact_orders`: ~4.5 Million rows
- `agg_customer_monthly`: ~4 Million rows

## 🥈 Medallion Layers & Processing Logic

The pipeline strictly follows the Medallion Architecture paradigm, processing data progressively to improve structure and quality. All execution logic is fully decoupled from the business rules, which are maintained in `.yml` configurations.

### 0. The "Core" (Source Data)
In a real enterprise, the "Core" refers to the transactional databases (OLTP) from different agencies or systems. In this project, the core is simulated using raw Parquet files located in `data/source/tpch/`. To scale this to multiple CRMs or agencies, we would simply add new paths to the configuration files without changing the Spark codebase.

### 1. 🥉 Bronze Layer (Raw Ingestion)
- **Goal:** Ingest raw data exactly as it arrives from the core systems, appending auditing metadata (`_ingestion_timestamp`).
- **Processing:** The Spark Driver reads the source files and performs an **insert-only** incremental load. It acts as an immutable historical archive. No data is altered or deleted in this stage.
- **Files:** `jobs/run_bronze.py`, `jobs/bronze_ingestion.py`, `jobs/bronze_config.yml`.

### 2. 🥈 Silver Layer (Cleansing & Enrichment)
- **Goal:** Filter, clean, and conform data to a strict corporate schema.
- **Processing:** The data is transformed by dropping invalid records, casting data types, and filtering out nulls based on the `.yml` rules. To handle slowly changing dimensions or updates from the core, this layer uses Delta Lake's native **Upserts** (`MERGE INTO`). This ensures that changes in the source system (e.g., an order status update) are accurately reflected without duplicating rows.
- **Files:** `jobs/silver_processor.py`, `jobs/silver_config.yml`.

### 3. 🥇 Gold Layer (Business Aggregations)
- **Goal:** Deliver finalized, business-ready data products.
- **Processing:** Transforms the normalized Silver data into a **Star Schema** (Fact and Dimension tables) tailored for downstream BI tools. It also calculates heavy monthly aggregations for faster reporting. Similar to Silver, it leverages Delta `MERGE` to efficiently update analytical tables on daily increments without recalculating the 18M rows.
- **Files:** `jobs/gold_processor.py`, `jobs/gold_config.yml`.

## ⚙️ Orchestration with Apache Airflow

The entire workflow is orchestrated using **Apache Airflow** (located in `dags/tpch_pipeline.py`). 

1. The DAG triggers on a daily schedule.
2. It executes the Bronze, Silver, and Gold jobs sequentially as ephemeral Spark tasks.
3. If a task fails, the pipeline halts, preventing downstream data corruption.
4. Because the codebase uses Delta Lake `MERGE`, running the DAG daily takes only seconds/minutes, as it processes strictly the specific increment (deltas) of that day.

## 📂 Repository Structure

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
└── podman-compose.yml           # Containerized Spark & Airflow infrastructure
```

## 🚀 Key Technologies
- **Apache Spark (PySpark):** Distributed data processing engine.
- **Delta Lake:** Brings ACID transactions, Time Travel, and Upserts (`MERGE`) to Spark workloads.
- **Apache Airflow:** Directed Acyclic Graph (DAG) task scheduling and orchestration.
- **Docker / Podman:** Infrastructure containerization (Spark Master, Workers, Airflow).
