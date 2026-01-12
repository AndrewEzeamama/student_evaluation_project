# Student Evaluation Analytics Pipeline

## High-Level Overview

This project implements a production-grade **Bronze → Silver → Gold** data pipeline for student evaluation analytics.  
It is designed to be **reliable, auditable, scalable**, and **AI-ready**, using Apache Airflow for orchestration and a modular Python codebase for transformations and data quality enforcement.

The pipeline ingests raw Excel-based assessment data, cleans and normalizes it into analytical entities, validates data quality using Great Expectations, and materializes a star schema for analytics and machine learning use cases.

Key design goals:
- Reliability through idempotent processing
- Data quality enforcement without pipeline blockage
- Auditability and traceability
- Clear scalability path

---

## Pipeline Architecture

The pipeline follows a layered data architecture:

- **Bronze Layer** – Raw, immutable ingestion
- **Silver Layer** – Cleaned, normalized entities
- **Data Quality Layer** – Validation and quarantine
- **Gold Layer** – Analytics-ready star schema

Each layer is orchestrated as a task within an Apache Airflow DAG.

---

## Component Details

### Orchestration
- **Apache Airflow**
- DAG-based execution with clear task boundaries
- Supports manual and scheduled runs

### Processing
- **Pandas** for data transformations
- Modular pipeline stages (ingest, transform, analytics)

### Data Quality
- **Great Expectations**
- Non-blocking validation
- Failed records quarantined for review

### Analytics Storage
- **DuckDB** for analytical querying
- Parquet files for interoperable storage

---

## Directory Structure

```
student-evaluation-pipeline/
├── airflow/                  # Airflow DAGs and Docker setup
├── data/
│   ├── bronze/               # Raw immutable data
│   ├── silver/               # Cleaned entity datasets
│   ├── gold/                 # Analytics-ready datasets
│   ├── quarantine/           # Data quality failures
│   └── db/duckdb/            # DuckDB database
├── src/
│   ├── pipeline/             # Ingest, transform, analytics logic
│   ├── data_quality/         # Great Expectations checks
│   └── core/                 # Config, logging, audit, idempotency
├── sql/                      # DDL and analytics queries
├── logs/                     # Pipeline logs
├── requirements.txt
├── README.md
└── architecture.md
```

---

## Data Flow

1. **Bronze Ingestion**
   - Excel file ingested
   - Column standardization
   - Written as Parquet

2. **Silver Transformation**
   - Split into domain entities
   - Deduplication and normalization
   - Invalid rows quarantined

3. **Data Quality Validation**
   - Completeness and uniqueness checks
   - Range and composite key validation
   - Non-blocking execution

4. **Gold Materialization**
   - Dimension and fact tables
   - Star schema modeling
   - Materialized into DuckDB and Parquet

---

## Operational Features

- **Idempotency** – Safe reruns without duplicate data
- **Audit Logging** – Run-level metadata stored in DuckDB
- **Quarantine Zone** – Isolated failed records
- **Observability** – Structured logging per pipeline stage
- **Failure Isolation** – Data quality issues do not stop delivery

---

## Data Quality Checks

### Data Quality

- Implemented using **Great Expectations**
- Checks include:
  - Completeness (non-null fields)
  - Uniqueness (primary and composite keys)
  - Valid score ranges
  - Date validity rules
- Data quality is **non-blocking**
- Failed records are written to a quarantine directory for investigation

### Audit Logging

- Every pipeline stage writes audit records to DuckDB
- Captured metadata:
  - Run ID
  - Stage name
  - Status
  - Row counts
  - Error messages
- Enables operational monitoring and lineage tracking


---

## Technical Specifications

| Category   | Technology           |
|------------|----------------------|
| Orchestration | Apache Airflow |
| Processing | Python, Pandas |
| Data Quality | Great Expectations |
| Storage | Parquet, DuckDB |
| Logging | Python logging |
| Auditing | DuckDB tables |
| Deployment | Docker / Local |

---

## Running the Pipeline Locally

### Prerequisites
- Python 3.9+
- Docker & Docker Compose
- Git

### Installation Steps

1. Clone the repository:
```bash
git clone <repository-url>
cd student-evaluation-pipeline
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Start Airflow using Docker:
```bash
cd airflow/docker
docker-compose up -d
```

5. Access Airflow UI:
```
http://localhost:8080
```

6. Trigger the DAG:
- Open Airflow UI
- Enable and trigger `student_test_pipeline`

---

## Scaling the Pipeline for Large Datasets

If data volume or complexity grows, the architecture scales naturally:

### Processing Scale
- Replace Pandas with **PySpark** or **Dask**
- Distribute transformations across a compute cluster

### Storage Scale
- Move from local Parquet files to cloud object storage (S3, GCS, ADLS)
- Partition datasets by date, school, or academic year

### Orchestration Scale
- Parallelize Silver transformations by entity
- Add SLAs and alerts in Airflow
- Introduce incremental processing instead of full reloads

### Analytics Scale
- Replace DuckDB with a cloud data warehouse (BigQuery, Snowflake, Redshift)
- Introduce dbt for warehouse-native transformations and versioned models

---

## Supporting AI and Predictive Use Cases

The Gold layer provides a strong foundation for AI and machine learning:

### Feature Engineering
- Clean fact and dimension tables
- Consistent keys and historical records
- High-quality, validated inputs

### Example Use Cases
- Student performance prediction
- Early identification of at-risk students
- Teacher effectiveness analysis
- School-level outcome forecasting
- Personalized learning recommendations

### MLOps Readiness
- Reproducible training datasets
- Historical lineage for retraining
- Data quality safeguards for model inputs

---

## Summary

This pipeline provides a strong foundation for:
- Reliable analytics delivery
- Enterprise-scale data engineering
- AI and predictive intelligence

It is intentionally designed to evolve without architectural rework. It provides a production-grade foundation for analytics today and predictive intelligence in the future.
