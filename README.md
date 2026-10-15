# Cloud-Native Transactional Data Lakehouse (PySpark + SQL + NoSQL)

A robust, cloud-ready data engineering pipeline designed to ingest, validate, and process high-throughput transactional events. Features dual-storage persistence (structured analytical aggregations to SQL / Parquet and unstructured audit logs to MongoDB) with automated unit testing and schema validation.

---

## 🎯 Business Context & Alignment
Built specifically to match enterprise data engineering consulting standards (e.g. Data Reply / Reply Group):
- **Batch & Stream Processing**: Cleans, deduplicates, and enriches raw transactions into partitioned analytical models.
- **Polyglot Persistence**: Bridges relational data warehouses (PostgreSQL / SQL) for BI reporting with document-oriented NoSQL stores (MongoDB) for semi-structured payload archiving.
- **Data Quality & Reliability**: Implements programmatic data quality gates, rejecting corrupt records before downstream consumption.

---

## 🏗️ Architecture

```text
[ Raw Transaction Stream / JSON ]
                │
                ▼
[ Ingestion & Schema Gatekeeper ] (Type checks, mandatory fields)
                │
                ├─────────────────────────────────────────┐
                ▼                                         ▼
   [ PySpark Transformation Engine ]         [ NoSQL Event Archive (MongoDB) ]
   ├── Deduplication by transaction_id       (Raw audit log, payload metadata)
   ├── Currency normalization
   └── Hourly merchant aggregations
                │
                ▼
   [ Structured Data Warehouse / Parquet ]
   (Optimized for BI dashboards and SQL querying)
```

---

## 🚀 Quickstart

1. Run the data pipeline:
```bash
python3 pipeline.py --input sample_transactions.json --out-dir output/
```

2. Run the automated data quality test suite:
```bash
python3 test_pipeline.py
```
