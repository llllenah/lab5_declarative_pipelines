# Lab 5 — Declarative Pipelines / Lakeflow

Catalog: `dbr_dev_ua5816bd`, personal schemas: `lena066636_bronze` / `lena066636_silver`.

Uses the new Lakeflow Spark Declarative Pipelines API (`from pyspark import pipelines as dp`),
the successor to Delta Live Tables. Old `@dlt.table` code still runs, but this lab uses the
new decorators (`@dp.table`, `@dp.materialized_view`, `@dp.expect*`) since that's what the
course now expects students to show.

## What's in this folder

| File | Task it covers |
|---|---|
| `pipelines/01_bronze_streaming.py` | Declarative pipeline table from a streaming source (Event Hub ticks, reused from the crypto-demo setup) |
| `pipelines/02_bronze_landing.py` | Declarative pipeline table from a CSV/JSON landing volume |
| `pipelines/03_silver_expectations.py` | Silver load with data-quality expectations |
| `pipelines/04_customers_scd2.py` | SCD Type 2 as a declarative pipeline, using `apply_changes` (parallel to the classic-MERGE version in `02_scd_type2.ipynb`) |
| `resources/lab5_pipeline.yml` | Asset Bundle definition for the pipeline itself |
| `resources/lab5_job.yml` | Asset Bundle job: runs the pipeline, then a maintenance task (OPTIMIZE/VACUUM) |
| `databricks.yml` | Bundle root config |
| `lineage_and_reload_notes.md` | How to check lineage, and how to reload safely |
| `comparison_declarative_vs_classic.md` | Declarative vs classic Spark pipelines — writeup |

## How the pieces fit together

1. Create the pipeline in Databricks (UI: **Pipelines → Create pipeline**, or via the bundle below), pointing it at the files in `pipelines/`.
2. Run it — it builds bronze and silver tables in one DAG, computed from the `pipelines/*.py` source files.
3. Deploy the whole thing (pipeline + a job that also runs table maintenance) through Databricks Asset Bundles — the same mechanism the course reuses for CI/CD in Week 8.

## Deploying with Asset Bundles

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run lab5_maintenance_job -t dev
```

This creates the pipeline and the wrapping job under your workspace, using the target defined in `databricks.yml`.
