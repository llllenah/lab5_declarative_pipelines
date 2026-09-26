# Databricks notebook source
# Maintenance task for the Lab 5 job: OPTIMIZE + VACUUM after the pipeline run.
# This is what Lukasz's feedback asked for: bronze -> silver scd2 -> maintenance,
# as a separate task in the job rather than folded into the pipeline itself.

dbutils.widgets.text("catalog", "dbr_dev_ua5816bd")
dbutils.widgets.text("silver_schema", "lena066636_silver")

catalog = dbutils.widgets.get("catalog")
silver_schema = dbutils.widgets.get("silver_schema")

tables = ["customers_scd2", "silver_customers_landing"]

for table in tables:
    full_name = f"{catalog}.{silver_schema}.{table}"
    spark.sql(f"OPTIMIZE {full_name}")
    spark.sql(f"VACUUM {full_name}")
