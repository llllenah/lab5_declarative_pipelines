# Silver: SCD Type 2, as a declarative pipeline using apply_changes.
# Same dataset/logic as the classic-MERGE version in 02_scd_type2.ipynb (Lab 4), rewritten
# declaratively for comparison. Population is tracked alongside city, so a value change is
# visible across SCD2 versions, not just a text change.

from pyspark import pipelines as dp

dp.create_streaming_table("customers_scd2")

dp.apply_changes(
    target="customers_scd2",
    source="silver_customers_landing",
    keys=["customer_id"],
    sequence_by="effective_date",
    stored_as_scd_type=2,
)
