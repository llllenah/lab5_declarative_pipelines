# Silver: load with data-quality expectations.
# Expectations replace ad-hoc "if row is bad, skip it" code with declared constraints
# the pipeline enforces and reports on automatically.

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="silver_customers_landing",
    comment="Cleaned customer records, with data-quality expectations applied.",
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_city", "city IS NOT NULL AND length(city) > 0")
@dp.expect("population_reasonable", "population IS NULL OR population > 0")
def silver_customers_landing():
    return (
        dp.read_stream("bronze_customers_landing")
        .withColumn("city", F.trim(F.col("city")))
        .dropDuplicates(["customer_id", "effective_date"])
    )

# expect_or_drop: rows failing this constraint are dropped and counted in the pipeline's
# data-quality metrics.
# expect (without _or_drop): rows are kept but flagged in the metrics — useful for
# a soft constraint you want visibility on without losing the row.
