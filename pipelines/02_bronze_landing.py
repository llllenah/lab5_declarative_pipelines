# Bronze: declarative pipeline table from a CSV/JSON landing volume.
# Per Lukasz's earlier comment on the crypto-demo project, this reads directly from
# the bronze schema's own volume rather than a separate "landing" schema.

from pyspark import pipelines as dp

LANDING_PATH = spark.conf.get(
    "landing_path",
    "/Volumes/dbr_dev_ua5816bd/lena066636_bronze/landing/customers/",
)


@dp.table(
    name="bronze_customers_landing",
    comment="Raw customer records landed as CSV/JSON files, ingested via Auto Loader.",
)
def bronze_customers_landing():
    return (
        dp.read_stream(
            "cloudFiles",
            options={
                "cloudFiles.format": "json",
                "cloudFiles.schemaLocation": f"{LANDING_PATH}_schema",
            },
        )
        .load(LANDING_PATH)
    )
