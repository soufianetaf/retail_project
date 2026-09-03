# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp, to_timestamp, trim, upper

# Liste officielle des statuts dans le dataset Olist
VALID_STATUSES = [
    "DELIVERED",
    "SHIPPED",
    "PROCESSING",
    "CANCELED",
    "INVOICED",
    "UNAVAILABLE",
    "APPROVED",
    "CREATED",
]


@dlt.table(
    name="silver_orders",
    comment="Table des commandes Olist nettoyées et typées.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.managed": "true"},
)
@dlt.expect_or_fail("valid_order_id", "order_id IS NOT NULL AND order_id != ''")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect(
    "valid_status", f"order_status IN ({','.join([repr(s) for s in VALID_STATUSES])})"
)
def silver_orders_clean():
    # LA CORRECTION EST ICI : on appelle le nom de la variable
    catalog = spark.conf.get("catalog_name")

    # On lit la table Bronze en utilisant la variable
    df = spark.readStream.table(f"{catalog}.bronze.bronze_orders")

    df_cleaned = (
        df.withColumn("order_id", trim(col("order_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("order_status", upper(trim(col("order_status"))))
        .withColumn(
            "order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))
        )
        .dropDuplicates(["order_id"])
        .withColumn("_silver_processed_at", current_timestamp())
    )

    return df_cleaned
