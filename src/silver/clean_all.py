# Databricks notebook source
import dlt
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    lit,
    to_timestamp,
    trim,
    upper,
)

# Récupération dynamique de l'environnement (dev, staging, prod)
catalog = spark.conf.get("catalog_name")

# =====================================================================================
# 1. ORDERS (Commandes)
# =====================================================================================
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


@dlt.table(name="silver_orders", table_properties={"quality": "silver"})
@dlt.expect_or_fail("valid_order", "order_id IS NOT NULL")
@dlt.expect(
    "valid_status",
    f"upper(order_status) IN ({','.join([repr(s) for s in VALID_STATUSES])})",
)
def silver_orders():
    return (
        spark.readStream.table(f"{catalog}.bronze.bronze_orders")
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("order_status", upper(trim(col("order_status"))))
        .withColumn(
            "order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))
        )
        .dropDuplicates(["order_id"])
        .withColumn("_silver_processed_at", current_timestamp())
    )


# =====================================================================================
# 2. ORDER_ITEMS (Contenu du panier / Lignes de commande)
# =====================================================================================
@dlt.table(name="silver_order_items", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_ids", "order_id IS NOT NULL AND product_id IS NOT NULL")
@dlt.expect_or_drop("positive_price", "price >= 0")
def silver_order_items():
    return (
        spark.readStream.table(f"{catalog}.bronze.bronze_order_items")
        .withColumn("price", coalesce(col("price").cast("decimal(10,2)"), lit(0.00)))
        .withColumn(
            "freight_value",
            coalesce(col("freight_value").cast("decimal(10,2)"), lit(0.00)),
        )
        .dropDuplicates(
            ["order_id", "order_item_id"]
        )  # Une ligne = 1 article précis d'1 commande
        .withColumn("_silver_processed_at", current_timestamp())
    )


# =====================================================================================
# 3. ORDER_PAYMENTS (Paiements)
# =====================================================================================
@dlt.table(name="silver_order_payments", table_properties={"quality": "silver"})
@dlt.expect_or_drop("positive_payment", "payment_value >= 0")
def silver_order_payments():
    return (
        spark.readStream.table(f"{catalog}.bronze.bronze_order_payments")
        .withColumn("payment_type", upper(trim(col("payment_type"))))
        .withColumn("payment_value", col("payment_value").cast("decimal(10,2)"))
        .dropDuplicates(["order_id", "payment_sequential"])
        .withColumn("_silver_processed_at", current_timestamp())
    )


# =====================================================================================
# 4. CUSTOMERS (Clients)
# =====================================================================================
@dlt.table(name="silver_customers", table_properties={"quality": "silver"})
@dlt.expect_or_fail("valid_customer", "customer_id IS NOT NULL")
def silver_customers():
    return (
        spark.readStream.table(f"{catalog}.bronze.bronze_customers")
        .withColumn("customer_city", upper(trim(col("customer_city"))))
        .withColumn("customer_state", upper(trim(col("customer_state"))))
        .dropDuplicates(["customer_id"])
        .withColumn("_silver_processed_at", current_timestamp())
    )


# =====================================================================================
# 5. PRODUCTS (Produits)
# =====================================================================================
@dlt.table(name="silver_products", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_product", "product_id IS NOT NULL")
def silver_products():
    return (
        spark.readStream.table(f"{catalog}.bronze.bronze_products")
        .dropDuplicates(["product_id"])
        .withColumn("_silver_processed_at", current_timestamp())
    )


# =====================================================================================
# 6. SELLERS (Vendeurs)
# =====================================================================================
@dlt.table(name="silver_sellers", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_seller", "seller_id IS NOT NULL")
def silver_sellers():
    return (
        spark.readStream.table(f"{catalog}.bronze.bronze_sellers")
        .withColumn("seller_city", upper(trim(col("seller_city"))))
        .withColumn("seller_state", upper(trim(col("seller_state"))))
        .dropDuplicates(["seller_id"])
        .withColumn("_silver_processed_at", current_timestamp())
    )


# =====================================================================================
# 7. CATEGORY TRANSLATION (Traductions des catégories)
# =====================================================================================
@dlt.table(name="silver_category_translation", table_properties={"quality": "silver"})
def silver_category_translation():
    return (
        spark.readStream.table(f"{catalog}.bronze.bronze_category_translation")
        .dropDuplicates(["product_category_name"])
        .withColumn("_silver_processed_at", current_timestamp())
    )


# =====================================================================================
# 8. GEOLOCATION (Géographie)
# =====================================================================================
@dlt.table(name="silver_geolocation", table_properties={"quality": "silver"})
def silver_geolocation():
    return (
        spark.readStream.table(f"{catalog}.bronze.bronze_geolocation")
        .withColumn("geolocation_city", upper(trim(col("geolocation_city"))))
        .withColumn("geolocation_state", upper(trim(col("geolocation_state"))))
        # Olist contient beaucoup de codes postaux en double, on nettoie tout ça :
        .dropDuplicates(["geolocation_zip_code_prefix"])
        .withColumn("_silver_processed_at", current_timestamp())
    )
