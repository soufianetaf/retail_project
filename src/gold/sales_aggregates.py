# Databricks notebook source
import dlt
from pyspark.sql.functions import sum as _sum

catalog = spark.conf.get("catalog_name")


# =====================================================================================
# 1. DIMENSION : CUSTOMERS
# =====================================================================================
@dlt.table(
    name="dim_customers",
    comment="Dimension Clients (Materialized View)",
    table_properties={"quality": "gold"},
)
def dim_customers():
    return spark.table(f"{catalog}.silver.silver_customers")


# =====================================================================================
# 2. DIMENSION : PRODUCTS
# =====================================================================================
@dlt.table(
    name="dim_products",
    comment="Dimension Produits avec traductions (Materialized View)",
    table_properties={"quality": "gold"},
)
def dim_products():
    df_products = spark.table(f"{catalog}.silver.silver_products")

    # CORRECTION : On ne prend QUE les deux colonnes utiles de la traduction
    df_translation = spark.table(
        f"{catalog}.silver.silver_category_translation"
    ).select("product_category_name", "product_category_name_english")

    return df_products.join(df_translation, on="product_category_name", how="left")


# =====================================================================================
# 3. FACT : SALES
# =====================================================================================
@dlt.table(
    name="fact_sales",
    comment="Table des faits des Ventes (Materialized View)",
    table_properties={"quality": "gold"},
)
def fact_sales():
    df_items = spark.table(f"{catalog}.silver.silver_order_items")

    # CORRECTION : On ne prend que les informations métier de la commande (pour éviter de dupliquer les colonnes techniques)
    df_orders = spark.table(f"{catalog}.silver.silver_orders").select(
        "order_id", "customer_id", "order_status", "order_purchase_timestamp"
    )

    df_payments = spark.table(f"{catalog}.silver.silver_order_payments")
    df_payments_agg = df_payments.groupBy("order_id").agg(
        _sum("payment_value").alias("total_paid")
    )

    return df_items.join(df_orders, on="order_id", how="inner").join(
        df_payments_agg, on="order_id", how="left"
    )
