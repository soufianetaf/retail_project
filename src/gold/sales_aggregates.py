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
    # Lecture classique (sans readStream) = Création d'une Materialized View
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
    df_translation = spark.table(f"{catalog}.silver.silver_category_translation")
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
    df_orders = spark.table(f"{catalog}.silver.silver_orders")
    df_items = spark.table(f"{catalog}.silver.silver_order_items")
    df_payments = spark.table(f"{catalog}.silver.silver_order_payments")

    # Agrégation des paiements par commande
    df_payments_agg = df_payments.groupBy("order_id").agg(
        _sum("payment_value").alias("total_paid")
    )

    # Jointure finale pour la table des faits
    return df_items.join(df_orders, on="order_id", how="inner").join(
        df_payments_agg, on="order_id", how="left"
    )
