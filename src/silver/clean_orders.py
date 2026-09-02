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

# ==============================================================================
# COUCHE SILVER : Commandes (Orders)
# Objectif : Nettoyage, typage strict, et validation des données brutes.
# ==============================================================================

# Règle métier : Liste officielle des statuts de commande acceptés
VALID_STATUSES = [
    "DELIVERED",
    "SHIPPED",
    "PROCESSING",
    "CANCELED",
    "INVOICED",
    "UNAVAILABLE",
]


@dlt.table(
    name="silver_orders",
    comment="Table des commandes nettoyées, dédoublonnées et typées. Prête pour l'analyse.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",  # Active le compactage automatique des fichiers
        "pipelines.autoOptimize.zOrderCols": "order_date",  # Optimise la lecture par date
    },
)
# 1. CONTRÔLE QUALITÉ INTRAITABLE (Data Quality)
# Clés primaires : Tolérance zéro (Fait planter le pipeline si manquant)
@dlt.expect_or_fail("valid_order_id", "order_id IS NOT NULL AND order_id != ''")

# Clés secondaires et règles métier : Rejet de la ligne corrompue
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "total_amount >= 0")
@dlt.expect(
    "valid_status", f"status IN ({','.join([repr(s) for s in VALID_STATUSES])})"
)
def silver_orders_clean():
    """
    Ingère la couche Bronze, applique les règles métiers, standardise les textes et les types.
    """

    # Lecture du flux Bronze
    df = dlt.read("bronze_orders")

    # 2. TRANSFORMATIONS ET TYPAGE
    df_cleaned = (
        df
        # A. Nettoyage des chaînes de caractères (Suppression des espaces cachés)
        .withColumn("order_id", trim(col("order_id")))
        .withColumn("customer_id", trim(col("customer_id")))
        .withColumn("status", upper(trim(col("status"))))
        # B. Typage strict
        .withColumn("order_date", to_timestamp(col("order_date")))
        # C. Typage sécurisé avec valeur par défaut (remplace NULL par 0.00)
        .withColumn(
            "total_amount",
            coalesce(col("total_amount"), lit(0.00)).cast("decimal(10,2)"),
        )
        # D. Dédoublonnage absolu sur la clé métier
        .dropDuplicates(["order_id"])
        # E. Traçabilité Silver (savoir exactement quand cette ligne a été nettoyée)
        .withColumn("_silver_processed_at", current_timestamp())
    )

    # 3. RETOUR DU DATAFRAME PROPRE
    return df_cleaned
