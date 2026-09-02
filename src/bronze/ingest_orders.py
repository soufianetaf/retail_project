# Databricks notebook source
import dlt
from pyspark.sql.functions import current_timestamp, col

# 1. On récupère le chemin dynamique
landing_path = spark.conf.get("landing_path")
orders_path = f"{landing_path}/orders/"


# 2. On déclare la table DLT
@dlt.table(
    name="bronze_orders",
    comment="Données brutes des commandes ingérées via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_orders_ingestion():
    return (
        # 3. Utilisation d'Auto Loader
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(orders_path)
        # 4. Traçabilité (Mise à jour pour Unity Catalog)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )
