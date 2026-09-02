# Databricks notebook source
import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

# 1. On récupère le chemin dynamique injecté par notre databricks.yml
landing_path = spark.conf.get("landing_path")
orders_path = f"{landing_path}/orders/"


# 2. On déclare une table DLT
@dlt.table(
    name="bronze_orders",
    comment="Données brutes des commandes ingérées via Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze_orders_ingestion():
    return (
        # 3. Utilisation d'Auto Loader (format "cloudFiles")
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option(
            "cloudFiles.inferColumnTypes", "true"
        )  # Devine si c'est du texte, chiffre, etc.
        .option("header", "true")
        .load(orders_path)
        # 4. Bonnes pratiques pro : on trace l'origine de la donnée
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )
