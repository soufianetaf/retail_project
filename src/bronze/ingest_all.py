# Databricks notebook source
import dlt
from pyspark.sql.functions import col, current_timestamp

# 1. Chemin de base
landing_path = spark.conf.get("landing_path")

# 2. Liste de tous les dossiers de ta landing_zone
TABLES_TO_INGEST = [
    "orders",
    "customers",
    "order_items",
    "order_payments",
    "products",
    "sellers",
    "geolocation",
    "category_translation",
]


# 3. Fonction génératrice de tables DLT
def generate_bronze_table(table_name):
    """
    Crée dynamiquement une table DLT Bronze pour un nom de dossier donné.
    """

    @dlt.table(
        name=f"bronze_{table_name}",
        comment=f"Données brutes de {table_name} ingérées via Auto Loader",
        table_properties={"quality": "bronze"},
    )
    def ingestion_function():
        folder_path = f"{landing_path}/{table_name}/"

        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            # Tolère les erreurs de format sur certains fichiers exotiques
            .option("cloudFiles.schemaEvolutionMode", "rescue")
            .load(folder_path)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
        )


# 4. La Boucle Magique
for table in TABLES_TO_INGEST:
    generate_bronze_table(table)
