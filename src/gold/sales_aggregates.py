# Databricks notebook source
from pyspark.sql.functions import col, count, to_date

# ==============================================================================
# COUCHE GOLD : Agrégations Métier
# ==============================================================================

# 1. On récupère le paramètre dynamique (dev/prod) envoyé par le Job
dbutils.widgets.text("catalog_name", "dev")  # 'dev' par défaut si lancé à la main
catalog = dbutils.widgets.get("catalog_name")

# 2. Lecture de la table Silver
df_silver = spark.table(f"{catalog}.silver.silver_orders")

# 3. Calculs Métier (Agrégations) : Nombre de commandes par jour et par statut
df_gold = (
    df_silver
    # On extrait juste la date (sans l'heure)
    .withColumn("order_day", to_date(col("order_purchase_timestamp")))
    # On regroupe par date et par statut
    .groupBy("order_day", "order_status")
    # On compte le nombre de commandes
    .agg(count("order_id").alias("total_orders"))
)

# 4. Sauvegarde dans le schéma Gold (Écrasement quotidien)
(
    df_gold.write.mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable(f"{catalog}.gold.daily_order_stats")
)

print(f"Table Gold mise à jour avec succès dans {catalog}.gold.daily_order_stats !")
