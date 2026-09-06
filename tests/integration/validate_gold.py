# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as _sum

# 1. Configuration (On récupère le catalogue : dev, staging ou prod)
dbutils.widgets.text("catalog_name", "dev")
catalog = dbutils.widgets.get("catalog_name")

print(f"Début des tests d'intégration sur le catalogue : {catalog}")

# 2. Chargement de la table des faits
df_sales = spark.table(f"{catalog}.gold.fact_sales")

# ==========================================================
# TEST 1 : Vérifier que la table n'est pas vide
# ==========================================================
sales_count = df_sales.count()
assert sales_count > 0, "ÉCHEC CRITIQUE : La table fact_sales est vide !"
print(f" Test 1 passé : fact_sales contient {sales_count} lignes.")

# ==========================================================
# TEST 2 : Vérifier l'intégrité (Pas de clés nulles)
# ==========================================================
null_orders = df_sales.filter(col("order_id").isNull()).count()
assert null_orders == 0, f"ÉCHEC : Il y a {null_orders} commandes sans order_id !"
print(" Test 2 passé : Aucun order_id nul trouvé.")

# ==========================================================
# TEST 3 : Validation Métier (Le Chiffre d'Affaires)
# ==========================================================
# On calcule la somme totale payée pour vérifier que les chiffres sont logiques
ca_total = df_sales.select(_sum("total_paid")).collect()[0][0]

# Le CA doit être supérieur à zéro s'il y a des ventes
assert (
    ca_total is not None and ca_total > 0
), "ÉCHEC : Le chiffre d'affaires total est inférieur ou égal à zéro !"
print(f" Test 3 passé : Le chiffre d'affaires total est valide ({ca_total}).")

print(" TOUS LES TESTS SONT AU VERT ! LES DONNÉES SONT PARFAITES !")
