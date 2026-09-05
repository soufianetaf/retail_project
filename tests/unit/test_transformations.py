from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper


def test_silver_status_cleaning():
    """
    Test unitaire pour vérifier que notre logique de nettoyage Silver
    met bien les statuts en majuscules et supprime les espaces.
    """
    # 1. Initialiser un mini-moteur Spark localement (uniquement pour le test)
    spark = SparkSession.builder.master("local[1]").appName("UnitTests").getOrCreate()

    # 2. Créer de fausses données corrompues (Mocks)
    fausses_donnees = [
        ("ORD-1", "  delivered  "),  # Espaces en trop et minuscules
        ("ORD-2", "shipped"),  # Minuscules
    ]
    df_test = spark.createDataFrame(fausses_donnees, ["order_id", "status"])

    # 3. Appliquer la logique exacte de notre couche Silver
    df_propre = df_test.withColumn("status", upper(trim(col("status"))))

    # 4. Récupérer les résultats
    resultats = [ligne["status"] for ligne in df_propre.collect()]

    # 5. Vérifier (Assert) que le nettoyage a parfaitement fonctionné !
    assert resultats[0] == "DELIVERED"
    assert resultats[1] == "SHIPPED"


def test_gold_payment_aggregation():
    """
    Vérifie que la couche Gold additionne correctement les paiements
    si une même commande a été payée en plusieurs fois.
    """
    spark = SparkSession.builder.master("local[1]").appName("UnitTests").getOrCreate()
    from pyspark.sql.functions import sum as _sum

    # 1. On crée des fausses données : La commande ORD-99 a été payée en 3 fois !
    donnees_paiements = [
        ("ORD-99", 50.0),
        ("ORD-99", 20.0),
        ("ORD-99", 30.0),
        ("ORD-42", 15.0),  # Une autre commande normale
    ]
    df_payments = spark.createDataFrame(
        donnees_paiements, ["order_id", "payment_value"]
    )

    # 2. On applique la logique exacte de notre fichier Gold
    df_agg = df_payments.groupBy("order_id").agg(
        _sum("payment_value").alias("total_paid")
    )

    # 3. On transforme le résultat en dictionnaire pour vérifier facilement
    resultats = {row["order_id"]: row["total_paid"] for row in df_agg.collect()}

    # 4. Le Test (Assert) : L'ordinateur doit trouver 100.0 (50+20+30) pour la commande 99
    assert resultats["ORD-99"] == 100.0
    assert resultats["ORD-42"] == 15.0
