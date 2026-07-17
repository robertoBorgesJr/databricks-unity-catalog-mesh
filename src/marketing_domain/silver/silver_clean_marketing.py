# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_INVESTIMENTO = "marketing_prod.bronze.investimento_marketing"
SILVER_INVESTIMENTO = "marketing_prod.silver.investimento_marketing"

# Ler da Bronze aplicando deduplicação inteligente (ex: mantendo o registro mais recente)
df_bronze = spark.read.table(BRONZE_INVESTIMENTO)

# Lógica de deduplicação por chave natural e data usando Window
window_spec = Window.partitionBy("id_campanha", "data_investimento").orderBy(F.col("dh_insercao_bronze").desc())
df_silver_clean = df_bronze.withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .drop("row_num") \
    .withColumn("canal_midia", F.upper(F.col("canal_midia"))) \
    .withColumn("dh_processamento_silver", F.current_timestamp())

# Gravação na Silver utilizando Liquid Clustering e MERGE para resiliência
if not spark.catalog.tableExists(SILVER_INVESTIMENTO):
    (df_silver_clean.write
     .format("delta")
     .mode("overwrite")
     .clusterBy("data_investimento", "id_campanha")
     .saveAsTable(SILVER_INVESTIMENTO))
else:
    delta_target = DeltaTable.forName(spark, SILVER_INVESTIMENTO)
    delta_target.alias("target").merge(
        source = df_silver_clean.alias("source"),
        condition = "target.id_campanha = source.id_campanha AND target.data_investimento = source.data_investimento"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

print(f"Camada Silver atualizada e limpa em: {SILVER_INVESTIMENTO}")
