# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run ../../../utils/watermark_control

# COMMAND ----------

# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

current_user = spark.sql("SELECT current_user()").collect()[0][0]

BRONZE_INVESTIMENTO = "marketing_prod.bronze.investimento_marketing"
SILVER_INVESTIMENTO = "marketing_prod.silver.investimento_marketing"
NOME_PIPELINE = "silver_clean_marketing"

watermark = get_watermark(spark=spark, nome_pipeline=NOME_PIPELINE)

# ==========================================================
# 1. LER APENAS O INCREMENTO DA BRONZE (dh_insercao_bronze > watermark)
# ==========================================================
df_incremento_bronze = (
    spark.read.table(BRONZE_INVESTIMENTO)
    .filter(F.col("dh_insercao_bronze") > F.lit(watermark))
)

if df_incremento_bronze.isEmpty():
    print(f"Nenhum dado novo na Bronze desde o watermark ({watermark}). Nada a processar.")
else:
    window_spec = Window.partitionBy("id_campanha", "data_investimento").orderBy(F.col("dh_insercao_bronze").desc())

    df_silver_clean = (
        df_incremento_bronze
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
        .withColumn("canal_midia", F.upper(F.col("canal_midia")))
        .withColumn("dh_processamento_silver", F.current_timestamp())
    )

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
            source=df_silver_clean.alias("source"),
            condition="target.id_campanha = source.id_campanha AND target.data_investimento = source.data_investimento"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()

    # ==========================================================
    # 3. ATUALIZAR O WATERMARK DE CONTROLE
    # ==========================================================
    novo_watermark = df_incremento_bronze.agg(F.max("dh_insercao_bronze")).collect()[0][0]

    update_watermark(
        spark=spark,
        tabela_controle=CTRL_WATERMARK_PIPELINES,
        nome_pipeline=NOME_PIPELINE,
        novo_watermark=novo_watermark,
        usuario_executor=current_user,
        qtd_registros_processados=df_incremento_bronze.count(),
    )

    print(f"Camada Silver atualizada e limpa em: {SILVER_INVESTIMENTO} (até {novo_watermark})")
