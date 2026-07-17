# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.sql.types import *

current_user = spark.sql("SELECT current_user()").collect()[0][0]

# Configurações de Tabelas
BRONZE_INVESTIMENTO = "marketing_prod.bronze.investimento_marketing"

# ==========================================
# 1. SETUP DE CONFIGURAÇÕES E PREMISSAS MOCK
# ==========================================
CANAIS = ["Google Ads", "Meta Ads", "TikTok Ads", "LinkedIn Ads"]
CAMPANHAS = {
    "Google Ads": ["Search_Institucional", "Performance_Max_Produtos", "Remarketing_Carrinho"],
    "Meta Ads": ["Lookalike_Compradores", "Instagram_Stories_Promo", "Carrossel_Lancamento"],
    "TikTok Ads": ["Video_Viral_Trend", "Spark_Ads_Influencers"],
    "LinkedIn Ads": ["ABM_Diretoria_B2B"]
}

campanhas_flat = []
campanha_id = 100
for canal, lista_camps in CAMPANHAS.items():
    for camp in lista_camps:
        campanhas_flat.append((campanha_id, camp, canal))
        campanha_id += 1

schema_campanhas = StructType([
    StructField("id_campanha", IntegerType(), False),
    StructField("nome_campanha", StringType(), False),
    StructField("canal_midia", StringType(), False)
])

df_base_campanhas = spark.createDataFrame(campanhas_flat, schema_campanhas)

# Simulação de Investimento Diário (Últimos 30 dias)
df_investimento_raw = df_base_campanhas.withColumn(
    "data_investimento", F.expr("explode(sequence(date_sub(current_date(), 30), current_date()))")
).withColumn(
    "id_investimento", F.expr("uuid()")
).withColumn(
    "impressoes", (F.rand() * 10000 + 500).cast(IntegerType())
).withColumn(
    "cliques", (F.col("impressoes") * (F.rand() * 0.05 + 0.01)).cast(IntegerType())
).withColumn(
    "custo_total", F.round(F.col("cliques") * (F.rand() * 2.5 + 0.5), 2).cast(DoubleType())
)

# Adicionando metadados de auditoria da Bronze
df_investimento_bronze = df_investimento_raw \
    .withColumn("dh_insercao_bronze", F.current_timestamp()) \
    .withColumn("nome_arquivo_origem", F.lit("MOCK_DATA_MEMORY")) \
    .withColumn("usuario_executor", F.lit(current_user))

# Gravando na Bronze (Append cego para histórico e volumetria)
df_investimento_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(BRONZE_INVESTIMENTO)

print(f"Dados gravados com sucesso na tabela Bronze: {BRONZE_INVESTIMENTO}")
