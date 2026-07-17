# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F

# =====================================================================
# 1. CONSTRUÇÃO DA DIMENSÃO CLIENTES (sales_prod.gold.dim_clientes)
# =====================================================================

# Configuração de auditoria
current_user = spark.sql("SELECT current_user()").collect()[0][0]
GOLD_TABLE_DIM = "sales_prod.gold.dim_clientes"
SILVER_TABLE_SOURCE = "sales_prod.silver.faturamento_nota_cabecalho"

# Leitura da tabela de cabeçalho da Silver (onde estão os dados cadastrais)
df_cabecalho_silver = spark.read.table(SILVER_TABLE_SOURCE)

df_dim_clientes = (
    df_cabecalho_silver
    # Seleciona apenas os campos demográficos do cliente
    .select("cliente_id", "cliente_nome", "cliente_tipo", "cliente_documento", "uf_cliente")
    # Remove as duplicidades para garantir 1 linha por cliente único
    .dropDuplicates(["cliente_id"])
    # Criação da Surrogate Key (SK) analítica estável
    .withColumn("sk_cliente", F.md5(F.col("cliente_id")))
    # Enriquecimento de Marketing: Classificação regional simplificada
    .withColumn("regiao_cliente",
        F.when(F.col("uf_cliente").isin("SP", "RJ", "MG", "ES"), "Sudeste")
         .when(F.col("uf_cliente").isin("PR", "SC", "RS"), "Sul")
         .when(F.col("uf_cliente").isin("BA", "SE", "AL", "PE", "PB", "RN", "CE", "PI", "MA"), "Nordeste")
         .when(F.col("uf_cliente").isin("AM", "PA", "AC", "RO", "RR", "AP", "TO"), "Norte")
         .when(F.col("uf_cliente").isin("MT", "MS", "GO", "DF"), "Centro-Oeste")
         .otherwise("Outros")
    )
    # Metadados de Auditoria da Gold
    .withColumn("dh_processamento_gold", F.current_timestamp())
    .withColumn("usuario_executor", F.lit(current_user))
    # Reorganização estética de colunas
    .select("sk_cliente", "cliente_id", "cliente_nome", "cliente_tipo", "cliente_documento", "uf_cliente", "regiao_cliente", "dh_processamento_gold", "usuario_executor")
)

# Salva no Unity Catalog
(
    df_dim_clientes.write
    .format("delta")
    .mode("overwrite")
    .clusterBy("sk_cliente", "uf_cliente")
    .option("mergeSchema", "true")
    .saveAsTable(GOLD_TABLE_DIM)
)

print("Dimensão de Clientes gerada e otimizada com sucesso na Gold!")
