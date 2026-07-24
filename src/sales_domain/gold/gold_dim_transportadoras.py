# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from utils.connections import get_spark_session
from utils.watermark_control import get_watermark, update_watermark

spark = get_spark_session("SALES")

# =====================================================================
# 1. CONFIGURAÇÃO E NOMENCLATURA DE TABELAS
# =====================================================================

GOLD_TABLE_DIM = "sales_prod.gold.dim_transportadoras"
SILVER_TABLE_SOURCE = "sales_prod.silver.faturamento_nota_transporte"
PIPELINE_NAME = GOLD_TABLE_DIM

# Configuração de auditoria
current_user = spark.sql("SELECT current_user()").collect()[0][0]

# =====================================================================
# 2. LEITURA DO WATERMARK (CARGA INCREMENTAL)
# =====================================================================
watermark = get_watermark(spark=spark, nome_pipeline=PIPELINE_NAME)

# =====================================================================
# 3. LEITURA DOS DADOS DA CAMADA SILVER
# =====================================================================
# Lemos os dados da camada Silver para consolidar o cadastro das transportadoras.
# Aplicamos um agrupamento (dropDuplicates/groupBy) para garantir um registro único por CNPJ.
df_silver_transporte = spark.read.table(SILVER_TABLE_SOURCE).filter(F.col("dh_processamento_silver") > watermark) 

max_silver_timestamp = df_silver_transporte.select(F.max("dh_processamento_silver")).collect()[0][0]

# ====================================================================
# 4. FILTRAGEM DE REGISTROS CONSISTENTES (CNPJ NÃO NULO/VÁZIO)
# ====================================================================
df_transportadoras_raw = (
    df_silver_transporte
    .filter(F.col("transportadora_cnpj").isNotNull() & (F.trim(F.col("transportadora_cnpj")) != ""))
    .select(
        F.col("transportadora_cnpj"),
        F.col("transportadora_id").alias("transportadora_nome"),
        F.col("dh_insercao_bronze")        # Usado para obter o cadastro mais recente
    )
)

# =====================================================================
# 5. DEDUPLICAÇÃO DOS DADOS
# =====================================================================
# Deduplicação para pegar apenas a última versão cadastrada de cada transportadora
# Evita que atualizações de nome ou endereço gerem chaves duplicadas
from pyspark.sql.window import Window

win_dedup = Window.partitionBy("transportadora_cnpj").orderBy(F.col("dh_insercao_bronze").desc())

df_transportadoras_dedup = (
    df_transportadoras_raw
    .withColumn("rn", F.row_number().over(win_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn", "dh_insercao_bronze")
)

# =====================================================================
# 6. GERAÇÃO DA SURROGATE KEY E HIGIENIZAÇÃO
# =====================================================================
df_dim_transportadoras = (
    df_transportadoras_dedup
    # Surrogate Key determinística baseada no CNPJ (chave natural)
    .withColumn("sk_transportadora", F.md5(F.trim(F.col("transportadora_cnpj"))))
    .select(
        "sk_transportadora",
        F.trim(F.col("transportadora_cnpj")).alias("transportadora_cnpj"),
        F.coalesce(F.upper(F.trim(F.col("transportadora_nome"))), F.lit("NÃO INFORMADO")).alias("transportadora_nome"),
        # Meta-colunas de auditoria corporativa
        F.current_timestamp().alias("dh_processamento_gold"),
        F.lit(current_user).alias("usuario_processamento")
    )
)

# =====================================================================
# 7. ESCRITA NA CAMADA GOLD COM MERGE E LIQUID CLUSTERING
# =====================================================================
records_processed = 0

if max_silver_timestamp is not None and df_dim_transportadoras.count() > 0:
    records_processed = df_dim_transportadoras.count()

    # Se a tabela não existir, realiza o setup inicial com Liquid Clustering (CLUSTER BY)
    if not spark.catalog.tableExists(GOLD_TABLE_DIM):
        (df_dim_transportadoras.write
        .format("delta")
        .mode("overwrite")
        # Liquid Clustering substitui o antigo Z-Order e organiza fisicamente pela Surrogate Key e CNPJ
        .clusterBy("sk_transportadora", "transportadora_cnpj") 
        .saveAsTable(GOLD_TABLE_DIM))
        print(f"Tabela {GOLD_TABLE_DIM} criada com sucesso e carga inicial realizada.")
    else:
        # Se a tabela já existir, fazemos o MERGE (Upsert) idempotente para evitar duplicações
        delta_target_dim = DeltaTable.forName(spark, GOLD_TABLE_DIM)
        
        (delta_target_dim.alias("target")
        .merge(
            source = df_dim_transportadoras.alias("source"),
            condition = "target.sk_transportadora = source.sk_transportadora"
        )
        .whenMatchedUpdateAll() # Atualiza se houver mudança cadastral (ex: endereço ou nome atualizados)
        .whenNotMatchedInsertAll() # Insere novas transportadoras mapeadas
        .execute())
        print(f"MERGE incremental executado com sucesso na tabela {GOLD_TABLE_DIM}.")

# =====================================================================
# 8. ATUALIZAÇÃO DO WATERMARK
# ====================================================================
if max_silver_timestamp:
    update_watermark(
        spark=spark,
        nome_pipeline=PIPELINE_NAME,
        novo_watermark=max_silver_timestamp,
        usuario_executor=current_user,
        qtd_registros_processados=records_processed
    )

print("Dimensão de Transportadoras gerada e otimizada com sucesso na Gold!")     
