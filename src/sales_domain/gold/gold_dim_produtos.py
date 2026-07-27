# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from delta import DeltaTable
from pyspark.sql import functions as F
from utils.connections import get_spark_session
from utils.watermark_control import get_watermark, update_watermark
from utils.environment import get_environment

environment = get_environment()
spark = get_spark_session("SALES")

# =====================================================================
# 1. CONFIGURAÇÃO E NOMENCLATURA DE TABELAS
# =====================================================================
SILVER_TABLE = f"sales_{environment}.silver.faturamento_nota_itens"
GOLD_DIM_PRODUTOS = f"sales_{environment}.gold.dim_produtos"
PIPELINE_NAME = GOLD_DIM_PRODUTOS

# Configuração de auditoria
current_user = spark.sql("SELECT current_user()").collect()[0][0]

# =====================================================================
# 2. LEITURA DO WATERMARK (CARGA INCREMENTAL)
# =====================================================================
watermark = get_watermark(spark=spark, nome_pipeline=PIPELINE_NAME)

# =====================================================================
# 3. LEITURA DOS DADOS DA CAMADA SILVER
# =====================================================================
df_itens_silver = spark.read.table(SILVER_TABLE)
if watermark:
    df_itens_silver = df_itens_silver.filter(F.col("dh_processamento_silver") > watermark)

max_silver_timestamp = df_itens_silver.select(F.max("dh_processamento_silver")).collect()[0][0]    

# =====================================================================
# 4. TRANSFORMAÇÃO E MODELAGEM DA DIMENSÃO DE PRODUTOS
# =====================================================================
df_dim_produtos = (
    df_itens_silver
    .select("produto_id", "produto_nome")
    .dropDuplicates(["produto_id"])
    # Criação da Surrogate Key (SK) para o Produto
    .withColumn("sk_produto", F.md5(F.col("produto_id")))
    # Engenharia de Atributo: Extrai a categoria que simulamos após o hífen "Produto X - Categoria"
    .withColumn("categoria_produto", F.coalesce(F.split(F.col("produto_nome"), " - ").getItem(1), F.lit("Sem Categoria")))
    # Limpa o nome do produto para tirar o sufixo da categoria
    .withColumn("nome_limpo_produto", F.split(F.col("produto_nome"), " - ").getItem(0))
    # Metadados de Auditoria da Gold
    .withColumn("dh_processamento_gold", F.current_timestamp())
    .withColumn("usuario_executor", F.lit(current_user))
    .select("sk_produto", "produto_id", "nome_limpo_produto", "categoria_produto", "dh_processamento_gold", "usuario_executor")
)

# =====================================================================
# 5. GRAVAÇÃO DA DIMENSÃO DE PRODUTOS NA GOLD
# =====================================================================
records_processed = 0

if max_silver_timestamp is not None and df_dim_produtos.count() > 0:
    records_processed = df_dim_produtos.count()

    # Cria a tabela de destino caso ainda não exista
    if not spark.catalog.tableExists(GOLD_DIM_PRODUTOS):
        (
            df_dim_produtos.write
            .format("delta")
            .mode("overwrite")
            .clusterBy("sk_produto", "categoria_produto") # Liquid Clustering para performance de Join
            .saveAsTable(GOLD_DIM_PRODUTOS)
        )
    else:
        # Operação de Upsert (MERGE) para atualização idempotente
        delta_target = DeltaTable.forName(spark, GOLD_DIM_PRODUTOS)
        (
            delta_target.alias("target")
            .merge(
                source=df_dim_produtos.alias("source"),
                condition="target.produto_id = source.produto_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

# =====================================================================
# 6. ATUALIZAÇÃO DO WATERMARK
# =====================================================================
if max_silver_timestamp:
    update_watermark(
        spark=spark, 
        nome_pipeline=PIPELINE_NAME, 
        novo_watermark=max_silver_timestamp,
        usuario_executor=current_user,
        qtd_registros_processados=records_processed
        )

print("Dimensão de Produtos gerada e otimizada com sucesso na Gold!")
