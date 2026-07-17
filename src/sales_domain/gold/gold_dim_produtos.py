# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F

# =====================================================================
# 2. CONSTRUÇÃO DA DIMENSÃO PRODUTOS (sales_prod.gold.dim_produtos)
# =====================================================================

# Configuração de auditoria
current_user = spark.sql("SELECT current_user()").collect()[0][0]

# Leitura da tabela de itens da Silver (onde estão os dados do catálogo de produtos)
df_itens_silver = spark.read.table("sales_prod.silver.faturamento_nota_itens")

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

# Salva no Unity Catalog
(
    df_dim_produtos.write
    .format("delta")
    .mode("overwrite")
    .clusterBy("sk_produto", "categoria_produto")
    .option("mergeSchema", "true")
    .saveAsTable("sales_prod.gold.dim_produtos")
)

print("Dimensão de Produtos gerada e otimizada com sucesso na Gold!")
