# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F

current_user = spark.sql("SELECT current_user()").collect()[0][0]

# Leitura dos dados da camada Bronze
df_cabecalho_bz = spark.read.table("sales_prod.bronze.faturamento_nota_cabecalho")
df_itens_bz = spark.read.table("sales_prod.bronze.faturamento_nota_itens")

# Transformação e enriquecimento da Silver

# Cabeçalho: Garantindo casting correto de dados críticos
df_cabecalho_silver = (df_cabecalho_bz
    .select(
        F.col("chave_acesso").cast("string"),
        F.col("numero_nota").cast("long"),
        F.col("data_emissao").cast("date"),
        F.col("cliente_id").cast("string"),
        F.col("cliente_nome").cast("string"),
        F.upper(F.col("cliente_tipo")).alias("cliente_tipo"), # Padronização de strings
        F.col("cliente_documento").cast("string"),
        F.upper(F.col("uf_cliente")).alias("uf_cliente"),
        F.col("cfop").cast("integer"),
        F.col("dh_insercao_bronze") # Preserva linhagem (lineage)
    )
    .withColumn("dh_processamento_silver", F.current_timestamp())
    .withColumn("usuario_executor", F.lit(current_user))
)

# Itens: Aplicação da regra de negócio líquida para Vendas e Marketing
df_itens_silver = (df_itens_bz
    .withColumn("valor_unitario", F.col("valor_unitario").cast("decimal(18,2)"))
    .withColumn("quantidade", F.col("quantidade").cast("integer"))
    .withColumn("valor_desconto", F.col("valor_desconto").cast("decimal(18,2)"))
    .withColumn("valor_frete", F.col("valor_frete").cast("decimal(18,2)"))
    # Cálculo da linha: (Unitario * Qtd) - Desconto + Frete
    .withColumn(
        "valor_total_item",
        F.round((F.col("valor_unitario") * F.col("quantidade")) - F.col("valor_desconto") + F.col("valor_frete"), 2).cast("decimal(18,2)")
    )
    .select(
        "chave_acesso", "numero_nota", "numero_item", "produto_id", "produto_nome",
        "valor_unitario", "quantidade", "valor_desconto", "valor_frete", "valor_total_item",
        "dh_insercao_bronze"
    )
    .withColumn("dh_processamento_silver", F.current_timestamp())
    .withColumn("usuario_executor", F.lit(current_user))
)

# Persistencia na camada Silver
(df_cabecalho_silver.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("sales_prod.silver.faturamento_nota_cabecalho")
)

(df_itens_silver.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("sales_prod.silver.faturamento_nota_itens")
)

# Otimização (Z-ORDER)]
# Otimizando a Silver pelas chaves de junção frequentes para acelerar a criação da Gold
spark.sql("OPTIMIZE sales_prod.silver.faturamento_nota_cabecalho ZORDER BY (chave_acesso, data_emissao)")
spark.sql("OPTIMIZE sales_prod.silver.faturamento_nota_itens ZORDER BY (chave_acesso, produto_id)")

print("Camada Silver processada, auditada e otimizada com sucesso!")
