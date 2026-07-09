# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

current_user = spark.sql("SELECT current_user()").collect()[0][0]

# Leitura dos dados da camada Bronze
df_cabecalho_bz = spark.read.table("sales_prod.bronze.faturamento_nota_cabecalho")
df_itens_bz = spark.read.table("sales_prod.bronze.faturamento_nota_itens")
df_dim_cfop = spark.read.table("sales_prod.gold.dim_cfop").select("cfop_codigo") # Carrega os CFOPs válidos

# Deduplicação da origem (Pega o estado mais recente baseado no timestamp da Bronze)
win_cabecalho = Window.partitionBy("chave_acesso").orderBy(F.col("dh_insercao_bronze").desc())
win_itens = Window.partitionBy("chave_acesso", "numero_item").orderBy(F.col("dh_insercao_bronze").desc())

df_cabecalho_dedup = df_cabecalho_bz.withColumn("_row_num", F.row_number().over(win_cabecalho)).filter("_row_num = 1").drop("_row_num")
df_itens_dedup = df_itens_bz.withColumn("_row_num", F.row_number().over(win_itens)).filter("_row_num = 1").drop("_row_num")                     

# Cabeçalho: Casting e padronizações iniciais
df_cabecalho_transformado = (df_cabecalho_dedup
    .select(
        F.col("chave_acesso").cast("string"),
        F.col("numero_nota").cast("long"),
        F.col("data_emissao").cast("date"),
        F.col("cliente_id").cast("string"),
        F.col("cliente_nome").cast("string"),
        F.upper(F.col("cliente_tipo")).alias("cliente_tipo"),
        F.col("cliente_documento").cast("string"),
        F.upper(F.col("uf_cliente")).alias("uf_cliente"),
        F.col("cfop").cast("integer"),
        F.col("dh_insercao_bronze")
    )
    .withColumn("dh_processamento_silver", F.current_timestamp())
    .withColumn("usuario_executor", F.lit(current_user))
)

# Itens: Aplicação dos cálculos e casting de negócio
df_itens_transformado = (df_itens_dedup
    .withColumn("valor_unitario", F.col("valor_unitario").cast("decimal(18,2)"))
    .withColumn("quantidade", F.col("quantidade").cast("integer"))
    .withColumn("valor_desconto", F.col("valor_desconto").cast("decimal(18,2)"))
    .withColumn("valor_frete", F.col("valor_frete").cast("decimal(18,2)"))
    # Cálculo do valor total do item
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

# --- [VALIDAÇÃO DO CFOP VIA LOOKUP JOIN] ---
# Fazemos um Left Join com a tabela de controle. Se o cfop não existir lá, 'cfop_valido' virá nulo.
df_cabecalho_validado = df_cabecalho_transformado.join(
    df_dim_cfop.withColumn("cfop_valido", F.lit(True)),
    df_cabecalho_transformado["cfop"] == df_dim_cfop["cfop_codigo"],
    "left"
).drop("cfop_codigo")

# --- [REGRAS DE FILTRO PARA QUARENTENA] ---
VALID_UFS = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]

# Condição de Erro do Cabeçalho: se QUALQUER um destes for verdadeiro, vai para a quarentena
condicao_erro_cabecalho = (
    F.col("chave_acesso").isNull() |
    F.col("numero_nota").isNull() |
    F.col("cliente_id").isNull() |
    F.col("cliente_nome").isNull() |
    F.col("cliente_tipo").isNull() |
    F.col("cliente_documento").isNull() |
    F.col("data_emissao").isNull() |
    F.col("cfop").isNull() |
    F.col("cfop_valido").isNull() |   # não deu match na dim_cfop
    (~F.col("uf_cliente").isin(VALID_UFS))
)

# Divisão Cabeçalho
df_cabecalho_quarentena = df_cabecalho_transformado.filter(condicao_erro_cabecalho)
df_cabecalho_silver = df_cabecalho_transformado.filter(~condicao_erro_cabecalho)

# Condição de Erro dos Itens: se QUALQUER um destes for verdadeiro, vai para a quarentena
condicao_erro_itens = (
    F.col("chave_acesso").isNull() |
    F.col("numero_item").isNull() |
    (F.col("quantidade") <= 0) |
    (F.col("valor_unitario") <= 0) |
    (F.col("valor_total_item") <= 0)
)

# Divisão Itens
df_itens_quarentena = df_itens_transformado.filter(condicao_erro_itens)
df_itens_silver = df_itens_transformado.filter(~condicao_erro_itens)

# --- [VERIFICAÇÃO / LOG DE VOLUMETRIA DE MIGRADO VS QUARENTENA] ---
total_cab_ruim = df_cabecalho_quarentena.count()
total_cab_bom = df_cabecalho_silver.count()
print(f"[Auditoria Cabeçalho] Registros Válidos (Silver): {total_cab_bom} | Registros Inválidos (Quarentena): {total_cab_ruim}")

total_itens_ruim = df_itens_quarentena.count()
total_itens_bom = df_itens_silver.count()
print(f"[Auditoria Itens] Registros Válidos (Silver): {total_itens_bom} | Registros Inválidos (Quarentena): {total_itens_ruim}")

# --- [ESCRITA DA QUARENTENA - APPEND (DADOS RUINS)] ---
if total_cab_ruim > 0:
    df_cabecalho_quarentena.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("sales_prod.bronze.quarentena_nota_cabecalho")
    print(f"-> {total_cab_ruim} cabeçalhos rejeitados movidos para a quarentena.")

if total_itens_ruim > 0:
    df_itens_quarentena.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("sales_prod.bronze.quarentena_nota_itens")
    print(f"-> {total_itens_ruim} itens rejeitados movidos para a quarentena.")


# --- [ESCRITA NA CAMADA SILVER - UPSERT/MERGE (DADOS BONS)] ---
SILVER_TABLE_CABECALHO = "sales_prod.silver.faturamento_nota_cabecalho"

if not spark.catalog.tableExists(SILVER_TABLE_CABECALHO):
    df_cabecalho_silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE_CABECALHO)
else:
    delta_target_cabecalho = DeltaTable.forName(spark, SILVER_TABLE_CABECALHO)
    delta_target_cabecalho.alias("target") \
        .merge(
            source = df_cabecalho_silver.alias("source"),
            condition = "target.chave_acesso = source.chave_acesso"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()        

SILVER_TABLE_ITENS = "sales_prod.silver.faturamento_nota_itens"

if not spark.catalog.tableExists(SILVER_TABLE_ITENS):
    df_itens_silver.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE_ITENS)
else:
    delta_target_itens = DeltaTable.forName(spark, SILVER_TABLE_ITENS)
    delta_target_itens.alias("target") \
        .merge(
            source = df_itens_silver.alias("source"),
            condition = "target.chave_acesso = source.chave_acesso AND target.numero_item = source.numero_item"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()  

# Otimização física das tabelas de destino (Z-ORDER)
spark.sql("OPTIMIZE sales_prod.silver.faturamento_nota_cabecalho ZORDER BY (chave_acesso, data_emissao)")
spark.sql("OPTIMIZE sales_prod.silver.faturamento_nota_itens ZORDER BY (chave_acesso, produto_id)")

print("Camada Silver finalizada com sucesso!")

