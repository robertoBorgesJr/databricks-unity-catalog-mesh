# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from delta.tables import DeltaTable

# Configuração de auditoria
current_user = spark.sql("SELECT current_user()").collect()[0][0]
GOLD_TABLE = "sales_prod.gold.fato_faturamento"

# --- [1. LEITURA DO WATERMARK (CARGA INCREMENTAL)] ---
try:
    watermark = spark.sql(f"SELECT MAX(dh_processamento_gold) FROM {GOLD_TABLE}").collect()[0][0]
    print(f"Carga incremental — watermark: {watermark}")
except Exception:
    watermark = None
    print("Tabela ainda não existe — executando carga inicial completa.")

# --- [2. LEITURA DOS DADOS REFINADOS DA CAMADA SILVER] ---
df_cabecalho = spark.read.table("sales_prod.silver.faturamento_nota_cabecalho")
df_itens = spark.read.table("sales_prod.silver.faturamento_nota_itens")

# Snapshot completo das chaves Silver (usado na detecção de soft delete)
df_chaves_silver = df_itens.select("chave_acesso", "numero_item")

# Filtrar apenas registros novos/atualizados desde o último processamento
df_itens_incremental = df_itens
if watermark:
    df_itens_incremental = df_itens.filter(F.col("dh_processamento_silver") > watermark)

# --- [3. JOIN E CONVERSÃO PARA SURROGATE KEYS (SK)] ---
df_fato_incremental = (
    df_itens_incremental.join(df_cabecalho, on="chave_acesso", how="inner")
    .select(
        # Criação/Mapeamento das chaves do Star Schema
        F.md5(df_itens_incremental["produto_id"]).alias("sk_produto"),
        F.md5(df_cabecalho["cliente_id"]).alias("sk_cliente"),
        # SK de tempo baseada na data de emissão (Ex: 20260708)
        F.date_format(F.col("data_emissao"), "yyyyMMdd").cast(IntegerType()).alias("sk_tempo"),

        # Chaves Naturais Degeneradas (auditoria/rastreabilidade)
        df_itens_incremental["chave_acesso"],
        df_itens_incremental["numero_nota"],
        df_itens_incremental["numero_item"],

        # Atributos de Negócio / Métricas
        df_cabecalho["cfop"],
        F.col("valor_unitario").cast("decimal(18,2)"),
        F.col("quantidade").cast("integer"),
        F.col("valor_desconto").cast("decimal(18,2)"),
        F.col("valor_frete").cast("decimal(18,2)"),
        F.col("valor_total_item").cast("decimal(18,2)")
    )
    # --- [4. ADIÇÃO DE METADADOS DE AUDITORIA CORPORATIVA] ---
    .withColumn("fl_excluido", F.lit(False))
    .withColumn("dh_exclusao", F.lit(None).cast("timestamp"))
    .withColumn("dh_processamento_gold", F.current_timestamp())
    .withColumn("usuario_executor", F.lit(current_user))
)

if spark.catalog.tableExists(GOLD_TABLE):
    delta_fato = DeltaTable.forName(spark, GOLD_TABLE)

    # --- [5. MERGE INCREMENTAL — UPSERT DE REGISTROS NOVOS/ATUALIZADOS] ---
    if not df_fato_incremental.isEmpty():
        (
            delta_fato.alias("gold")
            .merge(
                df_fato_incremental.alias("novos"),
                "gold.chave_acesso = novos.chave_acesso AND gold.numero_item = novos.numero_item"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("MERGE incremental (upsert) executado com sucesso.")
    else:
        print("Nenhum registro novo ou atualizado encontrado.")

    # --- [6. SOFT DELETE — MARCAR ITENS EXCLUÍDOS DA SILVER] ---
    # Identifica registros ativos no Gold que não existem mais no snapshot Silver
    df_excluidos = (
        delta_fato.toDF()
        .filter(F.col("fl_excluido") == False)
        .select("chave_acesso", "numero_item")
        .join(df_chaves_silver, on=["chave_acesso", "numero_item"], how="left_anti")
    )

    count_excluidos = df_excluidos.count()
    if count_excluidos > 0:
        (
            delta_fato.alias("gold")
            .merge(
                df_excluidos.alias("excluidos"),
                "gold.chave_acesso = excluidos.chave_acesso AND gold.numero_item = excluidos.numero_item"
            )
            .whenMatchedUpdate(set={
                "fl_excluido": F.lit(True),
                "dh_exclusao": F.current_timestamp()
            })
            .execute()
        )
        print(f"Soft delete aplicado: {count_excluidos} item(ns) marcado(s) como excluído(s).")
    else:
        print("Nenhum item excluído detectado na Silver.")

else:
    # Primeira execução — carga inicial completa
    (
        df_fato_incremental.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .clusterby("sk_tempo", "sk_produto", "sk_cliente)
        .saveAsTable(GOLD_TABLE)
    )
    print("Carga inicial completa executada com sucesso.")

print("Tabela Fato (fato_faturamento) gerada, auditada e otimizada com sucesso na Gold!")
