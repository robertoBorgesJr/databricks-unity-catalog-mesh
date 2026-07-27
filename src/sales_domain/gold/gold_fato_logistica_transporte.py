# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from utils.connections import get_spark_session
from utils.watermark_control import get_watermark, update_watermark
from utils.environment import get_environment

environment = get_environment()
spark = get_spark_session("SALES")

# Configuração de auditoria e tabelas
current_user = spark.sql("SELECT current_user()").collect()[0][0]

SILVER_FATURAMENTO_NOTA_CABECALHO = f"sales_{environment}.silver.faturamento_nota_cabecalho"
SILVER_FATURAMENTO_NOTA_TRANSPORTE = f"sales_{environment}.silver.faturamento_nota_transporte"
GOLD_FATO_LOGISTICA = f"sales_{environment}.gold.fato_logistica_transporte"

# --- [1. LEITURA DO WATERMARK (CARGA INCREMENTAL)] ---
NOME_PIPELINE = GOLD_FATO_LOGISTICA
watermark = get_watermark(spark=spark, nome_pipeline=NOME_PIPELINE)

# --- [2. LEITURA DOS DADOS DA CAMADA SILVER] ---
df_transporte_sil = spark.read.table(SILVER_FATURAMENTO_NOTA_TRANSPORTE)
df_cabecalho_sil = spark.read.table(SILVER_FATURAMENTO_NOTA_CABECALHO)

# Snapshot das chaves ativas na origem (Chave de Acesso)
df_chaves_silver = df_transporte_sil.select("chave_acesso")

# Filtro incremental por Watermark temporal
if watermark:
    df_transporte_sil = df_transporte_sil.filter(F.col("dh_processamento_silver") > watermark)

# --- [3. MODELAGEM DIMENSIONAL & SURROGATE KEYS] ---
df_logistica_incremental = (
    df_transporte_sil.alias("trans")
    .join(df_cabecalho_sil.alias("cab"), "chave_acesso", "inner")
    .select(
        # Surrogate Keys
        F.md5(F.coalesce(F.col("cab.cliente_id"), F.lit("-1"))).alias("sk_cliente"),
        F.md5(F.coalesce(F.col("trans.transportadora_id"), F.lit("-1"))).alias("sk_transportadora"),
        
        # Chaves Naturais / Rastreabilidade
        F.col("trans.chave_acesso"),
        F.col("cab.numero_nota").cast("long"),
        F.col("cab.data_emissao").cast("date"),
        
        # Dados Logísticos
        F.coalesce(F.col("trans.modalidade_frete"), F.lit("9 - SEM FRETE")).alias("modalidade_frete"),
        F.col("trans.placa_veiculo"),
        F.col("trans.uf_veiculo"),
        F.col("trans.peso_liquido").cast("decimal(18,4)"),
        F.col("trans.peso_bruto").cast("decimal(18,4)"),
        F.col("trans.quantidade_volumes").cast("integer"),
        F.col("trans.especie_volumes"),
        
        # Flags de Controle e Auditoria
        F.lit(False).alias("fl_excluido"),
        F.lit(None).cast("timestamp").alias("dh_exclusao"),
        F.current_timestamp().alias("dh_processamento_gold"),
        F.lit(current_user).alias("usuario_executor")
    )
)

# --- [4. ESCRITA DOS DADOS (MERGE OU INICIAL)] ---
if spark.catalog.tableExists(GOLD_FATO_LOGISTICA):
    delta_target = DeltaTable.forName(spark, GOLD_FATO_LOGISTICA)
    
    # 4.1 Upsert dos dados modificados
    (delta_target.alias("gold")
     .merge(
         source=df_logistica_incremental.alias("source"),
         condition="gold.chave_acesso = source.chave_acesso"
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    
    # --- [5. SOFT DELETE - TRATAMENTO DE EXCLUSÕES] ---
    df_excluidos = (
        delta_target.toDF()
        .filter(F.col("fl_excluido") == False)
        .select("chave_acesso")
        .join(df_chaves_silver, on="chave_acesso", how="left_anti")
    )
    
    count_excluidos = df_excluidos.count()
    if count_excluidos > 0:
        (delta_target.alias("gold")
         .merge(
             source=df_excluidos.alias("excluidos"),
             condition="gold.chave_acesso = excluidos.chave_acesso"
         )
         .whenMatchedUpdate(set={
             "fl_excluido": F.lit(True),
             "dh_exclusao": F.current_timestamp()
         })
         .execute())
        print(f"Soft delete aplicado: {count_excluidos} notas de transporte marcadas como excluídas.")
    else:
        print("Nenhuma exclusão detectada em Logística.")

else:
    # Escrita inicial completa utilizando Liquid Clustering
    (df_logistica_incremental.write
     .format("delta")
     .mode("overwrite")
     .clusterBy("data_emissao", "sk_transportadora", "sk_cliente")
     .saveAsTable(GOLD_FATO_LOGISTICA))
    print("Carga inicial de logística executada com sucesso.")

# --- [6. ATUALIZAÇÃO DO WATERMARK] ---
novo_watermark = df_logistica_incremental.select(F.max("dh_processamento_gold")).collect()[0][0]
if novo_watermark:
    update_watermark(
        spark=spark,
        nome_pipeline=NOME_PIPELINE,
        novo_watermark=novo_watermark,
        usuario_executor=current_user,
        qtd_registros_processados=df_logistica_incremental.count(),
    )