from pyspark.sql import functions as F
from utils.connections import get_spark_session
from utils.watermark_control import get_watermark, update_watermark
from utils.environment import get_environment

environment = get_environment()
spark = get_spark_session("SALES")

# =====================================================================
# 1. CONSTRUÇÃO DA DIMENSÃO CLIENTES (sales_{environment}.gold.dim_clientes)
# =====================================================================

# Configuração de auditoria
GOLD_DIM_CLIENTES = f"sales_{environment}.gold.dim_clientes"
SILVER_TABLE_SOURCE = f"sales_{environment}.silver.faturamento_nota_cabecalho"
PIPELINE_NAME = GOLD_DIM_CLIENTES

current_user = spark.sql("SELECT current_user()").collect()[0][0]

# =====================================================================
# 2. LEITURA DO WATERMARK (CARGA INCREMENTAL)
# =====================================================================
watermark = get_watermark(spark=spark, nome_pipeline=PIPELINE_NAME)

# =====================================================================
# 3. LEITURA DOS DADOS DA CAMADA SILVER
# =====================================================================
df_cabecalho_silver = (
    spark.read.table(SILVER_TABLE_SOURCE)
    .filter(F.col("dh_processamento_silver") > watermark) 
)

if df_cabecalho_silver.isEmpty():
    print("Nenhum registro novo ou atualizado desde o último processamento. Dimensão de Clientes não será atualizada.")
else:
    # --- [3. TRANSFORMAÇÃO E MODELAGEM DA DIMENSÃO CLIENTES] ---
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

    # Captura o novo watermark a partir do maior timestamp do lote processado
    max_silver_timestamp = df_dim_clientes.select(F.max("dh_processamento_silver")).collect()[0][0]
    total_records = df_dim_clientes.count()    

    # =====================================================================
    # 4. GRAVAÇÃO IDEMPOTENTE NA GOLD (Delta merge)
    # =====================================================================
    if not spark.catalog.tableExists(GOLD_DIM_CLIENTES):
        (
            df_dim_clientes.drop("dh_processamento_silver")
            .write.format("delta")
            .mode("overwrite")
            .clusterBy("sk_cliente", "uf_cliente")
            .saveAsTable(GOLD_DIM_CLIENTES)
        )
    else:
        delta_target = DeltaTable.forName(spark, GOLD_DIM_CLIENTES)
        (
            delta_target.alias("target")
            .merge(
                source=df_dim_clientes.drop("dh_processamento_silver").alias("source"),
                condition="target.cliente_id = source.cliente_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    # =====================================================================
    # 5. ATUALIZAÇÃO DO WATERMARK
    # =====================================================================
    if max_silver_timestamp:
        update_watermark(
            spark=spark,
            nome_pipeline=PIPELINE_NAME,
            novo_watermark=max_silver_timestamp,
            usuario_executor=current_user,
            qtd_registros_processados=total_records
        )
        
print("Dimensão de Clientes gerada e otimizada com sucesso na Gold!")
