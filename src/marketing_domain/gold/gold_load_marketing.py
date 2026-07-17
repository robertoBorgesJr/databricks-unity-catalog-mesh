# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F
from delta.tables import DeltaTable

current_user = spark.sql("SELECT current_user()").collect()[0][0]

SILVER_INVESTIMENTO = "marketing_prod.silver.investimento_marketing"
GOLD_DIM_CAMPANHA = "marketing_prod.gold.dim_campanha"
GOLD_FATO_INVESTIMENTO = "marketing_prod.gold.fato_investimento_marketing"
GOLD_FATO_ATRIBUICAO = "marketing_prod.gold.fato_atribuicao_conversao"

# 1. Carregar dados da Silver de Marketing e da Dimensão de Campanha (SCD2)
df_silver = spark.read.table(SILVER_INVESTIMENTO)
df_dim_campanha = spark.read.table(GOLD_DIM_CAMPANHA)

# 2. Criar a Fato de Investimentos
df_fato_investimento = (
    df_silver.alias("f")
    .join(
        df_dim_campanha.alias("d"),
        on=(
            (F.col("f.id_campanha") == F.col("d.id_campanha")) &
            (F.col("f.data_investimento") >= F.col("d.dt_inicio_vigencia")) &
            (F.col("f.data_investimento") <= F.col("d.dt_fim_vigencia"))
        ),
        how="left"
    )
    .select(
        F.col("f.id_investimento"),
        F.col("d.sk_campanha"),
        F.col("f.id_campanha"),
        F.col("f.data_investimento"),
        F.col("f.impressoes"),
        F.col("f.cliques"),
        F.col("f.custo_total")
    )
    .withColumn("dh_processamento_gold", F.current_timestamp())
    .withColumn("usuario_executor", F.lit(current_user))
)

# Gravação da Fato de Investimentos com Liquid Clustering
if not spark.catalog.tableExists(GOLD_FATO_INVESTIMENTO):
    (df_fato_investimento.write
     .format("delta")
     .mode("overwrite")
     .clusterBy("data_investimento", "sk_campanha")
     .saveAsTable(GOLD_FATO_INVESTIMENTO))
else:
    delta_invest = DeltaTable.forName(spark, GOLD_FATO_INVESTIMENTO)
    delta_invest.alias("target").merge(
        source = df_fato_investimento.alias("source"),
        condition = "target.id_investimento = source.id_investimento"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# ==========================================================
# 4. CROSS-JOIN COM DOMÍNIO DE VENDAS (DATA MESH COMPLIANCE)
# ==========================================================
# Consumindo dados refinados e autorizados do Catálogo de Vendas (sales_prod)
try:
    df_clientes_vendas = spark.read.table("sales_prod.gold.dim_clientes")

    # Amostra de clientes que vieram do esforço de Marketing (ex: 40% fictício)
    df_clientes_marketing = df_clientes_vendas.sample(withReplacement=False, fraction=0.4, seed=45)

    # Simulação da Jornada de Atribuição (Cliques anteriores)
    # canal_midia vem da dimensão (já resolvida por sk_campanha na fato acima)
    df_atribuicao = df_clientes_marketing.select("sk_cliente").crossJoin(
        df_fato_investimento.select("id_campanha", "sk_campanha").sample(fraction=0.3, seed=46)
    ).join(
        df_dim_campanha.select("sk_campanha", "canal_midia"),
        on="sk_campanha",
        how="left"
    )

    df_fato_atribuicao = df_atribuicao.withColumn(
        "data_clique", F.expr("date_sub(current_date(), cast(rand() * 30 as int))")
    ).withColumn(
        "utm_source", F.lower(F.col("canal_midia"))
    ).withColumn(
        "utm_medium", F.lit("cpc")
    ).withColumn(
        "token_rastreio", F.expr("md5(concat(sk_cliente, cast(id_campanha as string), cast(data_clique as string)))")
    ).withColumn(
        "modelo_atribuicao_sugerido", F.lit("First Touch")
    ).drop("canal_midia") \
     .withColumn("dh_processamento_gold", F.current_timestamp()) \
     .withColumn("usuario_executor", F.lit(current_user))

    # Gravação da Fato de Atribuição com Liquid Clustering
    if not spark.catalog.tableExists(GOLD_FATO_ATRIBUICAO):
        (df_fato_atribuicao.write
         .format("delta")
         .mode("overwrite")
         .clusterBy("data_clique", "sk_cliente")
         .saveAsTable(GOLD_FATO_ATRIBUICAO))
    else:
        delta_atrib = DeltaTable.forName(spark, GOLD_FATO_ATRIBUICAO)
        delta_atrib.alias("target").merge(
            source = df_fato_atribuicao.alias("source"),
            condition = "target.token_rastreio = source.token_rastreio"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    print("Massa de dados Gold (Investimentos e Atribuição) gerada perfeitamente!")
except Exception as e:
    print(f"Aviso: Não foi possível cruzar com sales_prod. Verifique as permissões de Grants no Unity Catalog. Erro: {e}")
