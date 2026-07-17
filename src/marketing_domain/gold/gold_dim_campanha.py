# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Import watermark_control
# MAGIC %run ../../../utils/watermark_control

# COMMAND ----------

# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

current_user = spark.sql("SELECT current_user()").collect()[0][0]

SILVER_INVESTIMENTO = "marketing_prod.silver.investimento_marketing"
GOLD_DIM_CAMPANHA = "marketing_prod.gold.dim_campanha"

# Controle de watermark
NOME_PIPELINE = "gold_dim_campanha"  # identifica a linha deste pipeline na tabela de controle
DATA_FIM_ABERTA = "9999-12-31"  # marca o registro vigente (evita NULL em dt_fim_vigencia)

# ==========================================================
# 0. LER O WATERMARK DA ÚLTIMA EXECUÇÃO (só a linha deste pipeline)
# ==========================================================
watermark = get_watermark(spark, NOME_PIPELINE)
# Na primeira execução (sem watermark), este filtro captura a Silver inteira,
# então o mesmo fluxo serve tanto para a carga inicial quanto para o incremento.

# ==========================================================
# 1. LER APENAS O INCREMENTO DA SILVER (data_investimento > watermark)
# ==========================================================
df_incremento = (
    spark.read.table(SILVER_INVESTIMENTO)
    .filter(F.col("data_investimento") > F.lit(watermark))
    .select("id_campanha", "nome_campanha", "canal_midia", "data_investimento")
    .dropDuplicates(["id_campanha", "data_investimento"])
)

if df_incremento.isEmpty():
    print(f"Nenhum dado novo desde o watermark ({watermark}). Nada a processar.")
else:
    # ==========================================================
    # 2. BUSCAR O "ESTADO ATUAL" (versão vigente) DAS CAMPANHAS AFETADAS
    #    Só das campanhas que aparecem no incremento — não da tabela toda.
    # ==========================================================
    ids_no_incremento = df_incremento.select("id_campanha").distinct()

    if spark.catalog.tableExists(GOLD_DIM_CAMPANHA):
        df_estado_atual = (
            spark.read.table(GOLD_DIM_CAMPANHA)
            .filter(F.col("flag_atual"))
            .join(ids_no_incremento, on="id_campanha", how="inner")
            .select(
                "id_campanha",
                "nome_campanha",
                "canal_midia",
                F.col("dt_inicio_vigencia").alias("data_investimento")  # usada só como âncora temporal
            )
        )
    else:
        df_estado_atual = spark.createDataFrame([], df_incremento.schema)

    # ==========================================================
    # 3. UNIR ÂNCORA (estado atual) + INCREMENTO E DETECTAR MUDANÇAS
    #    Mesma lógica de LAG de antes, mas aplicada só a este recorte pequeno.
    # ==========================================================
    df_combinado = df_estado_atual.unionByName(df_incremento)

    janela_campanha = Window.partitionBy("id_campanha").orderBy("data_investimento")

    df_versionado = (
        df_combinado
        .withColumn(
            "atributos_anteriores",
            F.lag(F.concat_ws("||", "nome_campanha", "canal_midia")).over(janela_campanha)
        )
        .withColumn(
            "mudou_atributo",
            F.when(
                F.col("atributos_anteriores").isNull()
                | (F.concat_ws("||", "nome_campanha", "canal_midia") != F.col("atributos_anteriores")),
                1
            ).otherwise(0)
        )
        .withColumn(
            "num_versao",
            F.sum("mudou_atributo").over(janela_campanha.rowsBetween(Window.unboundedPreceding, 0))
        )
    )

    # 4. Consolidar cada versão: início de vigência = menor data observada nela
    df_versoes = (
        df_versionado
        .groupBy("id_campanha", "num_versao", "nome_campanha", "canal_midia")
        .agg(F.min("data_investimento").alias("dt_inicio_vigencia"))
    )

    # 5. Calcular dt_fim_vigencia = dia anterior ao início da versão seguinte.
    #    A versão mais recente fica com DATA_FIM_ABERTA e flag_atual = true.
    janela_versao = Window.partitionBy("id_campanha").orderBy("num_versao")

    df_dim_campanha = (
        df_versoes
        .withColumn(
            "dt_fim_vigencia",
            F.date_sub(F.lead("dt_inicio_vigencia").over(janela_versao), 1)
        )
        .withColumn(
            "dt_fim_vigencia",
            F.coalesce(F.col("dt_fim_vigencia"), F.to_date(F.lit(DATA_FIM_ABERTA)))
        )
        .withColumn("flag_atual", F.col("dt_fim_vigencia") == F.to_date(F.lit(DATA_FIM_ABERTA)))
        .withColumn(
            # Sem dt_fim_vigencia/flag_atual no hash: se nada mudou, a versão
            # "âncora" recalcula a MESMA sk_campanha da tabela e o MERGE apenas
            # atualiza (idempotente). Se mudou, gera sk_campanha novo -> INSERT,
            # e a versão antiga é fechada (UPDATE do dt_fim_vigencia/flag_atual).
            "sk_campanha",
            F.expr("""md5(concat(
                cast(id_campanha as string), nome_campanha, canal_midia,
                cast(dt_inicio_vigencia as string)
            ))""")
        )
        .select(
            "sk_campanha",
            "id_campanha",
            "nome_campanha",
            "canal_midia",
            "dt_inicio_vigencia",
            "dt_fim_vigencia",
            "flag_atual"
        )
        .withColumn("dh_processamento_gold", F.current_timestamp())
        .withColumn("usuario_executor", F.lit(current_user))
    )

    # ==========================================================
    # 6. GRAVAÇÃO DA DIMENSÃO (MERGE só toca as campanhas afetadas)
    # ==========================================================
    if not spark.catalog.tableExists(GOLD_DIM_CAMPANHA):
        (df_dim_campanha.write
         .format("delta")
         .mode("overwrite")
         .clusterBy("id_campanha")
         .saveAsTable(GOLD_DIM_CAMPANHA))
    else:
        delta_dim_campanha = DeltaTable.forName(spark, GOLD_DIM_CAMPANHA)
        delta_dim_campanha.alias("target").merge(
            source=df_dim_campanha.alias("source"),
            condition="target.sk_campanha = source.sk_campanha"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    # ==========================================================
    # 7. ATUALIZAR O WATERMARK DE CONTROLE (MERGE só na linha deste pipeline)
    # ==========================================================
    novo_watermark = df_incremento.agg(F.max("data_investimento")).collect()[0][0]

    update_watermark(
        spark=spark,
        nome_pipeline=NOME_PIPELINE,
        novo_watermark=novo_watermark,
        usuario_executor=current_user,
        qtd_registros_processados=df_incremento.count(),
    )

    print(f"Dimensão Gold de Campanha (SCD Tipo 2) atualizada até {novo_watermark}.")
