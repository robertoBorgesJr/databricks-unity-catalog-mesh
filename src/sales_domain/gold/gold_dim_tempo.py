# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Configuração de localidade brasileira
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

# 1. Definir o intervalo da dimensão (Cobrir o passado recente e o futuro operacional)
start_date = "2015-01-01"
end_date = "2030-12-31"

# 2. Gerar a sequência diária de datas usando funções nativas do Spark
df_base_tempo = (
    spark.range(0, 1) # Dummy row para iniciar
    .select(F.explode(F.sequence(F.to_date(F.lit(start_date)), F.to_date(F.lit(end_date)), F.expr("interval 1 day"))).alias("data"))
)

# Mapas para nomes em português
dias_semana_pt = {
    1: "Domingo",
    2: "Segunda-feira",
    3: "Terça-feira",
    4: "Quarta-feira",
    5: "Quinta-feira",
    6: "Sexta-feira",
    7: "Sábado"
}
meses_pt = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro"
}

dias_semana_pt_expr = F.create_map([F.lit(x) for x in sum(dias_semana_pt.items(), ())])
meses_pt_expr = F.create_map([F.lit(x) for x in sum(meses_pt.items(), ())])

# 3. Enriquecer a matriz com os atributos analíticos que o Marketing e Vendas utilizam
df_dim_tempo = (
    df_base_tempo
    # Surrogate Key Inteira (Ex: 20260708) - Ideal para joins de performance
    .withColumn("sk_tempo", F.date_format("data", "yyyyMMdd").cast(IntegerType()))
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    # Nomes extensos e abreviações para os eixos de gráficos do Dashboard
    .withColumn("nome_mes", meses_pt_expr[F.month("data")])
    .withColumn("nome_mes_curto", F.substring("nome_mes", 1, 3))
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("semana_ano", F.weekofyear("data"))
    .withColumn("dia_semana_num", F.dayofweek("data"))
    .withColumn("nome_dia_semana", dias_semana_pt_expr[F.dayofweek("data")])
    # Flags de Negócio (Booleanas)
    .withColumn("is_final_semana", F.when(F.col("dia_semana_num").isin(1, 7), True).otherwise(False))
    # Carimbo de Auditoria
    .withColumn("dh_processamento_gold", F.current_timestamp())
)

# 4. Gravar no Unity Catalog na Camada Gold
(
    df_dim_tempo.write
    .format("delta")
    .mode("overwrite")
    .clusterby("ano", "mes")
    .option("mergeSchema", "true")
    .saveAsTable("sales_prod.gold.dim_tempo")
)

print("Dimensão de Tempo gerada com sucesso na Gold!")
