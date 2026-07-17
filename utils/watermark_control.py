# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
"""
watermark_control.py

Funções utilitárias e genéricas de controle de watermark, compartilhadas por
todos os pipelines de dimensão (SCD Tipo 2 incremental) do projeto.

A tabela de controle é ÚNICA no projeto (formato "linha por pipeline"), então
qualquer novo pipeline de dimensão reaproveita este módulo sem precisar de
alteração de schema nem lógica própria de leitura/escrita de watermark.

Uso esperado (dentro de um notebook de dimensão):

    from watermark_control import ler_watermark, atualizar_watermark

    CTRL_WATERMARK_PIPELINES = "marketing_prod.controle.watermark_pipelines"
    NOME_PIPELINE = "gold_dim_campanha"

    watermark = ler_watermark(spark, CTRL_WATERMARK_PIPELINES, NOME_PIPELINE)
    ...
    atualizar_watermark(
        spark=spark,
        tabela_controle=CTRL_WATERMARK_PIPELINES,
        nome_pipeline=NOME_PIPELINE,
        novo_watermark=novo_watermark,
        usuario_executor=current_user,
        qtd_registros_processados=df_incremento.count(),
    )
"""

from typing import Optional

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

DATA_WATERMARK_PADRAO = "1900-01-01"
CTRL_WATERMARK_PIPELINES_PADRAO = "marketing_prod.controle.watermark_pipelines"

def get_watermark(
    spark: SparkSession,
    nome_pipeline: str,
    tabela_controle: str = CTRL_WATERMARK_PIPELINES_PADRAO,
    watermark_padrao: str = DATA_WATERMARK_PADRAO,
):
    """
    Lê o watermark (maior data já processada) de um pipeline específico.

    Retorna `watermark_padrao` se a tabela de controle ainda não existir ou se
    este pipeline nunca tiver sido executado antes (primeira carga).
    """
    if not spark.catalog.tableExists(tabela_controle):
        return watermark_padrao

    watermark = (
        spark.read.table(tabela_controle)
        .filter(F.col("nome_pipeline") == nome_pipeline)
        .agg(F.max("data_maxima_processada"))
        .collect()[0][0]
    )
    return watermark or watermark_padrao


def update_watermark(
    spark: SparkSession,
    nome_pipeline: str,
    novo_watermark,
    usuario_executor: str,
    qtd_registros_processados: Optional[int] = None,
    tabela_controle: str = CTRL_WATERMARK_PIPELINES_PADRAO,
    status_execucao: str = "sucesso",
):
    """
    Faz o upsert (MERGE) do watermark de um pipeline específico na tabela de
    controle compartilhada — toca só a linha deste `nome_pipeline`, sem afetar
    o watermark de outras dimensões. Cria a tabela automaticamente na primeira
    chamada, caso ainda não exista.
    """
    df_watermark = (
        spark.createDataFrame(
            [(nome_pipeline, novo_watermark)],
            ["nome_pipeline", "data_maxima_processada"],
        )
        .withColumns({
            "dh_atualizacao": F.current_timestamp(),
            "usuario_executor": F.lit(usuario_executor),
            "qtd_registros_processados": F.lit(qtd_registros_processados),
            "status_execucao": F.lit(status_execucao),
        })
    )

    if not spark.catalog.tableExists(tabela_controle):
        (df_watermark.write
         .format("delta")
         .mode("overwrite")
         .clusterBy("nome_pipeline")
         .saveAsTable(tabela_controle))
        return

    delta_watermark = DeltaTable.forName(spark, tabela_controle)
    delta_watermark.alias("target").merge(
        source=df_watermark.alias("source"),
        condition="target.nome_pipeline = source.nome_pipeline",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
