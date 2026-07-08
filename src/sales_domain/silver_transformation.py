# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import col, to_timestamp, current_timestamp, when
from delta.tables import DeltaTable

# definição das tabelas de origem e destino
tabela_origem = "sales_prod.bronze.pedidos_raw"
tabela_destino = "sales_prod.silver.pedidos"

print(f"Lendo dados da tabela {tabela_origem}")
df_bronze = spark.read.table(tabela_origem)

df_silver_vendas = df_bronze \
    .withColumn("data_pedido", to_timestamp(col("data_pedido"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("valor_unitario", when(col("valor_unitario") < 0, 0.00).otherwise(col("valor_unitario"))) \
    .withColumn("_update_at", current_timestamp()) \
    .drop("_ingestion_time") # remove o metadado exclusivo da bronze

if not spark.catalog.tableExists(tabela_destino):
    print(f"Criando tabela {tabela_destino}")
    df_silver_vendas.write \
        .format("delta") \
        .mode("ignore") \
        .saveAsTable(tabela_destino)

print(f"Escrevendo dados na tabela {tabela_destino}")

target_table = DeltaTable.forName(spark, tabela_destino)
target_table.alias("target") \
    .merge(
        source = df_silver_vendas.alias("source"),
        condition = "target.id_pedido = source.id_pedido"
    ) \
    .whenMatchedUpdate(set = {
        "target.id_cliente": "source.id_cliente",
        "target.data_pedido": "source.data_pedido",
        "target.id_produto": "source.id_produto",
        "target.quantidade": "source.quantidade",
        "target.valor_unitario": "source.valor_unitario",
        "target.status": "source.status",
        "target._input_file_name": "source._input_file_name",
        "target._update_at": "source._update_at"
    }) \
    .whenNotMatchedInsert(values = {
        "id_pedido": "source.id_pedido",
        "id_cliente": "source.id_cliente",
        "data_pedido": "source.data_pedido",
        "id_produto": "source.id_produto",
        "quantidade": "source.quantidade",
        "valor_unitario": "source.valor_unitario",
        "status": "source.status",
        "_input_file_name": "source._input_file_name",
        "_update_at": "source._update_at"
    }) \
    .execute()

print(f"Total de registros na tabela {tabela_destino}: {target_table.toDF().count()}")

    

