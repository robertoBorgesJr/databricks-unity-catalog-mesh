# Databricks notebook source
import random
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

# Definição estrita do schema dos dados brutos de vendas
schema_vendas = StructType([
    StructField("id_pedido", StringType(), False),
    StructField("id_cliente", StringType(), True),
    StructField("data_pedido", StringType(), True),
    StructField("id_produto", StringType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("valor_unitario", DoubleType(), True),
    StructField("status", StringType(), True)
])

# Gerando 100.000 registros na memória do spark para simular volumetria de produção 
status_opcoes = ["ENTREGUE", "PROCESSANDO", "CANCELADO", "PAGO"]
dados_mock = []

base_date = datetime(2026, 7, 1)
for i in range(100000):
    data_fat = base_date + timedelta(days=random.randint(0, 5), hours=random.randint(0, 23))
    dados_mock.append((
        f"PED-{1000000 + i}",
        f"CLI-{random.randint(1000, 9999)}",
        data_fat.strftime("%Y-%m-%d %H:%M:%S"),
        f"PROD-{random.randint(10, 99)}",
        random.randint(1, 5),
        round(random.uniform(10.0, 500.0), 2),
        random.choice(status_opcoes)
    ))

# Criando o DataFrame inicial baseado no nosso Schema
df_raw = spark.createDataFrame(dados_mock, schema=schema_vendas)

from pyspark.sql.functions import input_file_name, current_timestamp

df_bronze = df_raw \
    .withColumn("_input_file_name", lit("MOCK_DATA_MEMORY")) \
    .withColumn("_ingestion_timestamp", current_timestamp())

# definição do destino na árvore de governança
nome_tabela_bronze = "vendas_prod.bronze.pedidos_raw"

# escrita delta
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(nome_tabela_bronze)

# print(f"Escrita concluída em {nome_tabela_bronze}")
