# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DoubleType, DateType
import random
from datetime import datetime, timedelta

# Simulação de dados
num_notas = 1000
estados = ["SP", "RJ", "PR", "SC", "RS", "MG", "BA", "GO", "ES", "MA", "PE", "CE", "PA", "AM", "TO", "AC", "DF", "MT", "MS", "PI", "RN", "AL", "PB", "PE", "SE", "AP", "RO", "RR"]
cfops = [1202, 2202, 5101, 5102, 6101, 6102, 5405, 6405]
categorias_marketing = ["Eletrônicos", "Eletrodomésticos", "Vestuário", "Alimentos", "Cosmeticos"]

notas_cabecalho = []
itens_nota = []
start_date = datetime(2026, 1, 1)

for i in range(1, num_notas + 1):
    numero_nota = 20260000 + i
    chave_acesso = f"4126070000000000000055001000{numero_nota}123456789"
    data_emissao = start_date + timedelta(days=random.randint(0, 180))
    cliente_id = f"CLI_{random.randint(100, 999)}"
    cliente_tipo = random.choice(["PF", "PJ"])
    cliente_documento = f"123456789{random.randint(10,99)}" if cliente_tipo == "PF" else f"123456780001{random.randint(10,99)}"
    uf = random.choice(estados)
    cfop = random.choice(cfops)
    
    notas_cabecalho.append((chave_acesso, numero_nota, data_emissao, cliente_id, f"Cliente {cliente_id}", cliente_tipo, cliente_documento, uf, cfop))
    
    qtd_itens = random.randint(1, 5)
    for item_seq in range(1, qtd_itens + 1):
        prod_id = f"PROD_{random.randint(10, 50)}"
        itens_nota.append((chave_acesso, numero_nota, item_seq, prod_id, f"Produto {prod_id} - {random.choice(categorias_marketing)}", round(random.uniform(15.0, 1200.0), 2), random.randint(1, 4), round(random.uniform(0.0, 50.0), 2), round(random.uniform(10.0, 45.0), 2)))

# Criação dos dataframes
df_cabecalho_raw = spark.createDataFrame(notas_cabecalho, ["chave_acesso", "numero_nota", "data_emissao", "cliente_id", "cliente_nome", "cliente_tipo", "cliente_documento", "uf_cliente", "cfop"])
df_itens_raw = spark.createDataFrame(itens_nota, ["chave_acesso", "numero_nota", "numero_item", "produto_id", "produto_nome", "valor_unitario", "quantidade", "valor_desconto", "valor_frete"])

# Enriquecimento com metadados de auditoria
current_user = spark.sql("SELECT current_user()").collect()[0][0]

df_cabecalho_bronze = (df_cabecalho_raw
    .withColumn("dh_insercao_bronze", F.current_timestamp())
    .withColumn("nome_arquivo_origem", F.lit("MOCK_DATA_MEMORY"))
    .withColumn("usuario_executor", F.lit(current_user))
)

df_itens_bronze = (df_itens_raw
    .withColumn("dh_insercao_bronze", F.current_timestamp())
    .withColumn("nome_arquivo_origem", F.lit("MOCK_DATA_MEMORY"))
    .withColumn("usuario_executor", F.lit(current_user))
)

# Persistência na camada Bronze
df_cabecalho_bronze.write.format("delta").mode("overwrite").saveAsTable("sales_prod.bronze.faturamento_nota_cabecalho")
df_itens_bronze.write.format("delta").mode("overwrite").saveAsTable("sales_prod.bronze.faturamento_nota_itens")

print("Camada Bronze carregada com sucesso!")
