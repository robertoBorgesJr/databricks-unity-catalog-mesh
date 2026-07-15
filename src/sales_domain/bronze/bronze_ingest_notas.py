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
cfops = [1101, 1102, 1202, 2202, 5101, 5102, 6101, 6102, 5405, 6405]
categorias_marketing = ["Eletrônicos", "Eletrodomésticos", "Vestuário", "Alimentos", "Cosmeticos"]

# Pool simulado de transportadoras e tipos de veículos para dados logísticos
transportadoras_pool = [f"CNPJ_TRANS_{random.randint(10000000, 99999999)}0001{random.randint(10,99)}" for _ in range(5)]
especies_pool = ["CAIXA", "PALETE", "FARDO", "CONTAINER"]

minuto_atual = datetime.now().minute

notas_cabecalho = []
itens_nota = []
impostos_nota = []
transporte_nota = []

start_date = datetime(2026, 1, 1)

for i in range(1, num_notas + 1):
    numero_nota = 20260000 + minuto_atual + i
    chave_acesso = f"4126070000000000000055001000{numero_nota}123456789"
    data_emissao = start_date + timedelta(days=random.randint(0, 180))
    cliente_id = f"CLI_{random.randint(100, 999)}"
    cliente_tipo = random.choice(["PF", "PJ"])
    cliente_documento = f"123456789{random.randint(10,99)}" if cliente_tipo == "PF" else f"123456780001{random.randint(10,99)}"
    uf = random.choice(estados)
    cfop = random.choice(cfops)
    
    notas_cabecalho.append((chave_acesso, numero_nota, data_emissao, cliente_id, f"Cliente {cliente_id}", cliente_tipo, cliente_documento, uf, cfop))

    # geração de dados de logística e transporte
    transportadora_id = random.choice(transportadoras_pool)    
    modalidade_frete = random.choice(["0", "1", "9"])  # 0=CIF, 1=FOB, 9=Sem Frete
    placa_veiculo = f"ABC{random.randint(1000, 9999)}"
    uf_veiculo = random.choice(estados)
    peso_liquido = round(random.uniform(5.0, 450.0), 3)
    peso_bruto = round(peso_liquido + random.uniform(0.5, 12.0), 3)
    quantidade_volumes = random.randint(1, 15)
    especie_volumes = random.choice(especies_pool)    

    transporte_nota.append((
        chave_acesso, numero_nota, transportadora_id, modalidade_frete, 
        placa_veiculo, uf_veiculo, peso_liquido, peso_bruto, 
        quantidade_volumes, especie_volumes
    ))    
    
    qtd_itens = random.randint(1, 5)
    for item_seq in range(1, qtd_itens + 1):
        prod_id = f"PROD_{random.randint(10, 50)}"
        vlr_unitario = round(random.uniform(15.0, 1200.0), 2)
        quantidade = random.randint(1, 4)
        vlr_desconto = round(random.uniform(0.0, 50.0), 2)
        vlr_frete = round(random.uniform(10.0, 45.0), 2)

        vlr_total_item = (vlr_unitario * quantidade) - vlr_desconto + vlr_frete

        itens_nota.append((chave_acesso, numero_nota, item_seq, prod_id, f"Produto {prod_id} - {random.choice(categorias_marketing)}", vlr_unitario, quantidade, vlr_desconto, vlr_frete))                      

        # geração de impostos por item
        # ICMS
        aliquota_icms = 18.0 if uf in ["SP", "RJ", "MG"] else 12.0
        cst_icms = random.choice(["000", "010", "020", "040"])
        vlr_icms = round(vlr_total_item * (aliquota_icms / 100), 2)
        impostos_nota.append((chave_acesso, numero_nota, item_seq, "ICMS", cst_icms, float(vlr_total_item), aliquota_icms, vlr_icms))

        # PIS
        cst_pis = random.choice(["01", "02", "03", "04", "05", "06", "07", "08", "09"])
        vlr_pis = round(vlr_total_item * 0.6 / 100, 2)
        impostos_nota.append((chave_acesso, numero_nota, item_seq, "PIS", cst_pis, float(vlr_total_item), 0.6, vlr_pis))

        # COFINS
        cst_cofins = random.choice(["01", "02", "03", "04", "05", "06", "07", "08", "09"])
        vlr_cofins = round(vlr_total_item * 1.2 / 100, 2)
        impostos_nota.append((chave_acesso, numero_nota, item_seq, "COFINS", cst_cofins, float(vlr_total_item), 1.2, vlr_cofins))


# Criação dos dataframes
df_cabecalho_raw = spark.createDataFrame(notas_cabecalho, ["chave_acesso", "numero_nota", "data_emissao", "cliente_id", "cliente_nome", "cliente_tipo", "cliente_documento", "uf_cliente", "cfop"])
df_itens_raw = spark.createDataFrame(itens_nota, ["chave_acesso", "numero_nota", "numero_item", "produto_id", "produto_nome", "valor_unitario", "quantidade", "valor_desconto", "valor_frete"])
df_impostos_raw = spark.createDataFrame(impostos_nota, ["chave_acesso", "numero_nota", "numero_item", "imposto_tipo", "CST", "valor_base_calculo", "aliquota", "valor_imposto"])
df_transporte_raw = spark.createDataFrame(transporte_nota, ["chave_acesso", "numero_nota", "transportadora_id", "modalidade_frete", "placa_veiculo", "uf_veiculo", "peso_liquido", "peso_bruto", "quantidade_volumes", "especie_volumes"])

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

df_impostos_bronze = (df_impostos_raw
    .withColumn("dh_insercao_bronze", F.current_timestamp())
    .withColumn("nome_arquivo_origem", F.lit("MOCK_DATA_MEMORY"))
    .withColumn("usuario_executor", F.lit(current_user))
)

df_transporte_bronze = (df_transporte_raw
    .withColumn("dh_insercao_bronze", F.current_timestamp())
    .withColumn("nome_arquivo_origem", F.lit("MOCK_DATA_MEMORY"))
    .withColumn("usuario_executor", F.lit(current_user))
)

# Persistência na camada Bronze
df_cabecalho_bronze.write.format("delta").mode("append").saveAsTable("sales_prod.bronze.faturamento_nota_cabecalho")
df_itens_bronze.write.format("delta").mode("append").saveAsTable("sales_prod.bronze.faturamento_nota_itens")
df_impostos_bronze.write.format("delta").mode("append").saveAsTable("sales_prod.bronze.faturamento_nota_itens_impostos") 
df_transporte_bronze.write.format("delta").mode("append").saveAsTable("sales_prod.bronze.faturamento_nota_transporte")

print("Camada Bronze carregada com sucesso!")
