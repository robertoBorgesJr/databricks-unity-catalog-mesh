# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType, TimestampType

# ==============================================================================
# 1. PARAMETRIZAÇÃO E LEITURA
# ==============================================================================

# Caminho do Volume Gerenciado no Unity Catalog
path_csv_cfop = "/Volumes/sales_prod/gold/arquivos_setup/Tabela_CFOP.csv"
tabela_destino = "sales_prod.gold.dim_cfop"

# Leitura do arquivo bruto
# Nota: Ajuste o "sep" caso seu arquivo utilize vírgula ao invés de ponto e vírgula
df_raw = (spark.read
    .format("csv")
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .load(path_csv_cfop)
)

# ==============================================================================
# 2. MAPEAMENTO DE REGRAS E TRANSFORMAÇÕES (Camada Gold)
# ==============================================================================

# Assumindo que o CSV original possui colunas parecidas com: 'codigo', 'descricao'
# Ajuste os nomes de origem ('codigo', 'descricao') conforme o cabeçalho real do seu CSV
df_transformed = (df_raw
    # 2.1. Seleção, renomeação e tipagem básica das colunas originais
    .withColumn("cfop_codigo", F.regexp_replace(F.col("CFOP"), '[^0-9]', '').cast(IntegerType()))
    .withColumn("cfop_nome_amigavel", F.col("DescricaoResumida").cast(StringType()))
    
    # 2.2. Determinação de Entrada vs Saída com base no primeiro dígito do CFOP
    # 1, 2, 3 = Entrada | 5, 6, 7 = Saída
    .withColumn("is_entrada", F.substring(F.col("CFOP").cast(StringType()), 1, 1).isin(["1", "2", "3"]))
    .withColumn("is_saida", F.substring(F.col("CFOP").cast(StringType()), 1, 1).isin(["5", "6", "7"]))
    
    # 2.3. Determinação do Escopo (Estadual, Interestadual, Exterior)
    .withColumn("cfop_escopo", 
        F.when(F.substring(F.col("CFOP").cast(StringType()), 1, 1).isin(["1", "5"]), F.lit("Estadual"))
         .when(F.substring(F.col("CFOP").cast(StringType()), 1, 1).isin(["2", "6"]), F.lit("Interestadual"))
         .when(F.substring(F.col("CFOP").cast(StringType()), 1, 1).isin(["3", "7"]), F.lit("Exterior"))
         .otherwise(F.lit("Desconhecido"))
    )
    
    # 2.4. Natureza Genérica (Exemplo: Venda, Devolução, Transferência)
    # Geralmente mapeada por substrings ou listas de CFOPs específicos da SEFAZ
    .withColumn("cfop_natureza", 
        F.when(F.col("cfop_codigo").isin([5101, 5102, 6101, 6102]), F.lit("Venda de Mercadoria"))
         .when(F.col("cfop_codigo").isin([1201, 1202, 5201, 5202]), F.lit("Devolução de Venda/Compra"))
         .when(F.col("cfop_codigo").isin([5151, 5152, 6151, 6152]), F.lit("Transferência"))
         .otherwise(F.lit("Outras Operações"))
    )
    
    # 2.5. Regras de Negócio de FinOps / Controladoria (Faturamento Bruto, Líquido e Deduções)
    # Substitua as listas abaixo pelos códigos reais que sua empresa considera para cada indicador
    .withColumn("is_faturamento_bruto", 
        F.when(F.col("cfop_codigo").isin([5101, 5102, 6101, 6102]), F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn("is_deducao_faturamento", 
        F.when(F.col("cfop_codigo").isin([1201, 1202, 2201, 2202]), F.lit(True)).otherwise(F.lit(False))
    )
    .withColumn("is_faturamento_liquido", 
        # Faturamento Líquido costuma ser (Bruto = True) AND (Dedução = False)
        F.when((F.col("is_faturamento_bruto") == True) & (F.col("is_deducao_faturamento") == False), F.lit(True)).otherwise(F.lit(False))
    )
    
    # 2.6. Auditoria e Linhagem de Dados
    .withColumn("dh_processamento_gold", F.current_timestamp())
)

# ==============================================================================
# 3. SELEÇÃO FINAL E HIGIENIZAÇÃO DO SCHEMA
# ==============================================================================
df_final = df_transformed.select(
    "cfop_codigo",
    "cfop_nome_amigavel",
    "cfop_natureza",
    "cfop_escopo",
    "is_faturamento_bruto",
    "is_deducao_faturamento",
    "is_faturamento_liquido",
    "is_entrada",
    "is_saida",
    "dh_processamento_gold"
)

# ==============================================================================
# 4. ESCRITA ACELERADA NA TABELA DELTA (UNITY CATALOG)
# ==============================================================================
# Como o volume de dados é extremamente baixo e atualizações são raras, 
# o modo 'overwrite' limpa a tabela e reinsere o espelho ideal sem gerar overhead.

(df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true") # Garante a aplicação do schema exato da Gold
    .saveAsTable(tabela_destino)
)

print(f"Carga da tabela {tabela_destino} realizada com sucesso!")
