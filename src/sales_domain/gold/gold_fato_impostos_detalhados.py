# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Configuração de auditoria e tabelas
current_user = spark.sql("SELECT current_user()").collect()[0][0]
GOLD_TABLE_IMPOSTOS = "sales_prod.gold.fato_impostos_detalhados"

# --- [1. LEITURA DO WATERMARK (CARGA INCREMENTAL)] ---
try:
    watermark = spark.sql(f"SELECT MAX(dh_processamento_gold) FROM {GOLD_TABLE_IMPOSTOS}").collect()[0][0]
    print(f"Carga incremental — watermark obtido: {watermark}")
except Exception:
    watermark = None
    print("Tabela ainda não existe ou vazia — executando carga inicial completa.")

# --- [2. LEITURA DOS DADOS DA CAMADA SILVER] ---
df_cabecalho_sil = spark.read.table("sales_prod.silver.faturamento_nota_cabecalho")
df_itens_sil = spark.read.table("sales_prod.silver.faturamento_nota_itens")
df_impostos_sil = spark.read.table("sales_prod.silver.faturamento_nota_itens_impostos")

# Snapshot completo das chaves lógicas na Silver (para controle de Soft Delete)
# Chave primária da fato de impostos: chave_acesso + numero_item + imposto_tipo
df_chaves_silver = df_impostos_sil.select("chave_acesso", "numero_item", "imposto_tipo")

# Se houver watermark, filtramos somente o processamento incremental na origem
if watermark:
    # Captura notas alteradas ou criadas na Silver após o último processamento
    df_cabecalho_sil = df_cabecalho_sil.filter(F.col("dh_processamento_silver") > watermark)
    # Filtra os impostos correspondentes a essas notas alteradas
    df_impostos_sil = df_impostos_sil.join(
        df_cabecalho_sil.select("chave_acesso"), 
        on="chave_acesso", 
        how="inner"
    )

# --- [3. MODELAGEM DIMENSIONAL & SURROGATE KEYS] ---
df_impostos_incremental = (
    df_impostos_sil.alias("imp")
    .join(df_cabecalho_sil.alias("cab"), "chave_acesso", "inner")
    .join(df_itens_sil.alias("item"), ["chave_acesso", "numero_item"], "inner")
    .select(
        # Surrogate Keys (MD5 determinístico com fallback de nulos)
        F.md5(F.coalesce(F.col("cab.cliente_id"), F.lit("-1"))).alias("sk_cliente"),
        F.md5(F.coalesce(F.col("item.produto_id"), F.lit("-1"))).alias("sk_produto"),
        
        # Chaves Naturais / Rastreabilidade
        F.col("imp.chave_acesso"),
        F.col("imp.numero_nota").cast("long"),
        F.col("imp.numero_item").cast("integer"),
        F.col("cab.data_emissao").cast("date"),
        
        # Atributos Tributários
        F.col("imp.imposto_tipo"),
        F.col("imp.CST").alias("cst_codigo"),
        F.col("imp.valor_base_calculo").cast("decimal(18,2)"),
        F.col("imp.aliquota").cast("decimal(5,2)"),
        F.col("imp.valor_imposto").cast("decimal(18,2)"),
        
        # Flags de Controle
        F.lit(False).alias("fl_excluido"),
        F.lit(None).cast("timestamp").alias("dh_exclusao"),
        F.current_timestamp().alias("dh_processamento_gold"),
        F.lit(current_user).alias("usuario_executor")
    )
)

# --- [4. ESCRITA DOS DADOS (MERGE OU INICIAL)] ---
if spark.catalog.tableExists(GOLD_TABLE_IMPOSTOS):
    delta_target = DeltaTable.forName(spark, GOLD_TABLE_IMPOSTOS)
    
    # 4.1 Upsert dos dados novos/modificados
    (delta_target.alias("gold")
     .merge(
         source=df_impostos_incremental.alias("source"),
         condition="""
            gold.chave_acesso = source.chave_acesso AND 
            gold.numero_item = source.numero_item AND 
            gold.imposto_tipo = source.imposto_tipo
         """
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute())
    
    # --- [5. SOFT DELETE - MARCAR REGISTROS EXCLUÍDOS DA SILVER] ---
    df_excluidos = (
        delta_target.toDF()
        .filter(F.col("fl_excluido") == False)
        .select("chave_acesso", "numero_item", "imposto_tipo")
        .join(df_chaves_silver, on=["chave_acesso", "numero_item", "imposto_tipo"], how="left_anti")
    )
    
    count_excluidos = df_excluidos.count()
    if count_excluidos > 0:
        (delta_target.alias("gold")
         .merge(
             source=df_excluidos.alias("excluidos"),
             condition="""
                gold.chave_acesso = excluidos.chave_acesso AND 
                gold.numero_item = excluidos.numero_item AND 
                gold.imposto_tipo = excluidos.imposto_tipo
             """
         )
         .whenMatchedUpdate(set={
             "fl_excluido": F.lit(True),
             "dh_exclusao": F.current_timestamp()
         })
         .execute())
        print(f"Soft delete aplicado: {count_excluidos} registro(s) de impostos marcado(s) como excluído(s).")
    else:
        print("Nenhum imposto excluído detectado na Silver.")

else:
    # Escrita inicial completa com suporte nativo a Liquid Clustering
    (df_impostos_incremental.write
     .format("delta")
     .mode("overwrite")
     .clusterBy("data_emissao", "sk_cliente", "imposto_tipo")
     .saveAsTable(GOLD_TABLE_IMPOSTOS))
    print("Carga inicial de impostos executada com sucesso.")
