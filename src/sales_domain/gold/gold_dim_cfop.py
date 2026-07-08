# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 1
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# 1. Capturar todos os CFOPs que já transacionaram na camada Silver
df_silver_cfop = (
    spark.read.table("sales_prod.silver.faturamento_nota_cabecalho")
    .select("cfop")
    .distinct()
    .filter(F.col("cfop").isNotNull())
)

# 2. Processamento de Regras Fiscais Brasileiras Baseadas em Intervalos Numéricos
df_dim_cfop = (
    df_silver_cfop
    .withColumn("cfop_codigo", F.col("cfop").cast(IntegerType()))
    # Surrogate Key estável para o Star Schema
    .withColumn("sk_cfop", F.md5(F.col("cfop_codigo").cast("string")))
    
    # --- 1. ESCOPO TERRITORIAL (Baseado no 1º dígito) ---
    .withColumn("cfop_escopo", 
        F.when(F.col("cfop_codigo").between(1000, 1999), "Entrada - Estadual")
         .when(F.col("cfop_codigo").between(2000, 2999), "Entrada - Interestadual")
         .when(F.col("cfop_codigo").between(3000, 3999), "Entrada - Exterior")
         .when(F.col("cfop_codigo").between(5000, 5999), "Saída - Estadual")
         .when(F.col("cfop_codigo").between(6000, 6999), "Saída - Interestadual")
         .when(F.col("cfop_codigo").between(7000, 7999), "Saída - Exterior")
         .otherwise("Não Identificado")
    )
    
    # --- 2. NATUREZA DA OPERAÇÃO (Grupos Oficiais de Negócio) ---
    .withColumn("cfop_natureza",
        # Grupo de Vendas Ativas e Prestações de Serviço que geram Receita Bruta
        F.when(
            F.col("cfop_codigo").between(5100, 5199) | F.col("cfop_codigo").between(6100, 6199) | F.col("cfop_codigo").between(7100, 7199) |
            F.col("cfop_codigo").between(5250, 5259) | F.col("cfop_codigo").between(6250, 6259) |
            F.col("cfop_codigo").between(5300, 5307) | F.col("cfop_codigo").between(6300, 6307), 
            "Venda / Prestação de Serviço"
        )
        # Grupo de Devoluções de Vendas (Anulações de Receita que afetam o Faturamento Líquido)
        .when(
            F.col("cfop_codigo").between(1200, 1299) | F.col("cfop_codigo").between(2200, 2299) | F.col("cfop_codigo").between(3200, 3299),
            "Devolução de Venda"
        )
        # Grupo de Compras (Entradas de estoque / Ativo Imobilizado / Consumo)
        .when(
            F.col("cfop_codigo").between(1100, 1199) | F.col("cfop_codigo").between(2100, 2199) | F.col("cfop_codigo").between(3100, 3199) |
            F.col("cfop_codigo").between(1400, 1449) | F.col("cfop_codigo").between(2400, 2449) |
            F.col("cfop_codigo").between(1550, 1559) | F.col("cfop_codigo").between(2550, 2559),
            "Compra / Aquisição"
        )
        # Grupo de Remessas Logísticas (Conserto, Demonstração, Depósito - Não geram receita)
        .when(
            F.col("cfop_codigo").between(5900, 5949) | F.col("cfop_codigo").between(6900, 6949) | F.col("cfop_codigo").between(7900, 7949),
            "Remessa Logística"
        )
        # Grupo de Retornos Logísticos
        .when(
            F.col("cfop_codigo").between(1900, 1949) | F.col("cfop_codigo").between(2900, 2949) | F.col("cfop_codigo").between(3900, 3949),
            "Retorno Logístico"
        )
        .otherwise("Outras Movimentações")
    )
    
    # --- 3. INDICADORES ANALÍTICOS (Essenciais para BI e Métricas de Finanças) ---
    # Is_Faturamento_Bruto: Registra se a operação representa uma saída de venda original
    .withColumn("is_faturamento_bruto", 
        F.col("cfop_natureza") == "Venda / Prestação de Serviço"
    )
    # Is_Deducao_Faturamento: Sinaliza o que deve abater o faturamento (Devoluções)
    .withColumn("is_deducao_faturamento", 
        F.col("cfop_natureza") == "Devolução de Venda"
    )
    # Is_Faturamento_Liquido: O que de fato entra no cálculo de receita final líquida
    .withColumn("is_faturamento_liquido", 
        (F.col("cfop_natureza") == "Venda / Prestação de Serviço") & 
        (~F.col("cfop_codigo").isin(5910, 6910, 5911, 6911)) # Remove CFOPs de Bonificação/Brinde se aplicável
    )
    
    # --- 4. INDICADORES DE SENTIDO DA OPERAÇÃO ---
    .withColumn("is_entrada",
        F.col("cfop_codigo").between(1000, 3999)
    )
    .withColumn("is_saida",
        F.col("cfop_codigo").between(5000, 7999)
    )

    # --- 5. TEXTOS DESCRITIVOS DOS MAIS COMUNS (Fallbacks para legibilidade) ---
    .withColumn("cfop_nome_amigavel",
        F.when(F.col("cfop_codigo") == 5101, "Venda de Produção do Estabelecimento (Interna)")
         .when(F.col("cfop_codigo") == 5102, "Venda de Mercadoria de Terceiros (Interna)")
         .when(F.col("cfop_codigo") == 5405, "Vendas dentro do mesmo estado (operação interna)")
         .when(F.col("cfop_codigo") == 6101, "Venda de Produção do Estabelecimento (Interestadual)")
         .when(F.col("cfop_codigo") == 6102, "Venda de Mercadoria de Terceiros (Interestadual)")
         .when(F.col("cfop_codigo") == 6405, "Vendas para outros estados (operação interestadual)")
         .when(F.col("cfop_codigo") == 1202, "Devolução de Venda de Mercadoria de Terceiros (Interna)")
         .when(F.col("cfop_codigo") == 2202, "Devolução de Venda de Mercadoria de Terceiros (Interestadual)")
         .when(F.col("cfop_codigo") == 1901, "Retorno de Mercadoria para o Estabelecimento (Interna)")
         .when(F.col("cfop_codigo") == 2901, "Retorno de Mercadoria para o Estabelecimento (Interestadual)")
         .when(F.col("cfop_codigo") == 1904, "Retorno de Mercadoria para o Estabelecimento após Remessa para Industrialização (Interna)")
         .when(F.col("cfop_codigo") == 2904, "Retorno de Mercadoria para o Estabelecimento após Remessa para Industrialização (Interestadual)")
         .when(F.col("cfop_codigo") == 1910, "Retorno de Mercadoria para o Estabelecimento após Remessa para Venda Fora do Estabelecimento (Interna)")
         .when(F.col("cfop_codigo") == 2910, "Retorno de Mercadoria para o Estabelecimento após Remessa para Venda Fora do Estabelecimento (Interestadual)")
         .otherwise(F.concat(F.lit("CFOP "), F.col("cfop_codigo").cast("string")))
    )
    
    # Auditoria
    .withColumn("dh_processamento_gold", F.current_timestamp())
    .select(
        "cfop_codigo", "cfop_nome_amigavel", 
        "cfop_natureza", "cfop_escopo", 
        "is_faturamento_bruto", "is_deducao_faturamento", "is_faturamento_liquido",
        "is_entrada", "is_saida",
        "dh_processamento_gold"
    )
)

# 3. Escrita na Gold via Overwrite (Garante sincronismo automático com novos códigos da Silver)
(
    df_dim_cfop.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .saveAsTable("sales_prod.gold.dim_cfop")
)

# 4. Otimização física
spark.sql("OPTIMIZE sales_prod.gold.dim_cfop ZORDER BY (cfop_codigo)")
