# databricks-datamesh-infra

Plataforma de dados baseada em **Data Mesh** com **arquitetura medallion** (Bronze/Silver/Gold) sobre Databricks e Azure. Implementa dois domínios de dados independentes — **Vendas** e **Marketing** — com catálogos isolados no Unity Catalog, pipelines incrementais com Delta Lake e infraestrutura provisionada via Terraform.

---

## Sumário

- [Visão Geral da Arquitetura](#visão-geral-da-arquitetura)
- [Stack Tecnológica](#stack-tecnológica)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Domínios de Dados](#domínios-de-dados)
- [Camadas da Arquitetura Medallion](#camadas-da-arquitetura-medallion)
- [Padrões de Engenharia](#padrões-de-engenharia)
- [Pacote de Utilitários](#pacote-de-utilitários)
- [Orquestração dos Jobs](#orquestração-dos-jobs)
- [Infraestrutura como Código (Terraform)](#infraestrutura-como-código-terraform)
- [CI/CD com GitHub Actions](#cicd-com-github-actions)
- [Testes e Qualidade de Código](#testes-e-qualidade-de-código)
- [Configuração do Ambiente](#configuração-do-ambiente)
- [Como Executar Localmente](#como-executar-localmente)
- [Como Fazer Deploy](#como-fazer-deploy)

---

## Visão Geral da Arquitetura

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA MESH — UNITY CATALOG                       │
│                                                                           │
│   ┌───────────────────────────┐   ┌───────────────────────────────────┐  │
│   │      DOMÍNIO VENDAS       │   │       DOMÍNIO MARKETING           │  │
│   │  Catálogo: sales_{env}    │   │  Catálogo: marketing_{env}        │  │
│   │                           │   │                                   │  │
│   │  bronze  │ silver │ gold  │   │  bronze  │  silver  │  gold       │  │
│   │  itens   │ clean  │ star  │◄──┤  invest. │  clean   │  star+scd2  │  │
│   │  impostos│ valid. │ schema│   │  campanha│  dedup.  │  cross-join │  │
│   │  transp. │ quaren.│ facts │   │          │          │  vendas     │  │
│   └───────────────────────────┘   └───────────────────────────────────┘  │
│                                                                           │
│         ┌────────────────────────────────────┐                           │
│         │   controle.watermark_pipelines     │   (compartilhado)         │
│         └────────────────────────────────────┘                           │
└─────────────────────────────────────────────────────────────────────────┘
          │                    │
          ▼                    ▼
   ADLS Gen2 (por domínio)   Databricks Jobs
   st{domain}datamesh{env}   (spark_python_task)
```

O projeto segue os princípios do Data Mesh:
- **Ownership por domínio:** cada domínio possui seu próprio catálogo, storage account e cluster
- **Produto de dados:** as tabelas Gold são os produtos de dados publicados
- **Governança federada:** Unity Catalog centraliza políticas de acesso e metadados
- **Interoperabilidade:** domínios consomem dados uns dos outros via Unity Catalog (ex: Marketing lê `sales_{env}.gold.dim_clientes`)

---

## Stack Tecnológica

| Camada | Tecnologia |
|--------|-----------|
| Processamento | Apache Spark (PySpark 3.x) |
| Plataforma | Databricks (Unity Catalog, Delta Lake, Lakehouse) |
| Cloud | Azure (ADLS Gen2, Access Connectors) |
| Linguagem | Python 3.12 |
| Formato de Dados | Delta Lake |
| IaC | Terraform 1.x |
| CI/CD | GitHub Actions |
| Empacotamento | Python Wheel (Databricks Asset Bundle) |
| Testes | pytest + ruff |
| Configuração | Databricks Bundle (YAML) |

---

## Estrutura do Projeto

```
databricks-datamesh-infra/
│
├── .github/
│   └── workflows/
│       ├── ci.yml                        # Lint, testes, build e validação do bundle
│       ├── cd-dev.yml                    # Deploy automático para dev (branch desenv_local)
│       └── cd-prod.yml                   # Deploy para prod com aprovação manual (branch main)
│
├── infra/                                # Infraestrutura como Código (Terraform)
│   ├── live/
│   │   ├── global-governance/            # Metastore, storage account raiz, access connector
│   │   ├── network-core/                 # VNets por domínio
│   │   └── domains/
│   │       ├── marketing_domain/         # Recursos de infra do domínio Marketing
│   │       └── sales_domain/             # Recursos de infra do domínio Vendas
│   └── modules/
│       ├── databricks_domain_config/     # Módulo reutilizável: cluster, catálogo, schemas
│       └── network/                      # Módulo de rede
│
├── src/
│   ├── marketing_domain/
│   │   ├── bronze/
│   │   │   └── bronze_ingest_marketing.py
│   │   ├── silver/
│   │   │   └── silver_clean_marketing.py
│   │   └── gold/
│   │       ├── gold_dim_campanha.py      # Dimensão com SCD Tipo 2
│   │       └── gold_load_marketing.py    # Fatos + consumo cross-domain
│   └── sales_domain/
│       ├── bronze/
│       │   └── bronze_ingest_notas.py
│       ├── silver/
│       │   └── silver_clean_notas.py
│       └── gold/
│           ├── gold_dim_cfop.py
│           ├── gold_dim_clientes.py
│           ├── gold_dim_produtos.py
│           ├── gold_dim_tempo.py
│           ├── gold_dim_transportadoras.py
│           ├── gold_fato_faturamento.py
│           ├── gold_fato_impostos_detalhados.py
│           └── gold_fato_logistica_transporte.py
│
├── utils/                                # Pacote compartilhado (distribuído como wheel)
│   ├── __init__.py
│   ├── connections.py                    # Gerenciamento de SparkSession
│   ├── environment.py                    # Resolução do ambiente (dev/prod)
│   └── watermark_control.py             # Controle incremental por watermark
│
├── resources/
│   ├── marketing_jobs.yml               # Definição dos jobs de Marketing
│   └── sales_jobs.yml                   # Definição dos jobs de Vendas
│
├── tests/
│   ├── __init__.py
│   ├── test_connections.py
│   └── test_environment.py
│
├── databricks.yaml                       # Configuração do Databricks Asset Bundle
├── pyproject.toml                        # Metadados do pacote Python
├── requirements.txt                      # Dependências Python
└── .env.example                          # Template de variáveis de ambiente
```

---

## Domínios de Dados

### Domínio Vendas (`sales_domain`)

Responsável pelo processamento de **notas fiscais eletrônicas (NF-e)** com modelo estrela completo para análise de faturamento, tributação e logística.

**Tabelas Bronze (raw):**
| Tabela | Descrição |
|--------|-----------|
| `faturamento_nota_cabecalho` | Cabeçalho das NF-e (emitente, destinatário, datas, valores totais) |
| `faturamento_nota_itens` | Itens de cada NF-e (produto, CFOP, quantidade, valor unitário) |
| `faturamento_nota_itens_impostos` | Detalhamento tributário por item (ICMS, IPI, PIS, COFINS) |
| `faturamento_nota_transporte` | Informações logísticas (transportadora, modal, peso, volume) |

**Modelo Estrela (Gold):**

```
                    dim_tempo
                       │
dim_cfop ──── fato_faturamento ──── dim_clientes
                       │
                  dim_produtos
                       │
               dim_transportadoras


fato_impostos_detalhados  (granularidade: item x nota)
fato_logistica_transporte (granularidade: nota x transportadora)
```

### Domínio Marketing (`marketing_domain`)

Responsável pelo processamento de **investimentos em campanhas publicitárias** com SCD Tipo 2 para rastrear mudanças históricas e análise de atribuição de conversão.

**Tabelas Bronze (raw):**
| Tabela | Descrição |
|--------|-----------|
| `investimento_marketing` | Investimentos diários por campanha e canal (Google Ads, Meta, TikTok, LinkedIn) |

**Modelo Estrela (Gold):**

```
dim_campanha (SCD Tipo 2)
      │
fato_investimento_marketing   ←──  dim_clientes (sales_domain) ← cross-domain
      │
fato_atribuicao_conversao
```

---

## Camadas da Arquitetura Medallion

### Bronze — Ingestão Bruta

- **Objetivo:** Captura de dados brutos com mínima transformação, preservando a origem
- **Padrão:** Append-only com metadata de rastreabilidade
- **Colunas de controle:** `dh_insercao_bronze`, `nome_arquivo_origem`, `usuario_executor`
- **Dados simulados:** Geração de dados mock representativos (NF-e com 1.000 registros, 30 dias de campanhas)

**Exemplo — estrutura de uma nota fiscal:**
```
chave_acesso | numero_nota | cnpj_emitente | cnpj_destinatario | uf_emitente
data_emissao | valor_total  | status_nota   | dh_insercao_bronze | nome_arquivo_origem
```

### Silver — Limpeza e Validação

- **Objetivo:** Dados deduplicados, padronizados e validados, prontos para consumo analítico
- **Deduplicação:** `row_number()` por janela de partição, mantendo o registro mais recente
- **Padronizações:** Cast de tipos, upper-case em campos categóricos, derivação de colunas
- **Validações de negócio implementadas:**
  - CFOP válido (lookup na tabela dim_cfop)
  - UF válida (lista dos 27 estados brasileiros)
  - Quantidade e valores > 0
  - Campos obrigatórios não nulos
- **Quarentena:** Registros inválidos são roteados para tabelas de quarentena em vez de quebrar o pipeline

```
silver_clean_notas.py
      │
      ├─► silver.faturamento_nota_cabecalho  (válidos)
      ├─► silver.faturamento_nota_itens      (válidos)
      ├─► quarentena.nota_cabecalho_invalida (inválidos)
      └─► quarentena.nota_itens_invalida     (inválidos)
```

### Gold — Camada Analítica (Esquema Estrela)

- **Objetivo:** Tabelas otimizadas para consumo por BI, SQL Analytics e Data Science
- **Padrão:** Processamento incremental via watermark + Delta MERGE idempotente
- **Chaves substitutas (Surrogate Keys):** MD5 sobre chaves naturais para estabilidade
- **Liquid Clustering:** Substitui particionamento estático com otimização dinâmica de layout

---

## Padrões de Engenharia

### Processamento Incremental com Watermark

Todas as pipelines Gold utilizam controle de watermark para processar apenas dados novos:

```python
# 1. Lê o último ponto processado
watermark = get_watermark(spark, "gold_fato_faturamento")

# 2. Filtra Silver apenas a partir do watermark
df_incremental = silver_df.filter(
    col("dh_processamento_silver") > watermark
)

# 3. Aplica transformações e faz MERGE no Gold
df_gold.write.format("delta").mode("merge")...

# 4. Atualiza o watermark
update_watermark(spark, "gold_fato_faturamento", nova_data_maxima, ...)
```

**Tabela de controle:**
```
{env}_prod.controle.watermark_pipelines

nome_pipeline          | data_maxima_processada | dh_atualizacao
gold_fato_faturamento  | 2025-06-15             | 2025-06-16 03:00:00
gold_dim_campanha      | 2025-06-14             | 2025-06-15 03:00:00
```

### SCD Tipo 2 (Slowly Changing Dimensions)

Implementado em `gold_dim_campanha.py` para rastrear mudanças históricas em atributos de campanhas:

```
sk_campanha | id_campanha | nome_campanha     | canal_midia | dt_inicio_vigencia | dt_fim_vigencia | flag_atual
abc123      | C001        | Campanha Verão    | Google Ads  | 2024-01-01         | 2024-06-30      | false
def456      | C001        | Campanha Verão 2  | Google Ads  | 2024-07-01         | 9999-12-31      | true
```

- `dt_fim_vigencia = 9999-12-31` indica a versão ativa
- Mudanças detectadas via window functions sobre atributos monitorados
- `sk_campanha` = MD5(id + nome + canal + dt_inicio) — garante idempotência no MERGE

### Soft Delete em Fatos

Fatos nunca são deletados fisicamente. Exclusões no Silver geram atualizações de flag:

```python
# Detecta registros que saíram do Silver desde o último processamento
df_deleted = gold_df.join(silver_df, on="chave_acesso", how="left_anti")

# Atualiza a fato com soft delete
delta_table.merge(df_deleted, condition).whenMatched().update({
    "fl_excluido": True,
    "dh_exclusao": current_timestamp()
})
```

### Cross-Domain no Data Mesh

O domínio Marketing consome a dimensão de clientes do domínio Vendas diretamente via Unity Catalog:

```python
# marketing/gold/gold_load_marketing.py
dim_clientes = spark.table(f"sales_{env}.gold.dim_clientes")
```

Isso demonstra o padrão de **produto de dados** do Data Mesh: o domínio Vendas publica `dim_clientes` como produto consumível por outros domínios.

### Liquid Clustering

Utilizado em substituição ao `partitionBy` para otimização automática de layout de dados:

```python
df.write.format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .clusterBy("data_investimento", "id_campanha") \
    .saveAsTable(...)
```

### Quarentena de Dados Inválidos

```python
df_valido    = df_silver.filter(col("fl_valido") == True)
df_invalido  = df_silver.filter(col("fl_valido") == False)

df_valido.write.format("delta").saveAsTable("silver.nota_cabecalho")
df_invalido.write.format("delta").saveAsTable("quarentena.nota_cabecalho_invalida")
```

---

## Pacote de Utilitários

O diretório `/utils` é empacotado como wheel Python e distribuído junto com cada job, garantindo reutilização de código entre todos os pipelines.

### `connections.py` — Gerenciamento de SparkSession

```python
from utils.connections import get_spark_session

spark = get_spark_session(domain="SALES")
```

- **No cluster Databricks:** retorna a SparkSession nativa (via `DATABRICKS_RUNTIME_VERSION`)
- **Localmente:** conecta via `databricks-connect` usando credenciais do `.env`

### `environment.py` — Resolução de Ambiente

```python
from utils.environment import get_environment

env = get_environment(default="prod")  # "dev" ou "prod"
```

**Ordem de prioridade:**
1. Argumento `--environment` passado pelo job Databricks
2. Variável de ambiente `ENVIRONMENT` (do `.env`)
3. Default: `"prod"`

### `watermark_control.py` — Controle de Processamento Incremental

```python
from utils.watermark_control import get_watermark, update_watermark

# Lê o watermark atual
watermark = get_watermark(spark, "gold_dim_clientes")

# Atualiza após processamento bem-sucedido
update_watermark(
    spark,
    nome_pipeline="gold_dim_clientes",
    novo_watermark=data_max_processada,
    qtd_registros=df.count(),
    status="SUCESSO"
)
```

- Usa Delta MERGE para garantir upsert idempotente
- Uma única tabela `watermark_pipelines` serve todos os pipelines
- Armazena: nome, data máxima processada, timestamp de atualização, usuário, quantidade de registros e status

---

## Orquestração dos Jobs

Os jobs são definidos em YAML e implantados pelo Databricks Asset Bundle como `spark_python_task`.

### Jobs do Domínio Vendas

| Job | Script | Descrição |
|-----|--------|-----------|
| `gold_dim_tempo_job` | `gold_dim_tempo.py` | Dimensão data com nomes em PT-BR (2015–2030) |
| `gold_dim_cfop_job` | `gold_dim_cfop.py` | Códigos CFOP com descrições fiscais |
| `gold_dim_clientes_job` | `gold_dim_clientes.py` | Dimensão clientes com classificação regional |
| `gold_dim_produtos_job` | `gold_dim_produtos.py` | Dimensão produtos |
| `gold_dim_transportadoras_job` | `gold_dim_transportadoras.py` | Dimensão transportadoras |
| `gold_fato_faturamento_job` | `gold_fato_faturamento.py` | Fato faturamento (sk_tempo, sk_cliente, sk_produto) |
| `gold_fato_impostos_detalhados_job` | `gold_fato_impostos_detalhados.py` | Fato impostos por item |
| `gold_fato_logistica_transporte_job` | `gold_fato_logistica_transporte.py` | Fato logística por nota |

### Jobs do Domínio Marketing

| Job | Script | Descrição |
|-----|--------|-----------|
| `gold_dim_campanha_job` | `gold_dim_campanha.py` | Dimensão campanhas com SCD Tipo 2 |
| `gold_load_marketing_job` | `gold_load_marketing.py` | Fatos de investimento + atribuição cross-domain |

### Configuração dos Jobs (`resources/*.yml`)

```yaml
jobs:
  - name: gold_dim_clientes_job
    tasks:
      - task_key: run_gold_dim_clientes
        spark_python_task:
          python_file: ../src/sales_domain/gold/gold_dim_clientes.py
          parameters:
            - "--environment"
            - "${var.environment}"
        existing_cluster_id: ${var.sales_cluster_id}
        libraries:
          - whl: ../dist/*.whl    # Pacote utils
```

---

## Infraestrutura como Código (Terraform)

### Governança Global (`infra/live/global-governance/`)

Provisionado uma única vez, compartilhado entre todos os domínios:

| Recurso | Nome / Descrição |
|---------|-----------------|
| Resource Group | `rg-datamesh-governance-prod` |
| Storage Account | `stucrootdatameshprod` (ADLS Gen2, HNS habilitado) |
| Access Connector | Identidade gerenciada Databricks para Unity Catalog |
| Storage Credential | Liga a identidade gerenciada ao metastore |
| Metastore ID | `0e36e8d6-6803-497f-9102-f2af71bec95e` |

### Módulo de Domínio (`infra/modules/databricks_domain_config/`)

Módulo reutilizável parametrizado por `domain_name` e `environment`:

```hcl
module "sales_prod" {
  source       = "../../modules/databricks_domain_config"
  domain_name  = "sales"
  environment  = "prod"
  manage_cluster = true
}
```

**Recursos provisionados por instância:**

| Recurso | Nomenclatura |
|---------|-------------|
| Storage Account | `st{domain}datamesh{env}` |
| External Location | Liga o storage ao Databricks |
| Catalog | `{domain}_{env}` (ex: `sales_prod`) |
| Schemas | `bronze`, `silver`, `gold`, `controle` |
| Cluster | `srv-{domain}-spark-shared-dev` (USER_ISOLATION, 1 worker, auto-off 20min) |

### Ambientes de Domínio

Cada domínio tem dois ambientes isolados:

```
sales_domain/
├── prod/     → storage st + catalog sales_prod + cluster próprio
└── dev/      → storage separado + catalog sales_dev (reutiliza cluster do prod)
```

### Rede (`infra/live/network-core/`)

- VNets por domínio com sub-redes pública e privada
- Faixas CIDR isoladas (ex: `10.1.0.0/16` para Vendas)

---

## CI/CD com GitHub Actions

### Pipeline de CI (`.github/workflows/ci.yml`)

**Gatilhos:** PR para `main` ou `desenv_local`, push para `desenv_local`

```
┌──────────────────────────────────────────────┐
│               lint-and-test                  │
│                                              │
│  ruff check src/ utils/                      │
│  pytest tests/ -v --cov=utils                │
└────────────────────┬─────────────────────────┘
                     │ sucesso
                     ▼
┌──────────────────────────────────────────────┐
│              build-and-validate              │
│                                              │
│  pip wheel . --no-deps -w dist               │
│  databricks bundle validate --target dev     │
└──────────────────────────────────────────────┘
```

### Pipeline de CD — Dev (`.github/workflows/cd-dev.yml`)

**Gatilho:** Push para `desenv_local`

```
Build wheel → Install Databricks CLI → databricks bundle deploy --target dev
```

### Pipeline de CD — Prod (`.github/workflows/cd-prod.yml`)

**Gatilho:** Push para `main`  
**Requisito:** Aprovação manual via GitHub Environment `prod`

```
Build wheel → Validate bundle (prod) → [APROVAÇÃO MANUAL] → databricks bundle deploy --target prod
```

### Secrets necessários no repositório

```
DATABRICKS_HOST          # Host do workspace Databricks
DATABRICKS_TOKEN         # Token de acesso pessoal (PAT)
SALES_CLUSTER_ID         # ID do cluster do domínio Vendas
MARKETING_CLUSTER_ID     # ID do cluster do domínio Marketing
```

---

## Testes e Qualidade de Código

### Testes Unitários (`tests/`)

```bash
pytest tests/ -v --cov=utils --cov-report=term-missing
```

| Arquivo | O que testa |
|---------|-------------|
| `test_connections.py` | Detecção de ambiente Databricks, criação de sessão local vs. cluster, tratamento de variáveis ausentes |
| `test_environment.py` | Prioridade de resolução de ambiente (arg CLI > env var > default) |

### Linting com Ruff

```bash
ruff check src/ utils/
```

**Regras ativas:** `E` (erros de sintaxe), `F` (imports/undefined), `W` (warnings), `I` (ordenação de imports)  
**Comprimento de linha:** 100 caracteres  
**Target:** Python 3.12

---

## Configuração do Ambiente

### Pré-requisitos

- Python 3.12+
- Databricks CLI v0.200+
- Terraform 1.x (para provisionamento de infra)
- Acesso a um workspace Databricks com Unity Catalog habilitado

### Instalação

```bash
# Clonar o repositório
git clone https://github.com/seu-usuario/databricks-datamesh-infra.git
cd databricks-datamesh-infra

# Criar ambiente virtual e instalar dependências
python -m venv .venv
source .venv/bin/activate          # Linux/macOS
# .venv\Scripts\activate           # Windows

pip install -r requirements.txt
pip install -e .                   # Instala o pacote utils em modo editável
```

### Variáveis de Ambiente

Copie o `.env.example` e preencha com suas credenciais:

```bash
cp .env.example .env
```

```dotenv
# Domínio Vendas
SALES_DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
SALES_DATABRICKS_TOKEN=dapi...
SALES_DATABRICKS_CLUSTER_ID=xxxx-xxxxxx-xxxxxxxx

# Domínio Marketing
MARKETING_DATABRICKS_HOST=https://adb-yyyy.azuredatabricks.net
MARKETING_DATABRICKS_TOKEN=dapi...
MARKETING_DATABRICKS_CLUSTER_ID=yyyy-yyyyyy-yyyyyyyy

ENVIRONMENT=dev
```

---

## Como Executar Localmente

### Executar um pipeline individualmente

```bash
# Bronze — Ingestão de notas fiscais
python src/sales_domain/bronze/bronze_ingest_notas.py --environment dev

# Silver — Limpeza e validação
python src/sales_domain/silver/silver_clean_notas.py --environment dev

# Gold — Dimensões
python src/sales_domain/gold/gold_dim_clientes.py --environment dev
python src/sales_domain/gold/gold_dim_tempo.py --environment dev

# Gold — Fatos
python src/sales_domain/gold/gold_fato_faturamento.py --environment dev
```

### Executar os testes

```bash
pytest tests/ -v --cov=utils --cov-report=term-missing
```

### Validar o bundle Databricks

```bash
databricks bundle validate --target dev
```

---

## Como Fazer Deploy

### Build do pacote Python

```bash
pip wheel . --no-deps -w dist
```

### Deploy para Dev

```bash
databricks bundle deploy --target dev
```

### Deploy para Prod

```bash
databricks bundle deploy --target prod
```

### Executar um job manualmente após o deploy

```bash
databricks bundle run --target dev gold_dim_clientes_job
databricks bundle run --target prod gold_fato_faturamento_job
```

---

## Fluxo de Dados Fim a Fim

```
┌─────────────────────────────────────────────────────────────────┐
│                        DOMÍNIO VENDAS                           │
│                                                                  │
│  [Fonte]  →  [Bronze]  →  [Silver]  →  [Gold Dimensões]         │
│                                    ↘  [Gold Fatos]              │
│                                                                  │
│  NF-e mock    bronze_ingest_notas     silver_clean_notas         │
│  (1.000 reg.)  ├─ nota_cabecalho       ├─ validação CFOP         │
│                ├─ nota_itens           ├─ validação UF           │
│                ├─ nota_impostos        ├─ deduplicação           │
│                └─ nota_transporte      └─ quarentena inválidos   │
│                                                                  │
│                                         dim_tempo  (5.479 dias) │
│                                         dim_cfop                │
│                                         dim_clientes  ──────────┼──► Marketing
│                                         dim_produtos            │
│                                         dim_transportadoras     │
│                                         fato_faturamento        │
│                                         fato_impostos_det.      │
│                                         fato_logistica_transp.  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      DOMÍNIO MARKETING                           │
│                                                                  │
│  Campanhas mock  →  bronze_invest.  →  silver_clean_marketing    │
│  (30 dias,              │                   │                    │
│   4 canais,             ▼                   ▼                    │
│   10 campanhas)   investimento       dim_campanha (SCD2)        │
│                   _marketing         └─ versões históricas      │
│                                                                  │
│                                      fato_investimento_mkt      │
│                                      fato_atribuicao_conversao  │
│                                      (+ dim_clientes de Vendas) │
└─────────────────────────────────────────────────────────────────┘

Controle incremental (todos os pipelines Gold):
watermark_pipelines → lê último ponto → filtra Silver → MERGE no Gold → atualiza watermark
```

---

## Branches e Ambientes

| Branch | Ambiente | Deploy |
|--------|----------|--------|
| `desenv_local` | `dev` | Automático (CD) |
| `main` | `prod` | Manual com aprovação |

---

## Contribuição

1. Crie um branch a partir de `desenv_local`
2. Implemente a feature ou correção
3. Execute os testes: `pytest tests/ -v`
4. Execute o linter: `ruff check src/ utils/`
5. Abra um Pull Request para `desenv_local`
6. Após aprovação e merge em `main`, faça o deploy para produção com aprovação manual no GitHub Actions
