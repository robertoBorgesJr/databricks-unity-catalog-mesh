import os

def _is_running_on_databricks() -> bool:
    """
    Detecta se o código está sendo executado dentro de um cluster Databricks.
    Variável definida automaticamente pelo Databricks Runtime, ausente em ambientes locais.
    """
    return "DATABRICKS_RUNTIME_VERSION" in os.environ

def get_spark_session(domain: str):
    """
    Retorna uma SparkSession válida tanto rodando localmente via Databricks Connect 
    quanto nativamente dentro de um cluster Databricks.

    domain: 'SALES' ou 'MARKETING'
    """
    if _is_running_on_databricks():
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()
    
    from dotenv import load_dotenv
    from databricks.connect import DatabricksSession

    load_dotenv()
    
    prefix = domain.upper()
    try:
        host = os.environ[f"{prefix}_DATABRICKS_HOST"]
        token = os.environ[f"{prefix}_DATABRICKS_TOKEN"]
        cluster_id = os.environ[f"{prefix}_DATABRICKS_CLUSTER_ID"]
    except KeyError as e:
        raise EnvironmentError(
            f"Variável de ambiente ausente para o domínio '{domain}': {str(e)}. "
            f"Confira o arquivo .env"
        )

    return DatabricksSession.builder.remote(
        host=host,
        token=token,
        cluster_id=cluster_id
    ).create()