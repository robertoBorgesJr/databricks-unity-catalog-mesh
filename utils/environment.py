import argparse
import os

 
def _is_running_on_databricks() -> bool:
    """
    Detecta se o código está executando nativamente dentro de um cluster
    Databricks (notebook ou job) — variável definida automaticamente pelo
    próprio runtime, ausente em execução local.
    """
    return "DATABRICKS_RUNTIME_VERSION" in os.environ
 
 
def _get_dbutils():
    """
    Recupera o objeto 'dbutils' já injetado no namespace do notebook em
    execução, sem precisar importá-lo diretamente (o que falharia fora do
    contexto de notebook). Retorna None se não for encontrado (execução
    local, ou dbutils indisponível por qualquer motivo).
    """
    try:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]
    except Exception:
        return None
 
def get_environment(default: str = "prod") -> str:
    """
    Resolve o ambiente (prefixo do catalogo: 'prod' ou 'dev') a ser usado
    pelo pipeline, seguindo esta ordem de prioridade:

      1. Argumento de linha de comando --environment
         (usado quando o script roda como Job/Task no Databricks via
         spark_python_task, passando parameters=["--environment", "dev"] --
         é o mecanismo que o DAB usa para parametrizar por target)
      2. Variável de ambiente ENVIRONMENT
         (usada na execução local, definida no .env via python-dotenv)
      3. Valor default = 'prod'
         (nunca cai silenciosamente em dev por falta de configuração --
          seguro por padrão)
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--environment", type=str, default=None)
    # parse_known_args ignora argumentos extras (ex: os que o próprio
    # Databricks injeta), evitando conflito com outros parsers do pipeline.
    args, _ = parser.parse_known_args()

    if args.environment:
        return args.environment
    
    if _is_running_on_databricks():
        dbutils = _get_dbutils()
        if dbutils is not None:
            try:
                valor = dbutils.widgets.get("environment")
                if valor:
                    return valor
            except Exception:
               # Widget 'environment' não existe nesta execução
                # (ex: rodando notebook manualmente sem parâmetro) -- segue
                # para os próximos fallbacks normalmente.
                pass                

    return os.environ.get("ENVIRONMENT", default)