import argparse
import os

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

    return os.environ.get("ENVIRONMENT", default)