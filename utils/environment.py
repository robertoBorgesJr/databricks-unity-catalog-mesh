import argparse
import os


def get_environment(default: str = "prod") -> str:
    """
    Resolve o ambiente ('prod' ou 'dev'), nesta ordem:
      1. --environment via linha de comando (usado pelo spark_python_task)
      2. Variável de ambiente ENVIRONMENT (.env, execução local)
      3. Default 'prod'
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--environment", type=str, default=None)
    args, _ = parser.parse_known_args()

    if args.environment:
        return args.environment

    return os.environ.get("ENVIRONMENT", default)