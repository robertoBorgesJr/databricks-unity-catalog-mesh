import os
import pytest

from utils.connections import _is_running_on_databricks


def test_not_on_databricks_by_default():
    # Em CI, a variável DATABRICKS_RUNTIME_VERSION não existe
    os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
    assert _is_running_on_databricks() is False


def test_detected_as_databricks(monkeypatch):
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "14.3.x-scala2.12")
    assert _is_running_on_databricks() is True


def test_get_spark_session_raises_without_env_vars():
    # Fora do Databricks e sem .env, deve lançar erro de importação ou env
    os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
    os.environ.pop("SALES_DATABRICKS_HOST", None)
    os.environ.pop("SALES_DATABRICKS_TOKEN", None)
    os.environ.pop("SALES_DATABRICKS_CLUSTER_ID", None)

    from utils.connections import get_spark_session

    with pytest.raises((EnvironmentError, ImportError, ModuleNotFoundError)):
        get_spark_session("SALES")
