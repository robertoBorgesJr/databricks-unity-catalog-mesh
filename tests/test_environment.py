import os
import sys
import pytest

from utils.environment import get_environment


def test_default_returns_prod():
    # Sem args de CLI e sem variável de ambiente, retorna 'prod'
    os.environ.pop("ENVIRONMENT", None)
    result = get_environment()
    assert result == "prod"


def test_env_var_overrides_default(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "dev")
    result = get_environment()
    assert result == "dev"


def test_cli_arg_overrides_env_var(monkeypatch):
    monkeypatch.setenv("ENVIRONMENT", "prod")
    monkeypatch.setattr(sys, "argv", ["prog", "--environment", "dev"])
    result = get_environment()
    assert result == "dev"


def test_custom_default(monkeypatch):
    os.environ.pop("ENVIRONMENT", None)
    monkeypatch.setattr(sys, "argv", ["prog"])
    result = get_environment(default="dev")
    assert result == "dev"


def test_invalid_environment_value(monkeypatch):
    # O módulo não valida o valor — apenas retorna o que foi passado
    monkeypatch.setenv("ENVIRONMENT", "staging")
    result = get_environment()
    assert result == "staging"
