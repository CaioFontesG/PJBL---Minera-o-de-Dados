from __future__ import annotations

import importlib

import shared.config as config_module


def test_get_settings_is_cached() -> None:
    config_module.get_settings.cache_clear()

    first = config_module.get_settings()
    second = config_module.get_settings()

    assert first is second


def test_postgres_and_asyncpg_dsn_from_env(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "db")
    monkeypatch.setenv("POSTGRES_PORT", "6543")
    monkeypatch.setenv("POSTGRES_DB", "bgp_test")
    monkeypatch.setenv("POSTGRES_USER", "tester")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    module = importlib.reload(config_module)
    module.get_settings.cache_clear()
    settings = module.get_settings()

    expected = "postgresql://tester:secret@db:6543/bgp_test"
    assert settings.postgres_dsn == expected
    assert settings.asyncpg_dsn == expected
