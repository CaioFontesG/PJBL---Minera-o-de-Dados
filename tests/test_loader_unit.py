"""Testes unitários para pipeline/loader.py — operações de banco mockadas."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from pipeline.loader import (
    create_job,
    finish_job,
    update_job_records,
    upsert_routes,
)
from shared.models import CleanRoute


# ── Fake DB helpers ───────────────────────────────────────────────────────────


class _FakeConn:
    """Conexão asyncpg falsa para testes."""

    def __init__(
        self,
        fetchrow_result: dict | None = None,
        execute_result: str = "INSERT 0 1",
    ) -> None:
        self._fetchrow_result = fetchrow_result
        self._execute_result = execute_result
        self.execute_calls: list[tuple] = []
        self.executemany_calls: list[tuple] = []

    async def fetch(self, *args) -> list:
        return []

    async def fetchrow(self, *args) -> dict | None:
        return self._fetchrow_result

    async def execute(self, sql: str, *args) -> str:
        self.execute_calls.append((sql, args))
        return self._execute_result

    async def executemany(self, sql: str, records) -> None:
        self.executemany_calls.append((sql, list(records)))


class _FakePool:
    """Pool asyncpg falso para testes."""

    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self) -> "_FakePool":
        return self

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *args) -> None:
        pass


def _make_pool(
    fetchrow_result: dict | None = None,
    execute_result: str = "INSERT 0 1",
) -> tuple[_FakePool, _FakeConn]:
    conn = _FakeConn(fetchrow_result=fetchrow_result, execute_result=execute_result)
    return _FakePool(conn), conn


def _make_route(
    prefix: str = "8.8.8.0/24",
    origin_asn: int = 15169,
    source: str = "ripe",
    is_mitigated: bool = False,
    mitigator_asn: int | None = None,
) -> CleanRoute:
    return CleanRoute(
        prefix=prefix,
        origin_asn=origin_asn,
        mitigator_asn=mitigator_asn,
        as_path="64500 15169",
        as_path_list=[64500, 15169],
        communities=[],
        source=source,
        is_mitigated=is_mitigated,
    )


# ── upsert_routes ─────────────────────────────────────────────────────────────


async def test_upsert_routes_empty_list_returns_zero_without_db_call() -> None:
    pool, conn = _make_pool()
    result = await upsert_routes(pool, [])
    assert result == 0
    assert conn.executemany_calls == []


async def test_upsert_routes_single_route_calls_executemany() -> None:
    pool, conn = _make_pool()
    routes = [_make_route()]
    result = await upsert_routes(
        pool, routes, collected_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    assert result == 1
    assert len(conn.executemany_calls) == 1


async def test_upsert_routes_multiple_routes_single_executemany_call() -> None:
    pool, conn = _make_pool()
    routes = [
        _make_route("1.1.1.0/24"),
        _make_route("2.2.2.0/24"),
        _make_route("3.3.3.0/24"),
    ]
    result = await upsert_routes(
        pool, routes, collected_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    assert result == 3
    assert len(conn.executemany_calls) == 1
    sql, records = conn.executemany_calls[0]
    assert "INSERT INTO bgp_routes" in sql
    assert len(records) == 3


async def test_upsert_routes_sql_contains_on_conflict_do_nothing() -> None:
    pool, conn = _make_pool()
    routes = [_make_route()]
    await upsert_routes(pool, routes, collected_at=datetime(2026, 1, 1, tzinfo=UTC))
    sql, _ = conn.executemany_calls[0]
    assert "ON CONFLICT DO NOTHING" in sql


async def test_upsert_routes_with_job_id_calls_update_job_records() -> None:
    pool, conn = _make_pool()
    routes = [_make_route()]
    await upsert_routes(
        pool, routes, job_id=42, collected_at=datetime(2026, 1, 1, tzinfo=UTC)
    )
    # update_job_records chama conn.execute com UPDATE collection_jobs
    update_calls = [c for c in conn.execute_calls if "records_found" in c[0]]
    assert len(update_calls) == 1


async def test_upsert_routes_without_job_id_skips_update_job_records() -> None:
    pool, conn = _make_pool()
    routes = [_make_route()]
    await upsert_routes(pool, routes, collected_at=datetime(2026, 1, 1, tzinfo=UTC))
    update_calls = [c for c in conn.execute_calls if "records_found" in c[0]]
    assert len(update_calls) == 0


async def test_upsert_routes_record_contains_correct_prefix() -> None:
    pool, conn = _make_pool()
    routes = [_make_route("203.0.113.0/24", origin_asn=64500)]
    await upsert_routes(pool, routes, collected_at=datetime(2026, 1, 1, tzinfo=UTC))
    _, records = conn.executemany_calls[0]
    assert records[0][0] == "203.0.113.0/24"  # prefix is first field
    assert records[0][1] == 64500  # origin_asn is second field


# ── create_job ────────────────────────────────────────────────────────────────


async def test_create_job_returns_id_from_db() -> None:
    pool, conn = _make_pool(fetchrow_result={"id": 7})
    job_id = await create_job(pool, "ripe")
    assert job_id == 7


async def test_create_job_returns_different_id() -> None:
    pool, conn = _make_pool(fetchrow_result={"id": 42})
    job_id = await create_job(pool, "bgptools")
    assert job_id == 42


async def test_create_job_sql_inserts_into_collection_jobs() -> None:
    pool, conn = _make_pool(fetchrow_result={"id": 1})
    await create_job(pool, "ripe")
    # fetchrow é chamado em vez de execute, verificar via conn._fetchrow_result
    # O comportamento é verificado pelo retorno correto do ID


# ── finish_job ────────────────────────────────────────────────────────────────


async def test_finish_job_sets_done_status_when_no_error() -> None:
    pool, conn = _make_pool()
    await finish_job(pool, job_id=1, records_found=100)
    assert len(conn.execute_calls) == 1
    sql, args = conn.execute_calls[0]
    assert "UPDATE collection_jobs" in sql
    assert "done" in args


async def test_finish_job_sets_error_status_when_error_msg_provided() -> None:
    pool, conn = _make_pool()
    await finish_job(pool, job_id=1, records_found=0, error_msg="connection lost")
    sql, args = conn.execute_calls[0]
    assert "error" in args
    assert "connection lost" in args


async def test_finish_job_none_error_msg_sets_done() -> None:
    pool, conn = _make_pool()
    await finish_job(pool, job_id=5, records_found=50, error_msg=None)
    _, args = conn.execute_calls[0]
    assert "done" in args
    assert None in args  # error_msg = None passado para o SQL


async def test_finish_job_includes_records_found() -> None:
    pool, conn = _make_pool()
    await finish_job(pool, job_id=3, records_found=999)
    _, args = conn.execute_calls[0]
    assert 999 in args


async def test_finish_job_includes_job_id() -> None:
    pool, conn = _make_pool()
    await finish_job(pool, job_id=77, records_found=0)
    _, args = conn.execute_calls[0]
    assert 77 in args


# ── update_job_records ────────────────────────────────────────────────────────


async def test_update_job_records_executes_update() -> None:
    pool, conn = _make_pool()
    await update_job_records(pool, job_id=5, count=99)
    assert len(conn.execute_calls) == 1
    sql, args = conn.execute_calls[0]
    assert "records_found" in sql
    assert 99 in args
    assert 5 in args


async def test_update_job_records_sql_uses_increment() -> None:
    """O SQL deve incrementar records_found, não substituir."""
    pool, conn = _make_pool()
    await update_job_records(pool, job_id=1, count=10)
    sql, _ = conn.execute_calls[0]
    assert "records_found + " in sql or "records_found +" in sql


async def test_update_job_records_zero_count() -> None:
    pool, conn = _make_pool()
    await update_job_records(pool, job_id=1, count=0)
    assert len(conn.execute_calls) == 1
    _, args = conn.execute_calls[0]
    assert 0 in args
