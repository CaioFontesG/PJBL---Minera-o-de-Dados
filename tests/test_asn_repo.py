"""Testes unitários para shared/asn_repo.py — consultas ao banco mockadas."""

from __future__ import annotations

from shared.asn_repo import get_mitigator_asns, get_owner_asns


# ── Fake DB helpers ───────────────────────────────────────────────────────────


class _FakeConn:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    async def fetch(self, *args) -> list[dict]:
        return self._rows


class _FakePool:
    def __init__(self, rows: list[dict]) -> None:
        self._conn = _FakeConn(rows)

    def acquire(self) -> "_FakePool":
        return self

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *args) -> None:
        pass


# ── get_owner_asns ─────────────────────────────────────────────────────────────


async def test_get_owner_asns_returns_list_of_ints() -> None:
    pool = _FakePool([{"asn": 64500}, {"asn": 64501}, {"asn": 64502}])
    result = await get_owner_asns(pool)
    assert result == [64500, 64501, 64502]


async def test_get_owner_asns_empty_table_returns_empty_list() -> None:
    pool = _FakePool([])
    result = await get_owner_asns(pool)
    assert result == []


async def test_get_owner_asns_single_row() -> None:
    pool = _FakePool([{"asn": 12345}])
    result = await get_owner_asns(pool)
    assert result == [12345]


async def test_get_owner_asns_returns_plain_list_not_frozenset() -> None:
    pool = _FakePool([{"asn": 64500}])
    result = await get_owner_asns(pool)
    assert isinstance(result, list)


# ── get_mitigator_asns ────────────────────────────────────────────────────────


async def test_get_mitigator_asns_returns_frozenset() -> None:
    pool = _FakePool([{"asn": 13335}, {"asn": 20940}])
    result = await get_mitigator_asns(pool)
    assert isinstance(result, frozenset)
    assert result == frozenset({13335, 20940})


async def test_get_mitigator_asns_empty_table_returns_empty_frozenset() -> None:
    pool = _FakePool([])
    result = await get_mitigator_asns(pool)
    assert result == frozenset()


async def test_get_mitigator_asns_single_entry() -> None:
    pool = _FakePool([{"asn": 262254}])
    result = await get_mitigator_asns(pool)
    assert 262254 in result


async def test_get_mitigator_asns_supports_membership_test() -> None:
    """frozenset deve suportar verificação de pertencimento eficientemente."""
    pool = _FakePool([{"asn": 13335}, {"asn": 20940}, {"asn": 262254}])
    result = await get_mitigator_asns(pool)
    assert 13335 in result
    assert 20940 in result
    assert 262254 in result
    assert 99999 not in result
