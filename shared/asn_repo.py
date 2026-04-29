"""Repositório de ASNs — CRUD no PostgreSQL (asyncpg)."""

from __future__ import annotations

import asyncpg


async def get_owner_asns(pool: asyncpg.Pool) -> list[int]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT asn FROM known_asns WHERE type = 'owner'")
    return [r["asn"] for r in rows]


async def get_mitigator_asns(pool: asyncpg.Pool) -> frozenset[int]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT asn FROM known_asns WHERE type = 'mitigator'")
    return frozenset(r["asn"] for r in rows)
