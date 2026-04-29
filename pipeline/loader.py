"""Inserção de rotas limpas no PostgreSQL via asyncpg."""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Sequence
from datetime import datetime
from itertools import groupby
from pathlib import Path

import asyncpg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.config import get_settings
from shared.models import CleanRoute
from datetime import UTC

logger = logging.getLogger(__name__)
settings = get_settings()


async def get_connection() -> asyncpg.Connection:
    return await asyncpg.connect(settings.asyncpg_dsn)


async def get_pool(min_size: int = 2, max_size: int = 10) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        settings.asyncpg_dsn,
        min_size=min_size,
        max_size=max_size,
    )


async def upsert_routes(
    pool: asyncpg.Pool,
    routes: Sequence[CleanRoute],
    job_id: int | None = None,
    collected_at: datetime | None = None,
) -> int:
    """Insere rotas em lote no banco.

    O insert usa ON CONFLICT DO NOTHING para ignorar duplicatas quando
    houver restricoes unicas aplicaveis no banco.

    Returns:
        Número de linhas inseridas com sucesso.
    """
    if not routes:
        return 0

    records = [
        (
            r.prefix,
            r.origin_asn,
            r.mitigator_asn,
            r.as_path,
            r.communities,
            r.source,
            r.is_mitigated,
            json.dumps(r.raw_data),
            collected_at or datetime.now(UTC),
        )
        for r in routes
    ]

    sql = """
        INSERT INTO bgp_routes
            (prefix, origin_asn, mitigator_asn, as_path, communities,
             source, is_mitigated, raw_data, collected_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT DO NOTHING
    """

    async with pool.acquire() as conn:
        await conn.executemany(sql, records)

    # asyncpg.executemany não retorna contagem de linhas inseridas;
    # usamos len(routes) como estimativa (inclui possíveis conflitos ignorados).
    inserted = len(routes)

    logger.info("Inseridas %d rotas no banco (de %d tentadas)", inserted, len(routes))

    if job_id is not None:
        await update_job_records(pool, job_id, inserted)

    return inserted


async def create_job(pool: asyncpg.Pool, source: str) -> int:
    """Cria um registro de job de coleta com status 'running'. Retorna o ID."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO collection_jobs (source, status, started_at)
            VALUES ($1, 'running', $2)
            RETURNING id
            """,
            source,
            datetime.now(UTC),
        )
    job_id = row["id"]
    logger.info("Job criado: id=%d source=%s", job_id, source)
    return job_id


async def finish_job(
    pool: asyncpg.Pool,
    job_id: int,
    records_found: int = 0,
    error_msg: str | None = None,
) -> None:
    """Marca o job como 'done' ou 'error'."""
    status = "error" if error_msg else "done"
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE collection_jobs
            SET status        = $1,
                finished_at   = $2,
                records_found = $3,
                error_msg     = $4
            WHERE id = $5
            """,
            status,
            datetime.now(UTC),
            records_found,
            error_msg,
            job_id,
        )
    logger.info(
        "Job finalizado: id=%d status=%s records=%d", job_id, status, records_found
    )


async def update_job_records(pool: asyncpg.Pool, job_id: int, count: int) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            (
                "UPDATE collection_jobs "
                "SET records_found = records_found + $1 "
                "WHERE id = $2"
            ),
            count,
            job_id,
        )


async def create_mitigation_snapshot(
    pool: asyncpg.Pool,
    source: str,
    collection_ts: datetime,
) -> int:
    """Agrega as rotas de uma coleta e salva snapshot de mitigacao."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            INSERT INTO mitigation_snapshots (
                source,
                collection_ts,
                prefix,
                origin_asn,
                total_paths,
                mitigated_paths,
                mitigation_ratio,
                is_fully_mitigated,
                mitigator_asn
            )
            SELECT
                source,
                $2 AS collection_ts,
                prefix,
                origin_asn,
                COUNT(*)::INT AS total_paths,
                COUNT(*) FILTER (WHERE is_mitigated)::INT AS mitigated_paths,
                (
                    COUNT(*) FILTER (WHERE is_mitigated)::NUMERIC
                    / NULLIF(COUNT(*)::NUMERIC, 0)
                ) AS mitigation_ratio,
                BOOL_AND(is_mitigated) AS is_fully_mitigated,
                MIN(mitigator_asn)
                    FILTER (WHERE mitigator_asn IS NOT NULL) AS mitigator_asn
            FROM bgp_routes
            WHERE source = $1
              AND collected_at = $2
            GROUP BY source, prefix, origin_asn
            ON CONFLICT (source, collection_ts, prefix, origin_asn)
            DO UPDATE SET
                total_paths = EXCLUDED.total_paths,
                mitigated_paths = EXCLUDED.mitigated_paths,
                mitigation_ratio = EXCLUDED.mitigation_ratio,
                is_fully_mitigated = EXCLUDED.is_fully_mitigated,
                mitigator_asn = EXCLUDED.mitigator_asn
            RETURNING id
            """,
            source,
            collection_ts,
        )

    inserted = len(rows)
    logger.info(
        "Snapshot de mitigacao salvo: source=%s coleta=%s itens=%d",
        source,
        collection_ts.isoformat(),
        inserted,
    )
    return inserted


def _build_attack_events(
    snapshots: list[asyncpg.Record],
) -> list[tuple[str, str, int, int | None, datetime, datetime | None, int | None]]:
    """Converte snapshots em eventos por transicao para 100% mitigado."""
    events: list[
        tuple[str, str, int, int | None, datetime, datetime | None, int | None]
    ] = []

    def key_fn(row: asyncpg.Record) -> tuple[str, object, int]:
        return (row["source"], row["prefix"], row["origin_asn"])

    for (source, prefix, origin_asn), group in groupby(
        sorted(snapshots, key=key_fn), key=key_fn
    ):
        ordered = sorted(group, key=lambda r: r["collection_ts"])
        open_start: datetime | None = None
        open_mitigator: int | None = None

        for row in ordered:
            current_ts = row["collection_ts"]
            is_full = bool(row["is_fully_mitigated"])

            if is_full and open_start is None:
                open_start = current_ts
                open_mitigator = row["mitigator_asn"]
                continue

            if (not is_full) and open_start is not None:
                duration = int((current_ts - open_start).total_seconds())
                events.append(
                    (
                        source,
                        str(prefix),
                        origin_asn,
                        open_mitigator,
                        open_start,
                        current_ts,
                        duration,
                    )
                )
                open_start = None
                open_mitigator = None

        if open_start is not None:
            events.append(
                (
                    source,
                    str(prefix),
                    origin_asn,
                    open_mitigator,
                    open_start,
                    None,
                    None,
                )
            )

    return events


async def rebuild_attack_events(pool: asyncpg.Pool, source: str) -> int:
    """Reconstroi eventos de ataque para uma fonte com base nos snapshots."""
    async with pool.acquire() as conn:
        snapshots = await conn.fetch(
            """
            SELECT
                source,
                prefix,
                origin_asn,
                mitigator_asn,
                is_fully_mitigated,
                collection_ts
            FROM mitigation_snapshots
            WHERE source = $1
            ORDER BY source, prefix, origin_asn, collection_ts
            """,
            source,
        )

    events = _build_attack_events(list(snapshots))

    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM ddos_attack_events WHERE source = $1", source)
        if events:
            await conn.executemany(
                """
                INSERT INTO ddos_attack_events (
                    source,
                    prefix,
                    origin_asn,
                    mitigator_asn,
                    started_at,
                    ended_at,
                    duration_seconds
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                events,
            )

    logger.info("Eventos reconstruidos: source=%s total=%d", source, len(events))
    return len(events)
