"""Coletor de dados BGP via RIPE RIS REST API.

Endpoints usados:
  - announced-prefixes: prefixos anunciados por ASN
  - routing-status:     detalhes de roteamento de um prefixo
"""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.cleaner import clean_routes
from pipeline.loader import (
    create_job,
    create_mitigation_snapshot,
    finish_job,
    get_pool,
    rebuild_attack_events,
    upsert_routes,
)
from shared.asn_repo import get_mitigator_asns, get_owner_asns
from shared.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

RIPE_STAT_BASE = "https://stat.ripe.net/data"
RIPE_ANNOUNCED = f"{RIPE_STAT_BASE}/announced-prefixes/data.json"
RIPE_ROUTING = f"{RIPE_STAT_BASE}/routing-status/data.json"


async def _get_with_retry(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any],
    max_retries: int = 5,
) -> dict[str, Any] | None:
    """Requisição GET com retry exponencial. Respeita rate limit (delay fixo)."""
    delay = settings.http_delay_seconds
    for attempt in range(1, max_retries + 1):
        try:
            resp = await client.get(
                url, params=params, timeout=settings.http_timeout_seconds
            )
            if resp.status_code == 200:
                await asyncio.sleep(delay)
                return resp.json()
            if resp.status_code == 429:
                wait = delay * (2**attempt)
                logger.warning("Rate limited por RIPE. Aguardando %.1fs...", wait)
                await asyncio.sleep(wait)
                continue
            logger.warning("HTTP %d para %s params=%s", resp.status_code, url, params)
            return None
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            wait = delay * (2**attempt)
            logger.warning(
                "Tentativa %d/%d falhou: %s. Retry em %.1fs",
                attempt,
                max_retries,
                exc,
                wait,
            )
            await asyncio.sleep(wait)

    logger.error("Todas as %d tentativas falharam para %s", max_retries, url)
    return None


async def _fetch_prefixes_for_asn(client: httpx.AsyncClient, asn: int) -> list[str]:
    """Retorna lista de prefixos anunciados por um ASN via RIPE RIS."""
    data = await _get_with_retry(
        client,
        RIPE_ANNOUNCED,
        {"resource": f"AS{asn}", "min_peers_seeing": 3},
    )
    if not data:
        return []

    prefixes: list[str] = []
    try:
        for entry in data.get("data", {}).get("prefixes", []):
            prefix = entry.get("prefix", "")
            if prefix:
                prefixes.append(prefix)
    except (KeyError, TypeError) as exc:
        logger.error("Erro ao parsear prefixos do ASN %d: %s", asn, exc)

    logger.info("ASN %d: %d prefixos encontrados no RIPE RIS", asn, len(prefixes))
    return prefixes


async def _fetch_routing_status(
    client: httpx.AsyncClient, prefix: str
) -> dict[str, Any] | None:
    return await _get_with_retry(client, RIPE_ROUTING, {"resource": prefix})


def _extract_routes_from_status(
    prefix: str, status_data: dict[str, Any], origin_asn: int
) -> list[dict[str, Any]]:
    routes: list[dict[str, Any]] = []
    try:
        data = status_data.get("data", {})
        announcing_asns = data.get("announcing_asns", [])

        for ann in announcing_asns:
            ann_asn = ann.get("asn") or origin_asn
            as_path = (
                f"{ann_asn} {origin_asn}" if ann_asn != origin_asn else str(origin_asn)
            )
            routes.append(
                {
                    "prefix": prefix,
                    "origin_asn": origin_asn,
                    "as_path": as_path,
                    "communities": [],
                    "source": "ripe",
                    "raw_data": {
                        "ripe_status": data,
                        "announcing_asn": ann_asn,
                    },
                }
            )

        if not announcing_asns:
            routes.append(
                {
                    "prefix": prefix,
                    "origin_asn": origin_asn,
                    "as_path": str(origin_asn),
                    "communities": [],
                    "source": "ripe",
                    "raw_data": {"ripe_status": data},
                }
            )
    except Exception as exc:
        logger.error("Erro ao extrair rotas de routing-status para %s: %s", prefix, exc)

    return routes


async def collect_ripe() -> None:
    """Pipeline: busca prefixos de cada ASN owner → routing-status → limpeza → banco."""
    pool = await get_pool()
    owner_asns = await get_owner_asns(pool)
    mitigator_asns = await get_mitigator_asns(pool)

    if not owner_asns:
        logger.warning("Nenhum ASN owner cadastrado. Cadastre ASNs no dashboard.")
        await pool.close()
        return

    job_id = await create_job(pool, "ripe")
    collection_ts = datetime.now(datetime.UTC)
    total_inserted = 0
    error_msg: str | None = None

    try:
        async with httpx.AsyncClient(
            headers={"Accept": "application/json"},
            follow_redirects=True,
        ) as client:
            for asn in owner_asns:
                logger.info("Consultando RIPE para ASN %d...", asn)
                prefixes = await _fetch_prefixes_for_asn(client, asn)

                for prefix in prefixes:
                    if "/24" not in prefix:
                        continue

                    status_data = await _fetch_routing_status(client, prefix)
                    if not status_data:
                        continue

                    raw_routes = _extract_routes_from_status(prefix, status_data, asn)
                    clean = clean_routes(raw_routes, mitigator_asns)
                    if clean:
                        inserted = await upsert_routes(
                            pool,
                            clean,
                            job_id,
                            collected_at=collection_ts,
                        )
                        total_inserted += inserted

    except Exception as exc:
        error_msg = str(exc)
        logger.exception("Erro crítico no coletor RIPE: %s", exc)
    finally:
        await finish_job(pool, job_id, total_inserted, error_msg)
        if error_msg is None:
            await create_mitigation_snapshot(pool, "ripe", collection_ts)
            await rebuild_attack_events(pool, "ripe")
        await pool.close()

    logger.info("Coleta RIPE finalizada. Total inserido: %d", total_inserted)


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(collect_ripe())
