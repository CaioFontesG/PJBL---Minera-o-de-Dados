"""Coletor de dados BGP via bgp.tools + RIPE RIS.

Fluxo:
  1. bgp.tools/table.jsonl  → lista todos os prefixos do ASN monitorado
  2. RIPE routing-status    → AS-PATH de cada prefixo /24 encontrado
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.cleaner import clean_routes
from pipeline.loader import create_job, finish_job, get_pool, upsert_routes
from shared.asn_repo import get_mitigator_asns, get_owner_asns
from shared.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

BGPTOOLS_TABLE = "https://bgp.tools/table.jsonl"
RIPE_ROUTING = "https://stat.ripe.net/data/routing-status/data.json"

HEADERS = {
    "User-Agent": "BGP-Mining-Bot/1.0 (academic research - UNDB)",
    "Accept": "application/json",
}


async def _get(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    delay = settings.http_delay_seconds
    for attempt in range(1, settings.max_retries + 1):
        try:
            resp = await client.get(
                url, params=params, timeout=settings.http_timeout_seconds
            )
            if resp.status_code == 200:
                await asyncio.sleep(delay)
                return resp.json()
            if resp.status_code == 429:
                wait = float(resp.headers.get("Retry-After", delay * (2**attempt)))
                logger.warning("Rate limited. Aguardando %.1fs...", wait)
                await asyncio.sleep(wait)
                continue
            logger.warning("HTTP %d para %s", resp.status_code, url)
            return None
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            wait = delay * (2**attempt)
            logger.warning(
                "Tentativa %d/%d falhou: %s. Retry em %.1fs",
                attempt,
                settings.max_retries,
                exc,
                wait,
            )
            await asyncio.sleep(wait)
    logger.error("Todas as tentativas falharam para %s", url)
    return None


async def _fetch_prefixes_from_bgptools(
    client: httpx.AsyncClient, asn: int
) -> list[str]:
    logger.info("Baixando tabela bgp.tools para ASN %d...", asn)
    try:
        async with client.stream("GET", BGPTOOLS_TABLE, timeout=120) as resp:
            if resp.status_code != 200:
                logger.warning("bgp.tools retornou HTTP %d", resp.status_code)
                return []

            import json as _json

            prefixes: list[str] = []
            async for line in resp.aiter_lines():
                if not line:
                    continue
                try:
                    row = _json.loads(line)
                    if row.get("ASN") == asn and row.get("CIDR", "").endswith("/24"):
                        prefixes.append(row["CIDR"])
                except Exception:
                    continue

    except Exception as exc:
        logger.error("Erro ao baixar tabela bgp.tools: %s", exc)
        return []

    logger.info(
        "bgp.tools: %d prefixos /24 encontrados para ASN %d", len(prefixes), asn
    )
    return prefixes


async def _fetch_routing_status(
    client: httpx.AsyncClient, prefix: str, origin_asn: int
) -> list[dict[str, Any]]:
    data = await _get(client, RIPE_ROUTING, {"resource": prefix})
    if not data:
        return [
            {
                "prefix": prefix,
                "origin_asn": origin_asn,
                "as_path": str(origin_asn),
                "communities": [],
                "source": "bgpview",
                "raw_data": {"bgptools": True, "ripe_status": None},
            }
        ]

    routes: list[dict[str, Any]] = []
    try:
        announcing = data.get("data", {}).get("announcing_asns", [])
        for ann in announcing:
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
                    "source": "bgpview",
                    "raw_data": {
                        "bgptools": True,
                        "announcing_asn": ann_asn,
                        "ripe_data": data.get("data", {}),
                    },
                }
            )
        if not announcing:
            routes.append(
                {
                    "prefix": prefix,
                    "origin_asn": origin_asn,
                    "as_path": str(origin_asn),
                    "communities": [],
                    "source": "bgpview",
                    "raw_data": {"bgptools": True, "ripe_data": data.get("data", {})},
                }
            )
    except Exception as exc:
        logger.error("Erro ao parsear routing-status para %s: %s", prefix, exc)

    return routes


async def collect_bgptools() -> None:
    """Pipeline: bgp.tools → prefixos /24 → RIPE AS-PATH → banco."""
    pool = await get_pool()
    owner_asns = await get_owner_asns(pool)
    mitigator_asns = await get_mitigator_asns(pool)

    if not owner_asns:
        logger.warning("Nenhum ASN owner cadastrado. Cadastre ASNs no dashboard.")
        await pool.close()
        return

    job_id = await create_job(pool, "bgpview")
    total_inserted = 0
    error_msg: str | None = None

    try:
        async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
            for asn in owner_asns:
                prefixes = await _fetch_prefixes_from_bgptools(client, asn)

                for prefix in prefixes:
                    routes = await _fetch_routing_status(client, prefix, asn)
                    clean = clean_routes(routes, mitigator_asns)
                    if clean:
                        inserted = await upsert_routes(pool, clean, job_id)
                        total_inserted += inserted

    except Exception as exc:
        error_msg = str(exc)
        logger.exception("Erro crítico no coletor bgp.tools: %s", exc)
    finally:
        await finish_job(pool, job_id, total_inserted, error_msg)
        await pool.close()

    logger.info("Coleta bgp.tools finalizada. Total inserido: %d", total_inserted)


if __name__ == "__main__":
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    asyncio.run(collect_bgptools())
