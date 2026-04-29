"""Descoberta automática de ASNs mitigadores via BGPView upstreams.

Usado em dois contextos:
  - Worker/scheduler (async): discover_and_save(pool, asns)
  - Dashboard (sync):         discover_for_asn_sync(asn) -> list[dict]
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)

RIPE_STAT_BASE = "https://stat.ripe.net/data"
HEADERS = {"User-Agent": "BGP-Mining-Bot/1.0 (academic research - UNDB)"}
DELAY = 0.5  # segundos entre requests

MITIGATION_KEYWORDS = frozenset(
    {
        "ddos",
        "mitigation",
        "scrub",
        "clean pipe",
        "cleantransit",
        "protection",
        "shield",
        "anti-ddos",
        "antiddos",
        "blackhole",
        "rtbh",
        "null route",
        "flowspec",
    }
)

# ASNs conhecidos como mitigadores — detectados pela lista mesmo sem keyword no nome.
# Fontes: BGPView, PeeringDB, LACNIC, análise de AS-PATH de ISPs brasileiros.
KNOWN_MITIGATOR_ASNS: dict[int, str] = {
    # ── Brasileiros ──────────────────────────────────────────────────────
    262254: "Huge Networks",  # maior mitigador DDoS do Brasil
    268696: "UPX Technologies",  # mitigação + trânsito BR
    14840: "Eletronet S.A.",  # backbone + clean pipe BR
    53013: "ANNY Redes Inteligentes",  # mitigação BR
    263138: "Link Layer Telecom",  # scrubbing center BR
    265649: "Layer2 Telecom",  # clean transit BR
    28329: "Brisanet / BRSK",  # backbone nordeste + mitigation
    52573: "NIC.br / IX.br",  # PTT Brasil (trânsito, às vezes path)
    # ── Internacionais com presença no Brasil ─────────────────────────────
    13335: "Cloudflare, Inc.",
    20940: "Akamai Technologies",
    19551: "Imperva / Incapsula",
    3356: "Lumen / Level 3",  # upstream comum com scrubbing
    174: "Cogent Communications",  # upstream com blackhole support
    6939: "Hurricane Electric",  # backbone com RTBH
}


def _is_candidate(asn: int, name: str, description: str) -> bool:
    if asn in KNOWN_MITIGATOR_ASNS:
        return True
    text = (name + " " + description).lower()
    return any(kw in text for kw in MITIGATION_KEYWORDS)


def _known_name(asn: int, fallback: str) -> str:
    return KNOWN_MITIGATOR_ASNS.get(asn) or fallback or f"AS{asn}"


# ── Async (worker) ────────────────────────────────────────────────────────────


async def _get_upstreams_async(client: Any, asn: int) -> "list[dict[str, Any]]":
    try:
        resp = await client.get(
            f"{RIPE_STAT_BASE}/asn-neighbours/data.json",
            params={"resource": f"AS{asn}"},
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        neighbours = resp.json().get("data", {}).get("neighbours", [])
    except Exception as exc:
        logger.warning("Erro ao buscar upstreams de AS%d: %s", asn, exc)
        return []

    return [
        {"asn": n["asn"], "name": "", "description": "", "country": "BR"}
        for n in neighbours
        if n.get("type") == "left" and n.get("asn")
    ]


async def _upsert_mitigator(pool: "Any", asn: int, name: str, country: str) -> bool:
    """Insere mitigador somente se ainda não existe. Retorna True se inserido."""
    async with pool.acquire() as conn:
        result = await conn.execute(
            """
            INSERT INTO known_asns (asn, name, type, country)
            VALUES ($1, $2, 'mitigator', $3)
            ON CONFLICT (asn) DO NOTHING
            """,
            asn,
            name or f"AS{asn}",
            country or "BR",
        )
    return result.split()[-1] != "0"  # "INSERT 0 0" = conflito, "INSERT 0 1" = inserido


async def discover_and_save(
    pool: "Any",
    asns: list[int] | None = None,
) -> int:
    """Descobre mitigadores para os ASNs owners e persiste no banco.

    Args:
        pool: Pool asyncpg já conectado.
        asns: Lista de ASNs owners a pesquisar. None = busca todos do banco.

    Returns:
        Número de novos mitigadores inseridos.
    """
    if asns is None:
        from shared.asn_repo import get_owner_asns

        asns = await get_owner_asns(pool)

    if not asns:
        logger.warning("Nenhum ASN owner para pesquisar mitigadores.")
        return 0

    candidates: dict[int, dict] = {}
    import httpx

    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        for asn in asns:
            logger.debug("Buscando upstreams de AS%d...", asn)
            upstreams = await _get_upstreams_async(client, asn)
            for up in upstreams:
                u_asn = up["asn"]
                if u_asn not in candidates and u_asn not in asns:
                    if _is_candidate(u_asn, up["name"], up["description"]):
                        candidates[u_asn] = up
            await asyncio.sleep(DELAY)

    inserted = 0
    for asn, info in candidates.items():
        name = _known_name(asn, info.get("description") or info.get("name", ""))
        ok = await _upsert_mitigator(pool, asn, name, info.get("country", "BR"))
        if ok:
            inserted += 1
            logger.info("Novo mitigador cadastrado: AS%d — %s", asn, name)

    logger.info(
        "Descoberta concluída: %d candidatos analisados, %d novos inseridos.",
        len(candidates),
        inserted,
    )
    return inserted


# ── Sync (dashboard) ──────────────────────────────────────────────────────────


def discover_for_asn_sync(asn: int) -> list[dict[str, Any]]:
    """Retorna mitigadores candidatos para um ASN (síncrono, para o dashboard)."""
    try:
        resp = requests.get(
            f"{RIPE_STAT_BASE}/asn-neighbours/data.json",
            params={"resource": f"AS{asn}"},
            headers=HEADERS,
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        neighbours = resp.json().get("data", {}).get("neighbours", [])
    except Exception as exc:
        logger.warning("Erro ao buscar upstreams de AS%d: %s", asn, exc)
        return []

    result: list[dict] = []
    for n in neighbours:
        if n.get("type") != "left":
            continue
        u_asn = n.get("asn")
        if u_asn and _is_candidate(u_asn, "", ""):
            result.append(
                {
                    "asn": u_asn,
                    "name": _known_name(u_asn, ""),
                    "country": "BR",
                }
            )
    return result
