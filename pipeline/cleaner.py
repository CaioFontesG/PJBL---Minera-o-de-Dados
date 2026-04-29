"""Limpeza e normalização de rotas BGP brutas.

Contrato:
    clean_route(raw: dict) -> CleanRoute | None
    Retorna None para dados inválidos que não devem ser inseridos no banco.
"""

from __future__ import annotations

import ipaddress
import logging
import re
import sys
from pathlib import Path
from typing import Any

# Permite importar shared/ de fora do pacote quando executado diretamente
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.models import CleanRoute, RawRoute

logger = logging.getLogger(__name__)

# Regex para capturar sequências de dígitos (ASNs) no AS-PATH
_ASN_RE = re.compile(r"\b(\d+)\b")
# Regex para detectar AS-SET no path (e.g., {1234,5678})
_ASSET_RE = re.compile(r"\{[^}]*\}")


def _parse_as_path(raw_path: str | list[int]) -> list[int]:
    """Converte AS-PATH bruto para lista de inteiros sem prepends nem AS-SETs."""
    if isinstance(raw_path, list):
        path_str = " ".join(str(a) for a in raw_path)
    else:
        path_str = str(raw_path)

    # Remove AS-SETs ({1234,5678})
    path_str = _ASSET_RE.sub("", path_str)

    asns = [int(m) for m in _ASN_RE.findall(path_str)]

    # Remove prepends consecutivos (ex: [1234, 1234, 5678] -> [1234, 5678])
    deduped: list[int] = []
    for asn in asns:
        if not deduped or deduped[-1] != asn:
            deduped.append(asn)

    return deduped


def _parse_communities(raw_communities: list[str] | list[Any]) -> list[str]:
    """Normaliza communities para o formato 'ASN:valor'."""
    result: list[str] = []
    for c in raw_communities:
        c_str = str(c).strip()
        # Aceita formatos: "13335:1000", "13335 1000", "(13335, 1000)"
        parts = re.split(r"[\s:,()]+", c_str)
        parts = [p for p in parts if p.isdigit()]
        if len(parts) == 2:
            result.append(f"{parts[0]}:{parts[1]}")
        elif c_str:
            # Mantém como está se não parsear
            result.append(c_str)
    return result


def _detect_mitigator(
    as_path_list: list[int], mitigator_asns: frozenset[int]
) -> tuple[bool, int | None]:
    """Retorna (is_mitigated, mitigator_asn) verificando o AS-PATH."""
    for asn in as_path_list:
        if asn in mitigator_asns:
            return True, asn
    return False, None


def clean_route(
    raw: dict[str, Any], mitigator_asns: frozenset[int]
) -> CleanRoute | None:
    """Limpa e valida uma rota BGP bruta.

    Args:
        raw: Dicionário com dados brutos do coletor.

    Returns:
        CleanRoute validada ou None se os dados forem inválidos.
    """
    try:
        # 1. Validação básica via Pydantic
        raw_route = RawRoute(**raw)
    except Exception as exc:
        logger.debug("Rota rejeitada na validação inicial: %s | %s", raw, exc)
        return None

    # 2. Validar prefixo e garantir que é /24
    try:
        net = ipaddress.ip_network(raw_route.prefix, strict=False)
    except ValueError:
        logger.debug("Prefixo CIDR inválido: %s", raw_route.prefix)
        return None

    if net.prefixlen != 24:
        logger.debug("Prefixo ignorado (não é /24): %s", raw_route.prefix)
        return None

    # Rejeitar prefixos privados / reservados
    if net.is_private or net.is_loopback or net.is_reserved or net.is_link_local:
        logger.debug("Prefixo reservado ignorado: %s", raw_route.prefix)
        return None

    # 3. Normalizar AS-PATH
    as_path_list = _parse_as_path(raw_route.as_path)
    if not as_path_list:
        logger.debug("AS-PATH vazio para prefixo %s", raw_route.prefix)
        return None

    # 4. Determinar ASN de origem (último hop no path = origem)
    origin_asn = raw_route.origin_asn or as_path_list[-1]

    # 5. Normalizar communities
    communities = _parse_communities(raw_route.communities)

    # 6. Detectar mitigação
    is_mitigated, mitigator_asn = _detect_mitigator(as_path_list, mitigator_asns)

    try:
        clean = CleanRoute(
            prefix=str(net),
            origin_asn=origin_asn,
            mitigator_asn=mitigator_asn,
            as_path=" ".join(str(a) for a in as_path_list),
            as_path_list=as_path_list,
            communities=communities,
            source=raw_route.source,
            is_mitigated=is_mitigated,
            raw_data=raw_route.raw_data or raw,
        )
    except Exception as exc:
        logger.debug("Falha na construção de CleanRoute: %s | %s", raw, exc)
        return None

    return clean


def clean_routes(
    raws: list[dict[str, Any]], mitigator_asns: frozenset[int]
) -> list[CleanRoute]:
    """Processa uma lista de rotas brutas, descartando inválidas."""
    results: list[CleanRoute] = []
    for raw in raws:
        route = clean_route(raw, mitigator_asns)
        if route is not None:
            results.append(route)

    total = len(raws)
    accepted = len(results)
    logger.info(
        "Limpeza concluída: %d/%d rotas aceitas (%.1f%%)",
        accepted,
        total,
        (accepted / total * 100) if total else 0,
    )
    return results
