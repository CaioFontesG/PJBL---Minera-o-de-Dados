"""Tarefas RQ executadas pelos workers.

Cada função aqui é referenciada pelo scheduler e executada de forma síncrona
pelo RQ (que roda o asyncio.run internamente).
"""

from __future__ import annotations

import asyncio
import logging

logger = logging.getLogger(__name__)


def run_ripe() -> None:
    """Executa o coletor RIPE RIS."""
    from collectors.ripe.collector import collect_ripe

    logger.info("Iniciando tarefa: ripe")
    asyncio.run(collect_ripe())
    logger.info("Tarefa concluída: ripe")


def run_bgptools() -> None:
    """Executa o coletor bgp.tools."""
    from collectors.bgptools.collector import collect_bgptools

    logger.info("Iniciando tarefa: bgptools")
    asyncio.run(collect_bgptools())
    logger.info("Tarefa concluída: bgptools")


def run_discover_mitigators() -> None:
    """Descobre e cadastra ASNs mitigadores a partir dos upstreams dos owners."""
    from pipeline.loader import get_pool
    from shared.mitigator_discovery import discover_and_save

    async def _run() -> None:
        pool = await get_pool()
        try:
            count = await discover_and_save(pool)
            logger.info("Descoberta de mitigadores: %d novos inseridos.", count)
        finally:
            await pool.close()

    logger.info("Iniciando tarefa: discover_mitigators")
    asyncio.run(_run())
    logger.info("Tarefa concluída: discover_mitigators")
