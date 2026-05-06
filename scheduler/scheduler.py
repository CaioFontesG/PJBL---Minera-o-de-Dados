"""Scheduler — dispara tarefas de coleta a cada N minutos via APScheduler + RQ."""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from redis import Redis
from rq import Queue

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

QUEUE_NAME = "bgp_collection"


def _enqueue_job(task_name: str, queue: Queue) -> None:
    """Enfileira uma tarefa de coleta no Redis via RQ."""
    job = queue.enqueue(
        f"worker.tasks.{task_name}",
        job_timeout=3600,  # 1 hora máxima por job
        job_id=f"{task_name}_{int(__import__('time').time())}",
    )
    logger.info("Job enfileirado: %s (id=%s)", task_name, job.id)


def main() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    redis_conn = Redis.from_url(settings.redis_url)
    queue = Queue(QUEUE_NAME, connection=redis_conn)

    scheduler = BlockingScheduler(timezone="UTC")
    collection_minute = settings.collection_minute

    # Descoberta de mitigadores — roda imediatamente no startup e depois semanalmente
    scheduler.add_job(
        _enqueue_job,
        trigger=IntervalTrigger(weeks=1),
        args=["run_discover_mitigators", queue],
        id="discover_mitigators",
        name="Descoberta de mitigadores",
        next_run_time=datetime.utcnow(),
    )

    # Coleta RIPE — dispara nos minutos configurados em COLLECTION_MINUTE
    scheduler.add_job(
        _enqueue_job,
        trigger=CronTrigger(minute=collection_minute),
        args=["run_ripe", queue],
        id="ripe",
        name="Coletor RIPE RIS",
        next_run_time=datetime.utcnow(),
    )

    # Escreve PID para healthcheck do Docker
    Path("/tmp/scheduler.pid").write_text(str(os.getpid()))

    logger.info(
        "Scheduler iniciado. Coleta nos minutos: %s. Jobs: discover_mitigators (startup+semanal), ripe",
        collection_minute,
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler encerrado.")


if __name__ == "__main__":
    main()
