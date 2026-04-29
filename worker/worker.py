"""Worker RQ — consome tarefas da fila Redis e executa os coletores."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from redis import Redis
from rq import Queue, Worker
from rq.timeouts import JobTimeoutException

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

QUEUE_NAME = "bgp_collection"


def main() -> None:
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    redis_conn = Redis.from_url(settings.redis_url)
    queue = Queue(QUEUE_NAME, connection=redis_conn)

    logger.info("Worker RQ iniciado. Aguardando tarefas na fila '%s'...", QUEUE_NAME)

    worker = Worker([queue], connection=redis_conn)
    worker.work(with_scheduler=False)


if __name__ == "__main__":
    main()
