FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false

COPY pyproject.toml .
RUN poetry install --only=main,worker --no-root --no-interaction

COPY worker/     ./worker/
COPY collectors/ ./collectors/
COPY pipeline/   ./pipeline/
COPY shared/     ./shared/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

CMD ["python", "worker/worker.py"]
