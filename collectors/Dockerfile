FROM python:3.12-slim

WORKDIR /app

# Dependências de sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Dependências Python
COPY collectors/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código dos coletores e módulos compartilhados
COPY collectors/ ./collectors/
COPY pipeline/   ./pipeline/
COPY shared/     ./shared/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Uso standalone (fora do docker-compose): executa o coletor bgp.tools diretamente.
# Em produção os coletores rodam dentro do container worker via RQ.
CMD ["python", "collectors/bgptools/collector.py"]
