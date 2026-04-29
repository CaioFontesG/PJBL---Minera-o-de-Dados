# BGP Mining — Sistema de Análise de Anúncios BGP

Sistema acadêmico (PjBL — 7º período, Mineração de Dados — UNDB) para coletar, limpar, armazenar e analisar rotas BGP de prefixos `/24`, com foco na detecção de redes 100% mitigadas via ASNs de proteção contra DDoS.

## Fontes de Dados

| Fonte | Método | Arquivo |
|---|---|---|
| RIPE RIS / RIPE Stat | REST API (httpx async) | `collectors/ripe/collector.py` |
| bgp.tools + RIPE Stat | REST API (httpx async) | `collectors/bgptools/collector.py` |

## Pré-requisitos

- Docker >= 24
- Docker Compose >= 2.20

## Como subir o ambiente

```bash
# 1. Clone o repositório
git clone <url>
cd bgp-mining

# 2. Configure o ambiente
cp .env.example .env
# Edite .env com suas senhas e configurações

# 3. Suba todos os serviços
DOCKER_BUILDKIT=0 docker compose -f docker-compose.prod.yml up --build -d
```

> **Nota:** `DOCKER_BUILDKIT=0` é necessário em algumas distribuições Linux para evitar um bug de gRPC do BuildKit.

Os logs de cada serviço aparecem no stdout. Para ver apenas um serviço:

```bash
docker compose -f docker-compose.prod.yml logs -f worker
```

## Serviços

| Serviço | Porta | Responsabilidade |
|---|---|---|
| `postgres` | — | Banco relacional |
| `redis` | — | Fila RQ |
| `scheduler` | — | Dispara coletas a cada `COLLECTION_INTERVAL_MINUTES` |
| `worker` | — | Executa os jobs da fila (coletores + descoberta de mitigadores) |
| `dashboard` | 8501 | Visualização, análise e gerenciamento de ASNs |

O dashboard exige senha de acesso (configurada via `DASHBOARD_PASSWORD`).

## Gerenciamento de ASNs

Os ASNs monitorados (owners) e mitigadores são gerenciados pelo dashboard na aba **⚙️ Gerenciar ASNs**. Ao informar um número de ASN, o sistema busca automaticamente nome, país, cidade e estado via BGPView API. ASNs mitigadores associados são detectados automaticamente via RIPE Stat.

## Variáveis de Ambiente

Veja [.env.example](.env.example) para a lista completa. As principais:

| Variável | Padrão | Descrição |
|---|---|---|
| `POSTGRES_PASSWORD` | `changeme` | Senha do PostgreSQL |
| `DASHBOARD_PASSWORD` | `Acesso14` | Senha de acesso ao dashboard |
| `COLLECTION_INTERVAL_MINUTES` | `60` | Intervalo entre coletas (minutos) |
| `HTTP_DELAY_SECONDS` | `1` | Delay entre requisições (rate limit) |
| `LOG_LEVEL` | `INFO` | Nível de log (DEBUG/INFO/WARNING/ERROR) |

## Schema do Banco

O schema é criado automaticamente na primeira inicialização via `db/init.sql`.

```
bgp_routes       — rotas coletadas e limpas
known_asns       — ASNs monitorados e mitigadores (gerenciado pelo dashboard)
collection_jobs  — histórico de execuções de coleta
```

### Consultas úteis

```sql
-- Prefixos 100% mitigados
SELECT prefix, mitigator_asn, source, collected_at
FROM bgp_routes
WHERE is_mitigated = true
ORDER BY collected_at DESC;

-- Contagem por fonte
SELECT source, COUNT(*) FROM bgp_routes GROUP BY source;

-- Top mitigadores
SELECT ka.name, ka.asn, COUNT(r.id) AS prefixes
FROM bgp_routes r
JOIN known_asns ka ON ka.asn = r.mitigator_asn
WHERE r.is_mitigated = true
GROUP BY ka.name, ka.asn
ORDER BY prefixes DESC;

-- Jobs das últimas 24h
SELECT source, status, records_found, started_at, finished_at
FROM collection_jobs
WHERE started_at > NOW() - INTERVAL '24 hours'
ORDER BY started_at DESC;
```

## Estrutura do Projeto

```
bgp-mining/
├── docker-compose.prod.yml
├── docker-compose.dev.yml
├── .env.example
├── pyproject.toml
├── scheduler/
│   ├── Dockerfile
│   └── scheduler.py          # APScheduler + RQ enqueue
├── worker/
│   ├── Dockerfile
│   ├── worker.py             # RQ Worker
│   └── tasks.py              # Tarefas executadas pelo worker
├── collectors/
│   ├── ripe/
│   │   └── collector.py      # RIPE RIS REST API
│   └── bgptools/
│       └── collector.py      # bgp.tools + RIPE routing-status
├── pipeline/
│   ├── cleaner.py            # Limpeza e normalização de rotas
│   └── loader.py             # Inserção no PostgreSQL (asyncpg)
├── db/
│   ├── init.sql              # Schema + seed de ASNs do Maranhão
│   └── migrations/
│       └── migration_001_add_city_state.sql
├── dashboard/
│   ├── Dockerfile
│   └── app.py                # Streamlit — dashboard + gestão de ASNs
├── shared/
│   ├── models.py             # Pydantic v2 models
│   ├── config.py             # Configurações via .env
│   ├── asn_repo.py           # Consulta de ASNs owners/mitigadores
│   └── mitigator_discovery.py # Descoberta automática via RIPE Stat
├── scripts/
│   └── find_mitigators.py    # Script de pesquisa de mitigadores
└── tests/
```

## Desenvolvimento local (sem Docker)

```bash
# Instale as dependências com Poetry
poetry install

# Suba apenas a infra (PostgreSQL + Redis)
docker compose -f docker-compose.dev.yml up -d

# Execute um coletor diretamente
poetry run python collectors/ripe/collector.py
poetry run python collectors/bgptools/collector.py

# Suba o dashboard
poetry run streamlit run dashboard/app.py

# Pesquise mitigadores (lê do banco se disponível)
poetry run python scripts/find_mitigators.py
poetry run python scripts/find_mitigators.py --output sql
```

## Parar o ambiente

```bash
docker compose -f docker-compose.prod.yml down      # Para e remove containers
docker compose -f docker-compose.prod.yml down -v   # Também remove volumes (apaga o banco)
```
