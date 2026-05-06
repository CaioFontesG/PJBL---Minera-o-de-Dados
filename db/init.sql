-- =============================================================================
-- BGP Mining — Schema Inicial
-- =============================================================================

-- Habilitar extensão para CIDR nativo do PostgreSQL
CREATE EXTENSION IF NOT EXISTS citext;

-- -----------------------------------------------------------------------------
-- Tabela principal de prefixos coletados
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bgp_routes (
    id              BIGSERIAL PRIMARY KEY,
    prefix          CIDR NOT NULL,
    origin_asn      INTEGER NOT NULL,
    mitigator_asn   INTEGER,
    as_path         TEXT NOT NULL,
    communities     TEXT[],
    source          VARCHAR(50) NOT NULL
                        CHECK (source IN ('ripe')),
    collected_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_mitigated    BOOLEAN DEFAULT FALSE,
    raw_data        JSONB
);

-- -----------------------------------------------------------------------------
-- Tabela de ASNs conhecidos (proprietários e mitigadores)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS known_asns (
    asn             INTEGER PRIMARY KEY,
    name            VARCHAR(255),
    type            VARCHAR(50)
                        CHECK (type IN ('mitigator', 'owner', 'transit')),
    country         CHAR(2),
    city            VARCHAR(255),
    state           VARCHAR(255),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Log de execuções de coleta
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS collection_jobs (
    id              BIGSERIAL PRIMARY KEY,
    source          VARCHAR(50) NOT NULL
                        CHECK (source IN ('ripe')),
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'running', 'done', 'error')),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    records_found   INTEGER DEFAULT 0,
    error_msg       TEXT
);

-- -----------------------------------------------------------------------------
-- Snapshots agregados por coleta para auditoria de mitigacao
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mitigation_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    source              VARCHAR(50) NOT NULL
                            CHECK (source IN ('ripe')),
    collection_ts       TIMESTAMPTZ NOT NULL,
    prefix              CIDR NOT NULL,
    origin_asn          INTEGER NOT NULL,
    total_paths         INTEGER NOT NULL CHECK (total_paths >= 0),
    mitigated_paths     INTEGER NOT NULL CHECK (mitigated_paths >= 0),
    mitigation_ratio    NUMERIC(6, 4) NOT NULL CHECK (mitigation_ratio >= 0 AND mitigation_ratio <= 1),
    is_fully_mitigated  BOOLEAN NOT NULL,
    mitigator_asn       INTEGER,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (source, collection_ts, prefix, origin_asn)
);

-- -----------------------------------------------------------------------------
-- Eventos de ataque inferidos por transicao para 100% mitigado
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ddos_attack_events (
    id                  BIGSERIAL PRIMARY KEY,
    source              VARCHAR(50) NOT NULL
                            CHECK (source IN ('ripe')),
    prefix              CIDR NOT NULL,
    origin_asn          INTEGER NOT NULL,
    mitigator_asn       INTEGER,
    started_at          TIMESTAMPTZ NOT NULL,
    ended_at            TIMESTAMPTZ,
    duration_seconds    INTEGER,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -----------------------------------------------------------------------------
-- Índices
-- -----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_bgp_routes_prefix
    ON bgp_routes (prefix);

CREATE INDEX IF NOT EXISTS idx_bgp_routes_origin_asn
    ON bgp_routes (origin_asn);

CREATE INDEX IF NOT EXISTS idx_bgp_routes_mitigator_asn
    ON bgp_routes (mitigator_asn);

CREATE INDEX IF NOT EXISTS idx_bgp_routes_collected_at
    ON bgp_routes (collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_bgp_routes_is_mitigated
    ON bgp_routes (is_mitigated);

CREATE INDEX IF NOT EXISTS idx_bgp_routes_source
    ON bgp_routes (source);

-- Índice GIN para busca dentro de raw_data JSONB
CREATE INDEX IF NOT EXISTS idx_bgp_routes_raw_data
    ON bgp_routes USING GIN (raw_data);

CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
    ON mitigation_snapshots (source, prefix, origin_asn, collection_ts);

CREATE INDEX IF NOT EXISTS idx_snapshots_collection_ts
    ON mitigation_snapshots (collection_ts DESC);

CREATE INDEX IF NOT EXISTS idx_events_started_at
    ON ddos_attack_events (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_lookup
    ON ddos_attack_events (source, prefix, origin_asn);

