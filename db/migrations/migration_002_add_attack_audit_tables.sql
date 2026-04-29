-- Migration 002 - adiciona tabelas de auditoria de mitigacao e eventos inferidos
-- Rodar em bancos existentes apos migration_001.

CREATE TABLE IF NOT EXISTS mitigation_snapshots (
    id                  BIGSERIAL PRIMARY KEY,
    source              VARCHAR(50) NOT NULL
                            CHECK (source IN ('ripe', 'bgpview')),
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

CREATE TABLE IF NOT EXISTS ddos_attack_events (
    id                  BIGSERIAL PRIMARY KEY,
    source              VARCHAR(50) NOT NULL
                            CHECK (source IN ('ripe', 'bgpview')),
    prefix              CIDR NOT NULL,
    origin_asn          INTEGER NOT NULL,
    mitigator_asn       INTEGER,
    started_at          TIMESTAMPTZ NOT NULL,
    ended_at            TIMESTAMPTZ,
    duration_seconds    INTEGER,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_snapshots_lookup
    ON mitigation_snapshots (source, prefix, origin_asn, collection_ts);

CREATE INDEX IF NOT EXISTS idx_snapshots_collection_ts
    ON mitigation_snapshots (collection_ts DESC);

CREATE INDEX IF NOT EXISTS idx_events_started_at
    ON ddos_attack_events (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_events_lookup
    ON ddos_attack_events (source, prefix, origin_asn);
