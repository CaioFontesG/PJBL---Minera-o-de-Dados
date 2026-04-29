-- Migration 001 — adiciona city e state à tabela known_asns
-- Rodar apenas em bancos já existentes (novos usam init.sql diretamente)

ALTER TABLE known_asns
    ADD COLUMN IF NOT EXISTS city  VARCHAR(255),
    ADD COLUMN IF NOT EXISTS state VARCHAR(255);
