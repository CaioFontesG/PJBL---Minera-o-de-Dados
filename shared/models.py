"""Modelos de dados compartilhados — validação com Pydantic v2."""

from __future__ import annotations

import ipaddress
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


# ─── Enums / Literals ─────────────────────────────────────────────────────────

SourceLiteral = str  # 'ripe' | 'bgpview'
StatusLiteral = str  # 'pending' | 'running' | 'done' | 'error'
AsnTypeLiteral = str  # 'mitigator' | 'owner' | 'transit'


# ─── BGP Route ────────────────────────────────────────────────────────────────


class RawRoute(BaseModel):
    """Dados brutos vindos de qualquer coletor antes da limpeza."""

    prefix: str
    origin_asn: int | None = None
    as_path: str | list[int] = ""
    communities: list[str] = Field(default_factory=list)
    source: SourceLiteral
    raw_data: dict[str, Any] = Field(default_factory=dict)

    @field_validator("prefix")
    @classmethod
    def validate_prefix(cls, v: str) -> str:
        try:
            ipaddress.ip_network(v, strict=False)
        except ValueError as exc:
            raise ValueError(f"Prefixo CIDR inválido: {v!r}") from exc
        return v


class CleanRoute(BaseModel):
    """Rota normalizada e validada, pronta para inserção no banco."""

    prefix: str
    origin_asn: int
    mitigator_asn: int | None = None
    as_path: str  # Serializado como string para o banco
    as_path_list: list[int] = Field(default_factory=list)
    communities: list[str] = Field(default_factory=list)
    source: SourceLiteral
    is_mitigated: bool = False
    raw_data: dict[str, Any] = Field(default_factory=dict)

    @field_validator("prefix")
    @classmethod
    def must_be_slash24(cls, v: str) -> str:
        net = ipaddress.ip_network(v, strict=False)
        if net.prefixlen != 24:
            raise ValueError(f"Apenas prefixos /24 são aceitos, recebido: {v}")
        return str(net)

    @field_validator("origin_asn")
    @classmethod
    def positive_asn(cls, v: int) -> int:
        if v <= 0 or v > 4_294_967_295:
            raise ValueError(f"ASN inválido: {v}")
        return v


# ─── Known ASN ────────────────────────────────────────────────────────────────


class KnownAsn(BaseModel):
    asn: int
    name: str | None = None
    type: AsnTypeLiteral = "owner"
    country: str | None = None
    city: str | None = None
    state: str | None = None


# ─── Collection Job ───────────────────────────────────────────────────────────


class CollectionJob(BaseModel):
    id: int | None = None
    source: SourceLiteral
    status: StatusLiteral = "pending"
    started_at: datetime | None = None
    finished_at: datetime | None = None
    records_found: int = 0
    error_msg: str | None = None
