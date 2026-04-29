"""Testes estendidos para _build_attack_events — casos de borda."""

from __future__ import annotations

from datetime import UTC, datetime

from pipeline.loader import _build_attack_events


def _snap(
    source: str,
    prefix: str,
    origin_asn: int,
    mitigator_asn: int | None,
    is_fully_mitigated: bool,
    hour: int,
) -> dict:
    return {
        "source": source,
        "prefix": prefix,
        "origin_asn": origin_asn,
        "mitigator_asn": mitigator_asn,
        "is_fully_mitigated": is_fully_mitigated,
        "collection_ts": datetime(2026, 1, 1, hour, tzinfo=UTC),
    }


def t(hour: int) -> datetime:
    return datetime(2026, 1, 1, hour, tzinfo=UTC)


# ── Casos base ─────────────────────────────────────────────────────────────────


def test_build_attack_events_empty_input() -> None:
    assert _build_attack_events([]) == []


def test_build_attack_events_never_mitigated_returns_empty() -> None:
    snaps = [
        _snap("ripe", "1.1.1.0/24", 64500, None, False, 10),
        _snap("ripe", "1.1.1.0/24", 64500, None, False, 11),
        _snap("ripe", "1.1.1.0/24", 64500, None, False, 12),
    ]
    assert _build_attack_events(snaps) == []


def test_build_attack_events_duration_computed_correctly() -> None:
    snaps = [
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 10),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 13),
    ]
    events = _build_attack_events(snaps)
    assert len(events) == 1
    assert events[0][6] == 10800  # 3 horas = 10800 segundos


def test_build_attack_events_1hour_duration() -> None:
    snaps = [
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 8),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 9),
    ]
    events = _build_attack_events(snaps)
    assert events[0][6] == 3600


# ── Múltiplos prefixos ─────────────────────────────────────────────────────────


def test_build_attack_events_two_prefixes_generate_independent_events() -> None:
    snaps = [
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 10),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 11),
        _snap("ripe", "2.2.2.0/24", 64500, 20940, True, 10),
        _snap("ripe", "2.2.2.0/24", 64500, 20940, False, 12),
    ]
    events = _build_attack_events(snaps)
    assert len(events) == 2
    prefixes = {e[1] for e in events}
    assert prefixes == {"1.1.1.0/24", "2.2.2.0/24"}


def test_build_attack_events_same_prefix_different_origin_asn() -> None:
    """Mesmo prefixo com origins diferentes são eventos independentes."""
    snaps = [
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 10),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 11),
        _snap("ripe", "1.1.1.0/24", 64501, 13335, True, 10),
        _snap("ripe", "1.1.1.0/24", 64501, 13335, False, 11),
    ]
    events = _build_attack_events(snaps)
    assert len(events) == 2


# ── Múltiplos ataques no mesmo prefixo ────────────────────────────────────────


def test_build_attack_events_two_attacks_same_prefix() -> None:
    """Um prefixo pode ter múltiplos ciclos de ataque/recuperação."""
    snaps = [
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 1),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 2),  # ataque 1 começa
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 3),  # ataque 1 termina
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 4),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 5),  # ataque 2 começa
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 6),  # ataque 2 termina
    ]
    events = _build_attack_events(snaps)
    assert len(events) == 2
    assert events[0][4] == t(2)
    assert events[0][5] == t(3)
    assert events[1][4] == t(5)
    assert events[1][5] == t(6)


def test_build_attack_events_consecutive_mitigated_counts_as_one_event() -> None:
    """Snapshots consecutivos mitigados não abrem múltiplos eventos."""
    snaps = [
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 10),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 11),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 12),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 13),
    ]
    events = _build_attack_events(snaps)
    assert len(events) == 1
    assert events[0][4] == t(10)  # evento começa no primeiro snapshot mitigado
    assert events[0][5] == t(13)


# ── Evento aberto (sem fim) ───────────────────────────────────────────────────


def test_build_attack_events_ongoing_attack_has_no_end() -> None:
    snaps = [
        _snap("bgptools", "8.8.8.0/24", 15169, 20940, True, 11),
        _snap("bgptools", "8.8.8.0/24", 15169, 20940, True, 12),
    ]
    events = _build_attack_events(snaps)
    assert len(events) == 1
    assert events[0][5] is None  # ended_at é None
    assert events[0][6] is None  # duration_seconds é None


# ── Ordenação ─────────────────────────────────────────────────────────────────


def test_build_attack_events_sorts_snapshots_by_timestamp() -> None:
    """Snapshots fora de ordem devem ser ordenados antes do processamento."""
    snaps = [
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 13),  # fora de ordem
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 10),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 11),
    ]
    events = _build_attack_events(snaps)
    assert len(events) == 1
    assert events[0][4] == t(10)  # evento deve começar no timestamp 10
    assert events[0][5] == t(13)


# ── Conteúdo das tuplas ───────────────────────────────────────────────────────


def test_build_attack_events_tuple_structure() -> None:
    """Verifica a estrutura completa da tupla de evento."""
    snaps = [
        _snap("ripe", "1.1.1.0/24", 64500, 13335, True, 10),
        _snap("ripe", "1.1.1.0/24", 64500, 13335, False, 11),
    ]
    events = _build_attack_events(snaps)
    assert len(events) == 1
    source, prefix, origin_asn, mitigator_asn, started_at, ended_at, duration = events[
        0
    ]
    assert source == "ripe"
    assert prefix == "1.1.1.0/24"
    assert origin_asn == 64500
    assert mitigator_asn == 13335
    assert started_at == t(10)
    assert ended_at == t(11)
    assert duration == 3600


# ── Fontes diferentes ─────────────────────────────────────────────────────────


def test_build_attack_events_different_sources_are_independent() -> None:
    """Mesmo prefixo em fontes distintas gera eventos separados."""
    snaps = [
        {
            "source": "ripe",
            "prefix": "1.1.1.0/24",
            "origin_asn": 64500,
            "mitigator_asn": 13335,
            "is_fully_mitigated": True,
            "collection_ts": t(10),
        },
        {
            "source": "ripe",
            "prefix": "1.1.1.0/24",
            "origin_asn": 64500,
            "mitigator_asn": 13335,
            "is_fully_mitigated": False,
            "collection_ts": t(11),
        },
        {
            "source": "bgptools",
            "prefix": "1.1.1.0/24",
            "origin_asn": 64500,
            "mitigator_asn": 13335,
            "is_fully_mitigated": True,
            "collection_ts": t(10),
        },
        {
            "source": "bgptools",
            "prefix": "1.1.1.0/24",
            "origin_asn": 64500,
            "mitigator_asn": 13335,
            "is_fully_mitigated": False,
            "collection_ts": t(12),
        },
    ]
    events = _build_attack_events(snaps)
    assert len(events) == 2
    sources = {e[0] for e in events}
    assert sources == {"ripe", "bgptools"}
