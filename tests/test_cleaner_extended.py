"""Testes estendidos para pipeline/cleaner.py — cobre casos de borda não testados."""

from __future__ import annotations

import pytest

from pipeline.cleaner import (
    _detect_mitigator,
    _parse_as_path,
    _parse_communities,
    clean_route,
    clean_routes,
)


# ── _parse_as_path ─────────────────────────────────────────────────────────────


def test_parse_as_path_empty_string_returns_empty() -> None:
    assert _parse_as_path("") == []


def test_parse_as_path_only_asset_returns_empty() -> None:
    """AS-PATH composto apenas de AS-SET deve gerar lista vazia."""
    assert _parse_as_path("{1234,5678}") == []


def test_parse_as_path_preserves_order() -> None:
    parsed = _parse_as_path("100 200 300")
    assert parsed == [100, 200, 300]


def test_parse_as_path_multiple_prepends_at_end() -> None:
    parsed = _parse_as_path("64500 13335 15169 15169 15169")
    assert parsed == [64500, 13335, 15169]


def test_parse_as_path_list_with_three_prepends() -> None:
    parsed = _parse_as_path([64500, 64500, 64500, 15169])
    assert parsed == [64500, 15169]


def test_parse_as_path_single_asn() -> None:
    assert _parse_as_path("15169") == [15169]


def test_parse_as_path_mixed_asset_and_prepends() -> None:
    parsed = _parse_as_path("64500 {1234,5678} 64500 13335 13335 15169")
    assert parsed == [64500, 13335, 15169]


# ── _parse_communities ─────────────────────────────────────────────────────────


def test_parse_communities_empty_list() -> None:
    assert _parse_communities([]) == []


def test_parse_communities_invalid_entry_preserved_as_is() -> None:
    result = _parse_communities(["invalid-community"])
    assert result == ["invalid-community"]


def test_parse_communities_tuple_string_format() -> None:
    result = _parse_communities(["(13335, 3000)"])
    assert result == ["13335:3000"]


def test_parse_communities_space_separated() -> None:
    result = _parse_communities(["13335 2000"])
    assert result == ["13335:2000"]


def test_parse_communities_multiple_formats_together() -> None:
    result = _parse_communities(["13335:1000", "13335 2000", "(13335, 3000)"])
    assert result == ["13335:1000", "13335:2000", "13335:3000"]


# ── _detect_mitigator ──────────────────────────────────────────────────────────


def test_detect_mitigator_not_present_returns_false_and_none() -> None:
    is_mit, asn = _detect_mitigator([64500, 15169], frozenset({13335}))
    assert is_mit is False
    assert asn is None


def test_detect_mitigator_found_mid_path() -> None:
    is_mit, asn = _detect_mitigator([64500, 13335, 15169], frozenset({13335}))
    assert is_mit is True
    assert asn == 13335


def test_detect_mitigator_returns_first_match_in_path_order() -> None:
    """Quando múltiplos mitigadores estão no path, retorna o primeiro encontrado."""
    is_mit, asn = _detect_mitigator(
        [64500, 13335, 20940, 15169], frozenset({13335, 20940})
    )
    assert is_mit is True
    assert asn == 13335


def test_detect_mitigator_empty_path_returns_false() -> None:
    is_mit, asn = _detect_mitigator([], frozenset({13335}))
    assert is_mit is False
    assert asn is None


def test_detect_mitigator_empty_mitigators_set() -> None:
    is_mit, asn = _detect_mitigator([64500, 13335, 15169], frozenset())
    assert is_mit is False
    assert asn is None


# ── clean_route ────────────────────────────────────────────────────────────────


def test_clean_route_empty_as_path_returns_none() -> None:
    raw = {"prefix": "8.8.8.0/24", "as_path": "", "communities": [], "source": "ripe"}
    assert clean_route(raw, frozenset()) is None


def test_clean_route_only_asset_in_path_returns_none() -> None:
    raw = {
        "prefix": "8.8.8.0/24",
        "as_path": "{1234,5678}",
        "communities": [],
        "source": "ripe",
    }
    assert clean_route(raw, frozenset()) is None


def test_clean_route_uses_explicit_origin_asn() -> None:
    """origin_asn explícito no dict tem precedência sobre o último hop do path."""
    raw = {
        "prefix": "8.8.8.0/24",
        "origin_asn": 99999,
        "as_path": "64500 15169",
        "communities": [],
        "source": "ripe",
    }
    result = clean_route(raw, frozenset())
    assert result is not None
    assert result.origin_asn == 99999


def test_clean_route_derives_origin_from_last_path_hop() -> None:
    raw = {
        "prefix": "8.8.8.0/24",
        "as_path": "64500 13335 15169",
        "communities": [],
        "source": "ripe",
    }
    result = clean_route(raw, frozenset())
    assert result is not None
    assert result.origin_asn == 15169


def test_clean_route_rejects_loopback_prefix() -> None:
    raw = {
        "prefix": "127.0.0.0/24",
        "as_path": "64500 15169",
        "communities": [],
        "source": "ripe",
    }
    assert clean_route(raw, frozenset()) is None


def test_clean_route_rejects_link_local_prefix() -> None:
    raw = {
        "prefix": "169.254.1.0/24",
        "as_path": "64500 15169",
        "communities": [],
        "source": "ripe",
    }
    assert clean_route(raw, frozenset()) is None


def test_clean_route_rejects_slash16() -> None:
    raw = {
        "prefix": "8.8.0.0/16",
        "as_path": "64500 15169",
        "communities": [],
        "source": "ripe",
    }
    assert clean_route(raw, frozenset()) is None


def test_clean_route_rejects_slash32() -> None:
    raw = {
        "prefix": "8.8.8.8/32",
        "as_path": "64500 15169",
        "communities": [],
        "source": "ripe",
    }
    assert clean_route(raw, frozenset()) is None


def test_clean_route_not_mitigated_when_mitigator_not_in_path() -> None:
    raw = {
        "prefix": "1.2.3.0/24",
        "as_path": "64500 15169",
        "communities": [],
        "source": "ripe",
    }
    result = clean_route(raw, frozenset({13335}))
    assert result is not None
    assert result.is_mitigated is False
    assert result.mitigator_asn is None


def test_clean_route_normalizes_host_bits_in_prefix() -> None:
    """8.8.8.128/24 deve ser normalizado para 8.8.8.0/24."""
    raw = {
        "prefix": "8.8.8.128/24",
        "as_path": "64500 15169",
        "communities": [],
        "source": "ripe",
    }
    result = clean_route(raw, frozenset())
    assert result is not None
    assert result.prefix == "8.8.8.0/24"


def test_clean_route_missing_source_returns_none() -> None:
    raw = {"prefix": "8.8.8.0/24", "as_path": "64500 15169"}
    assert clean_route(raw, frozenset()) is None


def test_clean_route_invalid_cidr_returns_none() -> None:
    raw = {
        "prefix": "not-a-cidr",
        "as_path": "64500 15169",
        "communities": [],
        "source": "ripe",
    }
    assert clean_route(raw, frozenset()) is None


def test_clean_route_sets_as_path_list_correctly() -> None:
    raw = {
        "prefix": "8.8.8.0/24",
        "as_path": "64500 13335 15169",
        "communities": [],
        "source": "ripe",
    }
    result = clean_route(raw, frozenset())
    assert result is not None
    assert result.as_path_list == [64500, 13335, 15169]


def test_clean_route_normalizes_communities() -> None:
    raw = {
        "prefix": "8.8.8.0/24",
        "as_path": "64500 15169",
        "communities": ["13335 1000", "(13335, 2000)"],
        "source": "ripe",
    }
    result = clean_route(raw, frozenset())
    assert result is not None
    assert "13335:1000" in result.communities
    assert "13335:2000" in result.communities


# ── clean_routes (batch) ──────────────────────────────────────────────────────


def test_clean_routes_empty_input_returns_empty() -> None:
    assert clean_routes([], frozenset()) == []


def test_clean_routes_all_invalid_returns_empty() -> None:
    raws = [
        {
            "prefix": "10.0.0.0/24",
            "as_path": "64500 15169",
            "communities": [],
            "source": "ripe",
        },
        {
            "prefix": "192.168.1.0/24",
            "as_path": "64500 15169",
            "communities": [],
            "source": "ripe",
        },
    ]
    assert clean_routes(raws, frozenset()) == []


def test_clean_routes_mixed_valid_invalid_keeps_only_valid() -> None:
    raws = [
        {
            "prefix": "1.1.1.0/24",
            "as_path": "64500 15169",
            "communities": [],
            "source": "ripe",
        },
        {
            "prefix": "10.0.0.0/24",  # private — invalid
            "as_path": "64500 15169",
            "communities": [],
            "source": "ripe",
        },
        {
            "prefix": "8.8.8.0/24",
            "as_path": "64500 15169",
            "communities": [],
            "source": "ripe",
        },
    ]
    result = clean_routes(raws, frozenset())
    assert len(result) == 2
    prefixes = {r.prefix for r in result}
    assert "1.1.1.0/24" in prefixes
    assert "8.8.8.0/24" in prefixes
