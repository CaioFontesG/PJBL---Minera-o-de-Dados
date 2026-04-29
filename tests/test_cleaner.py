from __future__ import annotations

from pipeline.cleaner import (
    _parse_as_path,
    _parse_communities,
    clean_route,
    clean_routes,
)


def test_parse_as_path_removes_asset_and_prepends() -> None:
    parsed = _parse_as_path("64500 {1234,5678} 64500 13335 13335 15169")
    assert parsed == [64500, 13335, 15169]


def test_parse_as_path_accepts_list() -> None:
    parsed = _parse_as_path([65000, 65000, 64512])
    assert parsed == [65000, 64512]


def test_parse_communities_normalizes_known_formats() -> None:
    communities = _parse_communities(
        ["13335:1000", "13335 2000", "(13335, 3000)", "invalid"]
    )
    assert communities == ["13335:1000", "13335:2000", "13335:3000", "invalid"]


def test_clean_route_accepts_valid_route_and_detects_mitigator() -> None:
    mitigators = frozenset({13335, 20940})
    raw = {
        "prefix": "8.8.8.0/24",
        "origin_asn": None,
        "as_path": "64500 13335 15169",
        "communities": ["13335:1000"],
        "source": "ripe",
    }

    cleaned = clean_route(raw, mitigators)

    assert cleaned is not None
    assert cleaned.origin_asn == 15169
    assert cleaned.is_mitigated is True
    assert cleaned.mitigator_asn == 13335
    assert cleaned.as_path_list == [64500, 13335, 15169]


def test_clean_route_rejects_non_slash24() -> None:
    mitigators = frozenset({13335})
    raw = {
        "prefix": "8.8.8.0/23",
        "as_path": "64500 13335 15169",
        "communities": [],
        "source": "bgpview",
    }

    assert clean_route(raw, mitigators) is None


def test_clean_route_rejects_private_prefix() -> None:
    mitigators = frozenset({13335})
    raw = {
        "prefix": "10.0.0.0/24",
        "as_path": "64500 13335 15169",
        "communities": [],
        "source": "bgpview",
    }

    assert clean_route(raw, mitigators) is None


def test_clean_routes_filters_invalid_routes() -> None:
    mitigators = frozenset({13335})
    raws = [
        {
            "prefix": "8.8.8.0/24",
            "as_path": "64500 13335 15169",
            "communities": [],
            "source": "ripe",
        },
        {
            "prefix": "10.0.0.0/24",
            "as_path": "64500 13335 15169",
            "communities": [],
            "source": "ripe",
        },
    ]

    cleaned = clean_routes(raws, mitigators)

    assert len(cleaned) == 1
    assert cleaned[0].prefix == "8.8.8.0/24"
