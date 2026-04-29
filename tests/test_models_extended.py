"""Testes estendidos para shared/models.py — validação Pydantic."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.models import CleanRoute, KnownAsn, RawRoute


# ── RawRoute ──────────────────────────────────────────────────────────────────


def test_raw_route_accepts_list_as_path() -> None:
    route = RawRoute(prefix="8.8.8.0/24", as_path=[64500, 15169], source="ripe")
    assert route.as_path == [64500, 15169]


def test_raw_route_default_communities_is_empty_list() -> None:
    route = RawRoute(prefix="8.8.8.0/24", as_path="64500 15169", source="ripe")
    assert route.communities == []


def test_raw_route_optional_origin_asn_defaults_to_none() -> None:
    route = RawRoute(prefix="8.8.8.0/24", as_path="64500 15169", source="ripe")
    assert route.origin_asn is None


def test_raw_route_valid_origin_asn_stored() -> None:
    route = RawRoute(
        prefix="8.8.8.0/24",
        as_path="64500 15169",
        source="ripe",
        origin_asn=15169,
    )
    assert route.origin_asn == 15169


def test_raw_route_accepts_bare_ip_as_slash32() -> None:
    """Um IP sem prefixo é aceito pelo Pydantic — ipaddress trata como /32."""
    route = RawRoute(prefix="8.8.8.8", as_path="64500 15169", source="ripe")
    assert route.prefix == "8.8.8.8"


def test_raw_route_rejects_completely_invalid_string() -> None:
    with pytest.raises(ValidationError):
        RawRoute(prefix="not-a-cidr", as_path="64500 15169", source="ripe")


def test_raw_route_accepts_cidr_with_host_bits() -> None:
    """RawRoute não normaliza o prefixo — apenas valida o formato CIDR."""
    route = RawRoute(prefix="8.8.8.128/24", as_path="64500 15169", source="ripe")
    assert route.prefix == "8.8.8.128/24"


def test_raw_route_accepts_bgptools_as_source() -> None:
    route = RawRoute(prefix="8.8.8.0/24", as_path="64500 15169", source="bgptools")
    assert route.source == "bgptools"


# ── CleanRoute ────────────────────────────────────────────────────────────────


def test_clean_route_max_valid_asn() -> None:
    route = CleanRoute(
        prefix="8.8.8.0/24",
        origin_asn=4_294_967_295,
        as_path="64500 4294967295",
        as_path_list=[64500, 4_294_967_295],
        source="ripe",
    )
    assert route.origin_asn == 4_294_967_295


def test_clean_route_asn_exceeds_max_raises() -> None:
    with pytest.raises(ValidationError):
        CleanRoute(
            prefix="8.8.8.0/24",
            origin_asn=4_294_967_296,
            as_path="1 4294967296",
            as_path_list=[1, 4_294_967_296],
            source="ripe",
        )


def test_clean_route_asn_of_zero_raises() -> None:
    with pytest.raises(ValidationError):
        CleanRoute(
            prefix="8.8.8.0/24",
            origin_asn=0,
            as_path="64500 0",
            as_path_list=[64500, 0],
            source="ripe",
        )


def test_clean_route_negative_asn_raises() -> None:
    with pytest.raises(ValidationError):
        CleanRoute(
            prefix="8.8.8.0/24",
            origin_asn=-1,
            as_path="64500 -1",
            as_path_list=[64500, -1],
            source="ripe",
        )


def test_clean_route_with_mitigator_asn_and_is_mitigated() -> None:
    route = CleanRoute(
        prefix="8.8.8.0/24",
        origin_asn=15169,
        mitigator_asn=13335,
        as_path="64500 13335 15169",
        as_path_list=[64500, 13335, 15169],
        source="ripe",
        is_mitigated=True,
    )
    assert route.is_mitigated is True
    assert route.mitigator_asn == 13335


def test_clean_route_default_is_mitigated_false() -> None:
    route = CleanRoute(
        prefix="8.8.8.0/24",
        origin_asn=15169,
        as_path="64500 15169",
        as_path_list=[64500, 15169],
        source="ripe",
    )
    assert route.is_mitigated is False
    assert route.mitigator_asn is None


def test_clean_route_normalizes_prefix_host_bits() -> None:
    """CleanRoute deve normalizar 8.8.8.128/24 → 8.8.8.0/24."""
    route = CleanRoute(
        prefix="8.8.8.128/24",
        origin_asn=15169,
        as_path="64500 15169",
        as_path_list=[64500, 15169],
        source="ripe",
    )
    assert route.prefix == "8.8.8.0/24"


def test_clean_route_rejects_slash23() -> None:
    with pytest.raises(ValidationError):
        CleanRoute(
            prefix="8.8.8.0/23",
            origin_asn=15169,
            as_path="64500 15169",
            as_path_list=[64500, 15169],
            source="ripe",
        )


def test_clean_route_rejects_slash16() -> None:
    with pytest.raises(ValidationError):
        CleanRoute(
            prefix="8.8.0.0/16",
            origin_asn=15169,
            as_path="64500 15169",
            as_path_list=[64500, 15169],
            source="ripe",
        )


def test_clean_route_rejects_slash32() -> None:
    with pytest.raises(ValidationError):
        CleanRoute(
            prefix="8.8.8.8/32",
            origin_asn=15169,
            as_path="64500 15169",
            as_path_list=[64500, 15169],
            source="ripe",
        )


# ── KnownAsn ─────────────────────────────────────────────────────────────────


def test_known_asn_type_defaults_to_owner() -> None:
    asn = KnownAsn(asn=64500)
    assert asn.type == "owner"


def test_known_asn_all_optional_fields_default_to_none() -> None:
    asn = KnownAsn(asn=64500)
    assert asn.name is None
    assert asn.country is None
    assert asn.city is None
    assert asn.state is None


def test_known_asn_mitigator_type() -> None:
    asn = KnownAsn(asn=13335, name="Cloudflare", type="mitigator", country="US")
    assert asn.type == "mitigator"
    assert asn.name == "Cloudflare"
    assert asn.country == "US"


def test_known_asn_transit_type() -> None:
    asn = KnownAsn(asn=174, type="transit")
    assert asn.type == "transit"
