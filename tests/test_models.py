from __future__ import annotations

import pytest
from pydantic import ValidationError

from shared.models import CleanRoute, RawRoute


def test_raw_route_rejects_invalid_prefix() -> None:
    with pytest.raises(ValidationError):
        RawRoute(prefix="not-a-cidr", as_path="64500 15169", source="ripe")


def test_clean_route_normalizes_prefix() -> None:
    route = CleanRoute(
        prefix="8.8.8.8/24",
        origin_asn=15169,
        as_path="64500 15169",
        as_path_list=[64500, 15169],
        source="ripe",
    )
    assert route.prefix == "8.8.8.0/24"


def test_clean_route_requires_slash24() -> None:
    with pytest.raises(ValidationError):
        CleanRoute(
            prefix="8.8.8.0/23",
            origin_asn=15169,
            as_path="64500 15169",
            as_path_list=[64500, 15169],
            source="ripe",
        )


def test_clean_route_requires_positive_asn() -> None:
    with pytest.raises(ValidationError):
        CleanRoute(
            prefix="8.8.8.0/24",
            origin_asn=0,
            as_path="64500 15169",
            as_path_list=[64500, 15169],
            source="ripe",
        )
