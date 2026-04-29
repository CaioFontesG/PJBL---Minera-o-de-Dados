"""Testes para shared/mitigator_discovery.py — funções puras e HTTP mockado."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from shared.mitigator_discovery import (
    KNOWN_MITIGATOR_ASNS,
    _get_upstreams_async,
    _is_candidate,
    _known_name,
    discover_for_asn_sync,
)


# ── _is_candidate ─────────────────────────────────────────────────────────────


def test_is_candidate_returns_true_for_known_asn() -> None:
    known_asn = next(iter(KNOWN_MITIGATOR_ASNS))
    assert _is_candidate(known_asn, "Anything", "") is True


def test_is_candidate_keyword_ddos_in_name() -> None:
    assert _is_candidate(99999, "AnyDDoS Provider", "") is True


def test_is_candidate_keyword_mitigation_in_name() -> None:
    assert _is_candidate(99999, "FastMitigation Networks", "") is True


def test_is_candidate_keyword_scrub_in_description() -> None:
    assert _is_candidate(99999, "Regular ISP", "scrubbing center Brasil") is True


def test_is_candidate_keyword_flowspec_in_description() -> None:
    assert _is_candidate(99999, "Telco X", "flowspec enabled backbone") is True


def test_is_candidate_keyword_rtbh_in_description() -> None:
    assert _is_candidate(99999, "Telco X", "supports rtbh") is True


def test_is_candidate_unknown_asn_no_keywords_returns_false() -> None:
    assert (
        _is_candidate(99999, "Regular Broadband ISP", "home internet provider") is False
    )


def test_is_candidate_case_insensitive_match() -> None:
    assert _is_candidate(99999, "DDOS PROTECTION SERVICE", "") is True


def test_is_candidate_keyword_blackhole_in_description() -> None:
    assert _is_candidate(99999, "Transit Co", "null route / blackhole support") is True


# ── _known_name ───────────────────────────────────────────────────────────────


def test_known_name_returns_registered_name_for_cloudflare() -> None:
    assert _known_name(13335, "Some Fallback") == "Cloudflare, Inc."


def test_known_name_returns_registered_name_for_huge_networks() -> None:
    assert _known_name(262254, "") == "Huge Networks"


def test_known_name_returns_fallback_for_unknown_asn() -> None:
    assert _known_name(99999, "My Fallback Name") == "My Fallback Name"


def test_known_name_generates_as_number_when_fallback_empty() -> None:
    assert _known_name(99999, "") == "AS99999"


def test_known_name_generates_as_number_when_fallback_is_none() -> None:
    assert _known_name(99999, None) == "AS99999"


# ── discover_for_asn_sync ─────────────────────────────────────────────────────


def test_discover_for_asn_sync_returns_known_mitigator_in_neighbours() -> None:
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {
        "data": {
            "neighbours": [
                {"asn": 13335, "type": "left"},  # Cloudflare — known mitigator
                {"asn": 99999, "type": "left"},  # regular ISP — not a mitigator
                {"asn": 20940, "type": "right"},  # wrong direction, deve ser ignorado
            ]
        }
    }

    with patch("shared.mitigator_discovery.requests.get", return_value=fake_response):
        results = discover_for_asn_sync(64500)

    assert len(results) == 1
    assert results[0]["asn"] == 13335


def test_discover_for_asn_sync_non_200_returns_empty() -> None:
    fake_response = MagicMock()
    fake_response.status_code = 503

    with patch("shared.mitigator_discovery.requests.get", return_value=fake_response):
        assert discover_for_asn_sync(64500) == []


def test_discover_for_asn_sync_request_exception_returns_empty() -> None:
    with patch(
        "shared.mitigator_discovery.requests.get",
        side_effect=ConnectionError("network timeout"),
    ):
        assert discover_for_asn_sync(64500) == []


def test_discover_for_asn_sync_empty_neighbours_returns_empty() -> None:
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {"data": {"neighbours": []}}

    with patch("shared.mitigator_discovery.requests.get", return_value=fake_response):
        assert discover_for_asn_sync(64500) == []


def test_discover_for_asn_sync_result_structure() -> None:
    """Verifica que cada resultado tem as chaves esperadas."""
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.json.return_value = {
        "data": {
            "neighbours": [
                {"asn": 262254, "type": "left"},  # Huge Networks — known mitigator
            ]
        }
    }

    with patch("shared.mitigator_discovery.requests.get", return_value=fake_response):
        results = discover_for_asn_sync(64500)

    assert len(results) == 1
    result = results[0]
    assert "asn" in result
    assert "name" in result
    assert "country" in result
    assert result["asn"] == 262254
    assert result["name"] == "Huge Networks"


# ── _get_upstreams_async ──────────────────────────────────────────────────────


async def test_get_upstreams_async_returns_left_neighbours_only() -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "data": {
            "neighbours": [
                {"asn": 13335, "type": "left"},
                {"asn": 64500, "type": "right"},  # deve ser excluído
                {"asn": 20940, "type": "left"},
            ]
        }
    }
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)

    result = await _get_upstreams_async(mock_client, 15169)

    asns = [r["asn"] for r in result]
    assert 13335 in asns
    assert 20940 in asns
    assert 64500 not in asns


async def test_get_upstreams_async_non_200_returns_empty() -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 404
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)

    result = await _get_upstreams_async(mock_client, 15169)
    assert result == []


async def test_get_upstreams_async_exception_returns_empty() -> None:
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(side_effect=Exception("connection refused"))

    result = await _get_upstreams_async(mock_client, 15169)
    assert result == []


async def test_get_upstreams_async_empty_neighbours_returns_empty() -> None:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"data": {"neighbours": []}}
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)

    result = await _get_upstreams_async(mock_client, 15169)
    assert result == []


async def test_get_upstreams_async_result_structure() -> None:
    """Verifica que cada upstream retornado tem as chaves esperadas."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "data": {"neighbours": [{"asn": 13335, "type": "left"}]}
    }
    mock_client = AsyncMock()
    mock_client.get = AsyncMock(return_value=mock_resp)

    result = await _get_upstreams_async(mock_client, 15169)

    assert len(result) == 1
    assert result[0]["asn"] == 13335
    assert "name" in result[0]
    assert "country" in result[0]
