from __future__ import annotations

from datetime import UTC, datetime

from pipeline.loader import _build_attack_events


def test_build_attack_events_detects_start_and_end() -> None:
    snapshots = [
        {
            "source": "ripe",
            "prefix": "1.1.1.0/24",
            "origin_asn": 64500,
            "mitigator_asn": 13335,
            "is_fully_mitigated": False,
            "collection_ts": datetime(2026, 4, 29, 10, 0, tzinfo=UTC),
        },
        {
            "source": "ripe",
            "prefix": "1.1.1.0/24",
            "origin_asn": 64500,
            "mitigator_asn": 13335,
            "is_fully_mitigated": True,
            "collection_ts": datetime(2026, 4, 29, 11, 0, tzinfo=UTC),
        },
        {
            "source": "ripe",
            "prefix": "1.1.1.0/24",
            "origin_asn": 64500,
            "mitigator_asn": 13335,
            "is_fully_mitigated": True,
            "collection_ts": datetime(2026, 4, 29, 12, 0, tzinfo=UTC),
        },
        {
            "source": "ripe",
            "prefix": "1.1.1.0/24",
            "origin_asn": 64500,
            "mitigator_asn": None,
            "is_fully_mitigated": False,
            "collection_ts": datetime(2026, 4, 29, 13, 0, tzinfo=UTC),
        },
    ]

    events = _build_attack_events(snapshots)

    assert len(events) == 1
    assert events[0][0] == "ripe"
    assert events[0][1] == "1.1.1.0/24"
    assert events[0][2] == 64500
    assert events[0][3] == 13335
    assert events[0][4] == datetime(2026, 4, 29, 11, 0, tzinfo=UTC)
    assert events[0][5] == datetime(2026, 4, 29, 13, 0, tzinfo=UTC)
    assert events[0][6] == 7200


def test_build_attack_events_keeps_open_event_without_end() -> None:
    snapshots = [
        {
            "source": "bgpview",
            "prefix": "8.8.8.0/24",
            "origin_asn": 15169,
            "mitigator_asn": 20940,
            "is_fully_mitigated": True,
            "collection_ts": datetime(2026, 4, 29, 11, 0, tzinfo=UTC),
        },
        {
            "source": "bgpview",
            "prefix": "8.8.8.0/24",
            "origin_asn": 15169,
            "mitigator_asn": 20940,
            "is_fully_mitigated": True,
            "collection_ts": datetime(2026, 4, 29, 12, 0, tzinfo=UTC),
        },
    ]

    events = _build_attack_events(snapshots)

    assert len(events) == 1
    assert events[0][0] == "bgpview"
    assert events[0][1] == "8.8.8.0/24"
    assert events[0][2] == 15169
    assert events[0][3] == 20940
    assert events[0][4] == datetime(2026, 4, 29, 11, 0, tzinfo=UTC)
    assert events[0][5] is None
    assert events[0][6] is None
