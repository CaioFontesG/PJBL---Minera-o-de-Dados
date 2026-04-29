"""
Pesquisa ASNs de mitigação DDoS usados pelos ISPs do Maranhão.

Estratégia:
  1. Lê os ASNs owners do banco de dados (fallback para lista hardcoded)
  2. Para cada ASN monitorado, busca os upstreams via RIPE Stat API
  3. Filtra candidatos a mitigador por nome (keywords) ou por ser
     um provedor conhecido
  4. Consolida e exibe uma tabela ranqueada por frequência de aparição

Uso:
    poetry run python scripts/find_mitigators.py
    poetry run python scripts/find_mitigators.py --output sql
"""

from __future__ import annotations

import argparse
import os
import time
from collections import Counter
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

# ── Fallback: ASNs hardcoded caso o banco esteja indisponível ────────────────
_FALLBACK_OWNER_ASNS = [
    28638,
    61588,
    262456,
    262503,
    262727,
    263508,
    265300,
    265939,
    265994,
    266339,
    266382,
    266616,
    267001,
    267575,
    268183,
    268314,
    268471,
    268544,
    268858,
    269301,
    269514,
    269528,
    269609,
    269634,
    269655,
    269712,
    270256,
    270350,
    270428,
]

# ── Mitigadores já conhecidos (seed inicial) ──────────────────────────────────
KNOWN_MITIGATORS: dict[int, str] = {
    262254: "Huge Networks",
    268696: "UPX Technologies",
    14840: "Eletronet S.A.",
    13335: "Cloudflare, Inc.",
    20940: "Akamai Technologies",
    19551: "Imperva / Incapsula",
}

MITIGATION_KEYWORDS = {
    "ddos",
    "mitigation",
    "scrub",
    "clean",
    "protection",
    "shield",
    "guard",
    "defend",
    "nexus",
    "cogent",
    "anti-ddos",
    "antiddos",
    "cleantransit",
}

RIPE_STAT_BASE = "https://stat.ripe.net/data"
DELAY = 0.8


def _load_owner_asns_from_db() -> list[int] | None:
    """Lê os ASNs owners do banco. Retorna None se o banco estiver indisponível."""
    try:
        import psycopg

        dsn = (
            f"postgresql://{os.getenv('POSTGRES_USER', 'bgp')}:"
            f"{os.getenv('POSTGRES_PASSWORD', 'changeme')}@"
            f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
            f"{os.getenv('POSTGRES_PORT', '5432')}/"
            f"{os.getenv('POSTGRES_DB', 'bgp_mining')}"
        )
        with psycopg.connect(dsn, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT asn FROM known_asns WHERE type = 'owner' ORDER BY asn"
                )
                return [row[0] for row in cur.fetchall()]
    except Exception:
        return None


def get(
    client: httpx.Client, url: str, params: dict | None = None
) -> dict[str, Any] | None:
    try:
        resp = client.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        print(f"  HTTP {resp.status_code} para {url}")
        return None
    except Exception as e:
        print(f"  Erro: {e}")
        return None


def is_mitigator_candidate(asn: int, name: str, description: str) -> bool:
    if asn in KNOWN_MITIGATORS:
        return True
    text = (name + " " + description).lower()
    return any(kw in text for kw in MITIGATION_KEYWORDS)


def fetch_upstreams(client: httpx.Client, asn: int) -> list[dict]:
    data = get(
        client, f"{RIPE_STAT_BASE}/asn-neighbours/data.json", {"resource": f"AS{asn}"}
    )
    if not data:
        return []
    neighbours = data.get("data", {}).get("neighbours", [])
    return [
        {
            "asn": n["asn"],
            "name": KNOWN_MITIGATORS.get(n["asn"], ""),
            "description": "",
            "country": "BR",
        }
        for n in neighbours
        if n.get("type") == "left" and n.get("asn")
    ]


def fetch_asn_info(client: httpx.Client, asn: int) -> dict:
    if asn in KNOWN_MITIGATORS:
        return {
            "asn": asn,
            "name": KNOWN_MITIGATORS[asn],
            "description": KNOWN_MITIGATORS[asn],
            "country": "BR",
        }
    data = get(
        client, f"{RIPE_STAT_BASE}/as-overview/data.json", {"resource": f"AS{asn}"}
    )
    if not data:
        return {}
    d = data.get("data", {})
    return {
        "asn": asn,
        "name": d.get("holder", ""),
        "description": d.get("holder", ""),
        "country": "BR",
    }


def main(output: str) -> None:
    # Tenta carregar do banco; usa fallback se indisponível
    owner_asns = _load_owner_asns_from_db()
    if owner_asns:
        print(f"ASNs carregados do banco: {len(owner_asns)} owners")
    else:
        print("Banco indisponível — usando lista hardcoded.")
        owner_asns = _FALLBACK_OWNER_ASNS

    candidate_counter: Counter = Counter()
    candidate_info: dict[int, dict] = {}

    print("=" * 60)
    print("Pesquisando upstreams dos ASNs do Maranhão via RIPE Stat...")
    print("=" * 60)

    with httpx.Client(
        headers={"User-Agent": "BGP-Mining-Research/1.0 (academic - UNDB)"},
        follow_redirects=True,
    ) as client:
        for asn in owner_asns:
            print(f"\nAS{asn}...", end=" ", flush=True)
            upstreams = fetch_upstreams(client, asn)
            found = 0
            for up in upstreams:
                u_asn = up["asn"]
                if u_asn in owner_asns:
                    continue
                if is_mitigator_candidate(u_asn, up["name"], up["description"]):
                    candidate_counter[u_asn] += 1
                    candidate_info[u_asn] = up
                    found += 1
            print(f"{len(upstreams)} upstreams, {found} candidatos a mitigador")
            time.sleep(DELAY)

        print("\nEnriquecendo informações dos candidatos...")
        for asn in list(candidate_info.keys()):
            if asn not in KNOWN_MITIGATORS and not candidate_info[asn].get(
                "description"
            ):
                info = fetch_asn_info(client, asn)
                if info:
                    candidate_info[asn].update(info)
                time.sleep(DELAY)

    print("\n" + "=" * 60)
    print("CANDIDATOS A MITIGADOR (ordenados por frequência)")
    print("=" * 60)
    print(f"{'ASN':<10} {'Freq':>5}  {'País':>4}  {'Nome'}")
    print("-" * 60)

    ranked = candidate_counter.most_common()
    for asn, freq in ranked:
        info = candidate_info.get(asn, {})
        name = info.get("name") or info.get("description") or "?"
        country = info.get("country", "?")
        marker = " ✓" if asn in KNOWN_MITIGATORS else ""
        print(f"AS{asn:<9} {freq:>5}  {country:>4}  {name}{marker}")

    if output == "sql":
        print("\n" + "=" * 60)
        print("INSERT SQL (copie para db/init.sql):")
        print("=" * 60)
        for asn, freq in ranked:
            info = candidate_info.get(asn, {})
            name = (info.get("description") or info.get("name") or "").replace(
                "'", "''"
            )
            country = info.get("country", "BR") or "BR"
            print(
                f"({asn}, '{name}', 'mitigator', '{country}', NULL, NULL),  -- freq={freq}"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pesquisa mitigadores DDoS para ASNs do MA"
    )
    parser.add_argument(
        "--output",
        choices=["table", "sql"],
        default="table",
        help="Formato de saída: table (padrão) ou sql",
    )
    args = parser.parse_args()
    main(args.output)
