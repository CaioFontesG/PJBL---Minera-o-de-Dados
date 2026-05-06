"""Dashboard Streamlit — BGP Mining."""

from __future__ import annotations

import os
import re
import sys

import altair as alt
import pandas as pd
import psycopg
import requests
import streamlit as st
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
load_dotenv()

st.set_page_config(
    page_title="BGP Mining",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="collapsed",
)


st.markdown(
    """
    <style>
    [data-testid="stMetricValue"] { font-size: 2rem; }
    div[data-testid="stForm"] { padding: 1rem; border: 1px solid #e0e0e0; border-radius: 8px; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ── Conexão ──────────────────────────────────────────────────────────────────

DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'bgp')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'changeme')}@"
    f"{os.getenv('POSTGRES_HOST', 'localhost')}:"
    f"{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'bgp_mining')}"
)


@st.cache_resource
def get_conn() -> psycopg.Connection:
    return psycopg.connect(DSN)


def _conn() -> psycopg.Connection:
    conn = get_conn()
    if conn.closed:
        st.cache_resource.clear()
        conn = get_conn()
    return conn


@st.cache_data(ttl=60)
def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    try:
        with _conn().cursor() as cur:
            cur.execute(sql, params)
            cols = [d.name for d in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    except Exception:
        return pd.DataFrame()


try:
    get_conn()
except Exception as e:
    st.error(f"Não foi possível conectar ao banco: {e}")
    st.stop()


# ── Lookup ASN ───────────────────────────────────────────────────────────────

_LOOKUP_UA = "bgp-mining UNDB research - caio.fontes.gondin@hotmail.com"


def _lookup_asn(asn: int) -> dict:
    # Tenta bgp.tools para extrair ownerid (CNPJ) — ASNs brasileiros
    try:
        resp = requests.get(
            f"https://bgp.tools/as/{asn}",
            timeout=10,
            headers={"User-Agent": _LOOKUP_UA},
        )
        if resp.status_code == 200:
            match = re.search(r"ownerid:.*?>([\d./-]+)<", resp.text)
            if match:
                cnpj = re.sub(r"[.\-/]", "", match.group(1))
                br = requests.get(
                    f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
                    timeout=10,
                    headers={"User-Agent": _LOOKUP_UA},
                )
                if br.status_code == 200:
                    d = br.json()
                    return {
                        "name": d.get("razao_social") or d.get("nome_fantasia") or "",
                        "country": "BR",
                        "city": d.get("municipio") or "",
                        "state": d.get("uf") or "",
                    }
    except Exception:
        pass

    # Fallback: RIPE Stat — ASNs internacionais ou quando bgp.tools falhar
    try:
        resp = requests.get(
            f"https://stat.ripe.net/data/as-overview/data.json?resource=AS{asn}",
            timeout=10,
            headers={"User-Agent": _LOOKUP_UA},
        )
        if resp.status_code == 200:
            data = resp.json().get("data", {})
            return {
                "name": data.get("holder") or "",
                "country": "",
                "city": "",
                "state": "",
            }
    except Exception:
        pass

    return {}


def _get_existing_asn(asn: int) -> dict | None:
    df = query("SELECT * FROM known_asns WHERE asn = %s", (asn,))
    if df.empty:
        return None
    r = df.iloc[0]
    return {
        "name": r.get("name") or "",
        "type": r.get("type") or "owner",
        "country": r.get("country") or "",
        "city": r.get("city") or "",
        "state": r.get("state") or "",
    }


# ── Header ────────────────────────────────────────────────────────────────────

col_title, col_status = st.columns([4, 1])
with col_title:
    st.title("BGP Mining Dashboard")
    st.caption("Monitoramento de anúncios BGP — detecção de mitigação DDoS")
with col_status:
    last_job = query(
        "SELECT status, finished_at FROM collection_jobs ORDER BY started_at DESC LIMIT 1"
    )
    if not last_job.empty:
        status = last_job.iloc[0]["status"]
        st.metric("Última coleta", status)
    else:
        st.metric("Última coleta", "sem dados")


# ── Auto Refresh ─────────────────────────────────────────────────────────────

ctrl_col, hint_col = st.columns([2, 3])
with ctrl_col:
    auto_refresh_enabled = st.toggle(
        "Atualização automática",
        value=True,
        help="Recarrega automaticamente os dados do dashboard.",
    )
with hint_col:
    auto_refresh_seconds = st.selectbox(
        "Intervalo",
        [15, 30, 60, 120],
        index=2,
        disabled=not auto_refresh_enabled,
        format_func=lambda x: f"{x}s",
    )

if auto_refresh_enabled:
    try:
        from streamlit_autorefresh import st_autorefresh

        st_autorefresh(
            interval=int(auto_refresh_seconds) * 1000,
            key="bgp_dashboard_autorefresh",
        )
        st.caption(f"Atualização automática a cada {auto_refresh_seconds}s.")
    except Exception:
        st.warning(
            "Auto refresh indisponível: instale o pacote 'streamlit-autorefresh'."
        )


# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_overview, tab_provider, tab_asns = st.tabs(
    [
        "Visão Geral",
        "Análise de Provedor",
        "Cadastro de ASN",
    ]
)


# ============================================================================
# Tab 1 — Visão Geral
# ============================================================================
with tab_overview:
    # ── KPIs ─────────────────────────────────────────────────────────────────
    totals = query("""
        SELECT
            COUNT(*)                                       AS total_rotas,
            COUNT(*) FILTER (WHERE is_mitigated)           AS mitigadas,
            COUNT(DISTINCT origin_asn)                     AS owners_ativos,
            (SELECT COUNT(*) FROM known_asns WHERE type = 'owner')     AS owners_cadastrados,
            (SELECT COUNT(*) FROM known_asns WHERE type = 'mitigator') AS mitigadores_cadastrados
        FROM bgp_routes
    """)

    if not totals.empty:
        r = totals.iloc[0]
        total = int(r.total_rotas)
        mitig = int(r.mitigadas)
        pct = round(100 * mitig / total, 1) if total else 0.0

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total de rotas coletadas", f"{total:,}")
        k2.metric("Rotas mitigadas", f"{mitig:,}", delta=f"{pct}%")
        k3.metric("Provedoras com dados", int(r.owners_ativos))
        k4.metric("Provedoras cadastradas", int(r.owners_cadastrados))
        k5.metric("Mitigadores conhecidos", int(r.mitigadores_cadastrados))

    st.divider()

    # ── Mitigação por provedora ───────────────────────────────────────────────
    st.subheader("% de mitigação por provedora")
    df_owner_mit = query("""
        SELECT
            k.asn,
            COALESCE(k.name, 'AS' || k.asn::text) AS nome,
            COALESCE(k.city || ' / ' || k.state, k.state, k.city, '—') AS localidade,
            COUNT(r.id) AS total_rotas,
            COUNT(r.id) FILTER (WHERE r.is_mitigated) AS mitigadas,
            ROUND(
                100.0 * COUNT(r.id) FILTER (WHERE r.is_mitigated)
                / NULLIF(COUNT(r.id), 0),
                1
            ) AS pct_mitigacao
        FROM known_asns k
        LEFT JOIN bgp_routes r ON r.origin_asn = k.asn
        WHERE k.type = 'owner'
        GROUP BY k.asn, k.name, k.city, k.state
        HAVING COUNT(r.id) > 0
        ORDER BY pct_mitigacao DESC NULLS LAST
    """)

    if not df_owner_mit.empty:
        col_chart, col_table = st.columns([2, 1])

        with col_chart:
            chart = (
                alt.Chart(df_owner_mit.head(15))
                .mark_bar()
                .encode(
                    x=alt.X(
                        "pct_mitigacao:Q",
                        title="% Mitigação",
                        scale=alt.Scale(domain=[0, 100]),
                    ),
                    y=alt.Y("nome:N", sort="-x", title=""),
                    color=alt.Color(
                        "pct_mitigacao:Q",
                        scale=alt.Scale(scheme="redyellowgreen", reverse=True),
                        legend=None,
                    ),
                    tooltip=[
                        "nome",
                        "localidade",
                        "total_rotas",
                        "mitigadas",
                        "pct_mitigacao",
                    ],
                )
                .properties(height=350)
            )
            st.altair_chart(chart, use_container_width=True)

        with col_table:
            st.dataframe(
                df_owner_mit[
                    ["nome", "localidade", "total_rotas", "mitigadas", "pct_mitigacao"]
                ].rename(
                    columns={
                        "nome": "Provedora",
                        "localidade": "Cidade/UF",
                        "total_rotas": "Total",
                        "mitigadas": "Mitigadas",
                        "pct_mitigacao": "% Mit.",
                    }
                ),
                use_container_width=True,
                hide_index=True,
                height=360,
            )
    else:
        st.info("Sem dados de rotas para os ASNs cadastrados ainda.")

    st.divider()

    # ── Mapa de calor temporal ────────────────────────────────────────────────
    st.subheader("Mapa de calor — mitigação por hora e dia da semana")
    st.caption(
        "Intensidade = % de rotas mitigadas naquele bloco de hora. Revela padrões temporais de ataques."
    )
    df_heatmap = query("""
        SELECT
            EXTRACT(DOW FROM collected_at)::int  AS dia,
            EXTRACT(HOUR FROM collected_at)::int AS hora,
            ROUND(100.0 * COUNT(*) FILTER (WHERE is_mitigated) / NULLIF(COUNT(*), 0), 1) AS pct
        FROM bgp_routes
        GROUP BY dia, hora
        ORDER BY dia, hora
    """)

    if not df_heatmap.empty:
        dias = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"]
        df_heatmap["dia_nome"] = df_heatmap["dia"].apply(lambda d: dias[int(d)])
        chart = (
            alt.Chart(df_heatmap)
            .mark_rect()
            .encode(
                x=alt.X("hora:O", title="Hora do dia"),
                y=alt.Y("dia_nome:N", sort=dias, title=""),
                color=alt.Color(
                    "pct:Q",
                    scale=alt.Scale(scheme="reds"),
                    legend=alt.Legend(title="% Mitigado"),
                ),
                tooltip=[
                    alt.Tooltip("dia_nome:N", title="Dia"),
                    alt.Tooltip("hora:O", title="Hora"),
                    alt.Tooltip("pct:Q", title="% Mitigado"),
                ],
            )
            .properties(height=220)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Sem dados suficientes para o mapa de calor.")

    st.divider()

    # ── Top mitigadores ───────────────────────────────────────────────────────
    left, right = st.columns(2)

    with left:
        st.subheader("Top mitigadores")
        df_mit = query("""
            SELECT
                COALESCE(k.name, 'AS' || r.mitigator_asn::text) AS mitigador,
                COUNT(*) AS total
            FROM bgp_routes r
            LEFT JOIN known_asns k ON k.asn = r.mitigator_asn
            WHERE r.is_mitigated AND r.mitigator_asn IS NOT NULL
            GROUP BY mitigador
            ORDER BY total DESC
            LIMIT 8
        """)
        if not df_mit.empty:
            chart = (
                alt.Chart(df_mit)
                .mark_bar(color="#ff4b4b")
                .encode(
                    x=alt.X("total:Q", title="Rotas"),
                    y=alt.Y("mitigador:N", sort="-x", title=""),
                    tooltip=["mitigador", "total"],
                )
                .properties(height=260)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Sem rotas mitigadas registradas.")

    with right:
        st.subheader("Histórico de coletas")
        df_jobs = query("""
            SELECT
                id, source, status, started_at, finished_at, records_found,
                ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 1) AS duração_s,
                error_msg
            FROM collection_jobs
            ORDER BY started_at DESC
            LIMIT 10
        """)
        if not df_jobs.empty:

            def _color_status(val: str) -> str:
                return {
                    "done": "background-color:#d4edda",
                    "error": "background-color:#f8d7da",
                    "running": "background-color:#fff3cd",
                }.get(val, "")

            st.dataframe(
                df_jobs.style.map(_color_status, subset=["status"]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Nenhum job registrado ainda.")


# ============================================================================
# Tab 2 — Análise de Provedor
# ============================================================================
with tab_provider:
    df_owners = query("""
        SELECT k.asn, COALESCE(k.name, 'AS' || k.asn::text) AS nome
        FROM known_asns k
        WHERE k.type = 'owner'
        ORDER BY k.name
    """)

    asn_sel = None

    if df_owners.empty:
        st.info(
            "Nenhuma provedora cadastrada. Cadastre ASNs do tipo owner na aba Cadastro de ASN."
        )
    else:
        opcoes = {f"AS{r.asn} — {r.nome}": r.asn for r in df_owners.itertuples()}
        escolha = st.selectbox(
            "Selecione a provedora", list(opcoes.keys()), key="provider_sel"
        )
        asn_sel = opcoes[escolha]

    if asn_sel is not None:
        st.divider()

        # ── KPIs do provedor ──────────────────────────────────────────────────
        df_kpi = query(
            """
            SELECT
                COUNT(DISTINCT prefix::text)                        AS prefixos,
                COUNT(*)                                            AS total_rotas,
                COUNT(*) FILTER (WHERE is_mitigated)                AS mitigadas,
                ROUND(100.0 * COUNT(*) FILTER (WHERE is_mitigated) / NULLIF(COUNT(*), 0), 1) AS pct,
                (
                    SELECT COALESCE(k2.name, 'AS' || r2.mitigator_asn::text)
                    FROM bgp_routes r2
                    LEFT JOIN known_asns k2 ON k2.asn = r2.mitigator_asn
                    WHERE r2.origin_asn = %s AND r2.is_mitigated AND r2.mitigator_asn IS NOT NULL
                    GROUP BY k2.name, r2.mitigator_asn
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                ) AS principal_mitigador
            FROM bgp_routes
            WHERE origin_asn = %s
        """,
            (asn_sel, asn_sel),
        )

        if not df_kpi.empty:
            r = df_kpi.iloc[0]
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Prefixos /24", int(r.prefixos) if r.prefixos else 0)
            k2.metric("Total de rotas", f"{int(r.total_rotas):,}" if r.total_rotas else "0")
            k3.metric("% mitigado", f"{r.pct}%" if r.pct else "0%")
            k4.metric("Principal mitigador", r.principal_mitigador or "—")

        st.divider()

        # ── Prefixos e status de mitigação ────────────────────────────────────
        st.subheader("Prefixos anunciados")
        df_prefixos = query(
            """
            SELECT
                prefix::text AS prefixo,
                COUNT(*) AS coletas,
                COUNT(*) FILTER (WHERE is_mitigated) AS mitigadas,
                ROUND(100.0 * COUNT(*) FILTER (WHERE is_mitigated) / NULLIF(COUNT(*), 0), 1) AS pct_mitigacao,
                COALESCE(
                    (SELECT k.name FROM known_asns k
                     WHERE k.asn = (
                         SELECT mitigator_asn FROM bgp_routes r2
                         WHERE r2.origin_asn = %s AND r2.prefix = bgp_routes.prefix AND r2.mitigator_asn IS NOT NULL
                         LIMIT 1
                     ) LIMIT 1),
                    '—'
                ) AS mitigador
            FROM bgp_routes
            WHERE origin_asn = %s
            GROUP BY prefix
            ORDER BY pct_mitigacao DESC NULLS LAST
        """,
            (asn_sel, asn_sel),
        )

        if not df_prefixos.empty:
            chart = (
                alt.Chart(df_prefixos)
                .mark_bar()
                .encode(
                    x=alt.X("pct_mitigacao:Q", title="% Mitigação", scale=alt.Scale(domain=[0, 100])),
                    y=alt.Y("prefixo:N", sort="-x", title=""),
                    color=alt.Color("pct_mitigacao:Q", scale=alt.Scale(scheme="redyellowgreen", reverse=True), legend=None),
                    tooltip=["prefixo", "coletas", "mitigadas", "pct_mitigacao", "mitigador"],
                )
                .properties(height=max(200, len(df_prefixos) * 25))
            )
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(
                df_prefixos.rename(columns={"prefixo": "Prefixo", "coletas": "Coletas", "mitigadas": "Mitigadas", "pct_mitigacao": "% Mit.", "mitigador": "Mitigador"}),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Sem rotas coletadas para esta provedora ainda.")

        st.divider()

        # ── Communities ───────────────────────────────────────────────────────
        st.subheader("Communities BGP observadas")
        df_comm = query(
            """
            SELECT unnest(communities) AS community, COUNT(*) AS freq
            FROM bgp_routes
            WHERE origin_asn = %s AND array_length(communities, 1) > 0
            GROUP BY community
            ORDER BY freq DESC
            LIMIT 12
        """,
            (asn_sel,),
        )

        if not df_comm.empty:
            chart = (
                alt.Chart(df_comm)
                .mark_bar(color="#7b61ff")
                .encode(
                    x=alt.X("freq:Q", title="Frequência"),
                    y=alt.Y("community:N", sort="-x", title=""),
                    tooltip=["community", "freq"],
                )
                .properties(height=280)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Sem communities registradas para esta provedora.")

        st.divider()

        # ── Mapa de calor — prefixo × tempo ──────────────────────────────────
        st.subheader("Mapa de calor — mitigação por prefixo ao longo do tempo")
        st.caption("Cada célula representa uma coleta. Vermelho = 100% mitigado, branco = sem mitigação.")
        df_heat_prov = query(
            """
            SELECT
                prefix::text                                                              AS prefixo,
                DATE_TRUNC('hour', collected_at)                                          AS hora,
                ROUND(100.0 * COUNT(*) FILTER (WHERE is_mitigated) / NULLIF(COUNT(*), 0), 1) AS pct
            FROM bgp_routes
            WHERE origin_asn = %s
            GROUP BY prefix, hora
            ORDER BY hora
        """,
            (asn_sel,),
        )

        if not df_heat_prov.empty:
            df_heat_prov["hora"] = pd.to_datetime(df_heat_prov["hora"])
            chart = (
                alt.Chart(df_heat_prov)
                .mark_rect()
                .encode(
                    x=alt.X("hora:T", title="Hora da coleta"),
                    y=alt.Y("prefixo:N", title=""),
                    color=alt.Color("pct:Q", scale=alt.Scale(scheme="reds", domain=[0, 100]), legend=alt.Legend(title="% Mitigado")),
                    tooltip=[
                        alt.Tooltip("prefixo:N", title="Prefixo"),
                        alt.Tooltip("hora:T", title="Hora", format="%d/%m %H:%M"),
                        alt.Tooltip("pct:Q", title="% Mitigado"),
                    ],
                )
                .properties(height=max(150, len(df_heat_prov["prefixo"].unique()) * 30))
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Sem dados suficientes para o mapa de calor.")

        st.divider()

        # ── Auditoria de rotas ────────────────────────────────────────────────
        st.subheader("Auditoria de rotas")
        st.caption("Cada linha representa uma rota coletada. O caminho mostra todas as ASNs pelo qual o tráfego passa naquele momento.")

        col_f1, col_f2 = st.columns([2, 1])
        with col_f1:
            prefixo_filtro = st.text_input("Filtrar por prefixo", placeholder="Ex: 45.160.192.0/24", key="audit_prefix")
        with col_f2:
            limite_audit = st.slider("Máx. linhas", 50, 500, 100, step=50, key="audit_limit")

        where_audit = ["r.origin_asn = %s"]
        params_audit: list = [asn_sel]
        if prefixo_filtro.strip():
            where_audit.append("r.prefix::text LIKE %s")
            params_audit.append(f"%{prefixo_filtro.strip()}%")

        df_rotas = query(
            f"""
            SELECT
                r.prefix::text   AS prefixo,
                r.as_path,
                array_to_string(r.communities, ', ') AS communities,
                r.is_mitigated   AS mitigada,
                r.collected_at   AS coletada_em
            FROM bgp_routes r
            WHERE {" AND ".join(where_audit)}
            ORDER BY r.collected_at DESC
            LIMIT %s
            """,
            tuple(params_audit + [limite_audit]),
        )

        if not df_rotas.empty:
            df_nomes = query("SELECT asn, name FROM known_asns WHERE name IS NOT NULL")
            nome_map: dict[int, str] = (
                {int(r.asn): r.name for r in df_nomes.itertuples()}
                if not df_nomes.empty else {}
            )

            def _fmt_path(raw: str) -> str:
                if not raw:
                    return "—"
                hops = []
                for part in raw.strip().split():
                    try:
                        asn = int(part)
                        label = nome_map.get(asn, f"AS{asn}")
                        hops.append(label)
                    except ValueError:
                        hops.append(part)
                return " → ".join(hops)

            df_rotas["caminho"] = df_rotas["as_path"].apply(_fmt_path)

            st.dataframe(
                df_rotas[["prefixo", "caminho", "communities", "mitigada", "coletada_em"]].rename(
                    columns={"prefixo": "Prefixo", "caminho": "Caminho (AS-PATH)", "communities": "Communities", "mitigada": "Mitigada", "coletada_em": "Coletada em"}
                ),
                use_container_width=True,
                hide_index=True,
            )
            st.caption(f"{len(df_rotas)} rotas exibidas")
            csv = df_rotas[["prefixo", "as_path", "caminho", "communities", "mitigada", "coletada_em"]].to_csv(index=False).encode("utf-8")
            st.download_button("Exportar CSV", csv, f"auditoria_as{asn_sel}.csv", "text/csv")
        else:
            st.info("Sem rotas para exibir.")


# ============================================================================
# Tab 3 — Cadastro de ASN
# ============================================================================
with tab_asns:
    # ── Lista de ASNs ─────────────────────────────────────────────────────────
    st.subheader("ASNs cadastrados")

    tipo_filtro = st.radio(
        "Filtrar por tipo",
        ["Todos", "owner", "mitigator", "transit"],
        horizontal=True,
    )

    sql_list = """
        SELECT
            asn,
            name AS nome,
            type AS tipo,
            country AS país,
            city AS cidade,
            state AS estado,
            updated_at AS atualizado_em
        FROM known_asns
        {where}
        ORDER BY type, asn
    """
    where_list = "" if tipo_filtro == "Todos" else "WHERE type = %s"
    params_list: tuple = () if tipo_filtro == "Todos" else (tipo_filtro,)
    df_asns = query(sql_list.format(where=where_list), params_list)

    null_count = 0
    if not df_asns.empty:
        null_count = df_asns["cidade"].isna().sum()
        if null_count:
            st.warning(f"{null_count} ASN(s) sem cidade cadastrada.")

        st.dataframe(
            df_asns.style.highlight_null(color="#fff3cd"),
            use_container_width=True,
            hide_index=True,
        )
        st.caption(f"{len(df_asns)} ASN(s) listados")
    else:
        st.info("Nenhum ASN cadastrado ainda.")

    st.divider()

    # ── Formulário de cadastro ────────────────────────────────────────────────
    st.subheader("Cadastrar / editar ASN")

    st.markdown(
        "**Como usar:** informe o número do ASN → clique em **Buscar** para "
        "pré-preencher os campos (ou preencha manualmente) → clique em **Salvar**."
    )

    # Step 1 — número + lookup
    asn_col, btn_col = st.columns([2, 1])
    with asn_col:
        asn_input = st.number_input(
            "Número do ASN *",
            min_value=1,
            step=1,
            value=None,
            placeholder="Ex: 268471",
            key="asn_input",
        )
    with btn_col:
        st.write("")
        st.write("")
        buscar = st.button(
            "Buscar informações",
            disabled=asn_input is None,
            help="Consulta BGPView e banco de dados para pré-preencher os campos",
        )

    if "asn_form_data" not in st.session_state:
        st.session_state.asn_form_data = {}

    if buscar and asn_input:
        asn_int = int(asn_input)
        existing = _get_existing_asn(asn_int)

        if existing:
            st.session_state.asn_form_data = existing
            st.info(
                f"AS{asn_int} já está no banco. Os campos foram preenchidos com os dados atuais — edite e salve para atualizar."
            )
        else:
            with st.spinner("Consultando RIPE Stat..."):
                looked_up = _lookup_asn(asn_int)
            if looked_up:
                st.session_state.asn_form_data = looked_up
                st.success(
                    "Dados encontrados no RIPE Stat. Confira, edite se necessário e salve."
                )
            else:
                st.session_state.asn_form_data = {}
                st.warning(
                    "Não foi possível buscar via RIPE Stat. Preencha os campos manualmente."
                )

    fd = st.session_state.asn_form_data

    # Step 2 — formulário
    with st.form("form_asn", clear_on_submit=False):
        c1, c2 = st.columns(2)
        with c1:
            nome = st.text_input("Nome / organização *", value=fd.get("name", ""))
            pais = st.text_input(
                "País (código 2 letras)",
                value=fd.get("country", "BR"),
                max_chars=2,
                help="Ex: BR, US, DE",
            )
            cidade = st.text_input("Cidade", value=fd.get("city", ""))
        with c2:
            tipo = st.segmented_control(
                "Tipo *",
                options=["owner", "mitigator", "transit"],
                default=fd.get("type", "owner"),
                help="owner = ISP monitorado | mitigator = provedor de mitigação DDoS | transit = trânsito",
            )
            estado = st.text_input(
                "Estado (sigla UF)",
                value=fd.get("state", ""),
                max_chars=10,
                help="Ex: MA, SP, RJ",
            )

        salvar = st.form_submit_button(
            "Salvar ASN",
            disabled=asn_input is None,
            type="primary",
        )

        if salvar and asn_input:
            if not nome.strip():
                st.error("O campo **Nome / organização** é obrigatório.")
            else:
                try:
                    with _conn().cursor() as cur:
                        cur.execute(
                            """
                            INSERT INTO known_asns (asn, name, type, country, city, state)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (asn) DO UPDATE
                                SET name       = EXCLUDED.name,
                                    type       = EXCLUDED.type,
                                    country    = EXCLUDED.country,
                                    city       = EXCLUDED.city,
                                    state      = EXCLUDED.state,
                                    updated_at = NOW()
                            """,
                            (
                                int(asn_input),
                                nome.strip(),
                                tipo,
                                pais.strip().upper() or None,
                                cidade.strip() or None,
                                estado.strip().upper() or None,
                            ),
                        )
                    _conn().commit()
                except Exception as e:
                    st.error(f"Erro ao salvar: {e}")
                else:
                    st.success(
                        f"AS{int(asn_input)} — {nome.strip()} salvo com sucesso."
                    )
                    st.session_state.asn_form_data = {}

                    if tipo == "owner":
                        try:
                            from shared.mitigator_discovery import discover_for_asn_sync

                            with st.spinner("Procurando mitigadores associados..."):
                                mitigators = discover_for_asn_sync(int(asn_input))
                            if mitigators:
                                inserted = 0
                                with _conn().cursor() as cur:
                                    for m in mitigators:
                                        cur.execute(
                                            """
                                            INSERT INTO known_asns (asn, name, type, country)
                                            VALUES (%s, %s, 'mitigator', %s)
                                            ON CONFLICT (asn) DO NOTHING
                                            """,
                                            (m["asn"], m["name"], m["country"]),
                                        )
                                        inserted += cur.rowcount
                                _conn().commit()
                                if inserted:
                                    st.info(
                                        f"{inserted} mitigador(es) identificados automaticamente."
                                    )
                        except Exception as e:
                            st.warning(
                                f"ASN salvo, mas a descoberta automática de mitigadores falhou: {e}"
                            )

                    st.cache_data.clear()
                    st.rerun()

    st.divider()

    # ── Remover ASN ───────────────────────────────────────────────────────────
    st.subheader("Remover ASN")

    df_all = query("SELECT asn, name, type FROM known_asns ORDER BY type, asn")

    if not df_all.empty:
        opcoes = {
            f"AS{r.asn} — {r.name or 'sem nome'} ({r.type})": r.asn
            for r in df_all.itertuples()
        }
        asn_remover = st.selectbox("Selecione o ASN", list(opcoes.keys()))

        confirmar, _ = st.columns([1, 3])
        with confirmar:
            if st.button("Remover", type="secondary"):
                asn_val = opcoes[asn_remover]
                try:
                    with _conn().cursor() as cur:
                        cur.execute("DELETE FROM known_asns WHERE asn = %s", (asn_val,))
                    _conn().commit()
                    st.success(f"AS{asn_val} removido.")
                    st.cache_data.clear()
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro ao remover: {e}")
    else:
        st.info("Nenhum ASN para remover.")
