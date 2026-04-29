"""Dashboard Streamlit — BGP Mining."""

from __future__ import annotations

import hashlib
import os
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
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# ── Autenticação ──────────────────────────────────────────────────────────────

_PASSWORD_HASH = hashlib.sha256(
    os.getenv("DASHBOARD_PASSWORD", "Acesso14").encode()
).hexdigest()


def _check_password(entered: str) -> bool:
    return hashlib.sha256(entered.encode()).hexdigest() == _PASSWORD_HASH


if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown(
        """
        <style>
        section[data-testid="stMain"] > div { max-width: 400px; margin: 10vh auto; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("🌐 BGP Mining")
    st.caption("Sistema de Monitoramento de Anúncios BGP — UNDB")
    st.divider()

    with st.form("login"):
        senha = st.text_input("Senha", type="password", placeholder="Digite a senha de acesso")
        entrar = st.form_submit_button("Entrar", type="primary", use_container_width=True)

    if entrar:
        if _check_password(senha):
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Senha incorreta.")

    st.stop()

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


# ── Lookup BGPView ────────────────────────────────────────────────────────────


def _lookup_asn(asn: int) -> dict:
    try:
        resp = requests.get(
            f"https://api.bgpview.io/asn/{asn}",
            timeout=10,
            headers={"User-Agent": "BGP-Mining-Dashboard/1.0"},
        )
        if resp.status_code != 200:
            return {}
        data = resp.json().get("data", {})
    except Exception:
        return {}

    country = data.get("country_code") or ""
    name = data.get("description_short") or data.get("name") or ""
    city, state = _parse_address(data.get("owner_address", []), country)
    return {"name": name, "country": country, "city": city, "state": state}


def _parse_address(address: list, country_code: str) -> tuple[str, str]:
    lines = [str(ln).strip() for ln in address if str(ln).strip()]
    lines = [ln for ln in lines if ln.upper() != country_code.upper()]
    if not lines:
        return "", ""
    state = ""
    if len(lines[-1]) <= 3 and lines[-1].isalpha():
        state = lines[-1].upper()
        lines = lines[:-1]
    city = lines[-1] if lines else ""
    return city, state


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

col_title, col_status, col_logout = st.columns([3, 1, 1])
with col_title:
    st.title("🌐 BGP Mining Dashboard")
    st.caption("Monitoramento de anúncios BGP — detecção de mitigação DDoS")
with col_status:
    last_job = query(
        "SELECT status, finished_at FROM collection_jobs ORDER BY started_at DESC LIMIT 1"
    )
    if not last_job.empty:
        status = last_job.iloc[0]["status"]
        color = {"done": "🟢", "running": "🟡", "error": "🔴"}.get(status, "⚪")
        st.metric("Última coleta", f"{color} {status}")
    else:
        st.metric("Última coleta", "⚪ sem dados")
with col_logout:
    st.write("")
    st.write("")
    if st.button("Sair", use_container_width=True):
        st.session_state.authenticated = False
        st.rerun()


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

tab_overview, tab_analysis, tab_routes, tab_events, tab_asns = st.tabs(
    [
        "📊 Visão Geral",
        "🔍 Análise BGP",
        "🗂️ Rotas",
        "🚨 Eventos",
        "⚙️ Gerenciar ASNs",
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
            COUNT(DISTINCT prefix::text)                   AS prefixos_unicos,
            COUNT(DISTINCT origin_asn)                     AS asns_origem,
            (SELECT COUNT(*) FROM known_asns WHERE type = 'mitigator') AS mitigadores_ativos
        FROM bgp_routes
    """)

    if not totals.empty:
        r = totals.iloc[0]
        total = int(r.total_rotas)
        mitig = int(r.mitigadas)
        pct = round(100 * mitig / total, 1) if total else 0.0

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total de rotas", f"{total:,}")
        k2.metric("Rotas mitigadas", f"{mitig:,}", delta=f"{pct}%")
        k3.metric("Prefixos únicos", f"{int(r.prefixos_unicos):,}")
        k4.metric("ASNs de origem", int(r.asns_origem))
        k5.metric("Mitigadores ativos", int(r.mitigadores_ativos))

    st.divider()

    # ── Gráficos principais ───────────────────────────────────────────────────
    left, right = st.columns(2)

    with left:
        st.subheader("Mitigação por fonte")
        df_src = query("""
            SELECT
                source,
                COUNT(*) FILTER (WHERE is_mitigated)     AS mitigadas,
                COUNT(*) FILTER (WHERE NOT is_mitigated) AS nao_mitigadas
            FROM bgp_routes
            GROUP BY source
            ORDER BY source
        """)
        if not df_src.empty:
            df_melt = df_src.melt("source", var_name="status", value_name="total")
            chart = (
                alt.Chart(df_melt)
                .mark_bar()
                .encode(
                    x=alt.X("source:N", title="Fonte"),
                    y=alt.Y("total:Q", title="Rotas"),
                    color=alt.Color(
                        "status:N",
                        scale=alt.Scale(
                            domain=["mitigadas", "nao_mitigadas"],
                            range=["#ff4b4b", "#4b9fff"],
                        ),
                        legend=alt.Legend(title="Status"),
                    ),
                    tooltip=["source", "status", "total"],
                )
                .properties(height=260)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Sem dados de coleta ainda.")

    with right:
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

    st.divider()

    # ── Série temporal ────────────────────────────────────────────────────────
    st.subheader("Volume coletado por hora")
    df_time = query("""
        SELECT
            DATE_TRUNC('hour', collected_at) AS hora,
            source,
            COUNT(*) AS total
        FROM bgp_routes
        GROUP BY hora, source
        ORDER BY hora
    """)

    if not df_time.empty:
        df_time["hora"] = pd.to_datetime(df_time["hora"])
        chart = (
            alt.Chart(df_time)
            .mark_line(point=True)
            .encode(
                x=alt.X("hora:T", title="Hora"),
                y=alt.Y("total:Q", title="Rotas"),
                color=alt.Color("source:N", legend=alt.Legend(title="Fonte")),
                tooltip=["hora:T", "source", "total"],
            )
            .properties(height=220)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Sem dados de série temporal ainda.")

    st.divider()

    # ── Histórico de coletas ──────────────────────────────────────────────────
    st.subheader("Histórico de coletas")
    df_jobs = query("""
        SELECT
            id, source, status, started_at, finished_at, records_found,
            ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 1) AS duração_s,
            error_msg
        FROM collection_jobs
        ORDER BY started_at DESC
        LIMIT 20
    """)

    if not df_jobs.empty:

        def _color_status(val: str) -> str:
            return {
                "done": "background-color:#d4edda",
                "error": "background-color:#f8d7da",
                "running": "background-color:#fff3cd",
            }.get(val, "")

        styled = df_jobs.style.map(_color_status, subset=["status"])
        st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhum job registrado ainda.")


# ============================================================================
# Tab 2 — Análise BGP
# ============================================================================
with tab_analysis:
    # ── % Mitigação por ASN owner ─────────────────────────────────────────────
    st.subheader("Mitigação por ASN monitorado")
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
        ORDER BY pct_mitigacao DESC NULLS LAST
    """)

    if not df_owner_mit.empty and df_owner_mit["total_rotas"].sum() > 0:
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
                        "nome": "ASN",
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
        st.info("Sem dados de rotas para os ASNs owners cadastrados.")
        if not df_owner_mit.empty:
            st.dataframe(
                df_owner_mit[["nome", "localidade"]].rename(
                    columns={"nome": "ASN", "localidade": "Cidade/UF"}
                ),
                use_container_width=True,
                hide_index=True,
            )

    st.divider()

    # ── Communities ───────────────────────────────────────────────────────────
    col_comm, col_geo = st.columns(2)

    with col_comm:
        st.subheader("Communities mais frequentes")
        df_comm = query("""
            SELECT unnest(communities) AS community, COUNT(*) AS freq
            FROM bgp_routes
            WHERE array_length(communities, 1) > 0
            GROUP BY community
            ORDER BY freq DESC
            LIMIT 12
        """)

        if not df_comm.empty:
            chart = (
                alt.Chart(df_comm)
                .mark_bar(color="#7b61ff")
                .encode(
                    x=alt.X("freq:Q", title="Frequência"),
                    y=alt.Y("community:N", sort="-x", title=""),
                    tooltip=["community", "freq"],
                )
                .properties(height=300)
            )
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Sem communities registradas.")

    with col_geo:
        st.subheader("Distribuição geográfica")
        df_geo = query("""
            SELECT
                COALESCE(k.state, 'N/D') AS uf,
                COUNT(DISTINCT k.asn) AS asns,
                COUNT(r.id) AS rotas,
                COUNT(r.id) FILTER (WHERE r.is_mitigated) AS mitigadas
            FROM known_asns k
            LEFT JOIN bgp_routes r ON r.origin_asn = k.asn
            WHERE k.type = 'owner'
            GROUP BY uf
            ORDER BY rotas DESC
        """)

        if not df_geo.empty:
            st.dataframe(
                df_geo.rename(
                    columns={
                        "uf": "UF",
                        "asns": "ASNs",
                        "rotas": "Rotas",
                        "mitigadas": "Mitigadas",
                    }
                ),
                use_container_width=True,
                hide_index=True,
                height=320,
            )
        else:
            st.info("Sem dados geográficos.")

    st.divider()

    # ── AS-PATH ───────────────────────────────────────────────────────────────
    st.subheader("Comprimento médio do AS-PATH")
    df_path = query("""
        SELECT
            is_mitigated,
            ROUND(AVG(array_length(string_to_array(trim(as_path), ' '), 1)), 2) AS avg_hops,
            MIN(array_length(string_to_array(trim(as_path), ' '), 1)) AS min_hops,
            MAX(array_length(string_to_array(trim(as_path), ' '), 1)) AS max_hops,
            COUNT(*) AS total
        FROM bgp_routes
        WHERE as_path <> ''
        GROUP BY is_mitigated
    """)

    if not df_path.empty:
        p1, p2, p3 = st.columns(3)
        for _, row in df_path.iterrows():
            label = "Mitigadas" if row["is_mitigated"] else "Não mitigadas"
            col = p1 if row["is_mitigated"] else p2
            col.metric(
                f"Hops médios — {label}",
                row["avg_hops"],
                help=f"Min: {row['min_hops']} | Max: {row['max_hops']} | Total: {row['total']:,} rotas",
            )
    else:
        st.info("Sem dados de AS-PATH.")


# ============================================================================
# Tab 3 — Rotas
# ============================================================================
with tab_routes:
    st.subheader("Tabela de rotas BGP")

    f1, f2, f3, f4 = st.columns(4)
    with f1:
        df_sources = query("SELECT DISTINCT source FROM bgp_routes ORDER BY source")
        source_values = (
            df_sources["source"].dropna().astype(str).tolist()
            if "source" in df_sources.columns
            else []
        )
        fonte_opts = ["Todas", *source_values]
        fonte_sel = st.selectbox("Fonte", fonte_opts)
    with f2:
        mitig_sel = st.selectbox("Mitigação", ["Todas", "Sim", "Não"])
    with f3:
        asn_filter = st.text_input("Origin ASN", placeholder="Ex: 268471")
    with f4:
        limite = st.slider("Máx. linhas", 50, 1000, 200, step=50)

    where: list[str] = []
    params: list = []

    if fonte_sel != "Todas":
        where.append("r.source = %s")
        params.append(fonte_sel)
    if mitig_sel == "Sim":
        where.append("r.is_mitigated = TRUE")
    elif mitig_sel == "Não":
        where.append("r.is_mitigated = FALSE")
    if asn_filter.strip().isdigit():
        where.append("r.origin_asn = %s")
        params.append(int(asn_filter.strip()))

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    df_routes = query(
        f"""
        SELECT
            r.id,
            r.prefix::text              AS prefixo,
            r.origin_asn,
            k_orig.name                 AS origem,
            r.is_mitigated              AS mitigada,
            COALESCE(k_mit.name, '')    AS mitigador,
            r.mitigator_asn,
            r.source                    AS fonte,
            r.as_path,
            array_to_string(r.communities, ', ') AS communities,
            r.collected_at
        FROM bgp_routes r
        LEFT JOIN known_asns k_orig ON k_orig.asn = r.origin_asn
        LEFT JOIN known_asns k_mit  ON k_mit.asn  = r.mitigator_asn
        {where_sql}
        ORDER BY r.collected_at DESC
        LIMIT %s
        """,
        tuple(params + [limite]),
    )

    if not df_routes.empty:
        st.dataframe(df_routes, use_container_width=True, hide_index=True)
        st.caption(f"{len(df_routes)} linhas exibidas")

        csv = df_routes.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Exportar CSV", csv, "rotas_bgp.csv", "text/csv")
    else:
        st.info("Nenhuma rota encontrada com os filtros selecionados.")


# ============================================================================
# Tab 4 — Eventos
# ============================================================================
with tab_events:
    st.subheader("Eventos de possível ataque DDoS")
    st.caption(
        "Heurística: evento inicia quando um prefixo /24 fica 100% mitigado "
        "e encerra quando deixa de estar 100% mitigado."
    )

    ev1, ev2, ev3 = st.columns(3)

    df_events_kpi = query(
        """
        SELECT
            COUNT(*) AS total_eventos,
            COUNT(*) FILTER (WHERE ended_at IS NULL) AS eventos_abertos,
            COUNT(*) FILTER (
                WHERE started_at >= NOW() - INTERVAL '24 hours'
            ) AS eventos_24h
        FROM ddos_attack_events
        """
    )

    if not df_events_kpi.empty:
        row = df_events_kpi.iloc[0]
        ev1.metric("Total de eventos", int(row["total_eventos"]))
        ev2.metric("Eventos em andamento", int(row["eventos_abertos"]))
        ev3.metric("Eventos nas últimas 24h", int(row["eventos_24h"]))

    st.divider()

    st.subheader("Horários de início dos eventos")
    df_timeline = query(
        """
        SELECT
            DATE_TRUNC('hour', started_at) AS hora,
            COUNT(*) AS eventos
        FROM ddos_attack_events
        GROUP BY hora
        ORDER BY hora
        """
    )
    if not df_timeline.empty:
        df_timeline["hora"] = pd.to_datetime(df_timeline["hora"])
        chart = (
            alt.Chart(df_timeline)
            .mark_bar(color="#d6336c")
            .encode(
                x=alt.X("hora:T", title="Hora"),
                y=alt.Y("eventos:Q", title="Quantidade de eventos"),
                tooltip=["hora:T", "eventos"],
            )
            .properties(height=240)
        )
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("Nenhum evento detectado ainda.")

    st.divider()

    st.subheader("Tabela de eventos detectados")
    df_events = query(
        """
        SELECT
            id,
            source AS fonte,
            prefix::text AS prefixo,
            origin_asn,
            mitigator_asn,
            started_at AS inicio,
            ended_at AS fim,
            duration_seconds AS duracao_segundos
        FROM ddos_attack_events
        ORDER BY started_at DESC
        LIMIT 500
        """
    )
    if not df_events.empty:
        st.dataframe(df_events, use_container_width=True, hide_index=True)
    else:
        st.info("Sem eventos para exibir.")

    st.divider()

    st.subheader("Auditoria completa dos dados capturados")
    st.caption(
        "Cada linha representa o consolidado de uma coleta por "
        "fonte + prefixo + ASN de origem."
    )

    c1, c2 = st.columns(2)
    with c1:
        filtro_fonte = st.selectbox(
            "Fonte",
            ["Todas", "ripe", "bgpview"],
            key="audit_fonte",
        )
    with c2:
        limite_auditoria = st.slider(
            "Máx. linhas da auditoria",
            100,
            5000,
            1000,
            step=100,
            key="audit_limite",
        )

    where_audit = ""
    params_audit: list = []
    if filtro_fonte != "Todas":
        where_audit = "WHERE source = %s"
        params_audit.append(filtro_fonte)

    df_audit = query(
        f"""
        SELECT
            id,
            source AS fonte,
            collection_ts AS horario_coleta,
            prefix::text AS prefixo,
            origin_asn,
            total_paths,
            mitigated_paths,
            ROUND((mitigation_ratio * 100)::numeric, 2) AS percentual_mitigado,
            is_fully_mitigated AS mitigado_100,
            mitigator_asn
        FROM mitigation_snapshots
        {where_audit}
        ORDER BY collection_ts DESC
        LIMIT %s
        """,
        tuple(params_audit + [limite_auditoria]),
    )

    if not df_audit.empty:
        st.dataframe(df_audit, use_container_width=True, hide_index=True)
        csv_audit = df_audit.to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Exportar auditoria CSV",
            csv_audit,
            "auditoria_mitigacao.csv",
            "text/csv",
        )
    else:
        st.info("Sem snapshots de auditoria ainda.")


# ============================================================================
# Tab 5 — Gerenciar ASNs
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
            st.warning(f"⚠️ {null_count} ASN(s) sem cidade cadastrada.")

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
            "🔍 Buscar informações",
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
            with st.spinner("Consultando BGPView..."):
                looked_up = _lookup_asn(asn_int)
            if looked_up:
                st.session_state.asn_form_data = looked_up
                st.success(
                    "Dados encontrados no BGPView. Confira, edite se necessário e salve."
                )
            else:
                st.session_state.asn_form_data = {}
                st.warning(
                    "Não foi possível buscar via BGPView. Preencha os campos manualmente."
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
            tipo = st.selectbox(
                "Tipo *",
                ["owner", "mitigator", "transit"],
                index=["owner", "mitigator", "transit"].index(fd.get("type", "owner")),
                help="owner = ISP monitorado | mitigator = provedor de mitigação DDoS | transit = trânsito",
            )
            estado = st.text_input(
                "Estado (sigla UF)",
                value=fd.get("state", ""),
                max_chars=10,
                help="Ex: MA, SP, RJ",
            )

        salvar = st.form_submit_button(
            "💾 Salvar ASN",
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
                        f"✅ AS{int(asn_input)} — {nome.strip()} salvo com sucesso!"
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
                                        f"🛡️ {inserted} mitigador(es) identificados automaticamente."
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
            if st.button("🗑️ Remover", type="secondary"):
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
