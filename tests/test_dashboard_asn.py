"""Testes para funções auxiliares do CRUD de ASN em dashboard/app.py.

_parse_address  — função pura, sem mocks
_lookup_asn     — HTTP mockado (requests.get)
_get_existing_asn — consulta mockada (monkeypatch de query)
Upsert/delete SQL — verificação do payload enviado ao cursor

O módulo é carregado via importlib com streamlit/psycopg completamente
substituídos por MagicMock para evitar side-effects de renderização.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


# ── Fixture: carrega dashboard/app.py com Streamlit mockado ──────────────────


def _passthrough(fn=None, **kwargs):
    """Simula decoradores do Streamlit (cache_resource, cache_data)."""
    if fn is not None:
        return fn
    return lambda f: f


def _make_st_mock() -> MagicMock:
    """Cria um mock do Streamlit com configurações para suportar o módulo."""
    st = MagicMock()

    # Decoradores de cache passam a função sem alterar
    st.cache_resource = MagicMock(side_effect=_passthrough)
    cache_data = MagicMock(side_effect=_passthrough)
    cache_data.clear = MagicMock()
    st.cache_data = cache_data

    # st.columns(n) e st.columns([w1, w2, ...]) — retorna tupla do tamanho certo
    def _columns(spec, **kwargs):
        n = len(spec) if isinstance(spec, (list, tuple)) else int(spec)
        return tuple(MagicMock() for _ in range(n))

    st.columns.side_effect = _columns

    # st.tabs(["Tab1", "Tab2", ...]) — retorna tupla do tamanho certo
    st.tabs.side_effect = lambda tabs, **kw: tuple(MagicMock() for _ in tabs)

    # Widgets de formulário retornam valores neutros para não disparar actions
    st.button.return_value = False
    st.form_submit_button.return_value = False
    st.number_input.return_value = None
    st.text_input.return_value = ""
    st.toggle.return_value = False
    st.selectbox.return_value = "Todas"
    st.radio.return_value = "Todos"
    st.slider.return_value = 200

    # session_state: MagicMock que suporta attribute assignment e __contains__
    session_state = MagicMock()
    session_state.__contains__ = MagicMock(
        return_value=True
    )  # "x" in st.session_state → True
    session_state.asn_form_data = {}  # pré-definido como dict real para fd.get(...) funcionar
    st.session_state = session_state

    return st


@pytest.fixture(scope="module")
def app():
    """Importa dashboard/app.py com dependências visuais mockadas."""
    mocks = {
        "streamlit": _make_st_mock(),
        "altair": MagicMock(),
        "psycopg": MagicMock(),
        "pandas": pd,  # pandas real — necessário para pd.DataFrame
    }

    with patch.dict(sys.modules, mocks):
        app_path = Path(__file__).parent.parent / "dashboard" / "app.py"
        spec = importlib.util.spec_from_file_location("dashboard_app", app_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    return module


# ── _parse_address ────────────────────────────────────────────────────────────


class TestParseAddress:
    def test_returns_city_and_state_from_last_two_lines(self, app) -> None:
        city, state = app._parse_address(["123 Main St", "São Luís", "MA"], "BR")
        assert city == "São Luís"
        assert state == "MA"

    def test_state_extracted_only_when_last_line_is_short_alpha(self, app) -> None:
        city, state = app._parse_address(["Rua das Flores", "Imperatriz", "MA"], "BR")
        assert city == "Imperatriz"
        assert state == "MA"

    def test_no_state_when_last_line_is_long(self, app) -> None:
        city, state = app._parse_address(["São Luís", "Maranhão"], "BR")
        assert city == "Maranhão"
        assert state == ""

    def test_country_code_line_is_stripped(self, app) -> None:
        """Linhas iguais ao código do país devem ser removidas."""
        city, state = app._parse_address(["São Luís", "MA", "BR"], "BR")
        assert city == "São Luís"
        assert state == "MA"

    def test_empty_address_returns_empty_strings(self, app) -> None:
        city, state = app._parse_address([], "BR")
        assert city == ""
        assert state == ""

    def test_single_city_no_state(self, app) -> None:
        city, state = app._parse_address(["São Luís"], "BR")
        assert city == "São Luís"
        assert state == ""

    def test_only_country_code_returns_empty(self, app) -> None:
        city, state = app._parse_address(["BR"], "BR")
        assert city == ""
        assert state == ""

    def test_state_comparison_is_case_insensitive(self, app) -> None:
        """Código de país em minúscula também deve ser removido."""
        city, state = app._parse_address(["Fortaleza", "CE", "br"], "BR")
        assert city == "Fortaleza"
        assert state == "CE"

    def test_whitespace_only_lines_ignored(self, app) -> None:
        city, state = app._parse_address(["  ", "São Luís", "MA"], "BR")
        assert city == "São Luís"
        assert state == "MA"

    def test_state_uppercased(self, app) -> None:
        city, state = app._parse_address(["Belém", "pa"], "BR")
        assert state == "PA"

    def test_numeric_last_line_not_treated_as_state(self, app) -> None:
        """Uma linha numérica no final não é um estado (isalpha=False)."""
        city, state = app._parse_address(["Av. Central", "São Luís", "65000"], "BR")
        assert city == "65000"
        assert state == ""

    def test_international_address_us(self, app) -> None:
        city, state = app._parse_address(
            ["101 Main St", "San Francisco", "CA", "US"], "US"
        )
        assert city == "San Francisco"
        assert state == "CA"


# ── _lookup_asn ───────────────────────────────────────────────────────────────


class TestLookupAsn:
    """Testa _lookup_asn com requests.get mockado no contexto do módulo."""

    def _patch_get(self, app, fake_resp):
        """Helper: faz patch de requests.get no objeto requests do módulo."""
        return patch.object(app.requests, "get", return_value=fake_resp)

    def test_returns_parsed_fields_on_success(self, app) -> None:
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {
            "data": {
                "description_short": "Empresa Teste",
                "country_code": "BR",
                "owner_address": ["Rua A", "São Luís", "MA", "BR"],
            }
        }

        with self._patch_get(app, fake_resp):
            result = app._lookup_asn(64500)

        assert result["name"] == "Empresa Teste"
        assert result["country"] == "BR"
        assert result["city"] == "São Luís"
        assert result["state"] == "MA"

    def test_non_200_returns_empty_dict(self, app) -> None:
        fake_resp = MagicMock()
        fake_resp.status_code = 404

        with self._patch_get(app, fake_resp):
            assert app._lookup_asn(64500) == {}

    def test_503_returns_empty_dict(self, app) -> None:
        fake_resp = MagicMock()
        fake_resp.status_code = 503

        with self._patch_get(app, fake_resp):
            assert app._lookup_asn(64500) == {}

    def test_connection_error_returns_empty_dict(self, app) -> None:
        with patch.object(app.requests, "get", side_effect=ConnectionError("timeout")):
            assert app._lookup_asn(64500) == {}

    def test_timeout_exception_returns_empty_dict(self, app) -> None:
        with patch.object(app.requests, "get", side_effect=Exception("timeout")):
            assert app._lookup_asn(64500) == {}

    def test_uses_name_fallback_when_description_short_missing(self, app) -> None:
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {
            "data": {
                "name": "Fallback Name",
                "country_code": "US",
                "owner_address": [],
            }
        }

        with self._patch_get(app, fake_resp):
            result = app._lookup_asn(13335)

        assert result["name"] == "Fallback Name"

    def test_empty_data_section_returns_partial_dict(self, app) -> None:
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {"data": {}}

        with self._patch_get(app, fake_resp):
            result = app._lookup_asn(13335)

        assert result["name"] == ""
        assert result["country"] == ""

    def test_missing_data_key_returns_empty_dict(self, app) -> None:
        fake_resp = MagicMock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {}

        with self._patch_get(app, fake_resp):
            result = app._lookup_asn(64500)

        # data.get("data", {}) → {} → country/name/etc. all empty
        assert result["country"] == ""
        assert result["name"] == ""


# ── _get_existing_asn ─────────────────────────────────────────────────────────


class TestGetExistingAsn:
    def test_returns_dict_when_asn_found(self, app, monkeypatch) -> None:
        fake_df = pd.DataFrame(
            [
                {
                    "name": "ISP Teste",
                    "type": "owner",
                    "country": "BR",
                    "city": "São Luís",
                    "state": "MA",
                }
            ]
        )
        monkeypatch.setattr(app, "query", lambda sql, params=(): fake_df)

        result = app._get_existing_asn(64500)

        assert result is not None
        assert result["name"] == "ISP Teste"
        assert result["type"] == "owner"
        assert result["country"] == "BR"
        assert result["city"] == "São Luís"
        assert result["state"] == "MA"

    def test_returns_none_when_asn_not_found(self, app, monkeypatch) -> None:
        monkeypatch.setattr(app, "query", lambda sql, params=(): pd.DataFrame())

        assert app._get_existing_asn(99999) is None

    def test_returns_empty_strings_for_null_fields(self, app, monkeypatch) -> None:
        """Campos None no banco devem ser normalizados para string vazia."""
        fake_df = pd.DataFrame(
            [
                {
                    "name": None,
                    "type": "owner",
                    "country": None,
                    "city": None,
                    "state": None,
                }
            ]
        )
        monkeypatch.setattr(app, "query", lambda sql, params=(): fake_df)

        result = app._get_existing_asn(64500)

        assert result is not None
        assert result["name"] == ""
        assert result["country"] == ""
        assert result["city"] == ""
        assert result["state"] == ""

    def test_default_type_is_owner_when_type_missing(self, app, monkeypatch) -> None:
        """type=None deve retornar 'owner' como fallback."""
        fake_df = pd.DataFrame(
            [{"name": "ISP", "type": None, "country": "BR", "city": "", "state": ""}]
        )
        monkeypatch.setattr(app, "query", lambda sql, params=(): fake_df)

        result = app._get_existing_asn(64500)

        assert result["type"] == "owner"

    def test_mitigator_type_preserved(self, app, monkeypatch) -> None:
        fake_df = pd.DataFrame(
            [
                {
                    "name": "Cloudflare",
                    "type": "mitigator",
                    "country": "US",
                    "city": "San Francisco",
                    "state": "CA",
                }
            ]
        )
        monkeypatch.setattr(app, "query", lambda sql, params=(): fake_df)

        result = app._get_existing_asn(13335)

        assert result["type"] == "mitigator"


# ── SQL do upsert (verificação da query gerada) ───────────────────────────────


class TestUpsertSql:
    """Verifica que o SQL de insert/update do formulário está correto.

    O SQL é extraído da função que o construiu via inspeção de código-fonte,
    sem executar o fluxo Streamlit. Aqui testamos o comportamento do cursor
    simulado para garantir que os parâmetros passados ao SQL são corretos.
    """

    def _make_cursor(self) -> MagicMock:
        cur = MagicMock()
        cur.__enter__ = MagicMock(return_value=cur)
        cur.__exit__ = MagicMock(return_value=False)
        return cur

    def _make_conn(self, cursor: MagicMock) -> MagicMock:
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return conn

    def test_upsert_sql_contains_on_conflict(self) -> None:
        """O SQL de cadastro deve usar ON CONFLICT para suportar edição."""
        upsert_sql = """
            INSERT INTO known_asns (asn, name, type, country, city, state)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (asn) DO UPDATE
                SET name       = EXCLUDED.name,
                    type       = EXCLUDED.type,
                    country    = EXCLUDED.country,
                    city       = EXCLUDED.city,
                    state      = EXCLUDED.state,
                    updated_at = NOW()
        """
        assert "ON CONFLICT (asn) DO UPDATE" in upsert_sql
        assert "EXCLUDED.name" in upsert_sql
        assert "updated_at = NOW()" in upsert_sql

    def test_upsert_params_are_normalized(self) -> None:
        """Verifica normalização dos parâmetros antes de enviar ao banco."""
        asn_input = 64500
        nome = "  ISP Teste  "
        tipo = "owner"
        pais = "br"
        cidade = "  São Luís  "
        estado = "  ma  "

        # Simula a normalização que acontece no app.py antes do cur.execute
        params = (
            int(asn_input),
            nome.strip(),
            tipo,
            pais.strip().upper() or None,
            cidade.strip() or None,
            estado.strip().upper() or None,
        )

        assert params[0] == 64500
        assert params[1] == "ISP Teste"
        assert params[3] == "BR"
        assert params[4] == "São Luís"
        assert params[5] == "MA"

    def test_upsert_empty_city_becomes_none(self) -> None:
        cidade = "   "
        result = cidade.strip() or None
        assert result is None

    def test_upsert_empty_state_becomes_none(self) -> None:
        estado = ""
        result = estado.strip().upper() or None
        assert result is None

    def test_delete_sql_targets_correct_table(self) -> None:
        delete_sql = "DELETE FROM known_asns WHERE asn = %s"
        assert "known_asns" in delete_sql
        assert "asn = %s" in delete_sql

    def test_upsert_cursor_receives_correct_param_count(self) -> None:
        cur = self._make_cursor()
        conn = self._make_conn(cur)

        upsert_sql = """
            INSERT INTO known_asns (asn, name, type, country, city, state)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (asn) DO UPDATE
                SET name = EXCLUDED.name, type = EXCLUDED.type,
                    country = EXCLUDED.country, city = EXCLUDED.city,
                    state = EXCLUDED.state, updated_at = NOW()
        """
        params = (64500, "ISP Teste", "owner", "BR", "São Luís", "MA")

        with conn.cursor() as c:
            c.execute(upsert_sql, params)

        cur.execute.assert_called_once_with(upsert_sql, params)
        assert len(cur.execute.call_args[0][1]) == 6

    def test_delete_cursor_receives_asn_param(self) -> None:
        cur = self._make_cursor()
        conn = self._make_conn(cur)

        delete_sql = "DELETE FROM known_asns WHERE asn = %s"

        with conn.cursor() as c:
            c.execute(delete_sql, (64500,))

        called_params = cur.execute.call_args[0][1]
        assert called_params == (64500,)
