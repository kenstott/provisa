# Copyright (c) 2026 Kenneth Stott
# Canary: b3c4d5e6-f7a8-9012-bcde-f01234567890
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Unit tests for REQ-807: Bolt auth model — principal=user, database=role.

Pure logic only — no I/O, no network, no DB, no docker.
Targets:
  - _show_databases_rows  (one db per view x role, provisa_<role> / provisa_ops_<role>)
  - BoltSession._resolve_db  (db-name → role/view selection, home-database default)
  - :use semantics via _resolve_db with user roles
"""

from __future__ import annotations

import io
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Helpers (same pattern as test_bolt_domain_ceiling.py)
# ---------------------------------------------------------------------------


class _FakeWriter:
    """Concrete writer that satisfies BoltWriter structurally and exposes written bytes."""

    def __init__(self) -> None:
        self._buf = io.BytesIO()

    def write(self, data: bytes) -> None:
        self._buf.write(data)

    async def drain(self) -> None:
        pass

    def get_extra_info(self, key: str, default: object = None) -> object:
        _ = key
        return default

    def close(self) -> None:
        pass

    async def wait_closed(self) -> None:
        pass

    def getvalue(self) -> bytes:
        return self._buf.getvalue()


def _make_session(roles: list[str], bolt_version: tuple[int, int] = (5, 4)):
    from provisa.bolt.session import BoltSession

    writer = _FakeWriter()
    session = BoltSession(writer, bolt_version)  # type: ignore[arg-type]
    session.roles = list(roles)
    session.role_id = roles[0] if roles else None
    return session, writer


# ---------------------------------------------------------------------------
# 1. SHOW DATABASES yields one db per (view × role)
# ---------------------------------------------------------------------------


class TestShowDatabasesYieldsViewRolePairs:
    def test_sales_and_finance_roles_yield_four_databases(self):
        """REQ-807: user with roles [sales, finance] → 4 databases (2 per role)."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["sales", "finance"])
        assert len(rows) == 4

    def test_sales_role_has_business_db(self):
        """REQ-807: provisa_sales present for sales role."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["sales", "finance"])
        names = [r[0] for r in rows]
        assert "provisa_sales" in names

    def test_sales_role_has_ops_db(self):
        """REQ-807: provisa_ops_sales present for sales role."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["sales", "finance"])
        names = [r[0] for r in rows]
        assert "provisa_ops_sales" in names

    def test_finance_role_has_business_db(self):
        """REQ-807: provisa_finance present for finance role."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["sales", "finance"])
        names = [r[0] for r in rows]
        assert "provisa_finance" in names

    def test_finance_role_has_ops_db(self):
        """REQ-807: provisa_ops_finance present for finance role."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["sales", "finance"])
        names = [r[0] for r in rows]
        assert "provisa_ops_finance" in names

    def test_exact_names_for_sales_finance(self):
        """REQ-807: exact name set matches the four expected databases."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["sales", "finance"])
        names = {r[0] for r in rows}
        assert names == {
            "provisa_sales",
            "provisa_ops_sales",
            "provisa_finance",
            "provisa_ops_finance",
        }

    def test_order_is_business_then_ops_per_role(self):
        """REQ-807: provisa_<role> appears before provisa_ops_<role> for each role."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["sales", "finance"])
        names = [r[0] for r in rows]
        assert names.index("provisa_sales") < names.index("provisa_ops_sales")
        assert names.index("provisa_finance") < names.index("provisa_ops_finance")

    def test_sales_group_precedes_finance_group(self):
        """REQ-807: rows grouped by role order."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["sales", "finance"])
        names = [r[0] for r in rows]
        assert names.index("provisa_sales") < names.index("provisa_finance")


# ---------------------------------------------------------------------------
# 2. HOME DATABASE defaults to first role's business db
# ---------------------------------------------------------------------------


class TestHomeDatabaseDefault:
    def test_home_is_business_db_of_first_role(self):
        """REQ-807: HOME DATABASE = provisa_<first_role>."""
        from provisa.bolt.session import _show_databases_rows

        cols, rows = _show_databases_rows(["sales", "finance"])
        home_idx = cols.index("home")
        home_names = [r[0] for r in rows if r[home_idx]]
        assert home_names == ["provisa_sales"]

    def test_default_flag_matches_home_flag(self):
        """REQ-807: 'default' column matches 'home' column."""
        from provisa.bolt.session import _show_databases_rows

        cols, rows = _show_databases_rows(["sales", "finance"])
        home_idx = cols.index("home")
        default_idx = cols.index("default")
        for r in rows:
            assert r[home_idx] == r[default_idx]

    def test_ops_db_of_first_role_is_not_home(self):
        """REQ-807: ops view of first role is never HOME."""
        from provisa.bolt.session import _show_databases_rows

        cols, rows = _show_databases_rows(["sales", "finance"])
        home_idx = cols.index("home")
        ops_home = [r[0] for r in rows if r[0] == "provisa_ops_sales" and r[home_idx]]
        assert ops_home == []

    def test_second_role_business_db_is_not_home(self):
        """REQ-807: only the first role is HOME."""
        from provisa.bolt.session import _show_databases_rows

        cols, rows = _show_databases_rows(["sales", "finance"])
        home_idx = cols.index("home")
        finance_home = [r[0] for r in rows if r[0] == "provisa_finance" and r[home_idx]]
        assert finance_home == []

    def test_single_role_home_is_its_business_db(self):
        """REQ-807: single-role user's home = provisa_<that_role>."""
        from provisa.bolt.session import _show_databases_rows

        cols, rows = _show_databases_rows(["finance"])
        home_idx = cols.index("home")
        home_names = [r[0] for r in rows if r[home_idx]]
        assert home_names == ["provisa_finance"]


# ---------------------------------------------------------------------------
# 3. DB name pattern: provisa_<role> vs provisa_ops_<role>
# ---------------------------------------------------------------------------


class TestDbNamePattern:
    def test_business_db_name_has_provisa_prefix(self):
        """REQ-807: business view name starts with 'provisa_' but not 'provisa_ops_'."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["sales"])
        biz_rows = [r for r in rows if not r[0].startswith("provisa_ops_")]
        assert len(biz_rows) == 1
        assert biz_rows[0][0] == "provisa_sales"

    def test_ops_db_name_has_provisa_ops_prefix(self):
        """REQ-807: ops view name starts with 'provisa_ops_'."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["sales"])
        ops_rows = [r for r in rows if r[0].startswith("provisa_ops_")]
        assert len(ops_rows) == 1
        assert ops_rows[0][0] == "provisa_ops_sales"

    def test_role_name_embedded_in_db_name(self):
        """REQ-807: role name is extractable from db name."""
        from provisa.bolt.session import _show_databases_rows

        _, rows = _show_databases_rows(["finance"])
        for r in rows:
            name: str = r[0]
            if name.startswith("provisa_ops_"):
                assert name[len("provisa_ops_") :] == "finance"
            else:
                assert name[len("provisa_") :] == "finance"

    def test_columns_include_required_fields(self):
        """REQ-807: result includes name, home, default, type columns."""
        from provisa.bolt.session import _show_databases_rows

        cols, _ = _show_databases_rows(["sales"])
        for required in ("name", "home", "default", "type", "access", "currentStatus"):
            assert required in cols

    def test_all_dbs_are_type_standard(self):
        """REQ-807: all databases report type='standard'."""
        from provisa.bolt.session import _show_databases_rows

        cols, rows = _show_databases_rows(["sales", "finance"])
        type_idx = cols.index("type")
        for r in rows:
            assert r[type_idx] == "standard"

    def test_all_dbs_are_online(self):
        """REQ-807: all databases report currentStatus='online'."""
        from provisa.bolt.session import _show_databases_rows

        cols, rows = _show_databases_rows(["sales", "finance"])
        status_idx = cols.index("currentStatus")
        for r in rows:
            assert r[status_idx] == "online"


# ---------------------------------------------------------------------------
# 4. :use semantics — _resolve_db selects role + view
# ---------------------------------------------------------------------------


class TestUseSelectsRoleAndView:
    def test_use_provisa_sales_selects_sales_business_view(self):
        """REQ-807: ':use provisa_sales' → role=sales, include_ops=False."""
        session, _ = _make_session(["sales", "finance"])
        result = session._resolve_db("provisa_sales")
        assert result == ("sales", False)

    def test_use_provisa_ops_sales_selects_sales_ops_view(self):
        """REQ-807: ':use provisa_ops_sales' → role=sales, include_ops=True."""
        session, _ = _make_session(["sales", "finance"])
        result = session._resolve_db("provisa_ops_sales")
        assert result == ("sales", True)

    def test_use_provisa_finance_selects_finance_business_view(self):
        """REQ-807: ':use provisa_finance' → role=finance, include_ops=False."""
        session, _ = _make_session(["sales", "finance"])
        result = session._resolve_db("provisa_finance")
        assert result == ("finance", False)

    def test_use_provisa_ops_finance_selects_finance_ops_view(self):
        """REQ-807: ':use provisa_ops_finance' → role=finance, include_ops=True."""
        session, _ = _make_session(["sales", "finance"])
        result = session._resolve_db("provisa_ops_finance")
        assert result == ("finance", True)

    def test_use_role_not_held_by_user_is_rejected(self):
        """REQ-807: user cannot :use a role they do not hold."""
        session, _ = _make_session(["sales"])
        result = session._resolve_db("provisa_finance")
        assert result is None

    def test_use_ops_role_not_held_by_user_is_rejected(self):
        """REQ-807: user cannot :use ops view for a role they do not hold."""
        session, _ = _make_session(["sales"])
        result = session._resolve_db("provisa_ops_finance")
        assert result is None

    def test_no_db_defaults_to_first_role_business_view(self):
        """REQ-807: no explicit db → first role, business view (home database)."""
        session, _ = _make_session(["sales", "finance"])
        result = session._resolve_db(None)
        assert result == ("sales", False)

    def test_empty_db_defaults_to_first_role_business_view(self):
        """REQ-807: empty db string → first role, business view."""
        session, _ = _make_session(["sales", "finance"])
        result = session._resolve_db("")
        assert result == ("sales", False)

    def test_use_sends_database_not_found_on_unknown_db(self):
        """REQ-807: RUN with db the user cannot access → DatabaseNotFound, no fallback."""

        async def _run() -> bytes:
            session, writer = _make_session(["sales"])
            with patch.object(session, "_resolve_db", return_value=None):
                await session.handle_run(["MATCH (n) RETURN n", {}, {"db": "provisa_finance"}])
            return writer.getvalue()

        import asyncio

        result = asyncio.run(_run())
        assert b"DatabaseNotFound" in result

    def test_use_does_not_execute_on_unauthorized_db(self):
        """REQ-807: execution short-circuited when db is unauthorized."""

        async def _run() -> int:
            session, _ = _make_session(["sales"])
            with (
                patch.object(session, "_resolve_db", return_value=None),
                patch("provisa.bolt.session._execute_cypher") as mock_exec,
            ):
                await session.handle_run(["MATCH (n) RETURN n", {}, {"db": "provisa_finance"}])
            return mock_exec.call_count

        import asyncio

        count = asyncio.run(_run())
        assert count == 0
