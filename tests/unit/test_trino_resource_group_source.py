# Copyright (c) 2026 Kenneth Stott
# Canary: e53c099c-f988-45b0-8d6a-1e6f6cd0bde8
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-056, REQ-1040: each org's statements reach Trino in that org's own resource group.

Trino places a statement by the selectors in resource-groups.json. Every statement arrives as
the one Trino user, so the user cannot tell orgs apart; the connection's ``source`` carries the
org instead, and the selector turns it into the group ``global.tenant-<org>``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from provisa.federation import trino_lifecycle
from provisa.federation.trino_lifecycle import terminal_conn_kwargs

REPO = Path(__file__).resolve().parents[2]
RESOURCE_GROUPS = REPO / "trino" / "etc" / "resource-groups.json"


class _State:
    active_engine_endpoint = ("trino-host", 8080)
    active_isolated_org = None
    active_org_id = "acme"


def _selected_group(config: dict, *, user: str, source: str | None) -> str | None:
    """The group Trino's file-based manager would pick: the first selector whose every
    condition matches, with named groups of the source pattern substituted in."""
    for selector in config["selectors"]:
        variables = {"USER": user}
        if "user" in selector and not re.fullmatch(selector["user"], user):
            continue
        if "source" in selector:
            # Trino's patterns are Java's; a named group is (?<name>...) there, (?P<name>...) here.
            pattern = re.sub(r"\(\?<(?![=!])", "(?P<", selector["source"])
            matched = re.fullmatch(pattern, source or "")
            if matched is None:
                continue
            variables.update(matched.groupdict())
        group = selector["group"]
        for name, value in variables.items():
            group = group.replace("${" + name + "}", value)
        return group
    return None


class TestTheEngineNamesTheOrg:
    def test_the_terminal_connection_carries_the_org_as_its_source(self):
        kwargs = terminal_conn_kwargs(_State())
        assert kwargs["source"] == "provisa/acme"
        assert kwargs["user"] == "provisa"  # the Trino principal is unchanged

    def test_the_subscription_poller_carries_it_too(self, monkeypatch):
        captured = {}

        class _Provider:
            def __init__(self, **kwargs):
                captured.update(kwargs)

        monkeypatch.setattr(
            "provisa.subscriptions.trino_polling_provider.TrinoPollingProvider", _Provider
        )

        class _Bound(_State):
            engine_conn_kwargs = terminal_conn_kwargs(_State())

        trino_lifecycle.polling_provider(_Bound(), "cat", "sch", "tbl", "updated_at")
        assert captured["source"] == "provisa/acme"

    def test_the_poller_passes_its_source_to_trino(self, monkeypatch):
        import trino

        from provisa.subscriptions.trino_polling_provider import TrinoPollingProvider

        seen = {}
        monkeypatch.setattr(trino.dbapi, "connect", lambda **kw: seen.update(kw))
        TrinoPollingProvider(
            host="h",
            port=8080,
            catalog="c",
            schema="s",
            table="t",
            watermark_column="w",
            source="provisa/acme",
        )._connect()
        assert seen["source"] == "provisa/acme"


class TestTheSelectors:
    def _config(self) -> dict:
        return json.loads(RESOURCE_GROUPS.read_text())

    def test_an_orgs_statement_lands_in_that_orgs_group(self):
        config = self._config()
        assert _selected_group(config, user="provisa", source="provisa/acme") == (
            "global.tenant-acme"
        )
        assert _selected_group(config, user="provisa", source="provisa/beta") == (
            "global.tenant-beta"
        )

    def test_the_group_an_org_lands_in_is_declared(self):
        subgroups = {g["name"] for g in self._config()["rootGroups"][0]["subGroups"]}
        assert "tenant-${tenant}" in subgroups

    def test_an_engine_statement_that_names_no_org_matches_no_selector(self):
        """Trino rejects a statement no selector matches, so an engine path that fails to name
        its org is seen rather than pooled with a tenant."""
        config = self._config()
        assert _selected_group(config, user="provisa", source=None) is None
        assert _selected_group(config, user="provisa", source="trino-cli") is None
        assert _selected_group(config, user="provisa", source="provisa/") is None
        assert _selected_group(config, user="provisa", source="provisa/a b") is None

    def test_other_users_get_a_small_group_of_their_own(self):
        """An operator's CLI session and the container healthcheck (`trino --execute`) are not
        the engine and name no org; they run, in a group that cannot crowd out a tenant."""
        config = self._config()
        assert _selected_group(config, user="trino", source="trino-cli") == "global.adhoc"
        assert _selected_group(config, user="itest", source=None) == "global.adhoc"
        # Naming an org in the source does not put another user in a tenant's group.
        assert _selected_group(config, user="itest", source="provisa/acme") == "global.adhoc"
        assert _selected_group(config, user="system", source=None) == "global.system"

    def test_no_engine_statement_can_run_without_an_org(self):
        from provisa.federation.trino_lifecycle import engine_source

        with pytest.raises(ValueError, match="must name its org"):
            engine_source("")

    def test_the_engine_and_the_selector_agree_on_the_source(self):
        source = terminal_conn_kwargs(_State())["source"]
        assert _selected_group(self._config(), user="provisa", source=source) == (
            "global.tenant-acme"
        )

    def test_the_k8s_provisioner_ships_the_same_file(self):
        from provisa.federation.k8s_provisioner import shared_resource_groups

        assert json.loads(shared_resource_groups()) == self._config()


def test_the_arrow_flight_proxy_has_one_shared_group():
    """REQ-056 gap, recorded: the Flight proxy (Zaychik) opens its own Trino connection with the
    JDBC driver's default source and cannot name the org, so its statements share one group."""
    config = json.loads(RESOURCE_GROUPS.read_text())
    assert _selected_group(config, user="provisa", source="trino-jdbc") == "global.flight"
