# Copyright (c) 2026 Kenneth Stott
# Canary: 09f4f269-a4cb-45b0-8eea-f180c8de8117
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The public address of a deployment that states none is where its UI is really served
(REQ-1923, REQ-1576).

The address an invitation links to and an identity provider returns a source's sign-in to is
``mail.base_url``. Its default was a port nothing served the UI on, so a local install's first
sign-in went to an address with nothing behind it. It is now the origin the launchers serve, and
this file holds the launchers and the default to one another so they cannot drift again.
"""

# Requirements: REQ-1923, REQ-1576
from __future__ import annotations

import re
from pathlib import Path

from provisa.core import source_sign_in
from provisa.core.mail import invite_redemption_url
from provisa.core.models import DEFAULT_UI_ORIGIN, DEFAULT_UI_PORT, MailConfig

ROOT = Path(__file__).resolve().parents[2]


def test_the_default_public_address_is_where_a_local_install_serves_its_ui():
    assert MailConfig().base_url == DEFAULT_UI_ORIGIN == f"http://localhost:{DEFAULT_UI_PORT}"


def test_the_ui_dev_server_serves_on_that_port_when_the_operator_states_none():
    vite = (ROOT / "provisa-ui" / "vite.config.ts").read_text()
    stated = re.search(
        r"DEV_SERVER_PORT = Number\(process\.env\.PROVISA_UI_PORT \?\? (\d+)\)", vite
    )
    assert stated is not None, "vite.config.ts no longer states its default port this way"
    assert int(stated.group(1)) == DEFAULT_UI_PORT


def test_the_start_script_serves_and_announces_that_address():
    script = (ROOT / "start-ui-install.sh").read_text()
    announced = set(re.findall(r"UI(?: ready on|:)\s+(http://localhost:\d+)", script))
    assert announced == {DEFAULT_UI_ORIGIN}


def test_the_bundled_compose_file_serves_the_ui_on_that_port():
    compose = (ROOT / "docker-compose.app.yml").read_text()
    served = re.findall(r'"provisa\.ui_server:app".*?"--port", "(\d+)"', compose)
    assert served and set(served) == {str(DEFAULT_UI_PORT)}


def test_a_local_installs_sign_in_returns_to_the_address_its_ui_is_opened_at():
    assert source_sign_in.redirect_address(MailConfig().base_url) == (
        f"{DEFAULT_UI_ORIGIN}/source-sign-in.html"
    )


def test_an_invitation_from_a_local_install_links_to_the_address_its_ui_is_opened_at():
    link = invite_redemption_url(MailConfig().base_url, "tok", "acme")
    assert link == f"{DEFAULT_UI_ORIGIN}/?invite=tok"
