# Copyright (c) 2026 Kenneth Stott
# Canary: 39c83938-0ebb-4b6f-9f1b-d96c46e3f3a3
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A notebook's connection comes from keywords, then the environment, then the profile file; a
username and password are exchanged for a session token, never pasted as one."""

import json

import httpx
import pytest
import respx

from provisa_client.credentials import resolve

URL = "https://provisa.example"


def _profile(tmp_path, text):
    path = tmp_path / "credentials"
    path.write_text(text)
    return path


def test_a_keyword_wins_over_the_environment_and_the_environment_over_the_profile(tmp_path):
    path = _profile(tmp_path, f"[default]\nurl = {URL}\nrole = from_profile\norg = acme\n")
    env = {"PROVISA_ROLE": "from_env", "PROVISA_TOKEN": "provisa_pat_env"}
    creds = resolve(environ=env, path=path, role="from_keyword")
    assert (creds.url, creds.role, creds.token, creds.org) == (
        URL,
        "from_keyword",
        "provisa_pat_env",
        "acme",
    )
    assert resolve(environ=env, path=path).role == "from_env"
    assert resolve(environ={}, path=path).role == "from_profile"


def test_a_named_profile_is_read_and_one_that_is_not_there_is_refused(tmp_path):
    path = _profile(tmp_path, f"[default]\nurl = {URL}\n[staging]\nurl = https://staging\n")
    assert resolve(environ={"PROVISA_PROFILE": "staging"}, path=path).url == "https://staging"
    with pytest.raises(ValueError, match=r"no profile \[prod\]"):
        resolve(profile="prod", environ={}, path=path)


def test_no_url_from_anywhere_is_refused(tmp_path):
    with pytest.raises(ValueError, match="no Provisa URL"):
        resolve(environ={}, path=tmp_path / "absent")


@respx.mock
def test_a_username_and_password_are_exchanged_for_a_session_token(tmp_path):
    route = respx.post(f"{URL}/auth/login").mock(
        return_value=httpx.Response(200, json={"access_token": "session", "token_type": "bearer"})
    )
    path = _profile(tmp_path, f"[default]\nurl = {URL}/\nusername = ana\n")
    creds = resolve(environ={"PROVISA_PASSWORD": "secret"}, path=path)
    assert creds.token == "session"
    # The JSON the request carries, not its spacing: httpx before 0.28 writes ", " between
    # members, and the client allows httpx>=0.27.
    assert json.loads(route.calls.last.request.read()) == {"username": "ana", "password": "secret"}


@respx.mock
def test_a_refused_sign_in_raises_with_the_servers_answer(tmp_path):
    respx.post(f"{URL}/auth/login").mock(
        return_value=httpx.Response(401, json={"detail": "invalid credentials"})
    )
    with pytest.raises(PermissionError, match="HTTP 401"):
        resolve(url=URL, username="ana", password="wrong", environ={}, path=tmp_path / "absent")


def test_a_username_without_a_password_and_an_unknown_transport_are_refused(tmp_path):
    absent = tmp_path / "absent"
    with pytest.raises(ValueError, match="no password"):
        resolve(url=URL, username="ana", environ={}, path=absent)
    with pytest.raises(ValueError, match="transport"):
        resolve(url=URL, transport="carrier-pigeon", environ={}, path=absent)


def test_the_flight_port_and_transport_default_to_the_clients(tmp_path):
    creds = resolve(url=URL, environ={}, path=tmp_path / "absent")
    assert (creds.flight_port, creds.transport, creds.token) == (8815, "flight", None)
