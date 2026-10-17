# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Reading from Provisa inside the browser (Pyodide), as the signed-in user of the page.

The notebook's kernel runs in a Web Worker beside the Provisa UI. It reaches the server that
served the page over ``/data/query`` and acts as the page's user: the page's auth bridge keeps
the user's acting role and credential on the kernel's global scope as ``provisaAuth``:

    provisaAuth = {
      baseUrl: string,        // the server's base URL; "" for the page's own origin
      role: string | null,    // the acting role(s), as X-Provisa-Role takes them ("a" or "a,b")
      bearer: string | null,  // the user's bearer token; null on a deployment without sign-in
      org: string | null,     // the org the page is for; null on a single-tenant deployment
    }

The bridge refreshes it as the token rotates or the role changes, so it is read on every request.

A result arrives as an Arrow IPC stream when pyarrow is importable. Without pyarrow it is asked
for as JSON: that is the only reason a JSON answer is requested. A Cypher statement is answered as
JSON whatever is asked for (the server's Cypher endpoint has no Arrow form).
"""

from __future__ import annotations

import importlib.util
import json
import sys
from typing import Any

BRIDGE = "provisaAuth"
ARROW = "application/vnd.apache.arrow.stream"
_BRIDGE_KEYS = ("baseUrl", "role", "bearer", "org")


def in_browser() -> bool:
    """Whether this Python is Pyodide (CPython compiled to WebAssembly by Emscripten)."""
    return sys.platform == "emscripten"


def arrow_available() -> bool:
    return importlib.util.find_spec("pyarrow") is not None


def read_bridge() -> dict[str, Any]:
    """The page's ``provisaAuth`` (module docstring), as a dict."""
    import js  # pyright: ignore[reportMissingImports]  # Pyodide's module of the JS global scope

    proxy = getattr(js, BRIDGE, None)
    if proxy is None:
        raise RuntimeError(
            f"no {BRIDGE} on the kernel's global scope: the page that hosts this notebook has not "
            "handed it the signed-in user"
        )
    auth = proxy.to_py()
    missing = [k for k in _BRIDGE_KEYS if k not in auth]
    if missing:
        raise RuntimeError(f"{BRIDGE} lacks {missing}")
    return auth


def request(
    text: str, *, role: str | None, variables: dict[str, Any] | None
) -> tuple[str, bytes, dict[str, str]]:
    """The URL, body and headers of a ``/data/query`` request as the bridge's user."""
    auth = read_bridge()
    headers = {
        "Content-Type": "application/json",
        "Accept": ARROW if arrow_available() else "application/json",
    }
    acting = role or auth["role"]
    if acting:
        headers["X-Provisa-Role"] = acting
    if auth["bearer"]:
        headers["Authorization"] = f"Bearer {auth['bearer']}"
    if auth["org"]:
        headers["X-Org-Provisa"] = auth["org"]
    payload: dict[str, Any] = {"query": text}
    if variables:
        payload["variables"] = variables
    return f"{auth['baseUrl']}/data/query", json.dumps(payload).encode(), headers


async def fetch(text: str, *, role: str | None, variables: dict[str, Any] | None):
    """``text``'s answer as a DataFrame, over the page's ``fetch`` (pyfetch)."""
    from pyodide.http import pyfetch  # pyright: ignore[reportMissingImports]

    url, body, headers = request(text, role=role, variables=variables)
    resp = await pyfetch(url, method="POST", body=body, headers=headers)
    return decode(resp.status, resp.headers.get("content-type", ""), await resp.bytes())


def fetch_sync(text: str, *, role: str | None, variables: dict[str, Any] | None):
    """``text``'s answer as a DataFrame, over a synchronous XMLHttpRequest: what a cell magic,
    which cannot await, uses. A synchronous request may read binary only off the page's main
    thread, which is where a notebook's kernel runs (a Web Worker)."""
    import js as _js  # pyright: ignore[reportMissingImports]

    js: Any = _js  # the JS global scope: its names are not known to the type checker
    if hasattr(js, "document"):
        raise RuntimeError(
            "a synchronous read runs only in a Web Worker (a notebook's kernel); on the page's "
            "main thread use `await provisa_client.aquery(...)`"
        )
    url, body, headers = request(text, role=role, variables=variables)
    xhr = js.XMLHttpRequest.new()
    xhr.open("POST", url, False)
    xhr.responseType = "arraybuffer"
    for name, value in headers.items():
        xhr.setRequestHeader(name, value)
    xhr.send(js.Uint8Array.new(body))
    data = js.Uint8Array.new(xhr.response).to_bytes()
    return decode(xhr.status, xhr.getResponseHeader("Content-Type") or "", data)


def decode(status: int, content_type: str, body: bytes):
    """A ``/data/query`` answer as a pandas DataFrame: an Arrow stream, a GraphQL ``data`` object
    (its one root field), or the ``columns``/``rows`` of a Cypher answer."""
    import pandas as pd

    if status != 200:
        raise RuntimeError(f"HTTP {status}: {body.decode(errors='replace')}")
    if content_type.startswith(ARROW):
        import pyarrow as pa

        return pa.ipc.open_stream(body).read_pandas()
    result = json.loads(body)
    if result.get("errors"):
        raise RuntimeError(result["errors"])
    if "rows" in result:
        return pd.DataFrame(result["rows"], columns=result["columns"])
    [rows] = result["data"].values()
    return pd.DataFrame(rows)
