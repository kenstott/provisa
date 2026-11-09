# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa api METHOD PATH`` -- call anything a running instance's API offers (REQ-2222).

One generic command, so the long tail of the API needs no code per feature. It decides nothing:
the server's checks apply as they do to any other client.

THE CONTRACT WITH A SCRIPT: stdout is the response body, byte for byte, on success and on refusal,
so a refusal's error code is there to read. The exit code comes from the response status:

* ``0`` -- the instance answered with a success status.
* ``1`` -- the instance answered with an error status (``HTTP <status>`` on stderr), or did not
  answer (the reason on stderr, nothing on stdout).
* ``2`` -- usage error; no call was made.
"""

# Requirements: REQ-2222

from __future__ import annotations

import argparse
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

METHODS = ("GET", "POST", "PUT", "PATCH", "DELETE")


def _request_body(value: str) -> bytes:
    """``--data``: the text itself, ``@file`` for a file's contents, ``@-`` for stdin."""
    if not value.startswith("@"):
        return value.encode()
    source = value[1:]
    if source == "-":
        return sys.stdin.buffer.read()
    try:
        return Path(source).read_bytes()
    except OSError as exc:
        raise argparse.ArgumentTypeError(f"cannot read {source}: {exc.strerror}") from exc


def _cmd_api(args: argparse.Namespace) -> int:
    api = args.api or os.environ.get("PROVISA_API_URL", "http://127.0.0.1:8000")
    token = args.token or os.environ.get("PROVISA_API_TOKEN", "")
    # The path always starts at the instance's root: joined bare, "@host/x" would make the
    # instance's address the userinfo of another host's URL and carry the token there.
    url = f"{api.rstrip('/')}/{args.path.lstrip('/')}"
    req = urllib.request.Request(url, method=args.method, data=args.data)
    if args.data is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        # B310 wants the scheme pinned: the URL is built from the operator's own --api
        # argument, so it is http(s) by construction, never file:// or a custom handler.
        with urllib.request.urlopen(req, timeout=args.timeout) as resp:  # nosec B310
            sys.stdout.buffer.write(resp.read())
    except urllib.error.HTTPError as exc:
        sys.stdout.buffer.write(exc.read())
        print(f"HTTP {exc.code}", file=sys.stderr)
        return 1
    except urllib.error.URLError as exc:
        raise SystemExit(f"cannot reach the Provisa API at {api}: {exc.reason}") from exc
    except TimeoutError as exc:
        raise SystemExit(f"no answer from the Provisa API at {api} within {args.timeout}s") from exc
    return 0


def register(sub) -> None:
    parser = sub.add_parser("api", help="Call any endpoint of a running instance's API")
    parser.add_argument("method", choices=METHODS, help="HTTP method")
    parser.add_argument("path", help="Path on the instance, with any query string")
    parser.add_argument(
        "--data",
        type=_request_body,
        default=None,
        help="JSON request body: the text itself, @file, or @- for stdin",
    )
    parser.add_argument(
        "--api",
        default=None,
        help="Provisa API base URL (default: $PROVISA_API_URL or http://127.0.0.1:8000)",
    )
    parser.add_argument("--token", default=None, help="Bearer token (default: $PROVISA_API_TOKEN)")
    parser.add_argument(
        "--timeout", type=int, default=30, help="Request timeout in seconds (default: 30)"
    )
    parser.set_defaults(func=_cmd_api)
