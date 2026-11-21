# Copyright (c) 2026 Kenneth Stott
# Canary: a94e0c63-1b7d-42f8-8e35-2c6fd8071ba5
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Named contexts: an instance and its credentials, under a name (REQ-2222).

Which instance a command addresses, strongest first: ``--api``; ``--context NAME``;
``$PROVISA_API_URL``; the current context; the local default. A context's token goes only to that
context's instance: when the address comes from anywhere else the context does not apply at all,
so a token is never carried to a host it was not saved for.

A production context asks before anything that can change or run: type its name at a terminal, or
pass ``--yes``. With nobody to ask, it refuses rather than hang or go ahead.

The file holds credentials, so it is written readable by its owner alone, and no command prints a
token.
"""

# Requirements: REQ-2222

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import yaml

USAGE = 2
REFUSED = 1

_PATH_ENV = "PROVISA_CONTEXTS"
_READS = {
    ("env", "list"),
    ("env", "show"),
    ("env", "diff"),
    ("maintenance", "status"),
    ("license", "status"),
}
_READING_COMMANDS = {"validate", "diff", "export", "can-i", "wait", "schema", "exit-codes"}


class ContextError(ValueError):
    """A command line naming a context that cannot be used."""


def _path() -> Path:
    named = os.environ.get(_PATH_ENV)
    return Path(named) if named else Path.home() / ".provisa" / "contexts.yaml"


def _load() -> dict[str, Any]:
    path = _path()
    try:
        data = yaml.safe_load(path.read_text())
    except FileNotFoundError:
        return {"current": None, "contexts": {}}
    except yaml.YAMLError as exc:
        # Never str(exc): that quotes the file, and this file holds tokens.
        raise ContextError(f"{path} is not valid YAML ({type(exc).__name__})") from exc
    if not isinstance(data, dict) or not isinstance(data.get("contexts"), dict):
        raise ContextError(f"{path} is not a contexts file")
    return data


def _save(data: dict[str, Any]) -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(descriptor, "w") as handle:
        yaml.safe_dump(data, handle, sort_keys=True)
    os.replace(temporary, path)


def bind(args: argparse.Namespace) -> None:
    """Fill the address and the token a command will use from the context that applies."""
    data = _load()
    named = getattr(args, "context", None)
    if named is not None and named not in data["contexts"]:
        raise ContextError(f"--context: no context named {named!r}")
    name = named or data.get("current")
    if name is None or not hasattr(args, "api") or args.command == "context":
        return
    if args.api or (named is None and os.environ.get("PROVISA_API_URL")):
        return
    context = data["contexts"][name]
    args.api = context["api"]
    if named is not None:
        # A named context's credentials are its own; a token in the environment is for elsewhere.
        os.environ.pop("PROVISA_API_TOKEN", None)
    if not args.token and (named is not None or not os.environ.get("PROVISA_API_TOKEN")):
        args.token = context.get("token")
    if context.get("production"):
        args.production_context = name


def _only_reads(args: argparse.Namespace) -> bool:
    if args.command in _READING_COMMANDS or getattr(args, "dry_run", False):
        return True
    if args.command == "api":
        return args.method == "GET"
    if args.command == "explain":
        return not args.analyze
    action = getattr(args, f"{args.command.replace('-', '_')}_command", None)
    return (args.command, action) in _READS


def _refuse(args: argparse.Namespace, code: str, detail: str) -> int:
    if getattr(args, "json", False):
        print(json.dumps({"refused": {"code": code, "detail": detail}}))
    else:
        print(f"refused ({code}): {detail}", file=sys.stderr)
    return REFUSED


def confirm(args: argparse.Namespace) -> int | None:
    """The exit code that stops this command, or None when it may go on."""
    name = getattr(args, "production_context", None)
    if name is None or getattr(args, "yes", False) or _only_reads(args):
        return None
    if not sys.stdin.isatty():
        return _refuse(
            args,
            "context.confirmation_required",
            f"{name} is a production context and there is nobody to ask; pass --yes",
        )
    print(f"{name} is a production context. Type its name to go on: ", end="", file=sys.stderr)
    sys.stderr.flush()
    if sys.stdin.readline().strip() != name:
        return _refuse(args, "context.not_confirmed", f"{name} was not confirmed; nothing was done")
    return None


# --- the commands ---------------------------------------------------------------------------- #


def _usage(message: str) -> int:
    print(f"provisa context: {message}", file=sys.stderr)
    return USAGE


def _cmd_set(args: argparse.Namespace) -> int:
    data = _load()
    held = data["contexts"].get(args.name, {})
    context: dict[str, Any] = {"api": args.api, "production": args.production}
    token = args.token or held.get("token")
    if token:
        context["token"] = token
    data["contexts"][args.name] = context
    if data.get("current") is None:
        data["current"] = args.name
    _save(data)
    print(json.dumps({"context": args.name}) if args.json else f"Saved context {args.name}")
    return 0


def _cmd_use(args: argparse.Namespace) -> int:
    data = _load()
    if args.name not in data["contexts"]:
        return _usage(f"no context named {args.name!r}")
    data["current"] = args.name
    _save(data)
    print(json.dumps({"current": args.name}) if args.json else f"Using context {args.name}")
    return 0


def _cmd_delete(args: argparse.Namespace) -> int:
    data = _load()
    if args.name not in data["contexts"]:
        return _usage(f"no context named {args.name!r}")
    del data["contexts"][args.name]
    if data.get("current") == args.name:
        data["current"] = None
    _save(data)
    print(json.dumps({"deleted": args.name}) if args.json else f"Deleted context {args.name}")
    return 0


def _cmd_list(args: argparse.Namespace) -> int:
    data = _load()
    contexts = [
        {
            "name": name,
            "api": context["api"],
            "has_token": bool(context.get("token")),
            "production": bool(context.get("production")),
            "current": name == data.get("current"),
        }
        for name, context in sorted(data["contexts"].items())
    ]
    if args.json:
        print(json.dumps({"contexts": contexts}))
        return 0
    for context in contexts:
        marks = ("*" if context["current"] else " ", " production" if context["production"] else "")
        print(f"{marks[0]} {context['name']}  {context['api']}{marks[1]}")
    return 0


def register(sub: Any) -> None:
    context = sub.add_parser("context", help="Name an instance and its credentials")
    actions = context.add_subparsers(dest="context_command", required=True)
    save = actions.add_parser("set", help="Create or replace a context")
    save.add_argument("name")
    save.add_argument("--api", required=True, help="Provisa API base URL")
    save.add_argument("--token", default=None, help="Bearer token; kept if not restated")
    save.add_argument(
        "--production", action="store_true", help="Ask before anything that can change or run"
    )
    save.set_defaults(func=_cmd_set)
    use = actions.add_parser("use", help="Make a context the current one")
    use.add_argument("name")
    use.set_defaults(func=_cmd_use)
    delete = actions.add_parser("delete", help="Delete a context")
    delete.add_argument("name")
    delete.set_defaults(func=_cmd_delete)
    listing = actions.add_parser("list", help="List the contexts, never their tokens")
    listing.set_defaults(func=_cmd_list)
