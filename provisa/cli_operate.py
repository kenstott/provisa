# Copyright (c) 2026 Kenneth Stott
# Canary: 0b7e3d91-f46a-4c28-b153-7ea9d26c48f1
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``can-i``, ``wait``, ``schema``, ``completion`` and ``exit-codes``, and what every command
shares: its context, its confirmation, one output option and one filter (REQ-2222).

``can-i`` reads the effective rights the admin UI shows, so its answer is the server's, as far as
the caller may read. ``no`` and ``cannot tell`` are different exits: a role the caller may not read
is not a role that may not act.
"""

# Requirements: REQ-2222

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import quote

from provisa import cli_context, cli_output

OK = 0
NO = 1
USAGE = 2
TIMED_OUT = 3
CANNOT_TELL = 4

#: Commands that take none of the shared options: one is a server, one prints a shell script.
NOT_SHAPED = ("run", "completion")

_REQUEST_SECONDS = 10

EXIT_CODES: dict[str, dict[str, str]] = {
    "query, explain": {
        "0": "answered, or refused with the code --expect-refused names",
        "1": "refused; the code is in the document and on stderr",
        "2": "the command line is wrong",
        "3": "--expect-refused was not met",
        "4": "failed: unreachable, or the instance answered 5xx",
    },
    "validate, export": {
        "0": "valid, or exported",
        "1": "refused, or the instance could not be reached",
        "2": "the command line is wrong",
    },
    "diff": {
        "0": "reported; with --exit-code, the instance matches the config",
        "1": "refused, or the instance could not be reached",
        "2": "the command line is wrong",
        "3": "with --exit-code: the instance has moved from the config",
    },
    "apply": {
        "0": "applied, or a dry run that a real apply would pass",
        "1": "refused, including a plan the instance has moved past (plan.stale)",
        "2": "the command line is wrong",
    },
    "promote, env": {
        "0": "done, or a dry run that applied nothing",
        "1": "refused",
        "2": "the command line is wrong, or proposed and waiting for an approval",
    },
    "can-i": {
        "0": "yes",
        "1": "no; the reason is printed",
        "2": "the command line is wrong",
        "4": "cannot tell: refused or unreachable",
    },
    "wait": {
        "0": "ready, or current",
        "1": "refused",
        "2": "the command line is wrong",
        "3": "timed out",
    },
    "api": {
        "0": "the instance answered with a success status",
        "1": "the instance answered with an error status, or did not answer",
        "2": "the command line is wrong",
    },
    "--output, --filter": {
        "4": "the command printed nothing that could be shaped",
    },
}


def _leaves(parser: argparse.ArgumentParser, path: tuple[str, ...] = ()) -> Any:
    subs = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
    if not subs:
        yield path, parser
        return
    for name, child in subs[0].choices.items():
        yield from _leaves(child, (*path, name))


def share_options(root: argparse.ArgumentParser) -> None:
    """Give every command the shared options. Done over the finished tree, so a command added
    anywhere has them without asking."""
    for path, leaf in _leaves(root):
        if path[0] in NOT_SHAPED:
            continue
        taken = {s for a in leaf._actions for s in a.option_strings}
        leaf.add_argument(
            "-o", "--output", choices=cli_output.FORMATS, default=None, help="Output format"
        )
        leaf.add_argument(
            "--filter", default=None, metavar="PATH", help="Print only this: a.b, a[0], a[], a[k=v]"
        )
        leaf.add_argument("--context", default=None, help="The named context for this call")
        leaf.add_argument(
            "--yes", action="store_true", help="Do not ask, even on a production context"
        )
        if "--json" not in taken:
            leaf.add_argument("--json", action="store_true", help="The same as --output json")


def dispatch(args: argparse.Namespace) -> int:
    """Run the parsed command with what every command shares."""
    try:
        shape = cli_output.Shape.of(args)
        cli_context.bind(args)
    except (cli_output.FilterError, cli_context.ContextError) as exc:
        print(f"provisa {args.command}: {exc}", file=sys.stderr)
        return USAGE
    if shape is not None:
        args.json = True
    stopped = cli_context.confirm(args)
    if stopped is not None:
        return stopped
    return args.func(args) if shape is None else cli_output.run_shaped(args, shape)


def _add_instance(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--api", default=None, help="API URL (default: $PROVISA_API_URL)")
    parser.add_argument("--token", default=None, help="Bearer token (default: $PROVISA_API_TOKEN)")


def _base(args: argparse.Namespace) -> str:
    return (args.api or os.environ.get("PROVISA_API_URL", "http://127.0.0.1:8000")).rstrip("/")


def _token(args: argparse.Namespace) -> str:
    return args.token or os.environ.get("PROVISA_API_TOKEN", "")


def _code_of(body: bytes, status: int) -> str:
    try:
        decoded = json.loads(body)
    except json.JSONDecodeError:
        return f"http.{status}"
    code = decoded.get("code") if isinstance(decoded, dict) else None
    return code if isinstance(code, str) else f"http.{status}"


# --- can-i ----------------------------------------------------------------------------------- #


def _capability(effective: dict[str, Any], right: str) -> dict[str, Any]:
    held = next((c for c in effective.get("capabilities", []) if c["right"] == right), None)
    if held is None:
        return {"allowed": False, "reason": f"role {effective['role']} does not hold {right}"}
    return {"allowed": True, "from": held["from"]}


def _find(data: list[dict[str, Any]], target: str) -> tuple[dict | None, str | None]:
    """The table ``target`` names and the column within it, if it names one."""
    by_name = {t["name"]: t for t in data}
    if target in by_name:
        return by_name[target], None
    table, _, column = target.rpartition(".")
    return by_name.get(table), column or None


def _out_of_reach(role: str, target: str) -> dict[str, Any]:
    return {
        "allowed": False,
        "reason": f"{target} is not in what role {role} is served, as far as this caller may read",
    }


def _read(effective: dict[str, Any], target: str) -> dict[str, Any]:
    role = effective["role"]
    table, name = _find(effective.get("data", []), target)
    if table is None:
        return _out_of_reach(role, target)
    readable = [c for c in table["columns"] if c["read"]]
    if name is None:
        if not readable:
            return {"allowed": False, "reason": f"role {role} reads no column of {target}"}
        return {"allowed": True, "columns": [c["name"] for c in readable]}
    column = next((c for c in readable if c["name"] == name), None)
    if column is None:
        return {
            "allowed": False,
            "reason": f"column {name} of {table['name']} is not read by {role}",
        }
    return {
        "allowed": True,
        "mask": column.get("mask"),
        "row_rule": column.get("row_rule"),
        "granted_by": column.get("granted_by"),
    }


def _write(effective: dict[str, Any], target: str) -> dict[str, Any]:
    role = effective["role"]
    table, name = _find(effective.get("data", []), target)
    if table is None:
        return _out_of_reach(role, target)
    columns = (
        table["columns"] if name is None else [c for c in table["columns"] if c["name"] == name]
    )
    operations = sorted({op for c in columns for op in c.get("write", [])})
    if not operations:
        return {"allowed": False, "reason": f"role {role} has no write grant on {target}"}
    return {"allowed": True, "operations": operations}


_QUESTIONS = {"capability": _capability, "read": _read, "write": _write}


def _usage(command: str, message: str) -> int:
    print(f"provisa {command}: {message}", file=sys.stderr)
    return USAGE


def _cmd_can_i(args: argparse.Namespace) -> int:
    if args.person:
        # REQ-2219: no instance applies a person's entitlements yet.
        return _usage("can-i", "--person: not available yet, no instance applies entitlements")
    if not args.role:
        return _usage("can-i", "give --role")
    from provisa.cli import ApiRefused
    from provisa.cli_config import _send

    try:
        raw = _send(args, "GET", f"/admin/roles/{quote(args.role, safe='')}/effective")
    except ApiRefused as refusal:
        code = _code_of(refusal.body.encode(), refusal.status)
        print(f"cannot tell ({code}): {refusal.body}", file=sys.stderr)
        return CANNOT_TELL
    except SystemExit as exc:
        print(f"cannot tell: {exc.code}", file=sys.stderr)
        return CANNOT_TELL
    answer = _QUESTIONS[args.question](json.loads(raw), args.target)
    document = {"role": args.role, "question": args.question, "target": args.target, **answer}
    if args.json:
        print(json.dumps(document, sort_keys=True))
    elif answer["allowed"]:
        print("yes")
    else:
        print(f"no: {answer['reason']}")
    return OK if answer["allowed"] else NO


# --- wait ------------------------------------------------------------------------------------ #

_REPLICAS = (
    "{ replicaBuilds { builds { sourceId schemaName tableName state partial completedAt "
    "lastError lastErrorCode } } }"
)


class _WaitRefused(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def _fetch(req: urllib.request.Request) -> tuple[bytes | None, str]:
    """The body, or None and why it is worth asking again. A refusal is not."""
    try:
        with urllib.request.urlopen(req, timeout=_REQUEST_SECONDS) as resp:  # nosec B310
            return resp.read(), ""
    except urllib.error.HTTPError as exc:
        body = exc.read()
        if exc.code >= 500:
            return None, f"HTTP {exc.code}"
        raise _WaitRefused(_code_of(body, exc.code), body.decode(errors="replace")) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        return None, f"unreachable: {getattr(exc, 'reason', exc)}"


def _request(
    args: argparse.Namespace, path: str, data: bytes | None = None
) -> urllib.request.Request:
    req = urllib.request.Request(f"{_base(args)}{path}", data=data)
    token = _token(args)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    org = getattr(args, "org", None)
    if org:
        req.add_header("X-Org-Provisa", org)
    return req


def _instance_ready(args: argparse.Namespace) -> tuple[bool, str]:
    body, why = _fetch(_request(args, "/ready"))
    return body is not None, why or "still starting"


def _replica_current(args: argparse.Namespace) -> tuple[bool, str]:
    data = json.dumps({"query": _REPLICAS}).encode()
    body, why = _fetch(_request(args, "/admin/graphql", data))
    if body is None:
        return False, why
    answer = json.loads(body)
    if answer.get("errors"):
        first = answer["errors"][0]
        code = (first.get("extensions") or {}).get("code") or "graphql.error"
        raise _WaitRefused(code, first.get("message") or "")
    source, schema, table = args.table.split("/")
    build = next(
        (
            b
            for b in answer["data"]["replicaBuilds"]["builds"]
            if (b["sourceId"], b["schemaName"], b["tableName"]) == (source, schema, table)
        ),
        None,
    )
    if build is None:
        return False, f"no replica of {args.table} is listed"
    if build["state"] == "idle" and build["completedAt"] and not build["partial"]:
        return True, ""
    if build["lastError"]:
        return False, f"state {build['state']}: {build['lastError']}"
    return False, f"state {build['state']}"


def _poll(args: argparse.Namespace, waited_for: str, probe: Any) -> int:
    deadline = time.monotonic() + args.timeout
    while True:
        try:
            done, note = probe(args)
        except _WaitRefused as refused:
            print(f"refused ({refused.code}): {refused.detail}", file=sys.stderr)
            return NO
        if done:
            if args.json:
                print(json.dumps({"ready": True, "waited_for": waited_for}, sort_keys=True))
            else:
                print(f"ready: {waited_for}")
            return OK
        if time.monotonic() >= deadline:
            detail = f"{waited_for} not ready after {args.timeout}s ({note})"
            if args.json:
                print(
                    json.dumps(
                        {
                            "code": "wait.timeout",
                            "detail": detail,
                            "ready": False,
                            "waited_for": waited_for,
                        },
                        sort_keys=True,
                    )
                )
            print(f"timed out (wait.timeout): {detail}", file=sys.stderr)
            return TIMED_OUT
        time.sleep(args.interval)


def _cmd_wait_ready(args: argparse.Namespace) -> int:
    return _poll(args, "instance", _instance_ready)


def _cmd_wait_replica(args: argparse.Namespace) -> int:
    if len(args.table.split("/")) != 3:
        return _usage("wait", "name the table as source/schema/table")
    if not args.org:
        return _usage("wait", "replica needs --org")
    return _poll(args, args.table, _replica_current)


# --- schema ---------------------------------------------------------------------------------- #


def _resolved(node: dict[str, Any], defs: dict[str, Any]) -> dict[str, Any]:
    """``node`` with a reference, a nullable wrapper or a list of one followed to what it holds."""
    while True:
        if "$ref" in node:
            node = defs[node["$ref"].rsplit("/", 1)[-1]]
        elif "items" in node:
            node = node["items"]
        elif "anyOf" in node:
            kept = [n for n in node["anyOf"] if n.get("type") != "null"]
            if len(kept) != 1:
                return node
            node = kept[0]
        else:
            return node


def _type_of(node: dict[str, Any]) -> str:
    if "$ref" in node:
        return node["$ref"].rsplit("/", 1)[-1]
    if "anyOf" in node:
        return " | ".join(_type_of(n) for n in node["anyOf"])
    if "enum" in node:
        return " | ".join(str(v) for v in node["enum"])
    if node.get("type") == "array":
        return f"list[{_type_of(node.get('items', {}))}]"
    return str(node.get("type", "any"))


def _fields(node: dict[str, Any]) -> list[dict[str, Any]]:
    required = set(node.get("required", []))
    return [
        {
            "name": name,
            "type": _type_of(field),
            "required": name in required,
            "default": field.get("default"),
            "description": field.get("description"),
        }
        for name, field in node["properties"].items()
    ]


def _cmd_schema(args: argparse.Namespace) -> int:
    from provisa.core.models import ProvisaConfig

    root = ProvisaConfig.model_json_schema(by_alias=True)
    defs = root["$defs"]
    if args.kind is None:
        kinds = sorted(
            name
            for name, field in root["properties"].items()
            if "properties" in _resolved(field, defs)
        )
        print(json.dumps({"kinds": kinds}) if args.json else "\n".join(kinds))
        return OK
    node = root
    for step in args.kind.split("."):
        field = node.get("properties", {}).get(step)
        if field is None:
            return _usage("schema", f"no kind {args.kind!r}; `provisa schema` lists them")
        node = _resolved(field, defs)
    if "properties" not in node:
        return _usage("schema", f"{args.kind!r} is a value and has no fields")
    fields = _fields(node)
    if args.json:
        print(json.dumps({"kind": args.kind, "fields": fields}, default=str))
        return OK
    for item in fields:
        mark = "required" if item["required"] else "optional"
        print(f"{item['name']}  {item['type']}  {mark}")
    return OK


# --- help ------------------------------------------------------------------------------------ #


def _cmd_exit_codes(args: argparse.Namespace) -> int:
    if args.json:
        # One key per command, so a script reads `can-i` and not the heading it is grouped under.
        print(
            json.dumps(
                {
                    name.strip(): codes
                    for heading, codes in EXIT_CODES.items()
                    for name in heading.split(",")
                }
            )
        )
        return OK
    for heading, codes in EXIT_CODES.items():
        print(heading)
        for code, meaning in codes.items():
            print(f"  {code}  {meaning}")
    return OK


def _words(parser: argparse.ArgumentParser, path: tuple[str, ...] = ()) -> Any:
    subs = [a for a in parser._actions if isinstance(a, argparse._SubParsersAction)]
    names = list(subs[0].choices) if subs else []
    options = [s for a in parser._actions for s in a.option_strings]
    yield path, names, options
    for name in names:
        yield from _words(subs[0].choices[name], (*path, name))


def _bash(root: argparse.ArgumentParser) -> str:
    cases = "\n".join(
        f'    "{" ".join(path)}") echo "{" ".join([*names, *options])}" ;;'
        for path, names, options in _words(root)
    )
    return f"""_provisa_words() {{
  case "$1" in
{cases}
  esac
}}
_provisa() {{
  local path="" word i
  for ((i = 1; i < COMP_CWORD; i++)); do
    word="${{COMP_WORDS[i]}}"
    case "$word" in -*) continue ;; esac
    case " $(_provisa_words "$path") " in
      *" $word "*) path="${{path:+$path }}$word" ;;
    esac
  done
  COMPREPLY=($(compgen -W "$(_provisa_words "$path")" -- "${{COMP_WORDS[COMP_CWORD]}}"))
}}
complete -F _provisa provisa
"""


def _fish(root: argparse.ArgumentParser) -> str:
    lines = []
    for path, names, options in _words(root):
        when = (
            "__fish_use_subcommand"
            if not path
            else " ; and ".join(f"__fish_seen_subcommand_from {step}" for step in path)
        )
        lines += [f"complete -c provisa -f -n '{when}' -a '{name}'" for name in names]
        lines += [
            f"complete -c provisa -f -n '{when}' -l {o[2:]}" for o in options if o.startswith("--")
        ]
    return "\n".join(lines) + "\n"


def _cmd_completion(args: argparse.Namespace) -> int:
    if args.shell == "fish":
        sys.stdout.write(_fish(args.root_parser))
    elif args.shell == "zsh":
        sys.stdout.write("autoload -U +X bashcompinit && bashcompinit\n" + _bash(args.root_parser))
    else:
        sys.stdout.write(_bash(args.root_parser))
    return OK


def register(sub: Any, root: argparse.ArgumentParser) -> None:
    can = sub.add_parser("can-i", help="Say whether a role may do something, and why not")
    can.add_argument("question", choices=tuple(_QUESTIONS), help="What is asked")
    can.add_argument("target", help="The right, or the table or column as domain.table[.column]")
    can.add_argument("--role", default=None, help="The role asked about")
    can.add_argument("--person", default=None, help="The person, once entitlements apply")
    can.add_argument("--org", required=True, help="The organization (X-Org-Provisa)")
    _add_instance(can)
    can.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds")
    can.set_defaults(func=_cmd_can_i)

    wait = sub.add_parser("wait", help="Block until an instance is ready or a replica is current")
    conditions = wait.add_subparsers(dest="wait_command", required=True)
    ready = conditions.add_parser("ready", help="Until the instance answers /ready")
    ready.set_defaults(func=_cmd_wait_ready)
    replica = conditions.add_parser("replica", help="Until a table's replica is current")
    replica.add_argument("table", help="source/schema/table")
    replica.add_argument("--org", default=None, help="The organization (X-Org-Provisa)")
    replica.set_defaults(func=_cmd_wait_replica)
    for parser in (ready, replica):
        _add_instance(parser)
        parser.add_argument(
            "--timeout", type=int, default=300, help="Give up after this many seconds"
        )
        parser.add_argument("--interval", type=int, default=2, help="Seconds between looks")

    schema = sub.add_parser("schema", help="Print the config fields for a kind, from the model")
    schema.add_argument("kind", nargs="?", default=None, help="A kind, or kind.field to descend")
    schema.set_defaults(func=_cmd_schema)

    codes = sub.add_parser("exit-codes", help="What each command's exit codes mean")
    codes.set_defaults(func=_cmd_exit_codes)

    completion = sub.add_parser("completion", help="Print a shell completion script")
    completion.add_argument("shell", choices=("bash", "zsh", "fish"))
    completion.set_defaults(func=_cmd_completion, root_parser=root)
