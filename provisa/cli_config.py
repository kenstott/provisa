# Copyright (c) 2026 Kenneth Stott
# Canary: 35b0636b-9cb6-4ccd-8d4d-ef29fbe7ae9a
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa validate``, ``diff``, ``apply``, ``export``, ``promote``: the CLI moves the config
(REQ-2222).

State changes through the config, so these five carry it and decide nothing. ``validate`` reads a
file here. The rest call the API the UI calls, as the organization named by ``--org``, so the
caller's rights and every rule are the server's.

A SECRET STAYS A REFERENCE. No command here resolves ``${provider:name}``: ``validate`` and ``diff``
need no vault and no environment variable the file names, ``apply`` sends the references as
written, and a report names fields, never their values.

An apply adds and updates what the file declares and removes nothing (REQ-1919), so ``diff`` lists
what is only on the instance apart from what would change.

THE CONTRACT WITH A SCRIPT is the exit code and ``--json``:

* ``0`` -- valid, reported, applied, exported or promoted. A diff that finds changes is still
  ``0`` unless ``--exit-code`` asks otherwise.
* ``1`` -- refused. The code is on stderr or, under ``--json``, in ``{"refused": {...}}`` on stdout.
  An instance that could not be reached also exits ``1``, with the reason on stderr.
* ``2`` -- the command line is wrong; or, from ``promote`` alone, PROPOSED and not applied: the
  target is protected and the change waits for an approval (``provisa env``'s contract, REQ-1504).
* ``3`` -- ``diff --exit-code`` only: the instance has moved from the config, for a scheduled drift
  check. What only the instance holds counts, though an apply would leave it.

A SAVED PLAN is what ``diff --out`` wrote: the file as it stood, references as written, and a
digest of what the instance held. ``apply --plan`` sends exactly that file, and refuses
(``plan.stale``) if the instance no longer holds what the plan was made against.
"""

# Requirements: REQ-2222

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml

from provisa.cli_envs import REFUSED, _call, _command, _env_path, _outcome

_REFERENCE = "${"

#: A reference in a typed field cannot be checked here, for its value is known only where it is
#: used. Each pydantic error for one maps to a value of that type, so the rest of the file is
#: still checked.
_STAND_INS: dict[str, Any] = {"int_parsing": 1, "float_parsing": 1.0, "bool_parsing": True}

_NAMING_RULES = "naming.rules"

DRIFT = 3
USAGE = 2
_PLAN_FORMAT = 1


class _Refused(Exception):
    """A config this command will not go on with, by the code a script reads."""

    def __init__(self, code: str, detail: str, **params: Any) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail
        self.params = params


def _refuse(args: argparse.Namespace, refusal: _Refused) -> int:
    if args.json:
        print(
            json.dumps(
                {
                    "refused": {
                        "code": refusal.code,
                        "detail": refusal.detail,
                        "params": refusal.params,
                    }
                }
            )
        )
        return REFUSED
    print(f"refused ({refusal.code}): {refusal.detail}", file=sys.stderr)
    for problem in refusal.params.get("errors", []):
        print(f"  {problem['loc']}: {problem['msg']}", file=sys.stderr)
    return REFUSED


def _reporting(command):
    """Run ``command``, turning a refused config into the exit that names its code."""

    def run(args: argparse.Namespace) -> int:
        try:
            return command(args)
        except _Refused as refusal:
            return _refuse(args, refusal)

    return run


def _yaml_problem(exc: yaml.YAMLError) -> str:
    """What is wrong and on which line. Never ``str(exc)``: that quotes the file's own text, and
    the line it quotes may hold a credential someone wrote by mistake."""
    mark = getattr(exc, "problem_mark", None)
    where = f" at line {mark.line + 1}" if mark is not None else ""
    return f"{getattr(exc, 'problem', None) or type(exc).__name__}{where}"


def _read(path: str) -> dict[str, Any]:
    """The file with its ``includes:`` spliced in, references as written."""
    from provisa.core.config_loader import read_config_with_includes

    try:
        return read_config_with_includes(path)
    except OSError as exc:
        raise _Refused("config.unreadable", f"cannot read {path}: {exc.strerror}") from exc
    except yaml.YAMLError as exc:
        raise _Refused("config.bad_yaml", f"{path}: {_yaml_problem(exc)}") from exc
    except ValueError as exc:
        raise _Refused("config.invalid", str(exc)) from exc


def _parent(data: Any, loc: tuple[Any, ...]) -> Any | None:
    """The container ``loc`` names its last step in, or None when ``loc`` does not lead to one."""
    for step in loc[:-1]:
        try:
            data = data[step]
        except (KeyError, IndexError, TypeError):
            return None
    return data


def _holder(data: Any, loc: tuple[Any, ...]) -> Any | None:
    """The container of the value at ``loc``, or None when ``loc`` does not lead to one."""
    parent = _parent(data, loc)
    try:
        parent[loc[-1]]  # type: ignore[index]
    except (KeyError, IndexError, TypeError):
        return None
    return parent


class _Unresolved(str):
    """A reference standing in a typed field, as written. What it resolves to is known only where
    it is used, so it can be compared with another reference and with nothing else."""


def _validated(raw: dict[str, Any], code: str) -> tuple[Any, dict[tuple[Any, ...], str]]:
    """``raw`` as the configuration model, with no reference resolved, and each reference that
    stood in a typed field, by where it stood.

    The same transforms and model as ``config_loader.parse_config_dict``, without the step that
    fetches secrets: a pipeline validating a branch holds none of them."""
    from pydantic import ValidationError

    from provisa.core.config_loader import (
        STORED_MODEL,
        commit_time_columns,
        kafka_topics_as_tables,
        views_as_tables,
    )
    from provisa.core.models import ProvisaConfig

    stood_in: dict[tuple[Any, ...], str] = {}
    try:
        data = kafka_topics_as_tables(commit_time_columns(views_as_tables(copy.deepcopy(raw))))
        while True:
            try:
                return ProvisaConfig.model_validate(data, context={STORED_MODEL: False}), stood_in
            except ValidationError as exc:
                found = _stand_in_references(data, exc)
                if not found:
                    raise
                stood_in.update(found)
    except ValidationError as exc:
        # include_input=False: a rejected value is never echoed, for it may be a credential.
        errors = [
            {"loc": ".".join(str(step) for step in e["loc"]), "msg": e["msg"]}
            for e in exc.errors(include_url=False, include_input=False, include_context=False)
        ]
        raise _Refused(code, f"{len(errors)} problem(s) in the config", errors=errors) from exc
    except (ValueError, KeyError) as exc:
        raise _Refused(code, str(exc.args[0]) if exc.args else type(exc).__name__) from exc


def _stand_in_references(data: dict[str, Any], exc: Any) -> dict[tuple[Any, ...], str]:
    """Give each reference pydantic could not type a value of its field's type; return each one
    by where it stood."""
    found: dict[tuple[Any, ...], str] = {}
    for error in exc.errors(include_url=False):
        value = error["input"]
        if error["type"] not in _STAND_INS or not isinstance(value, str) or _REFERENCE not in value:
            continue
        holder = _holder(data, error["loc"])
        if holder is None:
            continue
        holder[error["loc"][-1]] = _STAND_INS[error["type"]]
        found[tuple(error["loc"])] = value
    return found


# --- what would change ----------------------------------------------------------------------- #


def _identity(section: str, item: dict[str, Any]) -> str:
    """What names an item of a section, the same on both sides of a diff."""
    if section == "tables":
        if item.get("profile_of") is not None:
            return f"{item['profile_of']}/profile/{item['kind']}"
        return f"{item['source_id']}/{item['schema']}/{item['table']}"
    if section == "calendars":
        return f"{item['name']}@{item['version']}"
    if section == _NAMING_RULES:
        return str(item["pattern"])
    if section == "rls_rules":
        target = (item.get(k) for k in ("table_id", "domain_id", "action_name", "tag_id"))
        named = "/".join(str(t) for t in target if t is not None)
        return f"{item['role_id']}/{named}/{','.join(sorted(item.get('rights') or []))}"
    for name in ("id", "name"):
        if name in item:
            return str(item[name])
    return json.dumps(item, sort_keys=True, default=str)


def _canonical(value: Any) -> Any:
    """``value`` in an order that says nothing: both sides of a diff are written by different
    hands, and the export sorts what a file need not."""
    from provisa.api.admin.config_export import normalize_config

    def _scalars_sorted(node: Any) -> Any:
        if isinstance(node, dict):
            return {k: _scalars_sorted(v) for k, v in node.items()}
        if isinstance(node, list):
            items = [_scalars_sorted(i) for i in node]
            if items and not any(isinstance(i, (dict, list)) for i in items):
                return sorted(items, key=str)
            return items
        return node

    return _scalars_sorted(normalize_config(value))


def _comparable(raw: dict[str, Any], code: str) -> dict[str, dict[str, dict[str, Any]]]:
    """Each model section of ``raw``, keyed by what names each item: read through the model, so a
    default stated in one side and left out of the other is no difference, and with each reference
    in a typed field back in place of its stand-in, marked for ``_differing``."""
    from provisa.core.store_config import MODEL_SECTIONS

    config, stood_in = _validated(raw, code)
    dumped = config.model_dump(mode="json", by_alias=True, exclude_defaults=True)
    for loc, reference in stood_in.items():
        _restore_reference(dumped, loc, reference)
    sections: dict[str, list[dict[str, Any]]] = {s: dumped.get(s, []) for s in MODEL_SECTIONS}
    sections[_NAMING_RULES] = (dumped.get("naming") or {}).get("rules", [])
    return {
        section: {_identity(section, i): _canonical(i) for i in items}
        for section, items in sections.items()
    }


def _restore_reference(node: Any, loc: tuple[Any, ...], reference: str) -> None:
    """Put ``reference`` where its stand-in was. The stand-in may equal the field's default and be
    missing from the dump, so the field need not be there. A reference in a list of scalars stands
    for the whole list: the list has no element to compare it with."""
    while isinstance(loc[-1], int):
        loc = loc[:-1]
    parent = _parent(node, loc)
    if parent is not None:
        parent[loc[-1]] = _Unresolved(reference)


def _differing(ours: Any, theirs: Any, path: str = "") -> list[str]:
    """The dotted paths at which two items differ. Names only: a value may be a credential."""
    if isinstance(ours, _Unresolved) != isinstance(theirs, _Unresolved):
        return []
    if not isinstance(ours, dict) or not isinstance(theirs, dict):
        return [] if ours == theirs else [path]
    out: list[str] = []
    for key in sorted(ours.keys() | theirs.keys()):
        out += _differing(ours.get(key), theirs.get(key), f"{path}.{key}" if path else key)
    return out


def _report(
    ours: dict[str, dict[str, dict[str, Any]]], theirs: dict[str, dict[str, dict[str, Any]]]
) -> dict[str, Any]:
    sections: dict[str, Any] = {}
    unchanged = 0
    for section, items in ours.items():
        held = theirs[section]
        changed = {}
        for key in sorted(items.keys() & held.keys()):
            fields = _differing(items[key], held[key])
            if fields:
                changed[key] = fields
        unchanged += len(items.keys() & held.keys()) - len(changed)
        entry = {
            "add": sorted(items.keys() - held.keys()),
            "change": [{"item": key, "fields": fields} for key, fields in changed.items()],
            "instance_only": sorted(held.keys() - items.keys()),
        }
        if any(entry.values()):
            sections[section] = entry
    return {
        "changes": any(s["add"] or s["change"] for s in sections.values()),
        "unchanged": unchanged,
        "sections": sections,
    }


def _print_report(report: dict[str, Any]) -> None:
    for section, entry in report["sections"].items():
        for item in entry["add"]:
            print(f"  A {section}/{item}")
        for change in entry["change"]:
            print(f"  C {section}/{change['item']}  ({', '.join(change['fields'])})")
        for item in entry["instance_only"]:
            print(f"  I {section}/{item} (only on the instance; an apply leaves it)")
    print(f"  unchanged {report['unchanged']}")


# --- talking to an instance ------------------------------------------------------------------ #


def _send(args: argparse.Namespace, method: str, path: str, body: bytes | None = None) -> bytes:
    """One authenticated call acting in ``--org``, returning the body as sent: an export is YAML,
    which ``cli._api_call`` does not read, and the org goes in a header it does not set."""
    from provisa.cli import ApiRefused

    api = args.api or os.environ.get("PROVISA_API_URL", "http://127.0.0.1:8000")
    token = args.token or os.environ.get("PROVISA_API_TOKEN", "")
    req = urllib.request.Request(f"{api.rstrip('/')}{path}", method=method, data=body)
    req.add_header("X-Org-Provisa", args.org)
    if body is not None:
        req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        # B310 wants the scheme pinned: the URL is built from the operator's own --api
        # argument, so it is http(s) by construction, never file:// or a custom handler.
        with urllib.request.urlopen(req, timeout=args.timeout) as resp:  # nosec B310
            return resp.read()
    except urllib.error.HTTPError as exc:
        raise ApiRefused(method, path, exc.code, exc.read().decode(errors="replace")) from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"cannot reach the Provisa API at {api}: {exc.reason}") from exc
    except TimeoutError as exc:
        raise SystemExit(f"no answer from the Provisa API at {api} within {args.timeout}s") from exc


def _exported(args: argparse.Namespace) -> bytes:
    return _send(args, "GET", f"/admin/orgs/{quote(args.org, safe='')}/config-export")


# --- the commands ---------------------------------------------------------------------------- #


def _cmd_validate(args: argparse.Namespace) -> int:
    """`provisa validate` -- is this file a configuration? Reads the file and nothing else."""
    from provisa.core.store_config import MODEL_SECTIONS

    config, _ = _validated(_read(args.file), "config.invalid")
    counts = {s: len(getattr(config, s)) for s in MODEL_SECTIONS if getattr(config, s)}
    if args.json:
        print(json.dumps({"valid": True, "file": args.file, "sections": counts}))
    else:
        print(f"valid: {args.file}")
        for section, count in counts.items():
            print(f"  {section} {count}")
    return 0


def _held(args: argparse.Namespace) -> dict[str, dict[str, dict[str, Any]]]:
    """What the instance holds for the org, ready to compare."""
    try:
        held = yaml.safe_load(_exported(args)) or {}
    except yaml.YAMLError as exc:
        raise _Refused("config.instance_invalid", _yaml_problem(exc)) from exc
    return _comparable(held, "config.instance_invalid")


def _digest(held: dict[str, dict[str, dict[str, Any]]]) -> str:
    """One value for what the instance holds: compared through the model, so a reordering of an
    export is not a move."""
    return hashlib.sha256(json.dumps(held, sort_keys=True).encode()).hexdigest()


def _write_plan(
    args: argparse.Namespace,
    raw: dict[str, Any],
    held: dict[str, dict[str, dict[str, Any]]],
    report: dict[str, Any],
) -> None:
    plan = {
        "provisa_plan": _PLAN_FORMAT,
        "org": args.org,
        "instance": _digest(held),
        "config_yaml": yaml.safe_dump(raw, sort_keys=False),
        "report": report,
    }
    try:
        Path(args.out).write_text(json.dumps(plan, indent=2))
    except OSError as exc:
        raise _Refused("plan.unwritable", f"cannot write {args.out}: {exc.strerror}") from exc


def _cmd_diff(args: argparse.Namespace) -> int:
    """`provisa diff` -- what applying this file would change on this instance. A GET."""
    raw = _read(args.file)
    ours = _comparable(raw, "config.invalid")
    held = _held(args)
    report = _report(ours, held)
    if args.out is not None:
        _write_plan(args, raw, held, report)
    if args.json:
        print(json.dumps(report))
    else:
        _print_report(report)
    return DRIFT if args.exit_code and report["sections"] else 0


def _planned(args: argparse.Namespace) -> str:
    """The file a plan saved, once the instance is shown to be what the plan was made against."""
    try:
        plan = json.loads(Path(args.plan).read_text())
    except OSError as exc:
        raise _Refused("plan.unreadable", f"cannot read {args.plan}: {exc.strerror}") from exc
    except json.JSONDecodeError as exc:
        raise _Refused("plan.invalid", f"{args.plan} is not a plan") from exc
    if not isinstance(plan, dict) or plan.get("provisa_plan") != _PLAN_FORMAT:
        raise _Refused("plan.invalid", f"{args.plan} is not a plan")
    if plan["org"] != args.org:
        raise _Refused("plan.wrong_org", f"the plan was made for {plan['org']}, not {args.org}")
    if _digest(_held(args)) != plan["instance"]:
        raise _Refused(
            "plan.stale", "the instance has moved since this plan was made; run diff again"
        )
    return plan["config_yaml"]


def _cmd_apply(args: argparse.Namespace) -> int:
    """`provisa apply` -- apply this file to the org, as the UI's import does. Whether it may is
    the server's: it validates the file and asks each section's own right."""
    if (args.file is None) == (args.plan is None):
        print("provisa apply: give either a file or --plan", file=sys.stderr)
        return USAGE
    if args.plan is not None:
        config_yaml = _planned(args)
    else:
        # The includes are spliced in first: the instance cannot read the paths they name here.
        config_yaml = yaml.safe_dump(_read(args.file), sort_keys=False)
    body = json.dumps({"config_yaml": config_yaml})
    result = json.loads(_send(args, "POST", "/admin/import/hasura/apply", body.encode()))
    if args.json:
        print(json.dumps(result))
        return 0
    print(f"Applied {args.file or args.plan} to {args.org}")
    for name, value in result["summary"].items():
        if isinstance(value, int):
            print(f"  {name} {value}")
    return 0


def _cmd_export(args: argparse.Namespace) -> int:
    """`provisa export` -- the org's model as a configuration, byte for byte as the instance wrote
    it. Applied to an empty store it builds the same model."""
    content = _exported(args)
    if args.file is None:
        sys.stdout.buffer.write(content)
        return 0
    try:
        Path(args.file).write_bytes(content)
    except OSError as exc:
        raise _Refused("config.unwritable", f"cannot write {args.file}: {exc.strerror}") from exc
    print(f"Exported {args.org} to {args.file}", file=sys.stderr)
    return 0


def _cmd_promote(args: argparse.Namespace) -> int:
    """`provisa promote` -- carry one environment's model into another (REQ-1490). The API decides
    what a promotion carries: bindings, grants and secrets stay where they are."""
    result = _call(
        args,
        "POST",
        _env_path(args, args.to, "merge"),
        {"from_env": args.from_env, "message": args.message},
    )
    return _outcome(args, result, "Promote")


def register(sub: Any) -> None:
    """Add the five commands that move a config to the ``provisa`` parser."""
    validate = sub.add_parser("validate", help="Check that a file is a valid configuration")
    validate.add_argument("file", help="Configuration file")
    validate.add_argument("--json", action="store_true", help="Report as JSON, for a script")
    validate.set_defaults(func=_reporting(_cmd_validate))

    diff = _command(
        sub, "diff", _reporting(_cmd_diff), "Report what applying a file would change on an org"
    )
    diff.add_argument("file", help="Configuration file")
    diff.add_argument("--out", default=None, help="Save a plan that apply --plan applies exactly")
    diff.add_argument(
        "--exit-code",
        action="store_true",
        help="Exit 3 when the instance has moved from the config, for a scheduled drift check",
    )

    apply = _command(sub, "apply", _reporting(_cmd_apply), "Apply a configuration file to an org")
    apply.add_argument("file", nargs="?", default=None, help="Configuration file")
    apply.add_argument("--plan", default=None, help="Apply exactly what diff --out saved")

    export = _command(
        sub, "export", _reporting(_cmd_export), "Write an org's model as a configuration file"
    )
    export.add_argument("--file", default=None, help="Write here (default: stdout)")

    promote = _command(sub, "promote", _cmd_promote, "Promote one environment's model into another")
    promote.add_argument("--from", dest="from_env", required=True, help="Environment promoted from")
    promote.add_argument("--to", required=True, help="Environment promoted into")
    promote.add_argument("--message", required=True, help="What this promotion carries")
