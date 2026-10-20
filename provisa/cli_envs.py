# Copyright (c) 2026 Kenneth Stott
# Canary: fc991174-4864-48ab-8123-4a1e81f326af
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""``provisa env`` -- every environment operation from the command line (REQ-1498).

An environment change is rarely the only thing happening: it lands beside a schema change in a
source, a deployment, a data load or a test run, and those are scripted. So the whole lifecycle is
here, and EVERY COMMAND IS A CALL TO THE ADMIN API THE UI CALLS. Nothing is decided in this file:
the allow-list of what a copy carries (REQ-1489), the merge and removal rules (REQ-1490), binding
(REQ-1491), protection (REQ-1504) and the caller's rights are the server's, so they hold
identically however the operation was invoked. A command line that could copy what the API refuses
to copy would be the way secrets leave an environment.

THE CONTRACT WITH A SCRIPT is the exit code and ``--json``:

* ``0`` -- done, or a dry run that reported and applied nothing.
* ``1`` -- refused. The rule that refused is named by its code on stderr (or, under ``--json``, as
  ``{"refused": {...}}`` on stdout).
* ``2`` -- PROPOSED, not applied: the target is protected and the change waits for an approval. A
  pipeline that read a pending approval as success would report a release that has not happened.
"""

# Requirements: REQ-1496, REQ-1498, REQ-1504, REQ-1506, REQ-1541, REQ-1543, REQ-1548, REQ-1942

from __future__ import annotations

import argparse
import json
import sys
from typing import Any
from urllib.parse import quote, urlencode

REFUSED = 1
PROPOSED = 2


def _call(args: argparse.Namespace, method: str, path: str, body: dict | None = None) -> dict:
    """One call through the CLI's authenticated client (``cli._api_call``), resolved at call time
    so there is one client and one place it is replaced."""
    from provisa import cli

    return cli._api_call(args, method, path, body)


def _base(args: argparse.Namespace) -> str:
    return f"/admin/orgs/{quote(args.org, safe='')}/environments"


def _env_path(args: argparse.Namespace, name: str, act: str = "") -> str:
    return f"{_base(args)}/{quote(name, safe='')}{'/' + act if act else ''}"


def _flag(value: bool) -> str:
    return "true" if value else "false"


def _refused(args: argparse.Namespace, status: int, body: str) -> int:
    """Report a refusal naming the rule that refused, and exit non-zero (REQ-1498)."""
    try:
        answer = json.loads(body)
    except json.JSONDecodeError:
        answer = None
    if not isinstance(answer, dict) or "code" not in answer:
        # Not an answer from an API rule (a proxy's page, a transport failure's text): there is
        # no rule to name, so the status and the text are what there is to report.
        answer = {"code": f"http.{status}", "detail": body, "params": {}}
    if args.json:
        print(json.dumps({"refused": {"status": status, **answer}}))
    else:
        print(f"refused ({answer['code']}): {answer['detail']}", file=sys.stderr)
    return REFUSED


def _guarded(command):
    """Run ``command``, turning an API refusal into the non-zero exit that names its rule."""

    def run(args: argparse.Namespace) -> int:
        from provisa.cli import ApiRefused

        try:
            return command(args)
        except ApiRefused as refusal:
            return _refused(args, refusal.status, refusal.body)

    return run


def _print_report(report: dict[str, Any]) -> None:
    """A report as a log reads it. Two shapes reach here and both say the same four things -- what
    was added, changed, removed and left alone: a merge counts objects and names their paths
    (REQ-1548), a load lists paths (REQ-1496)."""
    if "ref" in report:
        print(f"  ref       {report['ref']}")
        # REQ-1496: what a load takes away is named first.
        for kind in ("removed", "added", "changed"):
            for path in report[kind]:
                print(f"  {kind[0].upper()} {path}")
        print(f"  unchanged {report['unchanged']}")
        return
    print(
        f"  added {report['added']}  changed {report['changed']}  "
        f"removed {report['removed']}  unchanged {report['unchanged']}"
    )
    for kind in ("added", "changed", "removed"):
        for path in report["paths"][kind]:
            print(f"  {kind[0].upper()} {path}")
        for key in report["settings"][kind]:
            print(f"  {kind[0].upper()} setting {key}")
    for path in report["pulled_in"]:
        print(f"  + {path} (required by the selection)")
    for conflict in report["conflicts"]:
        print(
            f"  ! {conflict['path']} ({conflict['source']} here, {conflict['target']} in the target)"
        )


def _outcome(args: argparse.Namespace, result: dict, verb: str, dry_run: bool = False) -> int:
    """Print what an applying command did and say it in the exit code."""
    proposed = bool(result.get("requires_approval")) and not result["applied"] and not dry_run
    if args.json:
        print(json.dumps(result))
        return PROPOSED if proposed else 0
    if proposed:
        request = result["request"]
        print(f"{verb} PROPOSED as request {request['id']} — {request['target_env']} is protected.")
        _print_report(request["report"])
        return PROPOSED
    report = result["report"]
    where = report["env"] if "env" in report else report["target_env"]
    print(f"{verb} {'APPLIED to' if result['applied'] else 'PLANNED for'} {where}")
    _print_report(report)
    return 0


# --- reading --------------------------------------------------------------------------------- #


def _cmd_list(args: argparse.Namespace) -> int:
    """`provisa env list` — the org's environments and the build each is at."""
    result = _call(args, "GET", _base(args))
    if args.json:
        print(json.dumps(result))
        return 0
    for env in result["environments"]:
        _print_env(env)
    return 0


def _print_env(env: dict[str, Any]) -> None:
    sha = env["deployed_sha"]
    notes = [
        "protected" if env["protected"] else "",
        f"from {env['parent']}" if env["parent"] else "",
        f"data {env['data_mode']}" if env["data_mode"] else "",
        f"expires {env['expires_at']}" if env["expires_at"] else "",
    ]
    print(f"{env['name']:<32} {sha[:12] if sha else '-':<12} {' '.join(n for n in notes if n)}")


def _cmd_show(args: argparse.Namespace) -> int:
    """`provisa env show` — one environment and its current build."""
    result = _call(args, "GET", _base(args))
    for env in result["environments"]:
        if env["name"] == args.env:
            if args.json:
                print(json.dumps(env))
            else:
                _print_env(env)
            return 0
    return _refused(
        args,
        404,
        json.dumps(
            {
                "code": "environments.unknown",
                "detail": f"Organization {args.org!r} has no environment {args.env!r}.",
                "params": {"org": args.org, "env": args.env},
            }
        ),
    )


def _cmd_diff(args: argparse.Namespace) -> int:
    """`provisa env diff` — what merging one environment into another would do. A GET: the method
    that cannot write is the one to hand a runner."""
    query = urlencode(
        [
            ("from_env", args.from_env),
            ("removals", _flag(args.removals)),
            *(("select", path) for path in args.select or []),
        ]
    )
    result = _call(args, "GET", f"{_env_path(args, args.into, 'merge-preview')}?{query}")
    return _outcome(args, result, "Merge", dry_run=True)


# --- writing --------------------------------------------------------------------------------- #


def _cmd_create(args: argparse.Namespace) -> int:
    """`provisa env create` — a new environment holding another's model, its data chosen at
    creation (REQ-1942): the API decides which choices the caller may make."""
    result = _call(
        args,
        "POST",
        _base(args),
        {
            "name": args.env,
            "from_env": args.from_env,
            "data_mode": args.data_mode,
            "mutation_handling": args.mutation_handling,
            "expires_at": args.expires_at,
        },
    )
    if args.json:
        print(json.dumps(result))
    else:
        print(f"Created {args.env} from {args.from_env}")
    return 0


def _cmd_delete(args: argparse.Namespace) -> int:
    """`provisa env delete` — retire an environment. Its branches go only when named."""
    query = urlencode(
        {
            "delete_branch": _flag(args.delete_branch),
            "delete_remote_branch": _flag(args.delete_remote_branch),
        }
    )
    result = _call(args, "DELETE", f"{_env_path(args, args.env)}?{query}")
    if args.json:
        print(json.dumps(result))
    else:
        print(f"Deleted {args.env}")
    return 0


def _cmd_merge(args: argparse.Namespace) -> int:
    """`provisa env merge` — merge one environment into another, by identity (REQ-1490)."""
    result = _call(
        args,
        "POST",
        _env_path(args, args.into, "merge"),
        {
            "from_env": args.from_env,
            "removals": args.removals,
            "selection": args.select,
            "dry_run": args.dry_run,
            "retire_source": args.retire_source,
            "retire_remote": args.retire_remote,
            "message": args.message or "",
        },
    )
    return _outcome(args, result, "Merge", dry_run=args.dry_run)


def _cmd_deploy(args: argparse.Namespace) -> int:
    """`provisa env deploy` — make the tree at a ref an environment's model (REQ-1496).

    This is the command a deployment pipeline runs, and the rule it obeys is the one the UI obeys:
    a deploy is an invocation CARRYING AN IDENTITY against a NAMED control plane. Nothing inside a
    Provisa deployment applies a merged branch on its own; the pipeline holding a credential is
    what makes the deploy happen, and revoking that credential is what stops it.
    """
    result = _call(
        args,
        "POST",
        _env_path(args, args.env, "deploy"),
        {"ref": args.ref, "dry_run": args.dry_run, "message": args.message or ""},
    )
    return _outcome(args, result, "Deploy", dry_run=args.dry_run)


def _cmd_revert(args: argparse.Namespace) -> int:
    """`provisa env revert` — put an environment back to a previous build (REQ-1543).

    With no ``--to`` it steps back one commit along the environment's own history. With ``--to`` it
    is the ordinary deploy of that build: there is one way to apply a tree, and a revert is it.
    """
    if args.to is not None:
        result = _call(
            args,
            "POST",
            _env_path(args, args.env, "deploy"),
            {"ref": args.to, "dry_run": args.dry_run, "message": args.message or ""},
        )
        return _outcome(args, result, "Revert", dry_run=args.dry_run)
    return _step(args, "undo", "Revert")


def _cmd_redo(args: argparse.Namespace) -> int:
    """`provisa env redo` — step forward again toward the build a revert departed from."""
    return _step(args, "redo", "Redo")


def _step(args: argparse.Namespace, act: str, verb: str) -> int:
    path = _env_path(args, args.env, act)
    if args.dry_run:
        # REQ-1496: what the step would remove is named before it is taken.
        result = _call(args, "POST", f"{path}?dry_run=true", {})
        return _outcome(args, result, verb, dry_run=True)
    result = _call(args, "POST", path, {})
    if "deployed_sha" not in result:
        return _outcome(args, result, verb)
    if args.json:
        print(json.dumps(result))
        return 0
    print(f"{verb} APPLIED to {args.env}: now at {result['deployed_sha'][:12]}")
    _print_report(result["report"])
    return 0


# --- the remote ------------------------------------------------------------------------------ #


def _cmd_fetch(args: argparse.Namespace) -> int:
    """`provisa env fetch` — bring the org's remote branches back (REQ-1541).

    The step BEFORE a deploy in the ordinary flow: the pipeline that just saw a pull request merge
    on the org's git host runs this, and then deploys ``origin/<branch>``. Provisa never runs it on
    a timer, so the branch a deploy names is one somebody fetched deliberately.
    """
    result = _call(args, "POST", f"{_base(args)}/-/repo-integration/fetch", {})
    if args.json:
        print(json.dumps(result))
        return 0
    for name, sha in sorted(result["branches"].items()):
        print(f"origin/{name}  {sha[:12]}")
    return 0


def _cmd_push(args: argparse.Namespace) -> int:
    """`provisa env push` — send an environment's branch to the org's remote (REQ-1546)."""
    result = _call(args, "POST", _env_path(args, args.env, "push"), {})
    if args.json:
        print(json.dumps(result))
    else:
        pushed = result["pushed"]
        print(f"Pushed {args.env}" + (f" at {pushed[:12]}" if pushed else ": nothing to send"))
    return 0


def _cmd_rebuild(args: argparse.Namespace) -> int:
    """`provisa env rebuild` — commit an environment's whole model again, clearing drift."""
    result = _call(args, "POST", _env_path(args, args.env, "rebuild"), {})
    if args.json:
        print(json.dumps(result))
    elif result["drifted"]:
        print(f"{args.env} is still drifted: the commit did not land", file=sys.stderr)
    else:
        sha = result["sha"]
        print(f"Rebuilt {args.env}" + (f" at {sha[:12]}" if sha else ": already up to date"))
    return REFUSED if result["drifted"] else 0


def _cmd_pull(args: argparse.Namespace) -> int:
    """`provisa env pull` — apply what the remote holds for an environment (REQ-1547)."""
    result = _call(args, "POST", _env_path(args, args.env, "pull"), {})
    if result["applied"] or result.get("requires_approval"):
        return _outcome(args, result, "Pull")
    if args.json:
        print(json.dumps(result))
    else:
        print(f"{args.env} already holds what the remote does")
    return 0


# --- the parser ------------------------------------------------------------------------------ #


def _command(env_sub, name: str, func, help_text: str, *, env: str | None = None):
    """One subcommand carrying what every one of them takes: the org, the control plane to
    address, the identity to address it as, and how to report."""
    parser = env_sub.add_parser(name, help=help_text)
    parser.add_argument("--org", required=True, help="Organization holding the environment")
    if env is not None:
        parser.add_argument("--env", required=True, help=env)
    parser.add_argument(
        "--json", action="store_true", help="Report as JSON: the API's answer, for a script"
    )
    parser.add_argument(
        "--api",
        default=None,
        help="Provisa API base URL (default: $PROVISA_API_URL or http://127.0.0.1:8000)",
    )
    parser.add_argument(
        "--token",
        default=None,
        help="Bearer token for an identity that may perform the operation "
        "(default: $PROVISA_API_TOKEN)",
    )
    parser.add_argument(
        "--timeout", type=int, default=300, help="Request timeout in seconds (default: 300)"
    )
    parser.set_defaults(func=_guarded(func))
    return parser


def register(sub) -> None:
    """Add the ``env`` command group to the ``provisa`` parser."""
    env = sub.add_parser("env", help="Environment operations against a running Provisa")
    env_sub = env.add_subparsers(dest="env_command", required=True)

    _command(env_sub, "list", _cmd_list, "List the org's environments and the build each is at")
    _command(
        env_sub,
        "show",
        _cmd_show,
        "Show one environment and its current build",
        env="Environment to show",
    )

    create = _command(
        env_sub,
        "create",
        _cmd_create,
        "Create an environment holding another's model",
        env="Name of the new environment",
    )
    create.add_argument(
        "--from", dest="from_env", default="prod", help="Environment to start from (default: prod)"
    )
    create.add_argument(
        "--data-mode",
        required=True,
        choices=["inherit", "unbound", "test_fake", "test_synthetic"],
        help="Where its data comes from: the connections of --from, copied as written there, "
        "none until it binds its own, or test data",
    )
    create.add_argument(
        "--mutation-handling",
        default="refused",
        choices=["refused", "reversible", "direct"],
        help="What a mutation in it does (default: refused)",
    )
    create.add_argument(
        "--expires-at", default=None, help="ISO-8601 instant after which the environment is deleted"
    )

    delete = _command(
        env_sub, "delete", _cmd_delete, "Delete an environment", env="Environment to delete"
    )
    delete.add_argument("--delete-branch", action="store_true", help="Also delete its branch")
    delete.add_argument(
        "--delete-remote-branch", action="store_true", help="Also delete its branch on the remote"
    )

    diff = _command(
        env_sub, "diff", _cmd_diff, "Report what merging one environment into another would do"
    )
    merge = _command(env_sub, "merge", _cmd_merge, "Merge one environment into another")
    for parser in (diff, merge):
        parser.add_argument(
            "--from", dest="from_env", required=True, help="Environment merged from"
        )
        parser.add_argument("--into", required=True, help="Environment merged into")
        parser.add_argument(
            "--removals",
            action="store_true",
            help="Also remove from the target what the source no longer has",
        )
        parser.add_argument(
            "--select",
            action="append",
            default=None,
            metavar="PATH",
            help="Scope the merge to this object, by its path in the model "
            "(metrics/revenue.yaml); repeat for more. What it requires is carried with it",
        )
    merge.add_argument(
        "--dry-run", action="store_true", help="Print the merge report and apply none of it"
    )
    merge.add_argument("--message", default=None, help="What the merge does; required to apply")
    merge.add_argument(
        "--retire-source", action="store_true", help="Delete the source once its work has landed"
    )
    merge.add_argument(
        "--retire-remote",
        action="store_true",
        help="With --retire-source, also delete the source's branch on the remote",
    )

    deploy = _command(
        env_sub,
        "deploy",
        _cmd_deploy,
        "Deploy the model at a ref into an environment, making it that environment's",
        env="Environment that will hold the result",
    )
    deploy.add_argument(
        "--ref", required=True, help="Branch or commit in the org's repository to deploy"
    )
    deploy.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what the deploy would do and apply none of it",
    )
    deploy.add_argument("--message", default=None, help="Note carried onto an approval request")

    revert = _command(
        env_sub,
        "revert",
        _cmd_revert,
        "Put an environment back to a previous build",
        env="Environment to revert",
    )
    revert.add_argument(
        "--to", default=None, help="Build to revert to (default: the one before the current build)"
    )
    revert.add_argument("--message", default=None, help="Note carried onto an approval request")
    redo = _command(
        env_sub,
        "redo",
        _cmd_redo,
        "Step forward again toward the build a revert departed from",
        env="Environment to step forward",
    )
    for parser in (revert, redo):
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report what the step would do, removals first, and apply none of it",
        )

    _command(env_sub, "fetch", _cmd_fetch, "Fetch the org's remote branches into its repository")
    _command(
        env_sub,
        "push",
        _cmd_push,
        "Send an environment's branch to the org's remote",
        env="Environment to push",
    )
    _command(
        env_sub,
        "rebuild",
        _cmd_rebuild,
        "Commit an environment's whole model again, clearing drift",
        env="Environment to rebuild",
    )
    _command(
        env_sub,
        "pull",
        _cmd_pull,
        "Apply what the remote holds for an environment",
        env="Environment to pull",
    )
