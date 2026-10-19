# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Central redaction: one rule list applied wherever Provisa writes text out (REQ-2087).

A rule is a named regular expression, or a ``${secret:...}`` / ``${env:...}`` reference resolved
when the rules are built and matched literally. A match becomes ``[REDACTED:<rule>]``. The list:

- the built-ins, a starter set of common shapes (card numbers checked by Luhn, US SSNs, bearer
  tokens, Provisa's own access tokens, common cloud keys and private-key blocks);
- the deployment's rules (``deployment_settings`` row ``redaction.rules``) and each org's
  (its ``org_settings`` block ``redaction``), edited on the Redaction page;
- the automatic rule: every secret the model can reach -- the names Provisa's own vault holds,
  every ``${secret:...}`` the org's stored model and settings reference (on any backend), and each
  ``${user:...}`` value as it resolves -- matched by value, listed by name only.

Patterns are best effort, and the built-ins are a start: an org adds rules for its own sensitive
formats (account numbers, internal ids). What leaves the process -- every span, every log record,
stdout -- is redacted with every org's rules together, since an exported span or log record is not
read in an org's context; a rule only removes text, so applying another org's rule removes more,
never less. The audit tables and a client's error message, written in an org's context, use the
deployment's rules, that org's, and the automatic rule.
"""

# Requirements: REQ-2087

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

_log = logging.getLogger(__name__)

#: The deployment_settings row holding the deployment's own rules, and the org_settings block.
DEPLOYMENT_KEY = "redaction.rules"
ORG_BLOCK = "redaction"
#: The automatic rule's name prefix: ``auto:<secret name>``.
AUTO = "auto"
#: A value shorter than this is not matched as a secret. A two-character secret matched literally
#: would rewrite every word holding those two letters in every log line and span, making the
#: records unreadable without protecting a value that short (REQ-2087).
MIN_SECRET_LENGTH = 4

_NAME = re.compile(r"^[A-Za-z0-9_.\-]{1,64}$")
_REFERENCE = re.compile(r"^\$\{(env|secret):([^}]+)\}$")


@dataclass(frozen=True)
class Rule:
    """One rule: a named pattern, or a named ``${secret:}`` / ``${env:}`` reference."""

    name: str
    pattern: str | None = None
    reference: str | None = None
    source: str = "deployment"  # "builtin", "deployment" or "org"

    def as_dict(self) -> dict:
        return {
            "name": self.name,
            "pattern": self.pattern,
            "reference": self.reference,
            "source": self.source,
        }


def _luhn(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def _card(match: re.Match) -> bool:
    digits = re.sub(r"[ -]", "", match.group(0))
    return 13 <= len(digits) <= 19 and _luhn(digits)


#: The starter set. Each is a common shape, not a guarantee: an org adds its own formats.
BUILTINS: tuple[Rule, ...] = (
    Rule("card_number", r"\b(?:\d[ -]?){12,18}\d\b", source="builtin"),
    Rule("us_ssn", r"\b(?!000|666|9\d\d)\d{3}-(?!00)\d{2}-(?!0000)\d{4}\b", source="builtin"),
    Rule("bearer_token", r"(?i)\bbearer\s+[A-Za-z0-9\-._~+/]+=*", source="builtin"),
    Rule("provisa_access_token", r"provisa_pat_[A-Za-z0-9_\-]{20,}", source="builtin"),
    Rule("aws_access_key_id", r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b", source="builtin"),
    Rule(
        "aws_secret_access_key",
        r"(?i)aws_secret_access_key[\"']?\s*[=:]\s*[\"']?[A-Za-z0-9/+=]{40}",
        source="builtin",
    ),
    Rule("gcp_api_key", r"\bAIza[0-9A-Za-z\-_]{35}\b", source="builtin"),
    Rule("azure_storage_key", r"AccountKey=[A-Za-z0-9+/=]{86,88}", source="builtin"),
    Rule("github_token", r"\bgh[pousr]_[A-Za-z0-9]{36,}\b", source="builtin"),
    Rule("slack_token", r"\bxox[abprs]-[A-Za-z0-9\-]{10,}\b", source="builtin"),
    Rule(
        "private_key",
        r"-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----",
        source="builtin",
    ),
)

_VALIDATORS: dict[str, Callable[[re.Match], bool]] = {"card_number": _card}


class RuleInvalid(ValueError):
    """A rule refused by name for what is wrong with it."""

    def __init__(self, name: str, why: str) -> None:
        super().__init__(f"redaction rule {name!r}: {why}")
        self.rule = name
        self.why = why


def parse_rules(raw: Iterable[dict], source: str) -> list[Rule]:
    """``raw`` (``[{name, pattern | reference}]``) as rules, each checked: a name of letters,
    digits, ``_ . -``; exactly one of a pattern that compiles or a ``${secret:}`` / ``${env:}``
    reference; no name twice, nor a built-in's."""
    rules: list[Rule] = []
    seen = {r.name for r in BUILTINS}
    for item in raw:
        name = str(item.get("name") or "").strip()
        pattern = item.get("pattern") or None
        reference = item.get("reference") or None
        if not _NAME.match(name):
            raise RuleInvalid(name, "a name is 1-64 letters, digits, '_', '.' or '-'")
        if name in seen:
            raise RuleInvalid(name, "the name is already used")
        if (pattern is None) == (reference is None):
            raise RuleInvalid(name, "give either a pattern or a reference")
        if pattern is not None:
            try:
                re.compile(pattern)
            except re.error as exc:
                raise RuleInvalid(name, f"the pattern does not compile: {exc}") from exc
        if reference is not None and not _REFERENCE.match(reference):
            raise RuleInvalid(name, "a reference is ${secret:NAME} or ${env:NAME}")
        seen.add(name)
        rules.append(Rule(name, pattern, reference, source))
    return rules


class Redactor:
    """Compiled rules: patterns in order, then the literal values (longest first)."""

    def __init__(self, rules: list[Rule], literals: dict[str, str]) -> None:
        self._patterns = [
            (r.name, re.compile(r.pattern), _VALIDATORS.get(r.name))
            for r in rules
            if r.pattern is not None
        ]
        usable = {v: n for v, n in literals.items() if len(v) >= MIN_SECRET_LENGTH}
        self._names = usable
        self._literal = (
            re.compile("|".join(re.escape(v) for v in sorted(usable, key=len, reverse=True)))
            if usable
            else None
        )

    def redact(self, text: str) -> str:
        if not text:
            return text
        if self._literal is not None:
            text = self._literal.sub(lambda m: f"[REDACTED:{self._names[m.group(0)]}]", text)
        for name, compiled, check in self._patterns:
            if check is None:
                text = compiled.sub(f"[REDACTED:{name}]", text)
            else:
                text = compiled.sub(
                    lambda m, n=name, c=check: f"[REDACTED:{n}]" if c(m) else m.group(0), text
                )
        return text

    def redact_value(self, value: Any) -> Any:
        """``value`` with every string inside it redacted (dicts, lists and tuples walked)."""
        if isinstance(value, str):
            return self.redact(value)
        if isinstance(value, dict):
            return {k: self.redact_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return type(value)(self.redact_value(v) for v in value)
        return value


# --- the rule list, held by the process ---------------------------------------------------------

_lock = threading.Lock()
_deployment: list[Rule] = []
_orgs: dict[str, list[Rule]] = {}
#: Resolved values by the rule they belong to: a reference rule's value, or a secret the model can
#: reach (``auto:<name>``). Per org, and "" for the deployment's own.
_values: dict[str, dict[str, str]] = {}
#: ``auto:<name>`` names per org, for the page: names only, never values.
_auto_names: dict[str, list[str]] = {}
#: ``${user:...}`` values as they resolve, by ``auto:user:<name>``.
_user_values: dict[str, str] = {}
_process = Redactor(list(BUILTINS), {})
_per_org: dict[str, Redactor] = {}


def _rebuild() -> None:
    """Swap in new redactors built from what is held now. Called under ``_lock``."""
    global _process, _per_org
    every_value: dict[str, str] = {}
    for values in _values.values():
        every_value.update(values)
    every_value.update(_user_values)
    every_org_rule = [r for rules in _orgs.values() for r in rules]
    _process = Redactor([*BUILTINS, *_deployment, *every_org_rule], every_value)
    _per_org = {
        org: Redactor([*BUILTINS, *_deployment, *rules], every_value)
        for org, rules in _orgs.items()
    }


def redact(text: str, org_id: str | None = None) -> str:
    """``text`` redacted: by every org's rules when ``org_id`` is None (what leaves the process),
    else by the deployment's, that org's and the automatic rule."""
    redactor = _process if org_id is None else _per_org.get(org_id, _process)
    return redactor.redact(text)


def redact_value(value: Any, org_id: str | None = None) -> Any:
    redactor = _process if org_id is None else _per_org.get(org_id, _process)
    return redactor.redact_value(value)


def note_user_value(reference: str, value: str) -> None:
    """A ``${user:...}`` value as it resolves (per request): matched from now on."""
    _note(value, f"{AUTO}:user:{reference}")


def note_secret_value(reference: str, value: str) -> None:
    """A ``${secret:...}`` value as it resolves anywhere: matched from now on."""
    _note(value, f"{AUTO}:{reference}")


def _note(value: str, name: str) -> None:
    if not value or len(value) < MIN_SECRET_LENGTH or value in _user_values:
        return
    with _lock:
        _user_values[value] = name
        _rebuild()


def is_loaded(org_id: str) -> bool:
    return org_id in _orgs


def _resolved(rules: list[Rule], resolve: Callable[[str], str]) -> dict[str, str]:
    """Each reference rule's value. One that does not resolve is logged at ERROR by name and left
    out: the other rules stand, and the page shows it unresolved (REQ-2087)."""
    out: dict[str, str] = {}
    for rule in rules:
        if rule.reference is None:
            continue
        try:
            out[resolve(rule.reference)] = rule.name
        except (KeyError, ValueError) as exc:
            _log.error("redaction rule %r: its reference does not resolve: %s", rule.name, exc)
    return out


def set_deployment_rules(rules: list[Rule], resolve: Callable[[str], str]) -> None:
    with _lock:
        _deployment[:] = rules
        _values[""] = _resolved(rules, resolve)
        _rebuild()


def set_org(
    org_id: str,
    rules: list[Rule],
    secrets: dict[str, str],
    resolve: Callable[[str], str],
) -> None:
    """The org's own rules and the secrets its model can reach (name -> value)."""
    with _lock:
        _orgs[org_id] = rules
        values = _resolved(rules, resolve)
        for name, value in secrets.items():
            values.setdefault(value, f"{AUTO}:{name}")
        _values[org_id] = values
        _auto_names[org_id] = sorted(secrets)
        _rebuild()


def described(org_id: str) -> dict:
    """What the Redaction page shows: every rule, by source; the automatic rule by name only."""
    with _lock:
        deployment_values = set(_values.get("", {}).values())
        org_values = set(_values.get(org_id, {}).values())
        unresolved = [
            r.name
            for r in [*_deployment, *_orgs.get(org_id, [])]
            if r.reference is not None and r.name not in deployment_values | org_values
        ]
        return {
            "builtins": [r.as_dict() for r in BUILTINS],
            "deployment": [r.as_dict() for r in _deployment],
            "org": [r.as_dict() for r in _orgs.get(org_id, [])],
            "automatic": _auto_names.get(org_id, []),
            "resolved_at_use": len(_user_values),
            "unresolved": unresolved,
        }


def reset() -> None:
    """Forget every rule but the built-ins (test isolation)."""
    global _process, _per_org
    with _lock:
        _deployment.clear()
        _orgs.clear()
        _values.clear()
        _auto_names.clear()
        _user_values.clear()
        _process = Redactor(list(BUILTINS), {})
        _per_org = {}
