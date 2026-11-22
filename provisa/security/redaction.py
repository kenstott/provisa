# Copyright (c) 2026 Kenneth Stott
# Canary: f42773bc-3fe6-4377-b8c5-4a9c02ad29b9
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Central redaction: one rule list applied wherever Provisa writes text out (REQ-2087).

A rule is a named regular expression, or a ``${secret:...}`` / ``${env:...}`` reference resolved
when the rules are built and matched literally, as a whole token only (never inside a longer
identifier). A match becomes ``[REDACTED:<rule>]`` in what
Provisa records (logs, spans, the audit trail), and a bare ``[REDACTED]`` in text sent to a client
(:func:`client_redact`): a client is never told which rule or which variable a word matched. The
list:

- the built-ins, a starter set of common shapes (card numbers checked by Luhn, US SSNs, bearer
  tokens, Provisa's own access tokens, common cloud keys and private-key blocks);
- the deployment's rules (``deployment_settings`` row ``redaction.rules``) and each org's
  (its ``org_settings`` block ``redaction``), edited on the Redaction page;
- the automatic rule: every secret the model can reach -- the names Provisa's own vault holds,
  every ``${secret:...}`` the org's stored model and settings reference (on any backend), and each
  ``${user:...}`` value as it resolves -- matched by value, listed by name only.

Patterns are best effort, and the built-ins are a start: an org adds rules for its own sensitive
formats (account numbers, internal ids).

An org's rules apply only to that org's text: a record (span, log record, audit row, admin trail)
the org's request or task produced, and the text its clients and models are sent. A record no org
produced (start-up, a task bound to none) is redacted by the built-ins, the deployment's rules and
every secret value Provisa holds, so a secret is never written out for want of an org. An org's
pattern runs on :mod:`provisa.security.bounded_pattern`'s matcher, checked when saved and limited
in steps at use, so no org's pattern can stall the thread that writes another org's record. A
held org (:func:`provisa.security.sandbox_egress.held`) may not reference the server's
environment (``${env:...}``): the redaction of a probe word would tell it the variable's value.
"""

# Requirements: REQ-2087

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from provisa.security.bounded_pattern import (
    BoundedPattern,
    PatternRefused,
    StepLimitExceeded,
    compile_bounded,
)

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
#: The marker in text sent to a client: it names no rule and no variable (REQ-2087).
CLIENT_MARKER = "[REDACTED]"
#: A marker that names its rule, as the records carry it.
_NAMED_MARKER = re.compile(r"\[REDACTED:[^\]\n]*\]")
#: A character that can be part of an identifier: a secret edged by one is matched only
#: where no identifier character stands beside that edge (REQ-2087, amended 2026-11-22).
_IDENTIFIER_CHAR = re.compile(r"\w")


def _whole_token(value: str) -> str:
    """``value`` as a literal pattern matching only a whole token (REQ-2087, amended 2026-11-22):
    an edge character that can be part of an identifier must not have an identifier character
    beside it, so ``provisa`` inside ``provisa_cache`` is left as written. An edge that cannot be
    part of an identifier needs no bound: no neighbour can make the value part of one. A quoted
    value is bounded by its quotes."""
    head = r"(?<!\w)" if _IDENTIFIER_CHAR.match(value[0]) else ""
    tail = r"(?!\w)" if _IDENTIFIER_CHAR.match(value[-1]) else ""
    return f"{head}{re.escape(value)}{tail}"


def _marker(name: str, named: bool) -> str:
    return f"[REDACTED:{name}]" if named else CLIENT_MARKER


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
    # A token is 16 or more token characters, so prose ("a bearer credential") is not one.
    Rule("bearer_token", r"(?i)\bbearer\s+[A-Za-z0-9\-._~+/]{16,}=*", source="builtin"),
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
    """A rule refused by name for what is wrong with it; ``code`` is the catalogued message."""

    def __init__(self, name: str, why: str, code: str = "redaction.rule_invalid") -> None:
        super().__init__(f"redaction rule {name!r}: {why}")
        self.rule = name
        self.why = why
        self.code = code


#: The catalogued message for each way the bounded matcher refuses an org's pattern.
_PATTERN_CODES = (
    ("backtrack catastrophically", "redaction.pattern_backtracks"),
    ("is not available in an org's rule", "redaction.pattern_unsupported"),
    ("is too large", "redaction.pattern_too_large"),
)


def _org_pattern(name: str, pattern: str) -> None:
    try:
        compile_bounded(pattern)
    except PatternRefused as exc:
        why = str(exc)
        code = next((c for said, c in _PATTERN_CODES if said in why), "redaction.rule_invalid")
        raise RuleInvalid(name, why, code) from exc


def parse_rules(raw: Iterable[dict], source: str, *, server_references: bool = True) -> list[Rule]:
    """``raw`` (``[{name, pattern | reference}]``) as rules, each checked: a name of letters,
    digits, ``_ . -``; exactly one of a pattern that compiles or a ``${secret:}`` / ``${env:}``
    reference; no name twice, nor a built-in's. An org's pattern must be one the bounded matcher
    runs (:mod:`provisa.security.bounded_pattern`); with ``server_references`` False (a held
    org's rules) an ``${env:...}`` reference is refused by name."""
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
            if source == "org":
                _org_pattern(name, pattern)
        if reference is not None:
            matched = _REFERENCE.match(reference)
            if matched is None:
                raise RuleInvalid(name, "a reference is ${secret:NAME} or ${env:NAME}")
            if matched.group(1) == "env" and not server_references:
                raise RuleInvalid(name, _SERVER_REFERENCE, "redaction.server_reference")
        seen.add(name)
        rules.append(Rule(name, pattern, reference, source))
    return rules


_SERVER_REFERENCE = (
    "an organization on this server may not reference the server's environment; "
    "use ${secret:NAME} from the organization's own vault"
)


def _replacer(rule: Rule) -> Callable[[str, str], str]:
    """``(text, marker) -> text`` for one pattern rule: an org's on the bounded matcher, the
    built-ins and the deployment's (the operator's own) on ``re``."""
    assert rule.pattern is not None
    if rule.source == "org":
        bounded: BoundedPattern = compile_bounded(rule.pattern)
        return lambda text, said: bounded.sub(lambda _m: said, text)
    compiled = re.compile(rule.pattern)
    check = _VALIDATORS.get(rule.name)
    if check is None:
        return lambda text, said: compiled.sub(lambda _m: said, text)
    return lambda text, said: compiled.sub(lambda m: said if check(m) else m.group(0), text)


class Redactor:
    """Compiled rules: patterns in order, then the literal values (longest first). ``org_id`` is
    the org whose records it redacts (None: no org's)."""

    def __init__(
        self, rules: list[Rule], literals: dict[str, str], org_id: str | None = None
    ) -> None:
        self._org = org_id
        self._patterns = [(r.name, _replacer(r)) for r in rules if r.pattern is not None]
        usable = {v: n for v, n in literals.items() if len(v) >= MIN_SECRET_LENGTH}
        self._names = usable
        self._literal = (
            re.compile("|".join(_whole_token(v) for v in sorted(usable, key=len, reverse=True)))
            if usable
            else None
        )

    def redact(self, text: str, *, named: bool = True) -> str:
        """``text`` with each match replaced: by the marker naming its rule, or (``named`` False,
        the client's form) by the bare marker, a marker already in the text losing its name too."""
        if not text:
            return text
        if self._literal is not None:
            text = self._literal.sub(lambda m: _marker(self._names[m.group(0)], named), text)
        for name, replace in self._patterns:
            marker = _marker(name, named)
            try:
                text = replace(text, marker)
            except StepLimitExceeded as exc:
                # The text is withheld whole: what the rule would have removed is not known, and
                # a rule only ever removes text (REQ-2087). Said once per rule, by name.
                _exceeded_once(self._org, name, exc)
                return marker
        return text if named else _NAMED_MARKER.sub(CLIENT_MARKER, text)

    def redact_value(self, value: Any, *, named: bool = True) -> Any:
        """``value`` with every string inside it redacted (dicts, lists and tuples walked)."""
        if isinstance(value, str):
            return self.redact(value, named=named)
        if isinstance(value, dict):
            return {k: self.redact_value(v, named=named) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return type(value)(self.redact_value(v, named=named) for v in value)
        return value


# --- the rule list, held by the process ---------------------------------------------------------

_lock = threading.Lock()
_deployment: list[Rule] = []
_orgs: dict[str, list[Rule]] = {}
#: Reference rules' resolved values (value -> rule name): per org, and "" for the deployment's.
_references: dict[str, dict[str, str]] = {}
#: The secrets each org's model can reach (value -> ``auto:<name>``).
_secrets: dict[str, dict[str, str]] = {}
#: ``auto:<name>`` names per org, for the page: names only, never values.
_auto_names: dict[str, list[str]] = {}
#: ``${secret:...}`` / ``${user:...}`` values as they resolve (value -> ``auto:...``), by the org
#: bound when they resolved; "" when none was.
_noted: dict[str, dict[str, str]] = {}
#: Pattern rules that exceeded the matcher's step limit, per org ("" for no org's records).
_exceeded: dict[str, set[str]] = {}
_process = Redactor(list(BUILTINS), {})
_per_org: dict[str, Redactor] = {}


def _exceeded_once(org_id: str | None, name: str, exc: StepLimitExceeded) -> None:
    said = _exceeded.setdefault(org_id or "", set())
    if name in said:
        return
    said.add(name)
    _log.error(
        "redaction rule %r of org %s: %s; the text was withheld",
        name,
        org_id,
        exc,
    )


def _rebuild() -> None:
    """Swap in new redactors built from what is held now. Called under ``_lock``."""
    global _process, _per_org
    shared = {**_references.get("", {}), **_noted.get("", {})}
    every_secret: dict[str, str] = {}
    for values in [*_secrets.values(), *_noted.values()]:
        every_secret.update(values)
    _process = Redactor([*BUILTINS, *_deployment], {**shared, **every_secret})
    _per_org = {
        org: Redactor(
            [*BUILTINS, *_deployment, *rules],
            {
                **shared,
                **_references.get(org, {}),
                **_secrets.get(org, {}),
                **_noted.get(org, {}),
            },
            org,
        )
        for org, rules in _orgs.items()
    }


def _redactor(org_id: str | None) -> Redactor:
    """The redactor for ``org_id``'s text: an org whose rules are not loaded holds none of its
    own yet, so the deployment's apply, as to a record of no org."""
    return _process if org_id is None else _per_org.get(org_id, _process)


def redact(text: str, org_id: str | None = None) -> str:
    """``text`` redacted for Provisa's own records (a log, a span, the audit trail), each match
    naming its rule: by the deployment's rules, ``org_id``'s and the secrets that org reaches;
    with ``org_id`` None (a record no org produced) by the deployment's rules and every secret
    value held. Never for text sent to a client: that is :func:`client_redact`."""
    return _redactor(org_id).redact(text)


def redact_value(value: Any, org_id: str | None = None) -> Any:
    """:func:`redact` over every string inside ``value``: for the records only."""
    return _redactor(org_id).redact_value(value)


def client_redact(text: str, org_id: str | None = None) -> str:
    """``text`` redacted as anything outside Provisa's own records is sent it -- a client's error
    on any transport, a prompt or a text sent to a model: the same matches as :func:`redact`, each
    replaced by the bare marker, and a named marker the text already carries made bare. The marker
    names no rule and no variable, so a client cannot learn which secret a word it typed matches
    (REQ-2087)."""
    return _redactor(org_id).redact(text, named=False)


def client_redact_value(value: Any, org_id: str | None = None) -> Any:
    """:func:`client_redact` over every string inside ``value``."""
    return _redactor(org_id).redact_value(value, named=False)


def note_user_value(reference: str, value: str) -> None:
    """A ``${user:...}`` value as it resolves (per request): matched from now on, in the bound
    org's text and in records of no org."""
    _note(value, f"{AUTO}:user:{reference}")


def note_secret_value(reference: str, value: str) -> None:
    """A ``${secret:...}`` value as it resolves anywhere: matched from now on, in the bound org's
    text and in records of no org."""
    _note(value, f"{AUTO}:{reference}")


def _note(value: str, name: str) -> None:
    from provisa.core.request_context import current_org

    org = current_org.get() or ""
    if not value or len(value) < MIN_SECRET_LENGTH or value in _noted.get(org, {}):
        return
    with _lock:
        _noted.setdefault(org, {})[value] = name
        _rebuild()


def is_loaded(org_id: str) -> bool:
    return org_id in _orgs


def _resolved(
    rules: list[Rule], resolve: Callable[[str], str], *, server_references: bool = True
) -> dict[str, str]:
    """Each reference rule's value. One that does not resolve is logged at ERROR by name and left
    out: the other rules stand, and the page shows it unresolved (REQ-2087). Without
    ``server_references`` (a held org) an ``${env:...}`` reference is never resolved: one stored
    is refused here, as the save refuses it."""
    out: dict[str, str] = {}
    for rule in rules:
        if rule.reference is None:
            continue
        if not server_references and rule.reference.startswith("${env:"):
            _log.error("redaction rule %r: %s", rule.name, _SERVER_REFERENCE)
            continue
        try:
            out[resolve(rule.reference)] = rule.name
        except (KeyError, ValueError) as exc:
            _log.error("redaction rule %r: its reference does not resolve: %s", rule.name, exc)
    return out


def set_deployment_rules(rules: list[Rule], resolve: Callable[[str], str]) -> None:
    with _lock:
        _deployment[:] = rules
        _references[""] = _resolved(rules, resolve)
        _rebuild()


def set_org(
    org_id: str,
    rules: list[Rule],
    secrets: dict[str, str],
    resolve: Callable[[str], str],
    *,
    server_references: bool = True,
) -> None:
    """The org's own rules and the secrets its model can reach (name -> value). A held org
    (``server_references`` False) has no ``${env:...}`` reference resolved."""
    with _lock:
        _orgs[org_id] = rules
        _references[org_id] = _resolved(rules, resolve, server_references=server_references)
        _secrets[org_id] = {value: f"{AUTO}:{name}" for name, value in secrets.items()}
        _auto_names[org_id] = sorted(secrets)
        _exceeded.pop(org_id, None)
        _rebuild()


def described(org_id: str) -> dict:
    """What the Redaction page shows: every rule, by source; the automatic rule by name only."""
    with _lock:
        deployment_values = set(_references.get("", {}).values())
        org_values = set(_references.get(org_id, {}).values())
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
            "resolved_at_use": len(_noted.get(org_id, {})) + len(_noted.get("", {})),
            "unresolved": unresolved,
            "exceeded": sorted(_exceeded.get(org_id, set())),
        }


def reset() -> None:
    """Forget every rule but the built-ins (test isolation)."""
    global _process, _per_org
    with _lock:
        _deployment.clear()
        _orgs.clear()
        _references.clear()
        _secrets.clear()
        _auto_names.clear()
        _noted.clear()
        _exceeded.clear()
        _process = Redactor(list(BUILTINS), {})
        _per_org = {}
