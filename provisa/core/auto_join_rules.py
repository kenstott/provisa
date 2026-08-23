# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How wide an auto-join email rule actually reaches (REQ-1567).

REQ-1477 refuses the extreme case — a rule admitting a consumer mailbox provider. Between that and
a safe rule sits the ordinary mistake: a rule is a regex matched with ``re.search``, so an
unanchored fragment matches any address CONTAINING it. ``acme\\.com`` admits notacme.com.au and
acme.com.attacker.net, and every one of those people arrives holding the org's default role.

The reverse case is equally real and must not be banned: a company genuinely receives mail at
subdivision domains carrying the parent inside them — eu.acme.com, acme.com.au for the same firm —
so a rule reaching past one exact domain is legitimate. Breadth is therefore MEASURED and CONSENTED
TO rather than guessed at.

The measurement is empirical, like REQ-1477's: the rule is matched against probe addresses built
from the domains it names, so it reaches the same verdict here as it will at sign-in and an
unanchored or over-broad rule is caught however it was spelled.
"""

# Requirements: REQ-1268, REQ-1269, REQ-1477, REQ-1567

from __future__ import annotations

import re
from dataclasses import dataclass, field

from provisa.core.org_membership import email_matches_rule

# The shape of a domain literal inside a rule, once the regex's own escaping is undone: at least
# two dot-separated labels ending in an alphabetic TLD. A rule naming no such literal names no
# organization — it is a fragment matching addresses by accident.
_DOMAIN_LITERAL = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9-]+)*\.[a-z]{2,}")

# Regex syntax carried by an ordinary domain rule. Removing it leaves the literal text the rule
# matches; anything else (a class, a group, a quantifier) is left in place, where it simply fails to
# look like a domain and so contributes no candidate.
_ESCAPES = re.compile(r"\\(.)")


@dataclass
class RuleRisk:
    """What an auto-join rule admits beyond the domains it names.

    ``refusal`` is a reason code for a rule no organization's membership could describe; ``admits``
    holds sample addresses the rule accepts that are NOT at one of the domains it names, which is
    what the author is shown and asked to accept.
    """

    domains: list[str] = field(default_factory=list)
    admits: list[str] = field(default_factory=list)
    refusal: str | None = None

    @property
    def needs_acknowledgement(self) -> bool:
        return self.refusal is None and bool(self.admits)


def domains_named_by(rule: str) -> list[str]:
    """The domain literals a rule spells out, in the order they appear.

    Read from the rule's text rather than probed, because a probe needs a domain to probe WITH.
    Escapes are undone first (``acme\\.com`` is the ordinary way to write a literal dot), and a
    match must run to the end of a label, so ``@acme\\.com$`` yields ``acme.com``.
    """
    unescaped = _ESCAPES.sub(r"\1", rule.lower())
    seen: list[str] = []
    for match in _DOMAIN_LITERAL.finditer(unescaped):
        domain = match.group(0)
        if domain not in seen:
            seen.append(domain)
    return seen


def rule_risk(rule: str | None) -> RuleRisk:
    """Measure what ``rule`` admits, as the org policy gate and the resolver both read it.

    A NULL rule admits every address; REQ-1477 already refuses auto-join without a rule, and this
    reports it as unbounded for the same reason rather than relying on that.

    Two shapes are refused outright, because no organization's membership is describable by them:

    * a rule naming no domain at all — a bare fragment that matches by coincidence;
    * a rule admitting an ARBITRARY SUFFIX after a domain it names, which is the unanchored case:
      ``acme\\.com`` accepts acme.com.attacker.net, a domain the attacker owns outright.

    What remains is reported rather than judged: any probe the rule accepts that is not at one of
    its own domains — a neighbouring domain that merely ends the same way, a subdomain — is returned
    as a sample for the author to look at, because only they know whether eu.acme.com is their
    European division or somebody else entirely.
    """
    if rule is None:
        return RuleRisk(refusal="unbounded_no_rule")
    domains = domains_named_by(rule)
    if not domains:
        return RuleRisk(refusal="unbounded_no_domain")
    risk = RuleRisk(domains=domains)
    for domain in domains:
        # The unanchored case. The suffix is a domain somebody else registers and controls.
        if email_matches_rule(f"someone@{domain}.attacker-owned.example", rule):
            return RuleRisk(domains=domains, refusal="unbounded_suffix")
    for domain in domains:
        # A neighbour that merely ends the same way. "not" is a real prefix, not a metacharacter,
        # so this is exactly the address a stranger could register tomorrow.
        for probe in (f"someone@not{domain}", f"someone@a-division.{domain}"):
            if email_matches_rule(probe, rule) and probe.split("@", 1)[1] not in domains:
                risk.admits.append(probe)
    return risk
