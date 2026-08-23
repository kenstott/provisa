# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""How wide an auto-join rule reaches, and which reach has to be consented to (REQ-1567)."""

# Requirements: REQ-1477, REQ-1567

import pytest

from provisa.core.auto_join_rules import domains_named_by, rule_risk


class TestTheDomainsARuleNames:
    """The literals a rule spells out — what the probes are then built from."""

    @pytest.mark.parametrize(
        "rule,expected",
        [
            (r"@acme\.com$", ["acme.com"]),
            (r"^[^@]+@acme\.com$", ["acme.com"]),
            (r"@(acme\.com|acme\.co\.uk)$", ["acme.com", "acme.co.uk"]),
            (r"@eu\.acme\.com$", ["eu.acme.com"]),
            # Escaping is a spelling choice, not a meaning: an unescaped dot names the same domain.
            (r"@acme.com$", ["acme.com"]),
            # A fragment that names no domain at all.
            (r"acme", []),
        ],
    )
    def test_reads_the_literals_out_of_the_rule(self, rule, expected):
        assert domains_named_by(rule) == expected


class TestRulesNothingLegitimateNeeds:
    """Shapes no organization's membership can be described by, refused outright."""

    def test_no_rule_at_all_admits_everyone(self):
        assert rule_risk(None).refusal == "unbounded_no_rule"

    def test_a_fragment_naming_no_domain_is_matching_by_coincidence(self):
        assert rule_risk("acme").refusal == "unbounded_no_domain"

    def test_an_unanchored_domain_admits_a_domain_somebody_else_owns(self):
        # acme.com.attacker-owned.example is registered by the attacker, and re.search finds
        # "acme.com" inside it.
        assert rule_risk(r"@acme\.com").refusal == "unbounded_suffix"


class TestBreadthThatIsShownRatherThanBanned:
    """A reach past one exact domain is legitimate often enough that the author decides."""

    def test_one_exact_domain_needs_no_acknowledgement(self):
        risk = rule_risk(r"@acme\.com$")
        assert risk.refusal is None
        assert risk.admits == []
        assert not risk.needs_acknowledgement

    def test_a_neighbour_that_merely_ends_the_same_way_is_reported(self):
        # No "@" boundary: notacme.com is a domain a stranger can register tomorrow.
        risk = rule_risk(r"acme\.com$")
        assert risk.refusal is None
        assert "someone@notacme.com" in risk.admits
        assert risk.needs_acknowledgement

    def test_a_subdomain_sweep_is_reported_not_refused(self):
        # The legitimate subdivision case: eu.acme.com may be the European division, or may not.
        risk = rule_risk(r"@([a-z-]+\.)?acme\.com$")
        assert risk.refusal is None
        assert "someone@a-division.acme.com" in risk.admits
        assert risk.needs_acknowledgement

    def test_naming_every_subdivision_exactly_needs_no_acknowledgement(self):
        risk = rule_risk(r"@(acme\.com|eu\.acme\.com|acme\.com\.au)$")
        assert risk.refusal is None
        assert risk.admits == []

    def test_the_domains_it_names_come_back_with_the_verdict(self):
        assert rule_risk(r"@acme\.com$").domains == ["acme.com"]
