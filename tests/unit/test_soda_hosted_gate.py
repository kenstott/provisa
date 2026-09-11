# Copyright (c) 2026 Kenneth Stott
# Canary: f1ac7e9e-82b5-4dcb-8d9e-79eb2e3a6437
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""soda-core's Elastic License 2.0 bar on the hosted plane (REQ-1725).

soda-core's terms prohibit offering it to third parties as a hosted or managed service
(config/capabilities.yaml's ``cloud_eligible: false`` on the ``soda`` option documents this). The
Sources form never lists ``soda`` as a choice, so the only reachable path is a caller naming the
type directly through ``createSource``/``updateSource`` — which took whatever type it was given,
with nothing checking it. ``commerce.enabled()`` is the same self-hosted-vs-hosted signal REQ-1469
and REQ-1513 already gate on: the commercial plugin is mounted only on the plane this license
bars soda from.
"""

from __future__ import annotations

from provisa.api.admin import schema_mutation


class TestRefuseSodaOnHostedPlane:
    def test_soda_is_refused_when_the_commercial_plugin_is_mounted(self, monkeypatch):
        import provisa.core.commerce as commerce

        monkeypatch.setattr(commerce, "enabled", lambda: True)
        result = schema_mutation._refuse_soda_on_hosted_plane("soda")
        assert result is not None
        assert result.success is False
        assert result.code == "schema.source_type_not_hosted"
        assert result.params == {"type": "soda"}

    def test_soda_is_admitted_on_a_self_hosted_deployment(self, monkeypatch):
        import provisa.core.commerce as commerce

        monkeypatch.setattr(commerce, "enabled", lambda: False)
        assert schema_mutation._refuse_soda_on_hosted_plane("soda") is None

    def test_great_expectations_is_never_refused_hosted_or_not(self, monkeypatch):
        """Apache 2.0 carries no hosted-service bar (capabilities.yaml has no cloud_eligible key
        on it) — the same checker pattern, a different license, and no gate."""
        import provisa.core.commerce as commerce

        monkeypatch.setattr(commerce, "enabled", lambda: True)
        assert schema_mutation._refuse_soda_on_hosted_plane("great_expectations") is None

    def test_ordinary_source_types_are_never_refused(self, monkeypatch):
        import provisa.core.commerce as commerce

        monkeypatch.setattr(commerce, "enabled", lambda: True)
        for stype in ("postgresql", "mongodb", "files", "snowflake"):
            assert schema_mutation._refuse_soda_on_hosted_plane(stype) is None
