# Copyright (c) 2026 Kenneth Stott
# Canary: 6d1f3b84-2a95-4c07-8e13-b5f9c2a07d61
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2247: an entry of the Syncing page.

One list of entries, each with a type (Data Catalog, Ontology, Custom) and a direction (Export,
Import or both). A data catalog's entry carries what a metadata export target carried: the role it
publishes as and its provider's export settings. What is asserted is what a wrong model gets wrong
silently: a direction that is off still reaching a publish, an ontology entry taking a provider's
settings, and export settings lost between the entry and the provider built from it.
"""

# Requirements: REQ-2247, REQ-2122

from __future__ import annotations

import pytest
from pydantic import ValidationError

from provisa.core.models import CatalogConnection, CatalogsConfig, SyncDirectionOff


def _catalog(**over) -> dict:
    return {
        "id": "mz",
        "type": "data_catalog",
        "direction": "both",
        "provider": "openlineage",
        "endpoint": "http://mz",
        "domains": ["sales"],
        **over,
    }


def test_an_entry_states_its_type_and_its_direction():
    for missing in ("type", "direction"):
        entry = _catalog()
        del entry[missing]
        with pytest.raises(ValidationError, match=missing):
            CatalogConnection(**entry)


@pytest.mark.parametrize("entry_type", ["data_catalog", "ontology", "custom"])
@pytest.mark.parametrize("direction", ["export", "import", "both"])
def test_every_type_takes_every_direction(entry_type, direction):
    entry = (
        _catalog(direction=direction)
        if entry_type == "data_catalog"
        else {"id": "terms", "type": entry_type, "direction": direction}
    )
    built = CatalogConnection(**entry)
    assert (built.type, built.direction) == (entry_type, direction)


def test_an_unknown_type_or_direction_is_refused():
    with pytest.raises(ValidationError):
        CatalogConnection(**_catalog(type="warehouse"))
    with pytest.raises(ValidationError):
        CatalogConnection(**_catalog(direction="sideways"))


@pytest.mark.parametrize(
    ("direction", "exports", "imports"),
    [("export", True, False), ("import", False, True), ("both", True, True)],
)
def test_the_direction_says_which_runs_the_entry_has(direction, exports, imports):
    entry = CatalogConnection(**_catalog(direction=direction))
    assert (entry.runs("export"), entry.runs("import")) == (exports, imports)


def test_a_run_in_a_direction_that_is_off_is_refused():
    entry = CatalogConnection(**_catalog(direction="import"))
    with pytest.raises(SyncDirectionOff, match="'mz' does not export"):
        entry.require_runs("export")
    entry.require_runs("import")
    with pytest.raises(SyncDirectionOff, match="'mz' does not import"):
        CatalogConnection(**_catalog(direction="export")).require_runs("import")


def test_the_entry_carries_the_export_settings_into_the_provider_it_builds():
    entry = CatalogConnection(
        **_catalog(
            provider="datazone",
            endpoint="",
            aws_region="us-east-1",
            datazone_domain_id="dzd_1",
            datazone_project_id="prj_1",
            role="steward",
        )
    )
    target = entry.export_target()
    assert (target.id, target.provider, target.role, target.datazone_project_id) == (
        "mz",
        "datazone",
        "steward",
        "prj_1",
    )
    assert target.domains == ["sales"]

    unity = CatalogConnection(**_catalog(provider="unity_catalog", endpoint="", delta_share=True))
    assert unity.export_target().delta_share is True


def test_a_datazone_entry_that_exports_names_the_project_owning_its_products():
    datazone = _catalog(
        provider="datazone", endpoint="", aws_region="us-east-1", datazone_domain_id="dzd_1"
    )
    with pytest.raises(ValidationError, match="needs the DataZone project"):
        CatalogConnection(**datazone)
    # Import alone publishes nothing, so it owns no products and needs no project.
    CatalogConnection(**{**datazone, "direction": "import"})


def test_a_data_catalog_names_its_provider_and_a_domain():
    with pytest.raises(ValidationError, match="names no provider"):
        CatalogConnection(**_catalog(provider=""))
    with pytest.raises(ValidationError, match="assigned to no domain"):
        CatalogConnection(**_catalog(domains=[]))


@pytest.mark.parametrize("entry_type", ["ontology", "custom"])
@pytest.mark.parametrize(
    "setting",
    [{"provider": "openlineage"}, {"role": "steward"}, {"datazone_project_id": "p"}],
)
def test_an_ontology_or_custom_entry_has_no_provider_settings(entry_type, setting):
    with pytest.raises(ValidationError, match=f"of type '{entry_type}', which has no"):
        CatalogConnection(id="terms", type=entry_type, direction="import", **setting)


def test_an_ontology_entry_builds_no_export_provider():
    entry = CatalogConnection(id="terms", type="ontology", direction="both")
    with pytest.raises(ValueError, match="has no provider"):
        entry.export_target()


def test_a_publish_reaches_only_the_data_catalogs_that_export():
    config = CatalogsConfig.model_validate(
        {
            "catalogs": [
                _catalog(id="out", direction="export"),
                _catalog(id="in", direction="import"),
                _catalog(id="both", direction="both"),
                {"id": "terms", "type": "ontology", "direction": "both"},
                {"id": "own", "type": "custom", "direction": "export"},
            ]
        }
    )
    assert [t.id for t in config.export_targets()] == ["out", "both"]


def test_publishing_one_entry_whose_export_is_off_is_refused():
    config = CatalogsConfig.model_validate({"catalogs": [_catalog(id="in", direction="import")]})
    with pytest.raises(SyncDirectionOff):
        config.export_targets(only="in")
    with pytest.raises(KeyError):
        config.export_targets(only="gone")


def test_two_entries_of_one_name_are_refused():
    with pytest.raises(ValidationError, match="entries named twice: mz"):
        CatalogsConfig.model_validate(
            {"catalogs": [_catalog(), {"id": "mz", "type": "ontology", "direction": "both"}]}
        )
