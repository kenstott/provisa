# Copyright (c) 2026 Kenneth Stott
# Canary: f15f7794-68f5-4926-bd03-bb9e5a200d8d
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""An AskAmerica source's schemas are the adapter bundle's, and each has a home (REQ-540, REQ-541).

One place states which schemas a subject brings (``core.models.GOVDATA_SUBJECT_SCHEMAS``) and
which every source serves (``GOVDATA_LINKER_SCHEMAS``). It is held here, with no allowlist, to
the schemas recorded for the pinned bundle; the record is held to the pin; and the search
catalog and the admin API are held to the same."""

# Requirements: REQ-540, REQ-541

from __future__ import annotations

import json
from pathlib import Path

import pytest

from provisa.core.models import (
    GOVDATA_LINKER_SCHEMAS,
    GOVDATA_SUBJECT_LABELS,
    GOVDATA_SUBJECT_SCHEMAS,
    GovDataSubject,
)
from provisa.govdata import subjects

_GOVDATA = Path(subjects.__file__).resolve().parent


def _in_a_subject() -> set[str]:
    return {schema for schemas in GOVDATA_SUBJECT_SCHEMAS.values() for schema in schemas}


def test_every_schema_the_bundle_serves_has_a_home():
    _, served = subjects.bundle_schemas()
    homeless = served - _in_a_subject() - set(GOVDATA_LINKER_SCHEMAS)
    assert not homeless, (
        f"the bundle serves {sorted(homeless)}, which no subject brings and which are not "
        "linker schemas: a source could never be given them"
    )


def test_no_subject_and_no_linker_names_a_schema_the_bundle_lacks():
    _, served = subjects.bundle_schemas()
    assert not _in_a_subject() - served, sorted(_in_a_subject() - served)
    assert not set(GOVDATA_LINKER_SCHEMAS) - served


def test_a_linker_schema_belongs_to_no_subject():
    assert not set(GOVDATA_LINKER_SCHEMAS) & _in_a_subject()


def test_the_record_is_of_the_pinned_bundle():
    """Moving the pin without recording what the new bundle serves fails here, by name."""
    from provisa.runtime_deps.pgwire_bundles import connector_release

    release, _ = subjects.bundle_schemas()
    assert release == connector_release("govdata"), (
        f"provisa/govdata/bundle_schemas.json records {release}; the pinned bundle is "
        f"{connector_release('govdata')}. Run scripts/record_govdata_bundle_schemas.py."
    )


def test_the_record_says_the_script_is_its_only_writer():
    record = json.loads((_GOVDATA / "bundle_schemas.json").read_text())
    assert "scripts/record_govdata_bundle_schemas.py" in record["generated_by"]
    assert "only writer" in record["generated_by"]
    assert record["schemas"] == sorted(record["schemas"])


def test_every_subject_has_schemas_a_label_and_a_place_in_the_enum():
    offered = {s.value for s in GovDataSubject} - {GovDataSubject.all.value}
    assert set(GOVDATA_SUBJECT_SCHEMAS) == offered == set(GOVDATA_SUBJECT_LABELS)
    assert all(GOVDATA_SUBJECT_SCHEMAS.values())


def test_the_search_catalog_describes_exactly_the_bundles_schemas():
    """The catalog the chat's topic search reads is generated from the pinned release's schema
    definitions (scripts/build_govdata_catalog.py); it names its schemas with hyphens."""
    _, served = subjects.bundle_schemas()
    described = {
        name.replace("-", "_")
        for name in json.loads((_GOVDATA / "catalog_metadata.json").read_text())
    }
    assert described == served, (sorted(described - served), sorted(served - described))


# -- what a source serves ---------------------------------------------------------------------


def test_a_source_serves_its_subjects_schemas_and_the_linker_schemas():
    assert subjects.schemas_for_subjects(["WEATHER"]) == ["weather", "ref", "geo"]
    assert subjects.schemas_for_subjects([]) == ["ref", "geo"]
    both = subjects.schemas_for_subjects(["EDUCATION", "DEMOGRAPHICS"])
    assert both.count("census") == 1 and both[-2:] == ["ref", "geo"]


def test_an_unknown_subject_is_refused_by_name():
    with pytest.raises(ValueError, match="NASA"):
        subjects.schemas_for_subjects(["WEATHER", "NASA"])


# -- a bundle that has moved on from the record is refused at server start ----------------------


def _model(names) -> dict:
    return {"schemas": [{"name": n} for n in names]}


def test_the_recorded_bundle_starts():
    release, served = subjects.bundle_schemas()
    subjects.require_recorded_schemas(_model(sorted(served)), release)


def test_a_bundle_serving_other_schemas_is_refused_naming_the_difference():
    release, served = subjects.bundle_schemas()
    moved = sorted((served - {"law"}) | {"space"})
    with pytest.raises(subjects.BundleSchemasChanged) as refused:
        subjects.require_recorded_schemas(_model(moved), release)
    assert "only in the bundle ['space']" in str(refused.value)
    assert "only in the record ['law']" in str(refused.value)


def test_a_bundle_of_another_release_is_refused():
    _, served = subjects.bundle_schemas()
    with pytest.raises(subjects.BundleSchemasChanged, match="engine-v9.9.9"):
        subjects.require_recorded_schemas(_model(sorted(served)), "engine-v9.9.9")


# -- the Sources form reads it from the server -------------------------------------------------


def test_the_admin_api_serves_the_subjects_and_the_linker_schemas():
    from provisa.api.admin.schema import admin_schema

    assert "govdataSubjects: GovDataSubjectsType!" in str(admin_schema)
    catalog = subjects.subject_catalog()
    assert [c["value"] for c in catalog] == list(GOVDATA_SUBJECT_SCHEMAS)
    assert all(c["label"] and c["schemas"] for c in catalog)
    assert "ALL" not in {c["value"] for c in catalog}


def test_the_ui_keeps_no_list_of_subjects_or_schemas_of_its_own():
    """The form's subjects come from admin `govdataSubjects`; a schema name written into the
    UI source would be a second list."""
    ui = Path(__file__).resolve().parents[2] / "provisa-ui" / "src"
    _, served = subjects.bundle_schemas()
    distinctive = sorted(s for s in served if "_" in s or s in ("fedregister", "cftc"))
    offenders = []
    for path in (ui / "pages").rglob("*.ts*"):
        if "__tests__" in path.parts or ".test." in path.name:
            continue
        text = path.read_text()
        if "GOVDATA_SUBJECTS" in text or any(f'"{name}"' in text for name in distinctive):
            offenders.append(str(path.relative_to(ui)))
    assert not offenders, offenders
