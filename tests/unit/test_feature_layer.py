# Copyright (c) 2026 Kenneth Stott
# Canary: 2f6c8b13-9a4e-4d71-8e05-c3b7d1a9f462
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The feature layer above the requirements (REQ-2004).

A feature is told in short paragraphs, each with a kebab id that is also its anchor; every
requirement not rejected names the one paragraph it serves, and every paragraph is served by at
least one. docs/features.md, the feature matrix, the traceability matrix and the roadmap are
generated from it, and none of them carries an internal feature.
"""

# Requirements: REQ-2004

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

from provisa.tools.feature_schema import FeaturesFile, feature_status
from provisa.tools.req_schema import RequirementsFile

ROOT = Path(__file__).resolve().parents[2]


def _script(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _feature(
    fid: str = "regions",
    visibility: str = "feature",
    audience: str | None = "modeler",
    paragraphs: list | None = None,
    **kw,
) -> dict:
    return {
        "id": fid,
        "name": kw.pop("name", fid.title()),
        "section": kw.pop("section", "Model your data"),
        "visibility": visibility,
        "audience": audience,
        "paragraphs": paragraphs
        if paragraphs is not None
        else [
            {"id": f"{fid}-placement", "text": "Data stays where it is placed. It is read there."}
        ],
        **kw,
    }


def _req(rid, feature, status="complete", **kw):
    entry = {
        "id": rid,
        "status": status,
        "group": "3. Source Registration & Data Modeling",
        "category": "Regions",
        "priority": "SHOULD",
        "type": "structural",
        "description": f"{rid} description",
        "feature": feature,
        **kw,
    }
    if status == "complete":
        entry.setdefault("since", "2026-10")
    if status == "rejected":
        entry.setdefault("rejection_reason", "not wanted")
    return entry


# -- the schema ------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"paragraphs": [{"id": "regions.placement", "text": "x"}]}, "kebab case"),
        ({"paragraphs": [{"id": "other-placement", "text": "x"}]}, "must start with 'regions-'"),
        ({"paragraphs": []}, "at least one paragraph"),
        ({"visibility": "internal"}, "no audience"),
        ({"audience": None}, "no audience"),
    ],
)
def test_a_malformed_feature_is_refused(change, message):
    with pytest.raises(ValueError, match=message):
        FeaturesFile.model_validate({"features": [{**_feature(), **change}]})


def test_paragraph_ids_are_unique_across_features():
    twin = _feature("regions", name="Twin")
    with pytest.raises(ValueError, match="duplicate"):
        FeaturesFile.model_validate({"features": [_feature(), twin]})


def test_an_internal_feature_has_no_audience_and_is_not_published():
    ff = FeaturesFile.model_validate(
        {"features": [_feature("ci", visibility="internal", audience=None)]}
    )
    assert not ff.features[0].published


def test_a_feature_is_available_only_when_none_of_its_requirements_is_open():
    done = [_req("REQ-1", "regions-placement"), _req("REQ-2", "other-x", "proposed")]
    rf = RequirementsFile.model_validate({"requirements": done})
    assert feature_status({"regions-placement"}, rf.requirements) == "Available"
    building = [*done, _req("REQ-3", "regions-placement", "in-progress")]
    rf = RequirementsFile.model_validate({"requirements": building})
    assert feature_status({"regions-placement"}, rf.requirements) == "In development"


# -- the validator ---------------------------------------------------------------------------------


def _validate(tmp_path, monkeypatch, features, requirements) -> int:
    validate = _script("validate_requirements")
    (tmp_path / "f.yaml").write_text(yaml.safe_dump(features))
    (tmp_path / "r.yaml").write_text(yaml.safe_dump(requirements))
    monkeypatch.setattr(validate, "FEATURES_YAML", tmp_path / "f.yaml")
    monkeypatch.setattr(validate, "REQUIREMENTS_YAML", tmp_path / "r.yaml")
    monkeypatch.setattr(sys, "argv", ["validate_requirements.py"])
    return validate.main()


def test_the_validator_accepts_a_tagged_model(tmp_path, monkeypatch):
    reqs = [_req("REQ-1", "regions-placement"), _req("REQ-2", None, "rejected")]
    assert _validate(tmp_path, monkeypatch, [_feature()], reqs) == 0


@pytest.mark.parametrize(
    ("reqs", "error"),
    [
        ([_req("REQ-1", "regions-placement"), _req("REQ-2", None)], "REQ-2: has no feature"),
        (
            [_req("REQ-1", "regions-placement"), _req("REQ-2", "regions-gone")],
            "REQ-2: feature 'regions-gone' is not a paragraph",
        ),
        ([_req("REQ-1", "regions-placement", "rejected")], "'regions-placement' is served by no"),
    ],
)
def test_the_validator_refuses_an_untagged_or_unserved_model(
    tmp_path, monkeypatch, capsys, reqs, error
):
    assert _validate(tmp_path, monkeypatch, [_feature()], reqs) == 1
    assert error in capsys.readouterr().err


# -- the generated documents -----------------------------------------------------------------------


@pytest.fixture
def model():
    ff = FeaturesFile.model_validate(
        {
            "features": [
                _feature(
                    paragraphs=[
                        {"id": "regions-placement", "text": "Data stays where it is placed. More."},
                        {"id": "regions-reads", "text": "Reads run in the region."},
                    ]
                ),
                _feature("ci", visibility="internal", audience=None, section="Internal"),
            ]
        }
    )
    rf = RequirementsFile.model_validate(
        {
            "requirements": [
                _req("REQ-12", "regions-placement"),
                _req("REQ-3", "regions-placement", "proposed", target="2026-Q4"),
                _req("REQ-5", "regions-reads"),
                _req("REQ-7", "ci-placement", "accepted", target="2026-Q4"),
            ]
        }
    )
    return ff, rf


def test_features_md_anchors_each_paragraph_and_names_no_requirement(model):
    text = _script("gen_features_doc").render(model[0])
    # Docs describe the target state: no feature carries a status (shipped or in development).
    assert "### Regions\n\nData stays where it is placed." in text
    assert "In development" not in text and "**Available**" not in text
    assert "Data stays where it is placed. More.\n{ #regions-placement }" in text
    # A customer page: the requirements behind a paragraph are traced in requirements.yaml only.
    assert "REQ-" not in text and "Requirements" not in text
    assert "Internal" not in text and "ci-" not in text


def _run(script: str, model, tmp_path, monkeypatch) -> Path:
    ff, rf = model
    module = _script(script)
    out = tmp_path / script
    monkeypatch.setattr(module, "OUT_PATH", out)
    monkeypatch.setattr(module.RequirementsFile, "load", classmethod(lambda _cls, _p: rf))
    monkeypatch.setattr(module.FeaturesFile, "load", classmethod(lambda _cls, *_a: ff))
    assert module.main() == 0
    return out


def test_the_feature_matrix_has_a_row_per_published_feature(model, tmp_path, monkeypatch):
    rows = list(csv.DictReader(_run("gen_feature_matrix", model, tmp_path, monkeypatch).open()))
    assert [r["Feature"] for r in rows] == ["Regions"]
    assert "Status" not in rows[0]  # target state: no status column
    assert rows[0]["Paragraphs"] == "regions-placement: REQ-12 REQ-3; regions-reads: REQ-5"


def test_the_traceability_matrix_names_the_paragraph(model, tmp_path, monkeypatch):
    out = _run("gen_traceability_matrix", model, tmp_path, monkeypatch)
    rows = list(csv.DictReader(out.open()))
    assert {(r["ID"], r["Feature"], r["Paragraph"]) for r in rows} == {
        ("REQ-12", "Regions", "regions-placement"),
        ("REQ-5", "Regions", "regions-reads"),
    }


def test_the_roadmap_groups_open_requirements_under_their_feature(model, tmp_path, monkeypatch):
    text = _run("gen_roadmap", model, tmp_path, monkeypatch).read_text()
    assert "### Regions\n\n- **REQ-3** [SHOULD] [proposed] [2026-Q4]" in text
    assert "REQ-7" not in text and "REQ-12" not in text


def test_the_repository_model_validates():
    """The checked-in features.yaml and requirements.yaml satisfy every rule above."""
    ff = FeaturesFile.load(ROOT / "docs/arch/features.yaml")
    rf = RequirementsFile.load(ROOT / "docs/arch/requirements.yaml")
    owner = ff.paragraph_owner()
    live = [r for r in rf.requirements if r.status.value != "rejected"]
    assert all(r.feature in owner for r in live)
    assert set(owner) == {r.feature for r in live}
