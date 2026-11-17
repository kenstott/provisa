# Copyright (c) 2026 Kenneth Stott
# Canary: c6536a83-24ec-460a-8d48-a9bb6afa751e
# Canary: placeholder
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The DDN parser reads only files whose real path is inside the project it was given."""

from __future__ import annotations

from pathlib import Path

import pytest

from provisa.ddn.parser import parse_hml_dir
from provisa.import_shared.refused import IncludeOutsideUpload

_CONNECTOR = """\
kind: DataConnectorLink
version: v1
definition:
  name: {name}
  url:
    singleUrl:
      value: http://localhost:8080/postgres
  schema: {{}}
"""


def _project(tmp_path: Path) -> tuple[Path, Path]:
    """A project dir and, beside it, a dir outside the project holding a DDN file."""
    project = tmp_path / "project"
    (project / "app" / "metadata").mkdir(parents=True)
    (project / "app" / "metadata" / "inside.hml").write_text(
        _CONNECTOR.format(name="inside"), encoding="utf-8"
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.hml").write_text(_CONNECTOR.format(name="secret"), encoding="utf-8")
    return project, outside


def test_linked_hml_to_absolute_outside_path_is_refused(tmp_path: Path) -> None:
    project, outside = _project(tmp_path)
    (project / "app" / "metadata" / "evil.hml").symlink_to(outside / "secret.hml")
    with pytest.raises(IncludeOutsideUpload) as caught:
        parse_hml_dir(project)
    assert caught.value.code == "import.include_outside_upload"
    assert caught.value.params == {"path": "app/metadata/evil.hml"}


def test_linked_hml_through_dotdot_is_refused(tmp_path: Path) -> None:
    project, _ = _project(tmp_path)
    (project / "app" / "metadata" / "climb.hml").symlink_to(Path("../../../outside/secret.hml"))
    with pytest.raises(IncludeOutsideUpload) as caught:
        parse_hml_dir(project)
    assert caught.value.params == {"path": "app/metadata/climb.hml"}


def test_dangling_link_outside_is_refused_not_looked_for(tmp_path: Path) -> None:
    """Confined before it is opened: whether a file outside the project exists is not told."""
    project, outside = _project(tmp_path)
    (project / "app" / "metadata" / "gone.hml").symlink_to(outside / "missing.hml")
    with pytest.raises(IncludeOutsideUpload) as caught:
        parse_hml_dir(project)
    assert caught.value.params == {"path": "app/metadata/gone.hml"}


def test_linked_directory_outside_is_refused(tmp_path: Path) -> None:
    project, outside = _project(tmp_path)
    (project / "app" / "linked").symlink_to(outside, target_is_directory=True)
    (project / "app" / "linked_hml.hml").symlink_to(project / "app" / "linked" / "secret.hml")
    with pytest.raises(IncludeOutsideUpload) as caught:
        parse_hml_dir(project)
    assert caught.value.params == {"path": "app/linked_hml.hml"}


def test_linked_yaml_outside_is_refused(tmp_path: Path) -> None:
    """The .yaml fallback (a project with no .hml files) is confined the same way."""
    project = tmp_path / "project"
    (project / "app").mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.yaml").write_text(_CONNECTOR.format(name="secret"), encoding="utf-8")
    (project / "app" / "evil.yaml").symlink_to(outside / "secret.yaml")
    with pytest.raises(IncludeOutsideUpload) as caught:
        parse_hml_dir(project)
    assert caught.value.params == {"path": "app/evil.yaml"}


def test_link_to_a_file_inside_the_project_loads(tmp_path: Path) -> None:
    project, _ = _project(tmp_path)
    (project / "app" / "metadata" / "alias.hml").symlink_to(
        project / "app" / "metadata" / "inside.hml"
    )
    md = parse_hml_dir(project)
    assert [c.name for c in md.connectors] == ["inside", "inside"]


def test_project_given_through_dotdot_and_link_loads(tmp_path: Path) -> None:
    """The root is compared on its real path: a project named through .. or a link still loads."""
    project, _ = _project(tmp_path)
    (tmp_path / "via").symlink_to(project, target_is_directory=True)
    for root in (project / "app" / "..", tmp_path / "via"):
        md = parse_hml_dir(root)
        assert [c.name for c in md.connectors] == ["inside"]
        assert md.subgraphs == {"app"}
