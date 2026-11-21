# Copyright (c) 2026 Kenneth Stott
# Canary: eed641a9-e3a7-4837-8fca-b97b1158d29c
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""What an import refuses by name: a file outside the uploaded project, and a permission filter
value or operator that cannot become SQL. Each carries the catalogue code and parameters that word
it for the reader (``serverErrors.import.*``)."""

from __future__ import annotations

from pathlib import Path


class ImportRefused(ValueError):
    """An upload the import will not read; ``code`` and ``params`` word it for the reader."""

    def __init__(self, message: str, code: str, **params: object) -> None:
        super().__init__(message)
        self.code = code
        self.params = params


class IncludeOutsideUpload(ImportRefused):
    """An ``!include`` (or a file of the project) whose real path is outside the upload."""

    def __init__(self, path: str) -> None:
        super().__init__(
            f"{path!r} is outside the uploaded project; a project reads only its own files",
            "import.include_outside_upload",
            path=path,
        )


def confined_to_project(path: Path, root: Path, named: str) -> Path:
    """``path``'s real place, refused by name (``named``, as the project wrote it) unless it is
    inside the project ``root``: through no ``..``, absolute path or symbolic link out. Callers
    confine before they look for the file, so whether a file outside the project exists is not told."""
    real = path.resolve()
    if not real.is_relative_to(root.resolve()):
        raise IncludeOutsideUpload(named)
    return real


class FilterValueRefused(ImportRefused):
    """A permission filter value that is no SQL literal and no session variable."""

    def __init__(self, value: object) -> None:
        super().__init__(
            f"{value!r} cannot be a value in a permission filter: a value is a string, a number, "
            "true, false, null, a list of these, or a session variable",
            "import.filter_value_refused",
            value=repr(value),
        )


class FilterOperatorRefused(ImportRefused):
    """A permission filter operator the import has no SQL for."""

    def __init__(self, operator: str) -> None:
        super().__init__(
            f"{operator!r} is not an operator a permission filter can be converted with",
            "import.filter_operator_refused",
            operator=operator,
        )
