# Copyright (c) 2026 Kenneth Stott
# Canary: 0c6f2d8e-91a7-4b3e-a5d2-7e4f19b8c630
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Polly's upload of code it wrote, as a code source's code (REQ-1984, REQ-2114).

The model hands ``upload_code`` the files it wrote, path -> text. They are zipped here and handed
to the browser as the ``admin_request`` the source form's own upload makes: POST
``/admin/orgs/{org}/code-uploads/archive``, sent from the user's session once they confirm the
code and its summary in the chat. The answer is the code's content hash, which the source's
``mappingJson.code`` names as ``{kind: 'upload', upload: <content_hash>}``.
"""

# Requirements: REQ-1984, REQ-2114
from __future__ import annotations

import base64
import io
import zipfile
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote

UPLOAD_CODE = "upload_code"

#: The archive's name, as the upload route records it.
ARCHIVE_NAME = "code.zip"

# A fixed timestamp, so the same files make the same archive.
_EPOCH = (1980, 1, 1, 0, 0, 0)


def _checked_files(files: Any) -> dict[str, str]:
    """``files`` as path -> text, or ValueError naming what is wrong with it."""
    if not isinstance(files, dict) or not files:
        raise ValueError("'files' must map at least one relative path to its text")
    for path, text in files.items():
        parts = PurePosixPath(path).parts
        if not path or path.startswith("/") or "\\" in path or ".." in parts:
            raise ValueError(f"{path!r} is not a relative path inside the code")
        if not isinstance(text, str):
            raise ValueError(f"{path!r}: a file's content must be its text")
    return files


def refusal(call_input: dict, org_id: str | None) -> str | None:
    """Why this upload cannot be put to the person, or None when it can."""
    if org_id is None:
        return "Not proposed: no organization is active, so there is nowhere to keep the code"
    try:
        _checked_files(call_input.get("files"))
    except ValueError as problem:
        return f"Not proposed: {problem}"
    if not str(call_input.get("summary") or "").strip():
        return "Not proposed: 'summary' must say what this code is"
    return None


def archive(files: dict[str, str]) -> bytes:
    """The files as a zip archive, in path order."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zipped:
        for path in sorted(files):
            zipped.writestr(zipfile.ZipInfo(path, _EPOCH), files[path].encode("utf-8"))
    return buffer.getvalue()


def as_admin_request(call_input: dict, org_id: str) -> dict:
    """The admin_request the browser sends: the files, for the person to read before they
    confirm, and their archive as the body."""
    files = _checked_files(call_input["files"])
    return {
        "method": "POST",
        "path": (
            f"/admin/orgs/{quote(org_id, safe='')}/code-uploads/archive?filename={ARCHIVE_NAME}"
        ),
        "summary": call_input["summary"],
        "files": files,
        "archive": base64.b64encode(archive(files)).decode("ascii"),
    }
