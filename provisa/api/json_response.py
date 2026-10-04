# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The app's JSON response class (REQ-1867): orjson encodes the body.

FastAPI deprecated its own ``ORJSONResponse`` in favour of response models; the routes here
return plain dicts, so the encoder is kept as the app's own class, with the same options.
"""

# Requirements: REQ-1867

from __future__ import annotations

from typing import Any

import orjson
from fastapi.responses import JSONResponse


class OrjsonResponse(JSONResponse):
    """A JSON response whose body orjson encodes."""

    def render(self, content: Any) -> bytes:  # noqa: ANN401 — any JSON-safe body
        return orjson.dumps(content, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY)
