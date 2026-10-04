# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-1867: the app's JSON responses are encoded by orjson, through the app's own response class
rather than FastAPI's deprecated ORJSONResponse, so serving one raises no deprecation warning."""

from __future__ import annotations

import warnings

from provisa.api.json_response import OrjsonResponse


def test_the_body_is_encoded_by_orjson_with_non_string_keys():
    response = OrjsonResponse({1: "a", "b": [1, 2]})
    assert response.body == b'{"1":"a","b":[1,2]}'
    assert response.media_type == "application/json"


def test_serving_a_response_raises_no_deprecation_warning():
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        OrjsonResponse({"ok": True})
