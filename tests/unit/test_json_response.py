# Copyright (c) 2026 Kenneth Stott
# Canary: 068f83d4-5978-432c-beed-9aec1d025ebd
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


def test_the_body_matches_what_jsonable_encoder_would_produce():
    # A route that returns this response directly skips FastAPI's jsonable_encoder pass over the
    # rows; the body has to be exactly what that pass would have produced.
    import datetime as dt
    import decimal
    import enum
    import json
    import uuid

    from fastapi.encoders import jsonable_encoder
    from pydantic import BaseModel

    class Colour(enum.Enum):
        RED = "red"

    class Pet(BaseModel):
        name: str
        price: decimal.Decimal

    body = {
        "data": {
            "pets": [
                {
                    "id": 1,
                    "price": decimal.Decimal("12.50"),
                    "whole": decimal.Decimal("3"),
                    "born": dt.datetime(2020, 1, 1, 12, 0, 5, 123456),
                    "seen": dt.datetime(2020, 1, 1, 12, 0, tzinfo=dt.timezone.utc),
                    "day": dt.date(2020, 1, 1),
                    "at": dt.time(9, 30),
                    "wait": dt.timedelta(seconds=90),
                    "uid": uuid.UUID(int=7),
                    "colour": Colour.RED,
                    "tags": {"a"},
                    "model": Pet(name="rex", price=decimal.Decimal("1.5")),
                    "none": None,
                    "nested": {2: [True, 1.5, "x"]},
                }
            ]
        }
    }
    expected = json.loads(json.dumps(jsonable_encoder(body)))
    assert json.loads(OrjsonResponse(body).body) == expected
