# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# Seed data and endpoint behaviour reproduce the swagger-api/swagger-petstore
# reference server (Apache-2.0). The vendored openapi.json is taken verbatim
# from that project with the `servers` url rewritten to `/api/v3`.

"""Demo Petstore OpenAPI server — host-runnable replacement for the
`swaggerapi/petstore3` Docker image.

Serves the identical seed data (10 pets, 11 users, 3 orders) and the same
`/api/v3/openapi.json` spec that Provisa introspects, so the demo behaves
the same whether the mock runs in Docker or on the host.

Run:
    pip install -r requirements.txt
    uvicorn server:app --host 0.0.0.0 --port 8080
Then point Provisa at it with PETSTORE_BASE_URL=http://localhost:8080/api/v3
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import (
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
    RedirectResponse,
    Response,
)
from starlette.routing import Route

# ---------------------------------------------------------------------------
# Vendored spec
# ---------------------------------------------------------------------------

_SPEC = json.loads((Path(__file__).parent / "openapi.json").read_text())

# ---------------------------------------------------------------------------
# Seed data (mirrors PetData.java / UserData.java / OrderData.java)
# ---------------------------------------------------------------------------

_CATEGORIES = {
    1: {"id": 1, "name": "Dogs"},
    2: {"id": 2, "name": "Cats"},
    3: {"id": 3, "name": "Rabbits"},
    4: {"id": 4, "name": "Lions"},
}


def _pet(pet_id: int, cat: int, name: str, tags: list[str], status: str) -> dict:
    return {
        "id": pet_id,
        "category": _CATEGORIES[cat],
        "name": name,
        "photoUrls": ["url1", "url2"],
        # Java assigns tag ids 1, 2 per pet (counter resets per pet).
        "tags": [{"id": i + 1, "name": t} for i, t in enumerate(tags)],
        "status": status,
    }


_PETS: dict[int, dict] = {
    p["id"]: p
    for p in [
        _pet(1, 2, "Cat 1", ["tag1", "tag2"], "available"),
        _pet(2, 2, "Cat 2", ["tag2", "tag3"], "available"),
        _pet(3, 2, "Cat 3", ["tag3", "tag4"], "pending"),
        _pet(4, 1, "Dog 1", ["tag1", "tag2"], "available"),
        _pet(5, 1, "Dog 2", ["tag2", "tag3"], "sold"),
        _pet(6, 1, "Dog 3", ["tag3", "tag4"], "pending"),
        _pet(7, 4, "Lion 1", ["tag1", "tag2"], "available"),
        _pet(8, 4, "Lion 2", ["tag2", "tag3"], "available"),
        _pet(9, 4, "Lion 3", ["tag3", "tag4"], "available"),
        _pet(10, 3, "Rabbit 1", ["tag3", "tag4"], "available"),
    ]
}


def _user(uid: int, username: str, status: int) -> dict:
    return {
        "id": uid,
        "username": username,
        "firstName": f"first name {uid}",
        "lastName": f"last name {uid}",
        "email": f"email{uid}@test.com",
        "password": "XXXXXXXXXXX",
        "phone": "123-456-7890",
        "userStatus": status,
    }


_USERS: dict[str, dict] = {
    u["username"]: u
    for u in [
        _user(1, "user1", 1),
        _user(2, "user2", 2),
        _user(3, "user3", 3),
        _user(4, "user4", 1),
        _user(5, "user5", 2),
        _user(6, "user6", 3),
        _user(7, "user7", 1),
        _user(8, "user8", 2),
        _user(9, "user9", 3),
        _user(10, "user10", 1),
        _user(11, "user?10", 1),
    ]
}
# createUser(11, "user?10", "first name ?10", "last name ?10", "email101@test.com", ...)
_USERS["user?10"].update(
    {
        "firstName": "first name ?10",
        "lastName": "last name ?10",
        "email": "email101@test.com",
    }
)

_SHIP_DATE = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

_ORDERS: dict[int, dict] = {
    o["id"]: o
    for o in [
        {
            "id": 1,
            "petId": 1,
            "quantity": 100,
            "shipDate": _SHIP_DATE,
            "status": "placed",
            "complete": True,
        },
        {
            "id": 2,
            "petId": 1,
            "quantity": 50,
            "shipDate": _SHIP_DATE,
            "status": "approved",
            "complete": True,
        },
        {
            "id": 3,
            "petId": 1,
            "quantity": 50,
            "shipDate": _SHIP_DATE,
            "status": "delivered",
            "complete": True,
        },
    ]
}


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

_SWAGGER_UI = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Swagger Petstore - OpenAPI 3.0</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist/swagger-ui.css"/>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({url: "/api/v3/openapi.json", dom_id: "#swagger-ui"});
  </script>
</body>
</html>"""


async def index(_: Request) -> Response:
    return HTMLResponse(_SWAGGER_UI)


class _BadBody(Exception):
    """Raised when a required JSON body is missing or unparseable."""


async def _json_body(request: Request):
    # Mirrors the reference server's 400 "No X provided. Try again?" responses.
    body = await request.body()
    if not body:
        raise _BadBody
    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise _BadBody from exc


async def openapi(_: Request) -> Response:
    return JSONResponse(_SPEC)


async def find_by_status(request: Request) -> Response:
    # `status` is required: true in the spec — the reference server 400s when
    # omitted rather than defaulting. Provisa always passes the enum values.
    status = request.query_params.get("status")
    if status is None:
        return PlainTextResponse("No status provided. Try again?", status_code=400)
    wanted = status.split(",")
    return JSONResponse([p for p in _PETS.values() if p["status"] in wanted])


async def find_by_tags(request: Request) -> Response:
    wanted = request.query_params.getlist("tags")
    result = [p for p in _PETS.values() if any(t["name"] in wanted for t in p["tags"])]
    return JSONResponse(result)


async def get_pet(request: Request) -> Response:
    pet = _PETS.get(int(request.path_params["petId"]))
    if pet is None:
        return JSONResponse({"message": "Pet not found"}, status_code=404)
    return JSONResponse(pet)


async def add_pet(request: Request) -> Response:
    # PUT /pet (updatePet) and POST /pet (addPet) both upsert by id.
    try:
        pet = await _json_body(request)
    except _BadBody:
        return PlainTextResponse("No Pet provided. Try again?", status_code=400)
    pet_id = int(pet["id"]) if pet.get("id") else max(_PETS, default=0) + 1
    pet["id"] = pet_id
    _PETS[pet_id] = pet
    return JSONResponse(pet)


async def update_pet_with_form(request: Request) -> Response:
    # POST /pet/{petId} — name/status supplied as query params.
    pet = _PETS.get(int(request.path_params["petId"]))
    if pet is None:
        return JSONResponse({"message": "Pet not found"}, status_code=404)
    name = request.query_params.get("name")
    if name is None:
        return PlainTextResponse("No Name provided. Try again?", status_code=400)
    pet["name"] = name
    if request.query_params.get("status") is not None:
        pet["status"] = request.query_params["status"]
    return JSONResponse(pet)


async def delete_pet(request: Request) -> Response:
    _PETS.pop(int(request.path_params["petId"]), None)
    return PlainTextResponse("Pet deleted")


async def upload_file(request: Request) -> Response:
    pet = _PETS.get(int(request.path_params["petId"]))
    if pet is None:
        return JSONResponse({"message": "Pet not found"}, status_code=404)
    body = await request.body()
    pet.setdefault("photoUrls", []).append(f"/uploaded/{len(body)}-bytes")
    return JSONResponse(pet)


async def get_inventory(_: Request) -> Response:
    counts: dict[str, int] = {}
    for o in _ORDERS.values():
        counts[o["status"]] = counts.get(o["status"], 0) + o["quantity"]
    return JSONResponse(counts)


async def place_order(request: Request) -> Response:
    try:
        order = await _json_body(request)
    except _BadBody:
        return PlainTextResponse("No Order provided. Try again?", status_code=400)
    if order.get("id"):
        _ORDERS[int(order["id"])] = order
    return JSONResponse(order)


async def get_order(request: Request) -> Response:
    order = _ORDERS.get(int(request.path_params["orderId"]))
    if order is None:
        return JSONResponse({"message": "Order not found"}, status_code=404)
    return JSONResponse(order)


async def delete_order(request: Request) -> Response:
    _ORDERS.pop(int(request.path_params["orderId"]), None)
    return JSONResponse(None)


async def create_user(request: Request) -> Response:
    try:
        user = await _json_body(request)
    except _BadBody:
        return PlainTextResponse("No User provided. Try again?", status_code=400)
    if user.get("username"):
        _USERS[user["username"]] = user
    return JSONResponse(user)


async def create_users_with_list(request: Request) -> Response:
    try:
        users = await _json_body(request)
    except _BadBody:
        return PlainTextResponse("No User provided. Try again?", status_code=400)
    for user in users:
        if user.get("username"):
            _USERS[user["username"]] = user
    return JSONResponse(users)


async def login_user(_: Request) -> Response:
    session = int.from_bytes(os.urandom(6), "big")
    resp = PlainTextResponse(f"Logged in user session: {session}")
    resp.headers["X-Rate-Limit"] = "5000"
    resp.headers["X-Expires-After"] = "1 hour from now"
    return resp


async def logout_user(_: Request) -> Response:
    return PlainTextResponse("User logged out")


async def get_user(request: Request) -> Response:
    user = _USERS.get(request.path_params["username"])
    if user is None:
        return JSONResponse({"message": "User not found"}, status_code=404)
    return JSONResponse(user)


async def update_user(request: Request) -> Response:
    username = request.path_params["username"]
    if username not in _USERS:
        return JSONResponse({"message": "User not found"}, status_code=404)
    try:
        user = await _json_body(request)
    except _BadBody:
        return PlainTextResponse("No User provided. Try again?", status_code=400)
    _USERS.pop(username, None)
    _USERS[user.get("username", username)] = user
    return JSONResponse(user)


async def delete_user(request: Request) -> Response:
    _USERS.pop(request.path_params["username"], None)
    return JSONResponse(None)


async def redirect_root(_: Request) -> Response:
    return RedirectResponse("/")


app = Starlette(
    routes=[
        Route("/", index),
        Route("/api/v3", redirect_root),
        Route("/api/v3/openapi.json", openapi),
        # pet
        Route("/api/v3/pet", add_pet, methods=["POST", "PUT"]),
        Route("/api/v3/pet/findByStatus", find_by_status),
        Route("/api/v3/pet/findByTags", find_by_tags),
        Route("/api/v3/pet/{petId:int}", get_pet, methods=["GET"]),
        Route("/api/v3/pet/{petId:int}", update_pet_with_form, methods=["POST"]),
        Route("/api/v3/pet/{petId:int}", delete_pet, methods=["DELETE"]),
        Route("/api/v3/pet/{petId:int}/uploadImage", upload_file, methods=["POST"]),
        # store
        Route("/api/v3/store/inventory", get_inventory),
        Route("/api/v3/store/order", place_order, methods=["POST"]),
        Route("/api/v3/store/order/{orderId:int}", get_order, methods=["GET"]),
        Route("/api/v3/store/order/{orderId:int}", delete_order, methods=["DELETE"]),
        # user
        Route("/api/v3/user", create_user, methods=["POST"]),
        Route("/api/v3/user/createWithList", create_users_with_list, methods=["POST"]),
        Route("/api/v3/user/login", login_user, methods=["GET"]),
        Route("/api/v3/user/logout", logout_user, methods=["GET"]),
        Route("/api/v3/user/{username:str}", get_user, methods=["GET"]),
        Route("/api/v3/user/{username:str}", update_user, methods=["PUT"]),
        Route("/api/v3/user/{username:str}", delete_user, methods=["DELETE"]),
    ]
)
