# Copyright (c) 2026 Kenneth Stott
# Canary: 02f9efb2-cd7e-420c-8af4-4c631c050003
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""Apache Ossie interchange REST endpoints (REQ-1316, REQ-1321)."""

# Requirements: REQ-1316, REQ-1321

from dataclasses import asdict

import yaml
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response

router = APIRouter()


@router.get("/admin/ossie")
async def download_ossie():  # REQ-1321
    """THE canonical live Ossie endpoint: the semantic model derived from live state on
    every read — no caching, no regeneration step."""
    from provisa.api.admin.config_export import build_live_config
    from provisa.core.models import ProvisaConfig
    from provisa.ossie.convert import ossie_yaml

    config = ProvisaConfig.model_validate(await build_live_config())
    return Response(
        content=ossie_yaml(config),
        media_type="text/yaml",
        headers={"Content-Disposition": "attachment; filename=provisa.ossie.yaml"},
    )


@router.post("/admin/ossie/import")
async def import_ossie(request: Request):  # REQ-1316
    """Parse a posted Ossie document (YAML or JSON) into registration PROPOSALS. Nothing
    is registered here — imported definitions never bypass registration review; the UI
    review screen applies proposals via the existing registration mutations."""
    from provisa.ossie.convert import parse_ossie_model

    body = (await request.body()).decode("utf-8")
    try:
        doc = yaml.safe_load(body)  # YAML superset: parses JSON bodies too
    except yaml.YAMLError as e:
        raise HTTPException(status_code=400, detail=f"Invalid YAML/JSON: {e}") from e
    try:
        proposals = parse_ossie_model(doc)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return asdict(proposals)
