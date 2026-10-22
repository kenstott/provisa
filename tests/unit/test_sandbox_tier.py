# Copyright (c) 2026 Kenneth Stott
# Canary: 6caffbdc-23f5-437a-8079-542709ce974b
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""REQ-2006: a demo sandbox's DuckDB file is opened by one process only -- the sandbox tier, which
serves the sandbox-tier orgs and nothing else -- and the UI server sends each request to the tier
that serves its org."""

# Requirements: REQ-2006

from __future__ import annotations

import pytest

from provisa.api.errors import ApiError
from provisa.api.sandbox_tier import (
    is_sandbox_tier_org,
    is_template_org,
    new_sandbox_org_id,
    require_this_tier,
    template_org_id,
)
from provisa.core.org_ids import is_org_id


def test_a_sandbox_and_a_template_are_org_ids_of_the_sandbox_tier_and_never_each_other():
    sandbox = new_sandbox_org_id()
    template = template_org_id("weblogs")
    assert is_org_id(sandbox) and is_org_id(template)
    assert is_sandbox_tier_org(sandbox) and is_sandbox_tier_org(template)
    assert is_template_org(template) and not is_template_org(sandbox)
    assert not is_sandbox_tier_org("acme") and not is_sandbox_tier_org("sandbox")
    assert len({new_sandbox_org_id() for _ in range(100)}) == 100


@pytest.mark.parametrize(
    ("tier", "org", "served"),
    [
        ("true", "sbx0123456789ab", True),
        ("true", "sbxtplweblogs", True),
        ("true", "acme", False),
        ("false", "acme", True),
        ("false", "sbx0123456789ab", False),
    ],
)
def test_each_tier_builds_its_own_orgs_only(monkeypatch, tier, org, served):
    monkeypatch.setenv("PROVISA_SANDBOX_TIER", tier)
    if served:
        require_this_tier(org)
        return
    with pytest.raises(ApiError) as refused:
        require_this_tier(org)
    assert refused.value.status_code == 421
    assert refused.value.code == "sandbox.wrong_tier"


def test_an_unset_tier_is_the_deployments_and_a_bad_value_is_refused(monkeypatch):
    monkeypatch.delenv("PROVISA_SANDBOX_TIER", raising=False)
    require_this_tier("acme")
    monkeypatch.setenv("PROVISA_SANDBOX_TIER", "yes")
    with pytest.raises(ValueError, match="PROVISA_SANDBOX_TIER"):
        require_this_tier("acme")


def test_the_ui_server_sends_a_sandbox_and_a_demo_opening_to_the_sandbox_tier(monkeypatch):
    import provisa.ui_server as ui_server

    monkeypatch.setattr(ui_server, "API_BASE_URL", "http://provisa:8000")
    monkeypatch.setattr(ui_server, "SANDBOX_API_URL", "http://provisa-sandbox:8000")
    main, tier = "http://provisa:8000", "http://provisa-sandbox:8000"
    route = ui_server.api_base_for
    assert route("data/sql", {"host": "sbx0123456789ab.provisa.dev"}) == tier
    assert (
        route("data/sql", {"host": "cloud.provisa.dev", "x-org-provisa": "sbxtplweblogs"}) == tier
    )
    assert route("public/demos/weblogs", {"host": "cloud.provisa.dev"}) == tier
    assert route("sandboxes/weblogs", {"host": "cloud.provisa.dev"}) == tier
    assert route("data/sql", {"host": "acme.provisa.dev"}) == main
    assert route("data/sql", {"host": "cloud.provisa.dev", "x-org-provisa": "acme"}) == main
    assert route("auth/me", {"host": "cloud.provisa.dev"}) == main


def test_a_regions_demo_and_its_sandboxes_reach_the_regions_tier(monkeypatch):
    """REQ-2068: a demo whose model selects regions is opened by, and its sandboxes (an ``sbxr``
    id) served by, the regions sandbox tier; every other demo and sandbox as before."""
    import provisa.ui_server as ui_server

    monkeypatch.setattr(ui_server, "API_BASE_URL", "http://provisa:8000")
    monkeypatch.setattr(ui_server, "SANDBOX_API_URL", "http://provisa-sandbox:8000")
    monkeypatch.setattr(ui_server, "SANDBOX_REGIONS_API_URL", "http://provisa-sandbox-regions:8100")
    monkeypatch.setattr(ui_server, "SANDBOX_REGIONS_DEMOS", frozenset({"retail"}))
    tier, regions = "http://provisa-sandbox:8000", "http://provisa-sandbox-regions:8100"
    route = ui_server.api_base_for
    assert route("public/demos/retail", {"host": "cloud.provisa.dev"}) == regions
    assert route("sandboxes/retail", {"host": "cloud.provisa.dev"}) == regions
    assert route("data/sql", {"host": "sbxr0123456789ab.provisa.dev"}) == regions
    assert route("public/demos/weblogs", {"host": "cloud.provisa.dev"}) == tier
    assert route("public/demos/retailer", {"host": "cloud.provisa.dev"}) == tier
    assert route("data/sql", {"host": "sbx0123456789ab.provisa.dev"}) == tier


def test_a_regions_tier_mints_its_own_sandbox_ids(monkeypatch):
    from provisa.api import demo_region_seed
    from provisa.core.org_ids import is_regions_sandbox, is_sandbox_tier_org, is_template_org

    monkeypatch.setattr(demo_region_seed, "regional", lambda: True)
    org = new_sandbox_org_id()
    assert is_regions_sandbox(org) and is_sandbox_tier_org(org) and not is_template_org(org)
    monkeypatch.setattr(demo_region_seed, "regional", lambda: False)
    assert not is_regions_sandbox(new_sandbox_org_id())


def test_with_no_sandbox_tier_everything_reaches_the_deployments_api(monkeypatch):
    import provisa.ui_server as ui_server

    monkeypatch.setattr(ui_server, "SANDBOX_API_URL", None)
    assert (
        ui_server.api_base_for("data/sql", {"host": "sbx0123456789ab.provisa.dev"})
        == ui_server.API_BASE_URL
    )


def test_a_demo_template_waits_only_for_what_it_serves_from_replicas(monkeypatch, tmp_path):
    """REQ-2049: a demo's own sources are read live -- the weblogs demo lands nothing -- and its
    views have no replica; only a table replicated always (a SaaS source's, landed once) is waited
    for. Counting its views kept the weblogs template waiting forever (run 38588163850)."""
    from pathlib import Path

    import yaml

    from provisa.api.demo_templates import replicated_tables
    from provisa.core.config_loader import parse_config_dict, read_config_with_includes

    monkeypatch.setenv("PROVISA_NAMED_DEMO_DIR", str(tmp_path))
    weblogs = Path(__file__).resolve().parents[2] / "demo" / "named" / "weblogs" / "config.yaml"
    from provisa.core.demo_bundles import store_namespaces

    # As a launcher running the demo's own stores sets them (start-ui-install.sh --demo).
    for name, value in store_namespaces(weblogs.parent).items():
        monkeypatch.setenv(name, value)
    raw = read_config_with_includes(weblogs)
    assert len(parse_config_dict(raw).tables) == 12  # six tables and six views
    assert replicated_tables(parse_config_dict(raw)) == []
    raw = yaml.safe_load(yaml.safe_dump(raw))
    for source in raw["sources"]:
        if source["id"] == "shop-db":
            source["replicate"] = 0
            source["change_signal"] = "signal"
    assert sorted(t.table_name for t in replicated_tables(parse_config_dict(raw))) == [
        "customers",
        "order_items",
        "orders",
        "products",
    ]


def test_a_bundles_stores_are_where_the_node_says(tmp_path):
    """REQ-2049: the node writes stores.env; each ${env:NAME} it sets takes its value, defaults
    and all, and any other reference is left to the loader."""
    from provisa.api.demo_templates import _with_stores
    from provisa.core.demo_bundles import BundleError, stores_env

    (tmp_path / "docker-compose.run.yml").write_text("services: {}\n")
    with pytest.raises(BundleError, match="wrote no stores.env"):
        stores_env(tmp_path)
    (tmp_path / "stores.env").write_text(
        "WEBLOGS_PG_HOST=provisa-demo-weblogs-1-postgres-1\nWEBLOGS_PG_PORT=5432\n"
    )
    values = stores_env(tmp_path)
    raw = {
        "sources": [
            {
                "host": "${env:WEBLOGS_PG_HOST:-localhost}",
                "port": "${env:WEBLOGS_PG_PORT:-28432}",
                "password": "${env:OTHER}",
            }
        ]
    }
    assert _with_stores(raw, values) == {
        "sources": [
            {
                "host": "provisa-demo-weblogs-1-postgres-1",
                "port": "5432",
                "password": "${env:OTHER}",
            }
        ]
    }


def test_the_sandbox_tier_never_provisions_an_engine(monkeypatch):
    """REQ-2006: the tier shares the control plane's environment, engine-cluster settings included,
    but every org on it runs an embedded DuckDB engine. cloud.provisa.dev's tier woke a GKE shard
    and a demo template's build failed there (restore_shared_terminal with no org bound)."""
    from provisa.federation import k8s_provisioner

    monkeypatch.setattr(k8s_provisioner, "provisioner_settings", lambda: object())
    monkeypatch.setenv("PROVISA_SANDBOX_TIER", "false")
    assert k8s_provisioner.provisioning_available() is True
    monkeypatch.setenv("PROVISA_SANDBOX_TIER", "true")
    assert k8s_provisioner.provisioning_available() is False


async def test_a_demos_own_embedding_models_reach_its_sandboxes(monkeypatch):
    """The sonnets demo declares a local embedding model; its template org's settings carry it,
    and a sandbox is cloned with them, so lines_like is not refused by the model allowlist."""
    from pathlib import Path

    from provisa.api.demo_templates import write_declared_vector_models
    from provisa.core import org_settings
    from provisa.core.config_loader import parse_config_dict, read_config_with_includes
    from provisa.core.env_classes import carries_setting

    written = []

    async def write(model_db, updates, *, updated_by):
        written.append((model_db, updates, updated_by))
        return list(updates)

    monkeypatch.setattr(org_settings, "write_org_overrides", write)
    root = Path(__file__).resolve().parents[2]
    from provisa.core.demo_bundles import store_namespaces

    for name, value in store_namespaces(root / "demo/named/sonnets").items():
        monkeypatch.setenv(name, value)
    seed = parse_config_dict(read_config_with_includes(root / "demo/named/sonnets/config.yaml"))
    db = object()
    await write_declared_vector_models(db, seed)
    assert written == [
        (
            db,
            {
                "vector_models": [
                    {
                        "id": "sentence-transformers/all-MiniLM-L6-v2",
                        "provider": "huggingface",
                        "dimensions": 384,
                        "api_key_env": None,
                        "base_url": None,
                        "enabled": True,
                        "version": None,
                    }
                ]
            },
            "platform",
        )
    ]
    assert carries_setting("vector_models")  # the template's settings travel to each sandbox


async def test_a_demo_that_declares_no_embedding_model_leaves_its_template_none(monkeypatch):
    from types import SimpleNamespace

    from provisa.api.demo_templates import write_declared_vector_models
    from provisa.core import org_settings

    written = []

    async def write(model_db, updates, *, updated_by):
        written.append(updates)
        return list(updates)

    monkeypatch.setattr(org_settings, "write_org_overrides", write)
    await write_declared_vector_models(object(), SimpleNamespace(vector_models=[]))
    assert written == [{"vector_models": None}]  # an earlier build's models are cleared


def test_the_demos_connector_bundles_are_fetched_outside_any_request(monkeypatch, tmp_path):
    """The sandbox tier fetches the bundle of every connector a published demo reads through (the
    files demo's file connector) into the runtime-deps cache at startup, so no visitor's request
    waits for hundreds of megabytes."""
    from provisa.api import demo_templates
    from provisa.core import request_deadline
    from provisa.runtime_deps import pgwire_bundles as rd

    monkeypatch.delenv("PROVISA_DEMO_BUNDLES", raising=False)
    monkeypatch.delenv("PROVISA_NAMED_DEMO_DIR", raising=False)
    monkeypatch.setenv("PROVISA_RUNTIME_DEPS_CACHE", str(tmp_path))
    fetched: list[tuple[str, float | None]] = []

    def _download(spec, dest):
        fetched.append((spec.artifact_name, request_deadline.remaining()))
        (dest / "bin").mkdir(parents=True)
        (dest / "bin" / spec.artifact_name).write_text("")

    monkeypatch.setattr(rd, "download_release_asset", _download)
    # files reads through the file connector, vulnerabilities and agriculture through govdata,
    # hasuramigration's CRM source through salesforce (its bundle build lands the copy), and
    # cloudpolicy's inventory and incidents through cloudops and servicenow.
    bundles = [
        "pgwire-cloudops",
        "pgwire-file",
        "pgwire-govdata",
        "pgwire-salesforce",
        "pgwire-servicenow",
    ]
    assert [s.artifact_name for s in demo_templates.connector_bundles()] == bundles
    demo_templates.prefetch_connector_bundles()
    assert fetched == [(name, None) for name in bundles]
    assert rd.BundleResolver().is_cached(rd.bundle_spec_for("files"))


def test_the_demos_embedding_models_are_loaded_outside_any_request(monkeypatch):
    """The sandbox tier loads the local embedding model every published demo declares (the
    sonnets demo's) at startup, so a visitor's first search does not spend its request timeout
    importing sentence-transformers and loading the model."""
    from provisa.api import demo_templates
    from provisa.core import request_deadline
    from provisa.vector.providers import HuggingFaceLocalProvider

    monkeypatch.delenv("PROVISA_DEMO_BUNDLES", raising=False)
    monkeypatch.delenv("PROVISA_NAMED_DEMO_DIR", raising=False)
    loaded: list[tuple[str, float | None]] = []

    def _loaded(self, model):
        loaded.append((model.id, request_deadline.remaining()))

    monkeypatch.setattr(HuggingFaceLocalProvider, "_loaded", _loaded)
    assert [m.id for m in demo_templates.embedding_models()] == [
        "sentence-transformers/all-MiniLM-L6-v2"
    ]
    demo_templates.load_embedding_models()
    assert loaded == [("sentence-transformers/all-MiniLM-L6-v2", None)]


def test_a_bundled_tier_loads_the_models_before_any_demos_stores_are_up(monkeypatch):
    """A bundled tier starts no demo's stores at boot, and a model left to the first search took
    it past its 60s request timeout: the models are read from each demo's configuration whether
    or not its stores are up."""
    from pathlib import Path

    from provisa.api import demo_templates
    from provisa.core import demo_bundles

    root = Path(__file__).resolve().parents[2] / "demo" / "named"
    pulled = {
        name: demo_templates.PublishedDemo(name, name, root / name)
        for name in ("sonnets", "cloudpolicy", "churnprod")
    }
    monkeypatch.setenv("PROVISA_DEMO_BUNDLES", ",".join(pulled))
    monkeypatch.setattr(demo_templates, "published_demos", lambda: pulled)
    monkeypatch.setattr(demo_bundles, "ready_version", lambda name: None)
    assert not any(demo.stores_ready for demo in pulled.values())
    assert [m.id for m in demo_templates.embedding_models()] == [
        "sentence-transformers/all-MiniLM-L6-v2"
    ]


def test_the_sandbox_tier_finds_the_deployments_demo_data():
    """The sandbox tier builds the deployment's org at startup too, so the demo config's SQLite
    sources resolve ${env:PROVISA_DEMO_DIR} there: both the compose a node is set up with and the
    overlay deploy-cloud.sh installs give it the directory the image bakes them into."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    setup = (root / "packaging" / "linux" / "first-launch.sh").read_text()
    sandbox = setup[setup.index("  provisa-sandbox:\n") :]
    sandbox = sandbox[: sandbox.index("\n  provisa-ui:\n")]
    assert 'PROVISA_DEMO_DIR: "/app/config/demo/files"' in sandbox
    overlay = (root / "scripts" / "deploy-cloud.sh").read_text()
    overlay = overlay[overlay.index("  provisa-sandbox:\n") :]
    assert 'PROVISA_DEMO_DIR: "/app/config/demo/files"' in overlay[: overlay.index("\nYAML\n")]
    assert (root / "demo" / "files" / "pet_store.sqlite").is_file()  # what the Dockerfile bakes


def test_every_cloud_deploy_refreshes_the_demo_stores_sync():
    """The node's demo-stores sync and its units are installed with the node; deploy-cloud.sh
    copies them on every deploy that pushes the app, or a fix to the sync never reaches a node."""
    import re
    from pathlib import Path

    deploy = (Path(__file__).resolve().parents[2] / "scripts" / "deploy-cloud.sh").read_text()
    body = deploy[deploy.index("push_demo_stores() {") :]
    assert '"$REPO/scripts/demo_stores_sync.sh"' in body
    assert "packaging/linux/demo-stores/provisa-demo-stores.*" in body
    arms = re.findall(r"^  (api|cfg|all|patch)\)\n(?:\s*#.*\n)*\s*(.*);;$", deploy, re.M)
    assert {arm for arm, _ in arms} == {"api", "cfg", "all", "patch"}
    for arm, steps in arms:
        assert "push_demo_stores" in steps.split("; "), arm


def test_a_cloud_deploy_reaches_the_sandbox_vm_too():
    """With the sandbox tier on a VM of its own (terraform/gcp-saas/sandbox.tf), every deploy
    that pushes the app pushes the same tree there, starting a stopped VM for it."""
    import re
    from pathlib import Path

    deploy = (Path(__file__).resolve().parents[2] / "scripts" / "deploy-cloud.sh").read_text()
    body = deploy[deploy.index("deploy_sandbox_vm() {") :]
    body = body[: body.index("\n}\n")]
    assert 'gcloud compute instances start "$SANDBOX_NODE"' in body
    assert 'NODE="$SANDBOX_NODE" API_CONTAINER=compose-provisa-sandbox-1' in body
    for step in ("push_demo_stores", "push_overlay", "push_api", "push_cfg", "restart"):
        assert step in body, step
    # The coordinator of a deployment with a sandbox VM gets no demo stores of its own.
    stores = deploy[deploy.index("push_demo_stores() {") :]
    stores = stores[: stores.index("\n}\n")]
    assert "if sandbox_on_own_vm; then" in stores and "return" in stores
    own_vm = deploy[deploy.index("sandbox_on_own_vm() {") :]
    assert '[ "$NODE" != "$SANDBOX_NODE" ]' in own_vm[: own_vm.index("\n}\n")]
    # Nor is the tier's container restarted, or given plugins, on it: it is on the VM.
    node = deploy[deploy.index("node_containers() {") :]
    node = node[: node.index("\n}\n")]
    assert "if sandbox_on_own_vm; then" in node and "grep -vx compose-provisa-sandbox-1" in node
    restart = deploy[deploy.index("restart() {") :]
    assert 'containers="$(node_containers)"' in restart[: restart.index("\n}\n")]
    plugins = deploy[deploy.index("push_plugins() {") :]
    assert 'CONTAINERS="$(node_containers)" push_commercial' in plugins[: plugins.index("\n}\n")]
    # The overlay publishes the tier on the VM's VPC address, read from the instance.
    assert "value(networkInterfaces[0].networkIP)" in body and "SANDBOX_PUBLISH=" in body
    assert '- "${SANDBOX_PUBLISH}:8000"' in deploy
    arms = re.findall(r"^  (api|cfg|all|patch)\)\n(?:\s*#.*\n)*\s*(.*);;$", deploy, re.M)
    for arm, steps in arms:
        assert steps.strip().split("; ")[-1] == "deploy_sandbox_vm", arm
