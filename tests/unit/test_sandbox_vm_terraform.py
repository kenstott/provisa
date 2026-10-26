# Copyright (c) 2026 Kenneth Stott
# Canary: 4f9fa68f-e7ce-41d0-b926-55aae0a632c0
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.
"""The sandbox VM (terraform/gcp-saas/sandbox.tf, REQ-2006): the sandbox tier and the demos'
stores on a VM of their own, against the coordinator's control plane, waking and stopping by
the Compute API -- and nothing changed for a deployment that does not ask for it."""

# Requirements: REQ-2006

from __future__ import annotations

from pathlib import Path

_TF = Path(__file__).resolve().parents[2] / "terraform" / "gcp-saas"


def test_the_sandbox_vm_runs_the_tier_alone_against_the_coordinator():
    sandbox = (_TF / "sandbox.tf").read_text()
    for line in (
        "export COMPOSE_PROFILES=sandbox",
        'export CONFIG_DB_HOST="${local.coordinator_ip}"',
        'export PROVISA_REDIS_HOST="${local.coordinator_ip}"',
        'export PROVISA_MINIO_HOST="${local.coordinator_ip}"',
        'export PROVISA_SANDBOX_PUBLISH="${var.sandbox_internal_ip}:8000"',
        'export PROVISA_SANDBOX_INSTANCE="${local.sandbox_instance_path}"',
        'export PROVISA_SANDBOX_IDLE_STOP_SECONDS="${var.sandbox_idle_stop_minutes * 60}"',
        "--role control-plane",
        # Started now, not at the next boot: the VM's first boot is already past timers.target.
        "systemctl enable --now provisa-demo-stores.timer provisa-demo-stores.path",
    ):
        assert line in sandbox, line
    # The VM's own control plane is not set up there: no database, no object-store disk.
    startup = sandbox[sandbox.index('resource "google_compute_instance" "sandbox"') :]
    assert "control_db_setup" not in startup and "object_store_mount" not in startup
    assert "desired_status = null" in startup


def test_the_coordinator_hands_the_tier_over_only_when_asked():
    sandbox = (_TF / "sandbox.tf").read_text()
    main = (_TF / "main.tf").read_text()
    assert "${local.coordinator_sandbox_exports}" in main
    assert (
        'coordinator_sandbox_exports    = var.sandbox_vm ? local.coordinator_sandbox_vm_exports : ""'
        in sandbox
    )
    for line in (
        "export COMPOSE_PROFILES=control",
        'export PROVISA_SANDBOX_API_URL="https://${var.sandbox_internal_ip}:8000"',
        # REQ-2068: the regions sandbox tier's nodes, on the same VM.
        'export PROVISA_SANDBOX_REGIONS_NODES="us=https://${var.sandbox_internal_ip}:8100"',
    ):
        assert line in sandbox, line
    variables = (_TF / "variables.tf").read_text()
    block = variables[variables.index('variable "sandbox_vm"') :]
    assert "default     = false" in block[: block.index("}\n") + 2]


def test_the_vm_may_be_started_and_stopped_by_the_node_account_and_on_it_alone():
    sandbox = (_TF / "sandbox.tf").read_text()
    role = sandbox[sandbox.index('resource "google_project_iam_custom_role" "sandbox_power"') :]
    for permission in ("start", "stop", "resume", "get"):
        assert f'"compute.instances.{permission}"' in role
    assert 'resource "google_compute_instance_iam_member" "sandbox_power"' in sandbox
    assert "instance_name = google_compute_instance.sandbox[0].name" in sandbox


def test_the_vm_runs_the_regions_demos_stores():
    """REQ-2068: the regions tier's demos are the VM's to run beside the sandbox tier's; their
    stores are synced once each has a bundle."""
    sandbox = (_TF / "sandbox.tf").read_text()
    lists = _TF.parents[1] / "packaging" / "linux" / "demo-stores"
    listed = (lists / "regions-tier-bundles").read_text().strip().split(",")
    assert "retail" in listed
    # The sync takes both tiers' demos, through the one composition deploy-cloud.sh shares.
    assert (
        'for d in split(",", "${local.sandbox_tier_bundles},${local.regions_tier_bundles}") : d'
        in sandbox
    )
    # Each node published on the VM's VPC address, where the coordinator reaches it.
    assert 'export PROVISA_SANDBOX_REGIONS_PUBLISH="${var.sandbox_internal_ip}:8100"' in sandbox
    assert (
        'export PROVISA_SANDBOX_REGIONS_WAREHOUSE_PUBLISH="${var.sandbox_internal_ip}:8101"'
        in sandbox
    )


# --- the sync's settings, at boot and at a patch ---------------------------------------------------

_REPO = _TF.parents[1]
_DEPLOY = _REPO / "scripts" / "deploy-cloud.sh"


def _function(name: str) -> str:
    text = _DEPLOY.read_text()
    start = text.index(f"{name}() {{")
    return text[start : text.index("\n}\n", start) + 3]


def _rendered(repo: Path) -> str:
    """What deploy-cloud.sh writes as the VM's demo-stores.env, for the checkout at ``repo``."""
    import subprocess

    script = f"{_function('demo_stores_env')}\ndemo_stores_env\n"
    return subprocess.run(
        ["bash", "-c", script],
        env={"REPO": str(repo), "PATH": "/usr/bin:/bin"},
        capture_output=True,
        text=True,
        check=True,
    ).stdout


def _composed(repo: Path) -> str:
    """The same settings as sandbox.tf composes them: the template, with both tiers' demos that
    are published, the file ending in one newline (the heredoc's)."""
    lists = repo / "packaging" / "linux" / "demo-stores"
    names = ",".join(
        (lists / f"{tier}-tier-bundles").read_text().strip() for tier in ("sandbox", "regions")
    ).split(",")
    published = [
        n
        for n in names
        if n
        and (repo / "demo" / "named" / n / "demo.yaml").is_file()
        and "published: true"
        in (repo / "demo" / "named" / n / "demo.yaml").read_text().splitlines()
    ]
    template = (lists / "sandbox-vm.env.tpl").read_text()
    return template.replace("${bundles}", ",".join(published)).strip() + "\n"


def test_boot_and_patch_compose_the_sync_settings_from_one_template():
    sandbox = (_TF / "sandbox.tf").read_text()
    assert "packaging/linux/demo-stores/sandbox-vm.env.tpl" in sandbox
    assert "{ bundles = local.demo_stores_bundles }" in sandbox
    assert 'regex("(?m)^published: true$"' in sandbox
    assert "${local.demo_stores_env}" in sandbox
    assert "PROVISA_DEMO_BUNDLES=" not in sandbox  # written from the template, nowhere else
    assert _rendered(_REPO) == _composed(_REPO)


def test_a_demo_without_a_bundle_is_not_synced(tmp_path):
    """A tier may name a demo that is not published yet (retail): it has no bundle, so the sync
    leaves it out until it is; every published one is synced."""
    lists = tmp_path / "packaging" / "linux" / "demo-stores"
    lists.mkdir(parents=True)
    (lists / "sandbox-tier-bundles").write_text("weblogs,starthere\n")
    (lists / "regions-tier-bundles").write_text("retail\n")
    (lists / "sandbox-vm.env.tpl").write_text(
        (_REPO / "packaging/linux/demo-stores/sandbox-vm.env.tpl").read_text()
    )
    for name, published in (("weblogs", True), ("starthere", True), ("retail", False)):
        demo = tmp_path / "demo" / "named" / name
        demo.mkdir(parents=True)
        (demo / "demo.yaml").write_text(f"name: {name}\npublished: {str(published).lower()}\n")
    rendered = _rendered(tmp_path)
    assert rendered.splitlines()[0] == "PROVISA_DEMO_BUNDLES=weblogs,starthere"
    assert rendered == _composed(tmp_path)
    # Published, it is synced.
    (tmp_path / "demo/named/retail/demo.yaml").write_text("name: retail\npublished: true\n")
    assert _rendered(tmp_path).splitlines()[0] == "PROVISA_DEMO_BUNDLES=weblogs,starthere,retail"


def test_a_patch_rewrites_the_vm_sync_settings_when_they_differ_and_runs_the_sync():
    deploy = _DEPLOY.read_text()
    body = deploy[deploy.index("deploy_sandbox_vm() {") :]
    body = body[: body.index("\n}\n")]
    assert "push_demo_stores; push_demo_stores_env; push_overlay" in body
    push = _function("push_demo_stores_env")
    assert 'demo_stores_env > "$STAGE/demo-stores.env"' in push
    assert '[ "$want" = "$have" ]' in push  # rewritten only when it differs
    assert "sudo install -m 644 /tmp/demo-stores.env $DEMO_STORES_ENV" in push
    assert "systemctl start --no-block provisa-demo-stores.service" in push
    assert 'DEMO_STORES_ENV="/root/.provisa/demo-stores.env"' in deploy


def _guard() -> str:
    """The boot guard at the head of base_startup (main.tf), as the shell sees it."""
    main = (_TF / "main.tf").read_text()
    body = main[main.index("base_startup = <<-SHELL") :]
    body = body[
        body.index('    startup_sha="') : body.index("    fi\n", body.index("startup_done=")) + 7
    ]
    return "\n".join(line[4:] for line in body.splitlines()).replace("$${", "${")


def test_a_wake_with_the_startup_script_it_last_completed_sets_nothing_up(tmp_path):
    """A boot whose startup script last ran to its end exits before the apt installs, the
    release download, the image loads and the stack recreate; a changed script runs in full."""
    import shutil
    import subprocess

    bindir = tmp_path / "bin"
    bindir.mkdir()
    (bindir / "curl").write_text('#!/bin/sh\ncat "$SCRIPT"\n')
    (bindir / "curl").chmod(0o755)
    if shutil.which("sha256sum", path="/usr/bin:/bin") is None:  # macOS has shasum; Ubuntu both
        (bindir / "sha256sum").write_text('#!/bin/sh\nexec shasum -a 256 "$@"\n')
        (bindir / "sha256sum").chmod(0o755)
    done = tmp_path / "startup.sha256"
    script = tmp_path / "run.sh"
    script.write_text(
        "set -euo pipefail\n"
        + _guard().replace("/var/lib/provisa-startup.sha256", str(done))
        + '\necho "FULL RUN"\necho "$startup_sha" > "$startup_done"\n'
    )
    metadata = tmp_path / "startup-script"

    def boot(content: str) -> str:
        metadata.write_text(content)
        env = {"PATH": f"{bindir}:/usr/bin:/bin", "SCRIPT": str(metadata)}
        return subprocess.run(
            ["bash", str(script)], env=env, capture_output=True, text=True, check=True
        ).stdout

    assert "FULL RUN" in boot("release 490")
    assert "FULL RUN" not in boot("release 490")
    assert "FULL RUN" in boot("release 491")
    # Each node records its completed run as its startup script's last line.
    for tf, launch in (("main.tf", "--ram-gb 0\n"), ("sandbox.tf", "--ram-gb 0\n")):
        text = (_TF / tf).read_text()
        after = text[text.rindex(launch) + len(launch) :]
        assert after.lstrip().startswith('echo "$startup_sha" > "$startup_done"'), tf
