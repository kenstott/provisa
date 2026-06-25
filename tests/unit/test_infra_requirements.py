# Copyright (c) 2026 Kenneth Stott
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit tests for infra requirements: REQ-055, REQ-056, REQ-057, REQ-069, REQ-070, REQ-071, REQ-072, REQ-073, REQ-074, REQ-169, REQ-170, REQ-171, REQ-223, REQ-224, REQ-225, REQ-226, REQ-227, REQ-228, REQ-254, REQ-255, REQ-294, REQ-330, REQ-539, REQ-558, REQ-559, REQ-561, REQ-563, REQ-564, REQ-618, REQ-619, REQ-630, REQ-631, REQ-632, REQ-633, REQ-634"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

# Project root - all paths derived from here
REPO_ROOT = Path(__file__).parent.parent.parent


# ---------------------------------------------------------------------------
# REQ-055: Docker Compose for development/small-team: single command,
#           Provisa + Trino coordinator + configurable workers, all connectors
#           pre-loaded.
# ---------------------------------------------------------------------------


class TestREQ055DockerComposeServices:
    """REQ-055"""

    def test_core_compose_has_trino_service(self):
        # REQ-055
        content = yaml.safe_load((REPO_ROOT / "docker-compose.core.yml").read_text())
        assert "trino" in content["services"]

    def test_core_compose_has_trino_worker_service(self):
        # REQ-055
        content = yaml.safe_load((REPO_ROOT / "docker-compose.core.yml").read_text())
        assert "trino-worker" in content["services"]

    def test_core_compose_has_postgres_service(self):
        # REQ-055
        content = yaml.safe_load((REPO_ROOT / "docker-compose.core.yml").read_text())
        assert "postgres" in content["services"]


# ---------------------------------------------------------------------------
# REQ-056: Helm chart for production Kubernetes: horizontal Trino worker
#           scaling, resource groups, HPA autoscaling.
# ---------------------------------------------------------------------------


class TestREQ056HelmChart:
    """REQ-056"""

    def test_helm_chart_directory_exists(self):
        # REQ-056
        helm_dir = REPO_ROOT / "helm" / "provisa"
        assert helm_dir.exists(), "helm/provisa directory must exist"

    def test_helm_chart_yaml_exists(self):
        # REQ-056
        chart_yaml = REPO_ROOT / "helm" / "provisa" / "Chart.yaml"
        assert chart_yaml.exists(), "helm/provisa/Chart.yaml must exist"


# ---------------------------------------------------------------------------
# REQ-057: Provisa container is stateless; deployment topology behind Trino
#           endpoint is configuration concern.
# ---------------------------------------------------------------------------


class TestREQ057StatelessContainer:
    """REQ-057"""

    def test_app_state_holds_no_file_based_persistent_state(self):
        # REQ-057 — AppState stores only in-memory references, not file paths
        # that would imply local persistent storage required for operation.
        import inspect
        from provisa.api.app import AppState

        state_source = inspect.getsource(AppState)
        # Stateless: no local DB path, no local file store path as instance state
        # The class should not hold sqlite or local-only file paths in its members
        assert "sqlite" not in state_source.lower()

    def test_core_compose_does_not_define_provisa_volume_for_state(self):
        # REQ-057 — provisa app service must not mount a named volume for
        # application state (only external DB/cache services hold state)
        content = yaml.safe_load((REPO_ROOT / "docker-compose.core.yml").read_text())
        services = content.get("services", {})
        # Core provisa API is run on the host in dev; its absence from compose
        # is consistent with stateless design
        if "provisa" in services:
            provisa_svc = services["provisa"]
            volumes = provisa_svc.get("volumes", [])
            # Any volumes should be config mounts (:ro) not writable state stores
            writable_named = [
                v
                for v in volumes
                if isinstance(v, str) and not v.endswith(":ro") and not v.startswith("./")
            ]
            assert writable_named == [], (
                f"Provisa service must not mount writable named volumes for state: {writable_named}"
            )


# ---------------------------------------------------------------------------
# REQ-069: Architecture docs in docs/arch/ ARE the planning documents
# ---------------------------------------------------------------------------


class TestREQ069ArchDocs:
    """REQ-069"""

    def test_arch_docs_directory_exists(self):
        # REQ-069
        arch_dir = REPO_ROOT / "docs" / "arch"
        assert arch_dir.exists(), "docs/arch/ must exist"

    def test_requirements_md_exists(self):
        # REQ-069
        req_file = REPO_ROOT / "docs" / "arch" / "requirements.md"
        assert req_file.exists(), "docs/arch/requirements.md must exist"


# ---------------------------------------------------------------------------
# REQ-070: Maximum brevity in communications — code and facts only.
# (Process/communication requirement — verified via CLAUDE.md presence)
# ---------------------------------------------------------------------------


class TestREQ070MaximumBrevity:
    """REQ-070"""

    def test_claude_md_contains_brevity_instruction(self):
        # REQ-070
        claude_md = REPO_ROOT / "CLAUDE.md"
        assert claude_md.exists(), "CLAUDE.md must exist"
        content = claude_md.read_text()
        assert "brevity" in content.lower() or "pleasantries" in content.lower()


# ---------------------------------------------------------------------------
# REQ-071: New requirements tracked via requirements-tracker agent
# ---------------------------------------------------------------------------


class TestREQ071RequirementsTracker:
    """REQ-071"""

    def test_requirements_tracker_agent_exists(self):
        # REQ-071
        tracker = REPO_ROOT / ".claude" / "agents" / "requirements-tracker.md"
        assert tracker.exists(), ".claude/agents/requirements-tracker.md must exist"


# ---------------------------------------------------------------------------
# REQ-072: Core product is open source: Docker Compose, Helm chart, UI,
#           compiler, SQLGlot layer, Trino backend.
# ---------------------------------------------------------------------------


class TestREQ072OpenSourceCore:
    """REQ-072"""

    def test_docker_compose_core_file_exists(self):
        # REQ-072
        assert (REPO_ROOT / "docker-compose.core.yml").exists()

    def test_helm_chart_exists(self):
        # REQ-072
        assert (REPO_ROOT / "helm" / "provisa").exists()

    def test_provisa_ui_directory_exists(self):
        # REQ-072
        assert (REPO_ROOT / "provisa-ui").exists()

    def test_compiler_directory_exists(self):
        # REQ-072
        assert (REPO_ROOT / "provisa" / "compiler").exists()


# ---------------------------------------------------------------------------
# REQ-073: SaaS tier: hosted control plane with customer-hosted data plane option.
# (Commercial positioning — verified by presence of multitenancy/control_plane code)
# ---------------------------------------------------------------------------


class TestREQ073SaaSMultitenancy:
    """REQ-073"""

    def test_control_plane_module_exists(self):
        # REQ-073
        cp = REPO_ROOT / "provisa" / "control_plane"
        assert cp.exists(), "provisa/control_plane must exist for SaaS tier"


# ---------------------------------------------------------------------------
# REQ-074: Enterprise tier: SLA guarantees, dedicated support, advanced audit
#           logging, compliance reporting.
# ---------------------------------------------------------------------------


class TestREQ074EnterpriseTier:
    """REQ-074"""

    def test_audit_logging_module_exists(self):
        # REQ-074
        audit_candidates = [
            REPO_ROOT / "provisa" / "audit",
            REPO_ROOT / "provisa" / "observability",
            REPO_ROOT / "provisa" / "api" / "audit.py",
        ]
        assert any(p.exists() for p in audit_candidates), (
            "An audit logging module must exist for enterprise tier"
        )


# ---------------------------------------------------------------------------
# REQ-169: Trino 480 with Iceberg results catalog
# ---------------------------------------------------------------------------


class TestREQ169TrinoVersion:
    """REQ-169"""

    def test_trino_image_version_in_core_compose(self):
        # REQ-169 — Trino image must be version 480 or later
        content = yaml.safe_load((REPO_ROOT / "docker-compose.core.yml").read_text())
        trino_image = content["services"]["trino"]["image"]
        # Extract version number from image tag e.g. "trinodb/trino:481"
        match = re.search(r":(\d+)$", trino_image)
        assert match is not None, f"Trino image tag must include numeric version: {trino_image}"
        version = int(match.group(1))
        assert version >= 480, f"Trino version must be >= 480, got {version}"


# ---------------------------------------------------------------------------
# REQ-170: `start-ui.sh --reset-volumes` for Docker crash recovery.
# ---------------------------------------------------------------------------


class TestREQ170ResetVolumes:
    """REQ-170"""

    def test_start_ui_script_exists(self):
        # REQ-170
        assert (REPO_ROOT / "start-ui.sh").exists()

    def test_start_ui_script_accepts_reset_volumes_flag(self):
        # REQ-170
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "--reset-volumes" in content


# ---------------------------------------------------------------------------
# REQ-171: MinIO results bucket auto-created at startup.
# ---------------------------------------------------------------------------


class TestREQ171MinioBucketAutoCreated:
    """REQ-171"""

    def test_minio_init_service_in_observability_compose(self):
        # REQ-171 — minio-init service creates the bucket at startup
        content = yaml.safe_load((REPO_ROOT / "docker-compose.observability.yml").read_text())
        assert "minio-init" in content["services"], (
            "minio-init service must exist to auto-create results bucket"
        )

    def test_minio_init_creates_provisa_results_bucket(self):
        # REQ-171
        content = yaml.safe_load((REPO_ROOT / "docker-compose.observability.yml").read_text())
        init_svc = content["services"]["minio-init"]
        entrypoint = str(init_svc.get("entrypoint", ""))
        assert "provisa-results" in entrypoint, "minio-init must create the provisa-results bucket"


# ---------------------------------------------------------------------------
# REQ-223: Single-executable installer
# ---------------------------------------------------------------------------


class TestREQ223SingleExecutableInstaller:
    """REQ-223"""

    def test_packaging_directory_exists(self):
        # REQ-223
        assert (REPO_ROOT / "packaging").exists()

    def test_macos_dmg_builder_exists(self):
        # REQ-223
        assert (REPO_ROOT / "packaging" / "macos" / "build-dmg.sh").exists()

    def test_linux_appimage_builder_exists(self):
        # REQ-223
        assert (REPO_ROOT / "packaging" / "linux" / "build-appimage.sh").exists()


# ---------------------------------------------------------------------------
# REQ-224: Installer expands into ~/.provisa/; user interacts via CLI
# ---------------------------------------------------------------------------


class TestREQ224InstallerHomeDirAndCLI:
    """REQ-224"""

    def test_macos_first_launch_uses_provisa_home(self):
        # REQ-224
        content = (REPO_ROOT / "packaging" / "macos" / "first-launch.sh").read_text()
        assert ".provisa" in content

    def test_linux_first_launch_uses_provisa_home(self):
        # REQ-224
        content = (REPO_ROOT / "packaging" / "linux" / "first-launch.sh").read_text()
        assert ".provisa" in content

    def test_macos_first_launch_installs_cli(self):
        # REQ-224
        content = (REPO_ROOT / "packaging" / "macos" / "first-launch.sh").read_text()
        assert "install_cli" in content


# ---------------------------------------------------------------------------
# REQ-225: Default deployment uses embedded PostgreSQL and bundled Trino
# ---------------------------------------------------------------------------


class TestREQ225EmbeddedPostgresAndTrino:
    """REQ-225"""

    def test_core_compose_uses_postgres_16_image(self):
        # REQ-225
        content = yaml.safe_load((REPO_ROOT / "docker-compose.core.yml").read_text())
        pg_image = content["services"]["postgres"]["image"]
        assert "postgres:16" in pg_image

    def test_core_compose_includes_trino(self):
        # REQ-225
        content = yaml.safe_load((REPO_ROOT / "docker-compose.core.yml").read_text())
        assert "trino" in content["services"]


# ---------------------------------------------------------------------------
# REQ-226: Users can connect external Trino, Spark, auth provider, or
#           PostgreSQL via config.
# ---------------------------------------------------------------------------


class TestREQ226ExternalServicesViaConfig:
    """REQ-226"""

    def test_provisa_config_yaml_exists(self):
        # REQ-226
        config_file = REPO_ROOT / "config" / "provisa.yaml"
        assert config_file.exists(), "config/provisa.yaml must exist"

    def test_config_yaml_has_trino_section(self):
        # REQ-226 — config must have a trino section allowing external Trino
        content = yaml.safe_load((REPO_ROOT / "config" / "provisa.yaml").read_text())
        assert "trino" in content or "trino_host" in str(content), (
            "provisa.yaml must allow Trino endpoint configuration"
        )


# ---------------------------------------------------------------------------
# REQ-227: AF2 delivered in OS phases: macOS DMG, Linux AppImage, Windows EXE
# ---------------------------------------------------------------------------


class TestREQ227OSPhasedDelivery:
    """REQ-227"""

    def test_macos_dmg_builder_present(self):
        # REQ-227
        assert (REPO_ROOT / "packaging" / "macos" / "build-dmg.sh").exists()

    def test_linux_appimage_builder_present(self):
        # REQ-227
        assert (REPO_ROOT / "packaging" / "linux" / "build-appimage.sh").exists()

    def test_windows_installer_builder_present(self):
        # REQ-227
        assert (REPO_ROOT / "packaging" / "windows" / "build-installer.ps1").exists()


# ---------------------------------------------------------------------------
# REQ-228: AF1 shell script installer with Docker Compose, provisa CLI wrapper,
#          state in ~/.provisa/. AF2 airgapped native app bundle.
# ---------------------------------------------------------------------------


class TestREQ228InstallerPhases:
    """REQ-228"""

    def test_macos_first_launch_references_airgapped_design(self):
        # REQ-228 — AF2 airgapped bundle
        content = (REPO_ROOT / "packaging" / "macos" / "first-launch.sh").read_text()
        # No outbound pull — uses bundled images
        assert "pull=false" in content or "--pull=false" in content or "import" in content

    def test_airgap_compose_file_exists(self):
        # REQ-228
        assert (REPO_ROOT / "docker-compose.airgap.yml").exists()

    def test_airgap_compose_uses_digests_not_tags(self):
        # REQ-228 — image references use digests for reproducibility
        content = yaml.safe_load((REPO_ROOT / "docker-compose.airgap.yml").read_text())
        services = content.get("services", {})
        for svc_name, svc in services.items():
            image = svc.get("image", "")
            if image and "${" in image:
                # References an env var — must contain DIGEST in the variable name
                assert "DIGEST" in image, (
                    f"Service {svc_name} image reference '{image}' must use a DIGEST variable"
                )


# ---------------------------------------------------------------------------
# REQ-254: Integration tests must use Docker — spin up containers, run tests,
#           tear down. Tests must not assume a pre-existing stack.
# ---------------------------------------------------------------------------


class TestREQ254IntegrationTestsUseDocker:
    """REQ-254"""

    def test_integration_test_directory_exists(self):
        # REQ-254
        assert (REPO_ROOT / "tests" / "integration").exists()

    def test_no_integration_test_assumes_running_stack(self):
        # REQ-254 — integration tests must not hard-code localhost:5432 without
        # providing their own Docker setup (via env vars or pytest fixtures)
        integration_dir = REPO_ROOT / "tests" / "integration"
        if not integration_dir.exists():
            return
        for py_file in integration_dir.glob("*.py"):
            content = py_file.read_text()
            # Check that tests use environment variables or fixtures rather than
            # hard-coded assumptions about a pre-running service
            # Hard-coded "localhost" with no env var or fixture is the anti-pattern
            if (
                "localhost:5432" in content
                and "os.environ" not in content
                and "pytest.fixture" not in content
            ):
                # Allow if there's a pytest.ini or conftest that sets env vars
                conftest = integration_dir / "conftest.py"
                if not conftest.exists():
                    assert False, (
                        f"{py_file.name} uses hard-coded localhost:5432 without env var or fixture"
                    )


# ---------------------------------------------------------------------------
# REQ-255: Unit tests must mock all external components. No unit test should
#           require a running external service.
# ---------------------------------------------------------------------------


class TestREQ255UnitTestsMockExternals:
    """REQ-255"""

    def test_unit_tests_do_not_import_live_trino(self):
        # REQ-255 — unit tests must not create live Trino connections
        unit_dir = REPO_ROOT / "tests" / "unit"
        bad_files = []
        for py_file in unit_dir.glob("*.py"):
            content = py_file.read_text()
            # Real Trino connect() call without a mock patch is a violation
            if "trino.dbapi.connect(" in content and "mock" not in content.lower():
                bad_files.append(py_file.name)
        assert bad_files == [], f"Unit tests must not make live Trino connections: {bad_files}"


# ---------------------------------------------------------------------------
# REQ-294: Distribution must be fully airgap-capable — no outbound network
#           at install or first launch.
# ---------------------------------------------------------------------------


class TestREQ294AirgapCapable:
    """REQ-294"""

    def test_airgap_compose_exists(self):
        # REQ-294
        assert (REPO_ROOT / "docker-compose.airgap.yml").exists()

    def test_macos_first_launch_uses_bundled_images(self):
        # REQ-294 — must import images from bundle, not pull from registry
        content = (REPO_ROOT / "packaging" / "macos" / "first-launch.sh").read_text()
        assert "import" in content, (
            "macOS first-launch.sh must import bundled images (no pull from registry)"
        )

    def test_linux_build_appimage_bundles_images(self):
        # REQ-294 — AppImage must bundle images as tarballs
        content = (REPO_ROOT / "packaging" / "linux" / "build-appimage.sh").read_text()
        assert "images" in content and (".tar" in content or "tarball" in content.lower()), (
            "Linux AppImage builder must bundle images as tar archives"
        )


# ---------------------------------------------------------------------------
# REQ-330: Development observability stack in docker-compose under
#           `observability` profile. OTel Collector on 4317/4318, Grafana on
#           port 3100.
# ---------------------------------------------------------------------------


class TestREQ330ObservabilityStack:
    """REQ-330"""

    def test_observability_compose_exists(self):
        # REQ-330
        assert (REPO_ROOT / "docker-compose.observability.yml").exists()

    def test_otel_collector_service_exists(self):
        # REQ-330
        content = yaml.safe_load((REPO_ROOT / "docker-compose.observability.yml").read_text())
        assert "otel-collector" in content["services"]

    def test_otel_collector_exposes_4317_grpc(self):
        # REQ-330
        content = yaml.safe_load((REPO_ROOT / "docker-compose.observability.yml").read_text())
        ports = content["services"]["otel-collector"].get("ports", [])
        port_strings = [str(p) for p in ports]
        assert any("4317" in p for p in port_strings), (
            "OTel Collector must expose port 4317 (OTLP gRPC)"
        )

    def test_otel_collector_exposes_4318_http(self):
        # REQ-330
        content = yaml.safe_load((REPO_ROOT / "docker-compose.observability.yml").read_text())
        ports = content["services"]["otel-collector"].get("ports", [])
        port_strings = [str(p) for p in ports]
        assert any("4318" in p for p in port_strings), (
            "OTel Collector must expose port 4318 (OTLP HTTP)"
        )

    def test_grafana_service_exists(self):
        # REQ-330
        content = yaml.safe_load((REPO_ROOT / "docker-compose.observability.yml").read_text())
        assert "grafana" in content["services"]

    def test_grafana_exposed_on_port_3100(self):
        # REQ-330
        content = yaml.safe_load((REPO_ROOT / "docker-compose.observability.yml").read_text())
        ports = content["services"]["grafana"].get("ports", [])
        port_strings = [str(p) for p in ports]
        assert any("3100" in p for p in port_strings), "Grafana must be exposed on host port 3100"

    def test_prometheus_service_exists(self):
        # REQ-330
        content = yaml.safe_load((REPO_ROOT / "docker-compose.observability.yml").read_text())
        assert "prometheus" in content["services"]

    def test_tempo_service_exists(self):
        # REQ-330
        content = yaml.safe_load((REPO_ROOT / "docker-compose.observability.yml").read_text())
        assert "tempo" in content["services"]


# ---------------------------------------------------------------------------
# REQ-539: GET /health and GET /setup/status are always unauthenticated.
# ---------------------------------------------------------------------------


class TestREQ539UnauthenticatedHealthEndpoints:
    """REQ-539"""

    def test_health_endpoint_defined_in_app(self):
        # REQ-539
        import inspect
        from provisa.api import app as app_module

        source = inspect.getsource(app_module)
        assert '"/health"' in source, "Health endpoint must be defined in app.py"

    def test_setup_router_has_status_endpoint(self):
        # REQ-539
        from provisa.api.setup_router import router

        from fastapi.routing import APIRoute

        route_paths = [r.path for r in router.routes if isinstance(r, APIRoute)]
        assert "/setup/status" in route_paths or any("status" in p for p in route_paths), (
            "/setup/status endpoint must exist"
        )

    def test_health_route_requires_no_auth_dependency(self):
        # REQ-539 — health endpoint must not declare a security dependency
        import inspect
        from provisa.api import app as app_module

        source = inspect.getsource(app_module)
        # The health route should not use the auth dependency
        # Find the health route definition and confirm no Depends(...) before it
        health_block_match = re.search(
            r'api_route\("/health".*?\).*?async def health\(\)',
            source,
            re.DOTALL,
        )
        assert health_block_match is not None, "Health route must be defined"
        health_block = health_block_match.group(0)
        # No auth dependency injected into health()
        assert "Depends" not in health_block, (
            "Health endpoint must not require auth (no Depends injection)"
        )


# ---------------------------------------------------------------------------
# REQ-558: Development backend API listens on port 8001 when launched via
#           start-ui.sh.
# ---------------------------------------------------------------------------


class TestREQ558BackendPort8001:
    """REQ-558"""

    def test_start_ui_sh_uses_port_8001(self):
        # REQ-558
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "--port 8001" in content, "start-ui.sh must start uvicorn on port 8001"

    def test_dev_compose_files_do_not_expose_port_8001(self):
        # REQ-558 — backend runs on host, not in container during dev
        for compose_name in [
            "docker-compose.dev.yml",
            "docker-compose.dev-install.yml",
            "docker-compose.core.yml",
        ]:
            compose_file = REPO_ROOT / compose_name
            if not compose_file.exists():
                continue
            content = yaml.safe_load(compose_file.read_text())
            services = content.get("services", {})
            for svc_name, svc in services.items():
                ports = svc.get("ports", [])
                for port in ports:
                    port_str = str(port)
                    assert "8001" not in port_str, (
                        f"Dev compose {compose_name} service {svc_name} must not expose port 8001 "
                        f"(backend runs on host)"
                    )


# ---------------------------------------------------------------------------
# REQ-559: start-ui.sh starts the Vite UI dev server on port 3000.
# ---------------------------------------------------------------------------


class TestREQ559UIPort3000:
    """REQ-559"""

    def test_start_ui_sh_starts_ui_on_port_3000(self):
        # REQ-559
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "3000" in content and "vite" in content.lower(), (
            "start-ui.sh must start Vite dev server on port 3000"
        )

    def test_dev_compose_files_do_not_expose_port_3000(self):
        # REQ-559 — UI dev server runs on host, not in container
        for compose_name in [
            "docker-compose.dev.yml",
            "docker-compose.dev-install.yml",
            "docker-compose.core.yml",
        ]:
            compose_file = REPO_ROOT / compose_name
            if not compose_file.exists():
                continue
            content = yaml.safe_load(compose_file.read_text())
            services = content.get("services", {})
            for svc_name, svc in services.items():
                ports = svc.get("ports", [])
                for port in ports:
                    port_str = str(port)
                    # 3000 exposed on host side (format "3000:..." or "3000") is not allowed
                    if re.match(r"^3000[:\s]", port_str) or port_str == "3000":
                        assert False, (
                            f"Dev compose {compose_name} service {svc_name} must not expose host port 3000 "
                            f"(UI dev server runs on host)"
                        )


# ---------------------------------------------------------------------------
# REQ-561: Multi-node AppImage — secondary nodes run only Provisa API and
#           federation engine worker. Stateful singletons run on primary only.
# ---------------------------------------------------------------------------


class TestREQ561MultiNodePrimarySecondary:
    """REQ-561"""

    def test_linux_first_launch_has_primary_role(self):
        # REQ-561
        content = (REPO_ROOT / "packaging" / "linux" / "first-launch.sh").read_text()
        assert "primary" in content and "secondary" in content, (
            "linux first-launch.sh must support primary and secondary node roles"
        )

    def test_linux_first_launch_primary_has_stateful_services(self):
        # REQ-561 — stateful singletons documented on primary
        content = (REPO_ROOT / "packaging" / "linux" / "first-launch.sh").read_text()
        # Primary node config references PostgreSQL, Redis, MinIO
        assert "PostgreSQL" in content or "postgres" in content.lower()
        assert "Redis" in content or "redis" in content.lower()
        assert "MinIO" in content or "minio" in content.lower()


# ---------------------------------------------------------------------------
# REQ-563: AppImage --non-interactive installs a systemd unit and generates
#           credentials in ~/.provisa/config.yaml
# ---------------------------------------------------------------------------


class TestREQ563SystemdAndAutoCredentials:
    """REQ-563"""

    def test_linux_first_launch_installs_systemd_unit(self):
        # REQ-563
        content = (REPO_ROOT / "packaging" / "linux" / "first-launch.sh").read_text()
        assert "systemd" in content or "provisa.service" in content, (
            "linux first-launch.sh must install a systemd unit"
        )

    def test_linux_first_launch_supports_non_interactive_flag(self):
        # REQ-563
        content = (REPO_ROOT / "packaging" / "linux" / "first-launch.sh").read_text()
        assert "--non-interactive" in content

    def test_linux_first_launch_writes_config_yaml(self):
        # REQ-563
        content = (REPO_ROOT / "packaging" / "linux" / "first-launch.sh").read_text()
        assert "config.yaml" in content


# ---------------------------------------------------------------------------
# REQ-564: Terraform AWS deployment provisions VPC, EC2, ALB on port 8000,
#           NLB on port 8815 for Arrow Flight/gRPC.
# ---------------------------------------------------------------------------


class TestREQ564TerraformAWS:
    """REQ-564"""

    def test_terraform_aws_directory_exists(self):
        # REQ-564
        assert (REPO_ROOT / "terraform" / "aws").exists()

    def test_terraform_main_tf_exists(self):
        # REQ-564
        assert (REPO_ROOT / "terraform" / "aws" / "main.tf").exists()

    def test_terraform_defines_alb_on_port_8000(self):
        # REQ-564
        content = (REPO_ROOT / "terraform" / "aws" / "main.tf").read_text()
        assert "8000" in content, "Terraform must configure ALB on port 8000"

    def test_terraform_defines_nlb_on_port_8815(self):
        # REQ-564
        content = (REPO_ROOT / "terraform" / "aws" / "main.tf").read_text()
        assert "8815" in content, "Terraform must configure NLB on port 8815 for Arrow Flight/gRPC"

    def test_terraform_defines_vpc(self):
        # REQ-564
        content = (REPO_ROOT / "terraform" / "aws" / "main.tf").read_text()
        assert "aws_vpc" in content, "Terraform must provision a VPC"

    def test_terraform_uses_two_public_subnets(self):
        # REQ-564 — two public subnets across two AZs
        content = (REPO_ROOT / "terraform" / "aws" / "main.tf").read_text()
        assert "aws_subnet" in content
        # count = 2 implies two subnets
        assert "count" in content and "2" in content


# ---------------------------------------------------------------------------
# REQ-618: Backend API runs on port 8001 with uvicorn hot-reload for provisa/
#           and config/ directories. Log to .logs/server.log.
# ---------------------------------------------------------------------------


class TestREQ618HotReload:
    """REQ-618"""

    def test_start_ui_sh_enables_hot_reload(self):
        # REQ-618
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "--reload" in content, "start-ui.sh must enable uvicorn hot-reload"

    def test_start_ui_sh_watches_provisa_dir(self):
        # REQ-618
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "--reload-dir provisa" in content, (
            "start-ui.sh must watch provisa/ directory for hot-reload"
        )

    def test_start_ui_sh_watches_config_dir(self):
        # REQ-618
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "--reload-dir config" in content, (
            "start-ui.sh must watch config/ directory for hot-reload"
        )

    def test_start_ui_sh_logs_to_server_log(self):
        # REQ-618
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "server.log" in content


# ---------------------------------------------------------------------------
# REQ-619: start-ui.sh manages full dev lifecycle: Ctrl+C stops all,
#           --keep-docker leaves Docker running, Ctrl+R (SIGUSR1) restarts backend.
# ---------------------------------------------------------------------------


class TestREQ619LifecycleControls:
    """REQ-619"""

    def test_start_ui_sh_has_keep_docker_flag(self):
        # REQ-619
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "--keep-docker" in content

    def test_start_ui_sh_traps_sigusr1_for_restart(self):
        # REQ-619 — Ctrl+R sends SIGUSR1
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "USR1" in content, "start-ui.sh must trap SIGUSR1 (Ctrl+R) for backend restart"

    def test_start_ui_sh_has_restart_backend_function(self):
        # REQ-619
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "restart_backend" in content

    def test_start_ui_sh_traps_exit_for_cleanup(self):
        # REQ-619 — Ctrl+C / SIGTERM stops all services
        content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "trap cleanup EXIT" in content or "trap cleanup INT" in content


# ---------------------------------------------------------------------------
# REQ-630: Provisa ships as three artifact packages per platform (Core, Obs, Demo)
#           to stay within 2 GB GitHub Actions artifact limit.
# ---------------------------------------------------------------------------


class TestREQ630ThreeArtifactPackages:
    """REQ-630"""

    def test_macos_core_builder_exists(self):
        # REQ-630
        assert (REPO_ROOT / "packaging" / "macos" / "build-dmg.sh").exists()

    def test_macos_obs_builder_exists(self):
        # REQ-630
        assert (REPO_ROOT / "packaging" / "macos" / "build-dmg-obs.sh").exists()

    def test_macos_demo_builder_exists(self):
        # REQ-630
        assert (REPO_ROOT / "packaging" / "macos" / "build-dmg-demo.sh").exists()

    def test_windows_core_builder_exists(self):
        # REQ-630
        assert (REPO_ROOT / "packaging" / "windows" / "build-installer.ps1").exists()

    def test_windows_obs_builder_exists(self):
        # REQ-630
        assert (REPO_ROOT / "packaging" / "windows" / "build-installer-obs.ps1").exists()

    def test_windows_demo_builder_exists(self):
        # REQ-630
        assert (REPO_ROOT / "packaging" / "windows" / "build-installer-demo.ps1").exists()

    def test_linux_appimage_builder_exists(self):
        # REQ-630
        assert (REPO_ROOT / "packaging" / "linux" / "build-appimage.sh").exists()


# ---------------------------------------------------------------------------
# REQ-631: Package dependency chain: Core → Obs → Demo. Demo installer checks
#           for ~/.provisa/extensions/observability/ before proceeding.
# ---------------------------------------------------------------------------


class TestREQ631PackageDependencyChain:
    """REQ-631"""

    def test_macos_demo_installer_checks_for_obs_extension(self):
        # REQ-631
        content = (REPO_ROOT / "packaging" / "macos" / "build-dmg-demo.sh").read_text()
        assert "extensions/observability" in content or "OBS_EXT" in content, (
            "Demo DMG builder must check for observability extension"
        )

    def test_windows_demo_installer_checks_for_obs_extension(self):
        # REQ-631
        content = (REPO_ROOT / "packaging" / "windows" / "build-installer-demo.ps1").read_text()
        assert "observability" in content.lower(), (
            "Windows demo installer must check for observability extension"
        )

    def test_macos_demo_installer_references_obs_ext_path(self):
        # REQ-631
        content = (REPO_ROOT / "packaging" / "macos" / "build-dmg-demo.sh").read_text()
        # Must reference the observability extension directory
        assert "extensions/observability" in content


# ---------------------------------------------------------------------------
# REQ-632: Linux AppImage bundles both core and obs images — no separate Obs
#           download for Linux. Demo package not included in Linux.
# ---------------------------------------------------------------------------


class TestREQ632LinuxBundlesCoreAndObs:
    """REQ-632"""

    def test_linux_build_appimage_exists(self):
        # REQ-632
        assert (REPO_ROOT / "packaging" / "linux" / "build-appimage.sh").exists()

    def test_linux_first_launch_starts_core_and_obs(self):
        # REQ-632 — Linux always starts core + obs together
        content = (REPO_ROOT / "packaging" / "linux" / "first-launch.sh").read_text()
        # Must reference both core and observability compose files
        assert "core" in content and ("observability" in content or "obs" in content), (
            "Linux first-launch.sh must start core and observability together"
        )

    def test_linux_has_no_separate_obs_builder(self):
        # REQ-632 — no separate obs download for Linux
        obs_builder = REPO_ROOT / "packaging" / "linux" / "build-appimage-obs.sh"
        assert not obs_builder.exists(), (
            "Linux must not have a separate obs AppImage builder — obs is bundled in core"
        )


# ---------------------------------------------------------------------------
# REQ-633: macOS and Windows use extension model. Core creates VM runtime.
#           Obs and Demo are extensions in ~/.provisa/extensions/<name>/.
#           Launcher enumerates extensions/*/docker-compose.*.yml at startup.
# ---------------------------------------------------------------------------


class TestREQ633ExtensionModel:
    """REQ-633"""

    def test_macos_obs_builder_writes_extension_compose(self):
        # REQ-633 — obs extension writes compose file into extensions/observability/
        content = (REPO_ROOT / "packaging" / "macos" / "build-dmg-obs.sh").read_text()
        assert "extensions/observability" in content or "EXT_DIR" in content

    def test_macos_demo_builder_writes_extension_compose(self):
        # REQ-633
        content = (REPO_ROOT / "packaging" / "macos" / "build-dmg-demo.sh").read_text()
        assert "extensions/demo" in content or "EXT_DIR" in content

    def test_macos_core_creates_vm_runtime(self):
        # REQ-633 — Core DMG must set up Lima (macOS VM runtime)
        content = (REPO_ROOT / "packaging" / "macos" / "build-dmg.sh").read_text()
        assert "lima" in content.lower() or "Lima" in content, (
            "Core DMG must create the Lima VM runtime"
        )

    def test_windows_obs_builder_writes_extension_files(self):
        # REQ-633
        content = (REPO_ROOT / "packaging" / "windows" / "build-installer-obs.ps1").read_text()
        assert "observability" in content.lower()


# ---------------------------------------------------------------------------
# REQ-634: Dev environment runs Python backend and UI on the host, never in
#           containers. docker-compose.app.yml and docker-compose.airgap.yml are
#           packaged-product only. Ports 8000 and 3000 must not appear in dev
#           compose files.
# ---------------------------------------------------------------------------


class TestREQ634DevHostNotContainerized:
    """REQ-634"""

    DEV_COMPOSE_FILES = [
        "docker-compose.dev.yml",
        "docker-compose.dev-install.yml",
        "docker-compose.core.yml",
    ]

    def test_dev_compose_files_do_not_expose_port_8000(self):
        # REQ-634
        for compose_name in self.DEV_COMPOSE_FILES:
            compose_file = REPO_ROOT / compose_name
            if not compose_file.exists():
                continue
            content = yaml.safe_load(compose_file.read_text())
            services = content.get("services", {})
            for svc_name, svc in services.items():
                ports = svc.get("ports", [])
                for port in ports:
                    port_str = str(port)
                    assert not re.match(r"^8000[:\s]", port_str) and port_str != "8000", (
                        f"Dev compose {compose_name} service {svc_name} must not expose port 8000"
                    )

    def test_start_ui_install_script_exists(self):
        # REQ-634 — start-ui-install.sh is the dev mode entrypoint
        # Either start-ui.sh or start-ui-install.sh must exist
        assert (REPO_ROOT / "start-ui.sh").exists() or (REPO_ROOT / "start-ui-install.sh").exists()

    def test_app_compose_not_included_in_dev_stack(self):
        # REQ-634 — docker-compose.app.yml is packaged-product only
        # Verify start-ui.sh does NOT reference docker-compose.app.yml
        start_ui_content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "docker-compose.app.yml" not in start_ui_content, (
            "start-ui.sh must not include docker-compose.app.yml (packaged-product only)"
        )

    def test_airgap_compose_not_included_in_dev_stack(self):
        # REQ-634
        start_ui_content = (REPO_ROOT / "start-ui.sh").read_text()
        assert "docker-compose.airgap.yml" not in start_ui_content, (
            "start-ui.sh must not include docker-compose.airgap.yml (packaged-product only)"
        )
