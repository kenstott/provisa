# Copyright (c) 2026 Kenneth Stott
# Canary: 07693fcd-ad4d-4055-a1c3-44b984f0d858
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
"""Unit guard tests for the frontend-modernization requirements: REQ-1009, REQ-1010,
REQ-1011, REQ-1012, REQ-1013, REQ-1014, REQ-1016.

These assert the shipped state of provisa-ui source (dependency set, theme/i18n wiring,
axe-core a11y enforcement, provider-wrapped test harness) so a regression that removes
the modernized scaffolding fails CI. UI behavior beyond static wiring is exercised by the
Playwright e2e suite (provisa-ui/e2e/) under the axe-core coverage fixture."""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent.parent
UI = REPO_ROOT / "provisa-ui"
SRC = UI / "src"


def _package_json() -> dict:
    return json.loads((UI / "package.json").read_text())


def _all_deps() -> dict:
    pkg = _package_json()
    return {**pkg.get("dependencies", {}), **pkg.get("devDependencies", {})}


# ---------------------------------------------------------------------------
# REQ-1009: Standardize on Mantine; remove dead @mui/material + Emotion deps.
# ---------------------------------------------------------------------------
class TestREQ1009MantineStandardization:
    """REQ-1009"""

    def test_mantine_core_is_a_dependency(self):
        # REQ-1009
        assert "@mantine/core" in _all_deps()

    def test_mui_material_removed(self):
        # REQ-1009
        assert "@mui/material" not in _all_deps()

    def test_emotion_removed(self):
        # REQ-1009
        deps = _all_deps()
        assert not any(d.startswith("@emotion/") for d in deps)


# ---------------------------------------------------------------------------
# REQ-1010: CSS-variable design tokens (indigo #6366f1) mapped into the theme.
# ---------------------------------------------------------------------------
class TestREQ1010DesignTokens:
    """REQ-1010"""

    def test_app_css_declares_primary_token(self):
        # REQ-1010
        css = (SRC / "App.css").read_text()
        assert ":root" in css
        assert "--primary: #6366f1" in css

    def test_theme_module_exists(self):
        # REQ-1010
        assert (SRC / "theme" / "theme.ts").exists()


# ---------------------------------------------------------------------------
# REQ-1011: Dark + Light color schemes via Mantine, with a user-facing toggle.
# ---------------------------------------------------------------------------
class TestREQ1011Theming:
    """REQ-1011"""

    def test_color_scheme_manager_wired(self):
        # REQ-1011 — persisted color scheme (localStorage manager) in the app root.
        main = (SRC / "main.tsx").read_text()
        assert "localStorageColorSchemeManager" in main
        assert "colorSchemeManager=" in main

    def test_theme_toggle_control_exists(self):
        # REQ-1011 — user-facing toggle component.
        toggle = SRC / "theme" / "ColorSchemeToggle.tsx"
        assert toggle.exists()
        assert "useMantineColorScheme" in toggle.read_text()


# ---------------------------------------------------------------------------
# REQ-1012: react-i18next, 'en' base catalog, no hardcoded user-facing strings.
# ---------------------------------------------------------------------------
class TestREQ1012Internationalization:
    """REQ-1012"""

    def test_react_i18next_is_a_dependency(self):
        # REQ-1012
        deps = _all_deps()
        assert "react-i18next" in deps
        assert "i18next" in deps

    def test_i18n_runtime_initializes_react_i18next(self):
        # REQ-1012
        idx = (SRC / "i18n" / "index.ts").read_text()
        assert "initReactI18next" in idx
        assert 'fallbackLng: "en"' in idx

    def test_en_base_catalog_exists(self):
        # REQ-1012
        assert (SRC / "i18n" / "locales" / "en.json").exists()

    def test_app_root_wraps_i18next_provider(self):
        # REQ-1012
        assert "I18nextProvider" in (SRC / "main.tsx").read_text()


# ---------------------------------------------------------------------------
# REQ-1013 / REQ-1014: WCAG 2.1 AA enforced by an axe-core e2e coverage fixture.
# ---------------------------------------------------------------------------
class TestREQ1013And1014Accessibility:
    """REQ-1013, REQ-1014"""

    def test_axe_core_playwright_is_a_dependency(self):
        # REQ-1014
        assert "@axe-core/playwright" in _all_deps()

    def test_coverage_fixture_runs_axe(self):
        # REQ-1013, REQ-1014 — every e2e run asserts zero a11y violations.
        cov = (UI / "e2e" / "coverage.ts").read_text()
        assert "@axe-core/playwright" in cov
        assert "AxeBuilder" in cov
        assert "violations" in cov


# ---------------------------------------------------------------------------
# REQ-1016: test suite migrated in lockstep — provider-wrapped harness + specs.
# ---------------------------------------------------------------------------
class TestREQ1016TestSuiteMigration:
    """REQ-1016"""

    def test_render_harness_wraps_mantine_and_i18n(self):
        # REQ-1016 — component tests exercise real theming + translated strings.
        harness = (SRC / "test-utils" / "render.tsx").read_text()
        assert "MantineProvider" in harness
        assert "I18nextProvider" in harness

    def test_e2e_specs_present(self):
        # REQ-1016 — the migrated Playwright suite exists.
        specs = list((UI / "e2e").glob("*.spec.ts"))
        assert len(specs) >= 20
