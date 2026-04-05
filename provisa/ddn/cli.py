# Copyright (c) 2026 Kenneth Stott
# Canary: ad0c1515-3c0a-4989-89b0-a35caf26c8d5
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CLI entry point for DDN (Hasura v3) HML converter."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from provisa.core.models import GovernanceLevel, ProvisaConfig
from provisa.ddn.mapper import convert_hml
from provisa.ddn.parser import parse_hml_dir
from provisa.import_shared.warnings import WarningCollector


def _parse_domain_map(pairs: list[str] | None) -> dict[str, str]:
    """Parse KEY=VAL pairs into a dict."""
    if not pairs:
        return {}
    result: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            print(
                f"Warning: ignoring invalid domain-map entry: {pair}",
                file=sys.stderr,
            )
            continue
        k, v = pair.split("=", 1)
        result[k.strip()] = v.strip()
    return result


def _load_source_overrides(path: str | None) -> dict | None:
    if not path:
        return None
    override_path = Path(path)
    if not override_path.exists():
        print(
            f"Warning: source-overrides file not found: {path}",
            file=sys.stderr,
        )
        return None
    with open(override_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="provisa.ddn",
        description="Convert DDN (Hasura v3) HML metadata to Provisa configuration.",
    )
    parser.add_argument(
        "hml_dir",
        type=str,
        help="Path to DDN HML project directory",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default=None,
        help="Output YAML file path (default: stdout)",
    )
    parser.add_argument(
        "--source-overrides",
        type=str,
        default=None,
        help="YAML file with per-source connection overrides",
    )
    parser.add_argument(
        "--domain-map",
        nargs="*",
        help="Subgraph-to-domain mappings as KEY=VAL pairs",
    )
    parser.add_argument(
        "--governance-default",
        type=str,
        choices=["pre-approved", "registry-required"],
        default="pre-approved",
        help="Default governance level for tables",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate without writing output",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    hml_dir = Path(args.hml_dir)
    if not hml_dir.exists():
        print(f"Error: HML directory not found: {hml_dir}", file=sys.stderr)
        return 1

    collector = WarningCollector()

    # Parse
    metadata = parse_hml_dir(hml_dir, collector)

    # Map
    governance = GovernanceLevel(args.governance_default)
    domain_map = _parse_domain_map(args.domain_map)
    source_overrides = _load_source_overrides(args.source_overrides)

    config = convert_hml(
        metadata,
        collector=collector,
        governance_default=governance,
        domain_map=domain_map,
        source_overrides=source_overrides,
    )

    # Validate
    ProvisaConfig.model_validate(config.model_dump(by_alias=True))

    if args.dry_run:
        print("Dry run: validation passed.", file=sys.stderr)
        if collector.has_warnings():
            print(collector.summary(), file=sys.stderr)
        return 0

    # Output
    output_data = config.model_dump(by_alias=True, exclude_none=True, mode="json")
    yaml_str = yaml.dump(output_data, default_flow_style=False, sort_keys=False)

    if args.output:
        out_path = Path(args.output)
        out_path.write_text(yaml_str, encoding="utf-8")
        print(f"Written to {out_path}", file=sys.stderr)
    else:
        print(yaml_str)

    if collector.has_warnings():
        print(collector.summary(), file=sys.stderr)

    return 0
