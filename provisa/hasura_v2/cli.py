# Copyright (c) 2025 Kenneth Stott
# Canary: 453a6754-ec90-4b38-a053-f6531a7f1382
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""CLI entry point for Hasura v2 metadata converter."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

from provisa.core.models import GovernanceLevel
from provisa.hasura_v2.mapper import convert_metadata
from provisa.hasura_v2.parser import parse_metadata_dir
from provisa.import_shared.warnings import WarningCollector


def _parse_domain_map(pairs: list[str] | None) -> dict[str, str]:
    """Parse KEY=VAL pairs into a dict."""
    if not pairs:
        return {}
    result: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            print(f"Warning: ignoring invalid domain-map entry: {pair}", file=sys.stderr)
            continue
        k, v = pair.split("=", 1)
        result[k.strip()] = v.strip()
    return result


def _load_auth_env(path: str | None) -> dict[str, str] | None:
    """Load auth environment variables from a .env-style file."""
    if not path:
        return None
    env_path = Path(path)
    if not env_path.exists():
        print(f"Warning: auth-env-file not found: {path}", file=sys.stderr)
        return None
    result: dict[str, str] = {}
    with open(env_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                result[k.strip()] = v.strip()
    return result


def _load_source_overrides(path: str | None) -> dict | None:
    if not path:
        return None
    override_path = Path(path)
    if not override_path.exists():
        print(f"Warning: source-overrides file not found: {path}", file=sys.stderr)
        return None
    with open(override_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="provisa.hasura_v2",
        description="Convert Hasura v2 metadata to Provisa configuration.",
    )
    parser.add_argument(
        "metadata_dir",
        type=str,
        help="Path to Hasura v2 metadata directory",
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
        help="Schema-to-domain mappings as KEY=VAL pairs",
    )
    parser.add_argument(
        "--governance-default",
        type=str,
        choices=["pre-approved", "registry-required"],
        default="pre-approved",
        help="Default governance level for tables",
    )
    parser.add_argument(
        "--auth-env-file",
        type=str,
        default=None,
        help="Path to .env file with auth configuration",
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

    metadata_dir = Path(args.metadata_dir)
    if not metadata_dir.exists():
        print(f"Error: metadata directory not found: {metadata_dir}", file=sys.stderr)
        return 1

    collector = WarningCollector()

    # Parse
    metadata = parse_metadata_dir(metadata_dir, collector)

    # Map
    governance = GovernanceLevel(args.governance_default)
    domain_map = _parse_domain_map(args.domain_map)
    auth_env = _load_auth_env(args.auth_env_file)
    source_overrides = _load_source_overrides(args.source_overrides)

    config = convert_metadata(
        metadata,
        collector=collector,
        governance_default=governance,
        domain_map=domain_map,
        auth_env=auth_env,
        source_overrides=source_overrides,
    )

    # Validate
    from provisa.core.models import ProvisaConfig
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
