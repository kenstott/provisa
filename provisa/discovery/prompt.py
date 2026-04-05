# Copyright (c) 2026 Kenneth Stott
# Canary: 4db5b062-5f96-47fb-accc-51bdd87c6c05
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Build structured prompt for LLM relationship discovery."""

from __future__ import annotations

import json

from provisa.discovery.collector import DiscoveryInput


def build_prompt(discovery_input: DiscoveryInput) -> str:
    """Build a prompt requesting relationship suggestions as JSON."""
    sections: list[str] = []

    sections.append(
        "You are a database relationship analyst. Given the following table schemas "
        "and sample data, identify potential foreign key relationships between tables.\n"
    )

    # Table metadata
    sections.append("## Tables\n")
    for t in discovery_input.tables:
        cols_desc = ", ".join(
            f"{c['name']} ({c['type']})" for c in t.columns
        )
        sections.append(
            f"### {t.schema_name}.{t.table_name} (id={t.table_id}, domain={t.domain_id})\n"
            f"Columns: {cols_desc}\n"
        )
        if t.sample_values:
            samples_str = json.dumps(t.sample_values[:5], default=str)
            sections.append(f"Sample rows: {samples_str}\n")

    # Type compatibility hints
    sections.append(
        "## Type Compatibility Hints\n"
        "- integer/bigint columns commonly join to other integer/bigint columns\n"
        "- varchar/text columns can join if they share the same value domain\n"
        "- Do not suggest joins between incompatible types (e.g., integer to timestamp)\n"
    )

    # Existing relationships to exclude
    if discovery_input.existing_relationships:
        sections.append("## Already Existing Relationships (EXCLUDE these)\n")
        for rel in discovery_input.existing_relationships:
            sections.append(
                f"- source_table_id={rel['source_table_id']}, "
                f"source_column={rel['source_column']} -> "
                f"target_table_id={rel['target_table_id']}, "
                f"target_column={rel['target_column']}\n"
            )

    # Rejected relationships to exclude
    if discovery_input.rejected_pairs:
        sections.append("## Previously Rejected Relationships (EXCLUDE these)\n")
        for rej in discovery_input.rejected_pairs:
            sections.append(
                f"- source_table_id={rej['source_table_id']}, "
                f"source_column={rej['source_column']} -> "
                f"target_table_id={rej['target_table_id']}, "
                f"target_column={rej['target_column']}\n"
            )

    sections.append(
        "## Output Format\n"
        "Return ONLY a JSON array of relationship candidates. No other text.\n"
        "Each element:\n"
        "```json\n"
        "{\n"
        '  "source_table_id": <int>,\n'
        '  "source_column": "<column_name>",\n'
        '  "target_table_id": <int>,\n'
        '  "target_column": "<column_name>",\n'
        '  "cardinality": "many-to-one" | "one-to-many",\n'
        '  "confidence": <float 0.0-1.0>,\n'
        '  "reasoning": "<brief explanation>"\n'
        "}\n"
        "```\n"
    )

    return "\n".join(sections)
