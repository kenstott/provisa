# Copyright (c) 2026 Kenneth Stott
# Canary: 133fe227-295f-4a4e-9fe5-9088f4b94202
# Canary: placeholder
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""Warning collector for unsupported features during metadata import."""

from __future__ import annotations

from dataclasses import dataclass, field

# Requirements: REQ-192, REQ-628


@dataclass
class ImportWarning:  # REQ-192, REQ-628
    """A single warning about an unsupported or partially-supported feature."""

    category: str  # e.g. "remote_schemas", "event_triggers"
    message: str
    source_path: str = ""  # file/location where the issue was found


#: The order a report lists its items in: what the model is built from first.
_REPORT_ORDER = (
    "sources",
    "remote_schemas",
    "tables",
    "names",
    "relationships",
    "roles",
    "permissions",
    "computed_fields",
    "functions",
    "actions",
    "triggers",
)


def in_report_order(items: list) -> list:
    """Items grouped by category, in report order; within a category, as converted."""
    rank = {c: i for i, c in enumerate(_REPORT_ORDER)}
    return sorted(items, key=lambda i: rank.get(i.category, len(rank)))


@dataclass
class ReportItem:  # REQ-417
    """One line of an import's report: what was converted and how. ``detail`` is what it is
    carried as, or the Provisa feature it was mapped to and why."""

    category: str
    item: str
    detail: str


@dataclass
class WarningCollector:  # REQ-192, REQ-628, REQ-417
    """An import conversion's report. ``carried``: what converts as it is; ``mapped``: what a
    Provisa feature does better, each saying why; ``warnings``: what has no Provisa equivalent
    and is redone by hand, each saying what to do."""

    warnings: list[ImportWarning] = field(default_factory=list)
    carried: list[ReportItem] = field(default_factory=list)
    mapped: list[ReportItem] = field(default_factory=list)
    # Tracked functions whose signature only the database knows: completed at preview, where the
    # database is reached (api/admin/import_typing.py), else reported to redo.
    pending_functions: list[dict] = field(default_factory=list)

    def carry(self, category: str, item: str, detail: str) -> None:
        self.carried.append(ReportItem(category, item, detail))

    def map(self, category: str, item: str, detail: str) -> None:
        self.mapped.append(ReportItem(category, item, detail))

    def pending_function_to_redo(self, pending: dict) -> None:
        """A tracked function whose signature the database was not reached for."""
        self.warn(
            "functions",
            f"Function {pending['schema_name']}.{pending['name']} is tracked in Hasura; its "
            "arguments and what it returns are read from the database, which this conversion "
            f"did not reach. Enter the settings of source {pending['source_id']!r} and convert "
            "again, or add it on the Commands page.",
        )

    def warn(self, category: str, message: str, source_path: str = "") -> None:
        self.warnings.append(
            ImportWarning(
                category=category,
                message=message,
                source_path=source_path,
            )
        )

    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    def summary(self) -> str:
        lines: list[str] = []
        for title, items in (("Carried as", self.carried), ("Mapped to", self.mapped)):
            if items:
                lines.append(f"{title} ({len(items)}):")
                lines += [f"  [{i.category}] {i.item}: {i.detail}" for i in items]
        if not self.warnings:
            return "\n".join([*lines, "No warnings."])
        lines.append(f"Redo by hand ({len(self.warnings)}):")
        for w in self.warnings:
            loc = f" [{w.source_path}]" if w.source_path else ""
            lines.append(f"  [{w.category}]{loc} {w.message}")
        return "\n".join(lines)
