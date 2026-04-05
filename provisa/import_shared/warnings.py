# Copyright (c) 2025 Kenneth Stott
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


@dataclass
class ImportWarning:
    """A single warning about an unsupported or partially-supported feature."""

    category: str  # e.g. "remote_schemas", "event_triggers"
    message: str
    source_path: str = ""  # file/location where the issue was found


@dataclass
class WarningCollector:
    """Accumulates warnings during an import conversion run."""

    warnings: list[ImportWarning] = field(default_factory=list)

    def warn(self, category: str, message: str, source_path: str = "") -> None:
        self.warnings.append(ImportWarning(
            category=category,
            message=message,
            source_path=source_path,
        ))

    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    def summary(self) -> str:
        if not self.warnings:
            return "No warnings."
        lines = [f"Import warnings ({len(self.warnings)}):"]
        for w in self.warnings:
            loc = f" [{w.source_path}]" if w.source_path else ""
            lines.append(f"  [{w.category}]{loc} {w.message}")
        return "\n".join(lines)
