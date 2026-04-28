# Implementation Gaps (as of 2026-04-11)

All previously listed gaps have been resolved.

| REQ | Description | Status |
|-----|-------------|--------|
| REQ-218 | Cursor-based pagination | Implemented — `cursor.py` wired into schema (`edges`/`pageInfo`) |
| REQ-219 | SSE subscriptions | Implemented — SSE wire format in `subscriptions/` |
| REQ-221 | Enum table auto-detection | Implemented — `enum_detect.py` integrated into schema gen |
| REQ-305 | Admin UI for custom return schemas | Implemented — JSON Schema inference/edit UI in `MVManager.tsx` |
| REQ-360–362 | Action query filter/sort/pagination + relationship resolution | Implemented |
| REQ-214 | Column presets steward UI | Implemented — `ColumnPresetsEditor` component wired into `TablesPage`; presets stored in `registered_tables.column_presets` JSONB; applied at mutation compile time |
