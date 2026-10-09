# Copyright (c) 2026 Kenneth Stott
# Canary: 2334054e-f39a-4e62-a3da-66bda0d138eb
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.

"""The canonical mail, calendar and task tables: the module both mail sources import and the
document a reader is given state the same thing, every column lands as a type the replica
write face carries, and an enumeration value a provider's table does not list is refused."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pytest

from provisa.core import canonical_mail as cm
from provisa.core.ir_arrow import arrow_schema

DOC = Path(__file__).resolve().parents[2] / "docs" / "arch" / "canonical-mail-schema.md"


def _between(text: str, start: str, end: str) -> str:
    head = text.index(start)
    body = text.index("-->", head) + len("-->")
    return text[body : text.index(end, body)].strip("\n") + "\n"


def test_the_document_states_the_tables_of_the_module():
    stated = _between(DOC.read_text(), "<!-- tables:", "<!-- end tables -->")
    assert stated == cm.render_tables()


def test_the_document_states_the_enumerations_of_the_module():
    stated = _between(DOC.read_text(), "<!-- enums:", "<!-- end enums -->")
    assert stated == cm.render_enums()


def test_the_tables_are_the_eleven_agreed():
    assert list(cm.TABLES) == [
        "messages",
        "message_recipients",
        "folders",
        "message_folders",
        "threads",
        "attachments",
        "calendars",
        "events",
        "event_attendees",
        "task_lists",
        "tasks",
    ]


@pytest.mark.parametrize("table", list(cm.TABLES))
def test_every_table_leads_with_provider_and_account_and_lands_as_ir_types(table):
    columns = cm.ir_columns(table)
    assert columns[:2] == [("provider", "text"), ("account", "text")]
    names = [name for name, _ in columns]
    assert len(names) == len(set(names))
    assert {ir for _, ir in columns} <= cm.IR_TYPES
    assert arrow_schema(columns).names == names  # every type has an Arrow type to land as


def test_timestamps_and_dates_land_as_such():
    import pyarrow as pa

    schema = arrow_schema(cm.ir_columns("messages"))
    assert schema.field("received_at").type == pa.timestamp("us")
    assert schema.field("is_read").type == pa.bool_()
    assert schema.field("size_bytes").type == pa.int64()
    assert arrow_schema(cm.ir_columns("events")).field("start_date").type == pa.date32()
    for table in cm.TABLES.values():
        for column in table.columns:
            if column.name.endswith("_at"):
                assert column.type == "timestamp", (table.name, column.name)
            if column.name.endswith("_date"):
                assert column.type == "date", (table.name, column.name)


def test_every_column_says_who_populates_it_and_only_json_states_a_shape():
    for table in cm.TABLES.values():
        for column in table.columns:
            where = (table.name, column.name)
            assert column.populated in ("G", "M", "GM"), where
            assert column.populated_by(cm.GOOGLE) == ("G" in column.populated), where
            assert column.populated_by(cm.MICROSOFT) == ("M" in column.populated), where
            if column.shape is not None:
                assert column.type == "json", where
                assert column.json_schema["type"] == "array", where
            if column.enum is not None:
                assert column.type == "text" and column.enum in cm.ENUMS, where


def test_an_address_list_states_its_json_schema():
    assert cm.TABLES["messages"].column("to_addresses").json_schema == {
        "type": "array",
        "items": {"type": "string"},
    }
    assert cm.TABLES["messages"].column("headers").json_schema["items"]["required"] == [
        "name",
        "value",
    ]
    assert cm.TABLES["events"].column("reminders").json_schema is None  # states no shape


def test_an_enumeration_column_is_populated_by_the_providers_that_have_a_table_for_it():
    """A provider that populates an enumeration column has a translation table or a stated
    derivation for it, and one that does not populate it has neither."""
    for table in cm.TABLES.values():
        for column in table.columns:
            if column.enum is None or column.enum == "provider":
                continue
            for provider in cm.PROVIDERS:
                has = provider in cm.ENUMS[column.enum].providers
                assert has == column.populated_by(provider), (table.name, column.name, provider)


def test_every_enumeration_is_used_and_maps_only_to_its_own_values():
    used = {c.enum for t in cm.TABLES.values() for c in t.columns if c.enum}
    assert used == set(cm.ENUMS)
    for name, spec in cm.ENUMS.items():
        for provider, table in spec.providers.items():
            assert provider in cm.PROVIDERS
            if isinstance(table, Mapping):
                assert {v for v in table.values() if v is not None} <= set(spec.values), name
        assert set(spec.catch_all.values()) <= set(spec.values)


def test_a_listed_value_is_translated():
    assert cm.translate("flag_status", cm.MICROSOFT, "notFlagged") == "none"
    assert cm.translate("show_as", cm.MICROSOFT, "oof") == "out_of_office"
    assert cm.translate("show_as", cm.GOOGLE, "opaque") == "busy"
    assert cm.translate("task_status", cm.GOOGLE, "needsAction") == "open"
    assert cm.translate("importance", cm.MICROSOFT, None) is None


def test_a_value_declared_to_mean_no_value_is_null():
    assert cm.translate("attendee_response", cm.MICROSOFT, "organizer") is None


@pytest.mark.parametrize(
    ("enum", "provider", "value"),
    [
        ("importance", cm.MICROSOFT, "urgent"),
        ("task_list_role", cm.MICROSOFT, "unknownFutureValue"),
        ("show_as", cm.GOOGLE, "busy"),  # a canonical value is not a provider's value
        ("attendee_response", cm.GOOGLE, "maybe"),
    ],
)
def test_a_value_the_providers_table_does_not_list_is_refused_by_name(enum, provider, value):
    with pytest.raises(cm.UnmappedValue) as refused:
        cm.translate(enum, provider, value)
    message = str(refused.value)
    assert enum in message and provider in message and repr(value) in message
    assert (refused.value.enum, refused.value.provider, refused.value.value) == (
        enum,
        provider,
        value,
    )


def test_the_folder_role_alone_has_a_catch_all():
    assert [name for name, spec in cm.ENUMS.items() if spec.catch_all] == ["folder_role"]
    assert cm.translate("folder_role", cm.MICROSOFT, "conversationhistory") == "other"
    assert cm.translate("folder_role", cm.GOOGLE, "CHAT") == "other"
    assert cm.translate("folder_role", cm.GOOGLE, "CATEGORY_SOCIAL") == "category"
    assert cm.translate("folder_role", cm.MICROSOFT, "sentitems") == "sent"


def test_a_provider_without_a_table_is_not_translated():
    with pytest.raises(ValueError, match="no translation table"):
        cm.translate("importance", cm.GOOGLE, "high")  # Google has no importance
    with pytest.raises(ValueError, match="no translation table"):
        cm.translate("event_kind", cm.GOOGLE, "single")  # derived: the loader works it out


def test_a_value_a_loader_works_out_must_be_canonical():
    assert cm.canonical("event_kind", "series_master") == "series_master"
    assert cm.canonical("event_kind", None) is None
    with pytest.raises(ValueError, match="not a canonical value"):
        cm.canonical("event_kind", "seriesMaster")
