# Copyright (c) 2026 Kenneth Stott
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""The canonical mail, calendar and task tables, as data.

The Google Workspace source and the Microsoft 365 source each produce every table here with the
same column names, IR types and order, so a union of the two needs no cast or rename. The column
set is the superset of both providers; a column a provider has no value for is a NULL of the
column's type. Both loaders take their landing columns from :func:`ir_columns` and pass every
enumeration value through :func:`translate`.

``docs/arch/canonical-mail-schema.md`` states the same tables for a reader; its table and
enumeration sections are :func:`render_tables` and :func:`render_enums` of this module, and a
test holds the two together.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

GOOGLE = "google"
MICROSOFT = "microsoft"
PROVIDERS = (GOOGLE, MICROSOFT)

#: The IR types a canonical column may have (``provisa/core/ir_arrow.py``).
IR_TYPES = frozenset({"text", "bigint", "boolean", "date", "timestamp", "json"})

#: What a ``json`` column holds, by the shape its column states: a JSON Schema. A json column
#: that states no shape holds what its provider gives, and has none.
JSON_SHAPES: Mapping[str, Mapping] = MappingProxyType(
    {
        "text list": {"type": "array", "items": {"type": "string"}},
        "list of {name, value}": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"name": {"type": "string"}, "value": {"type": "string"}},
                "required": ["name", "value"],
            },
        },
    }
)


@dataclass(frozen=True)
class Column:
    name: str
    type: str  # an IR type
    populated: str  # "G", "M" or "GM": which providers have a value for it
    google: str  # where Google's value comes from, for the reader
    microsoft: str  # where Microsoft's value comes from
    enum: str | None = None  # the enumeration its values belong to (ENUMS)
    shape: str | None = None  # what a json column holds (JSON_SHAPES)

    @property
    def json_schema(self) -> Mapping | None:
        return None if self.shape is None else JSON_SHAPES[self.shape]

    def populated_by(self, provider: str) -> bool:
        return provider[0].upper() in self.populated


@dataclass(frozen=True)
class Table:
    name: str
    note: str
    columns: tuple[Column, ...]

    def column(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)


_TABLES = (
    Table(
        "messages",
        "`folder_id` (single) is not a column: it is the same fact as `folder_ids`.",
        (
            Column("provider", "text", "GM", "`google`", "`microsoft`", enum="provider"),
            Column("account", "text", "GM", "mailbox address", "`{user-id}` of the path"),
            Column("id", "text", "GM", "id", "id"),
            Column("thread_id", "text", "GM", "threadId", "conversationId"),
            Column("internet_message_id", "text", "GM", "Message-ID header", "internetMessageId"),
            Column("in_reply_to", "text", "GM", "In-Reply-To header", "same header"),
            Column(
                "reference_ids", "json", "GM", "References header", "same header", shape="text list"
            ),
            Column("subject", "text", "GM", "Subject header", "subject"),
            Column("snippet", "text", "GM", "snippet", "bodyPreview (255 characters)"),
            Column("from_address", "text", "GM", "From header", "from.emailAddress.address"),
            Column("from_name", "text", "GM", "From header", "from.emailAddress.name"),
            Column("sender_address", "text", "GM", "Sender header", "sender.emailAddress.address"),
            Column("to_addresses", "json", "GM", "To header", "toRecipients", shape="text list"),
            Column("cc_addresses", "json", "GM", "Cc header", "ccRecipients", shape="text list"),
            Column("bcc_addresses", "json", "GM", "Bcc header", "bccRecipients", shape="text list"),
            Column(
                "reply_to_addresses", "json", "GM", "Reply-To header", "replyTo", shape="text list"
            ),
            Column(
                "sent_at", "timestamp", "GM", "Date header, NULL when unparsable", "sentDateTime"
            ),
            Column("received_at", "timestamp", "GM", "internalDate", "receivedDateTime"),
            Column("created_at", "timestamp", "M", "NULL", "createdDateTime"),
            Column("updated_at", "timestamp", "M", "NULL", "lastModifiedDateTime"),
            Column("is_read", "boolean", "GM", "UNREAD label absent", "isRead"),
            Column("is_draft", "boolean", "GM", "DRAFT label present", "isDraft"),
            Column(
                "is_flagged", "boolean", "GM", "STARRED label present", "flag.flagStatus = flagged"
            ),
            Column("flag_status", "text", "GM", "see enum", "flag.flagStatus", enum="flag_status"),
            Column("importance", "text", "M", "NULL", "importance", enum="importance"),
            Column(
                "has_attachments",
                "boolean",
                "GM",
                "a part with a filename that is not inline",
                "hasAttachments (inline not counted)",
            ),
            Column("size_bytes", "bigint", "G", "sizeEstimate", "NULL (v1.0 message has no size)"),
            Column(
                "folder_ids",
                "json",
                "GM",
                "labelIds",
                "one element: parentFolderId",
                shape="text list",
            ),
            Column("categories", "json", "M", "NULL", "categories", shape="text list"),
            Column(
                "inference_classification",
                "text",
                "M",
                "NULL",
                "inferenceClassification",
                enum="inference_classification",
            ),
            Column("is_read_receipt_requested", "boolean", "M", "NULL", "isReadReceiptRequested"),
            Column(
                "is_delivery_receipt_requested",
                "boolean",
                "M",
                "NULL",
                "isDeliveryReceiptRequested",
            ),
            Column(
                "body_text",
                "text",
                "GM",
                "text/plain part, NULL when the message has none",
                "body asked as text",
            ),
            Column(
                "body_html",
                "text",
                "GM",
                "text/html part, NULL when the message has none",
                "body asked as html",
            ),
            Column(
                "headers",
                "json",
                "GM",
                "payload.headers",
                "internetMessageHeaders",
                shape="list of {name, value}",
            ),
            Column("change_key", "text", "GM", "historyId", "changeKey"),
            Column("web_link", "text", "M", "NULL", "webLink"),
            Column("classification_labels", "json", "G", "classificationLabelValues", "NULL"),
            Column("conversation_index", "text", "M", "NULL", "conversationIndex (base64)"),
            Column("flag_start_at", "timestamp", "M", "NULL", "flag.startDateTime"),
            Column("flag_due_at", "timestamp", "M", "NULL", "flag.dueDateTime"),
            Column("flag_completed_at", "timestamp", "M", "NULL", "flag.completedDateTime"),
        ),
    ),
    Table(
        "message_recipients",
        "Names beside addresses, as rows. `kind`: `from`, `sender`, `to`, `cc`, `bcc`, `reply_to`.",
        (
            Column("provider", "text", "GM", "", "", enum="provider"),
            Column("account", "text", "GM", "", ""),
            Column("message_id", "text", "GM", "", ""),
            Column("kind", "text", "GM", "", "", enum="recipient_kind"),
            Column("position", "bigint", "GM", "", ""),
            Column("address", "text", "GM", "", ""),
            Column("name", "text", "GM", "", ""),
        ),
    ),
    Table(
        "folders",
        "A named container of messages: a Gmail label, an Exchange mail folder.",
        (
            Column("provider", "text", "GM", "", "", enum="provider"),
            Column("account", "text", "GM", "", ""),
            Column("id", "text", "GM", "id", "id"),
            Column("name", "text", "GM", "name", "displayName"),
            Column("parent_id", "text", "M", "NULL", "parentFolderId"),
            Column(
                "kind",
                "text",
                "GM",
                "type",
                "`system` when it has a role, else `user`",
                enum="folder_kind",
            ),
            Column(
                "role",
                "text",
                "GM",
                "see enum",
                "well-known folder name, see enum",
                enum="folder_role",
            ),
            Column("is_hidden", "boolean", "GM", "labelListVisibility = labelHide", "isHidden"),
            Column("total_count", "bigint", "GM", "messagesTotal", "totalItemCount"),
            Column("unread_count", "bigint", "GM", "messagesUnread", "unreadItemCount"),
            Column("child_count", "bigint", "M", "NULL", "childFolderCount"),
            Column("threads_total", "bigint", "G", "threadsTotal", "NULL"),
            Column("threads_unread", "bigint", "G", "threadsUnread", "NULL"),
            Column("color_text", "text", "G", "color.textColor", "NULL"),
            Column("color_background", "text", "G", "color.backgroundColor", "NULL"),
            Column(
                "message_list_visibility",
                "text",
                "G",
                "messageListVisibility",
                "NULL",
                enum="message_list_visibility",
            ),
            Column(
                "label_list_visibility",
                "text",
                "G",
                "labelListVisibility",
                "NULL",
                enum="label_list_visibility",
            ),
        ),
    ),
    Table(
        "message_folders",
        "One row per message per container. Google: many per message. Microsoft: one.",
        (
            Column("provider", "text", "GM", "", "", enum="provider"),
            Column("account", "text", "GM", "", ""),
            Column("message_id", "text", "GM", "", ""),
            Column("folder_id", "text", "GM", "", ""),
        ),
    ),
    Table(
        "threads",
        "Derived by both sources from the messages they hold, grouped by `thread_id`.",
        (
            Column("provider", "text", "GM", "", "", enum="provider"),
            Column("account", "text", "GM", "", ""),
            Column("id", "text", "GM", "thread_id", "thread_id"),
            Column(
                "subject",
                "text",
                "GM",
                "of the earliest message by received_at",
                "of the earliest message by received_at",
            ),
            Column("snippet", "text", "GM", "of the latest message", "of the latest message"),
            Column("first_message_at", "timestamp", "GM", "", ""),
            Column("last_message_at", "timestamp", "GM", "", ""),
            Column("message_count", "bigint", "GM", "", ""),
        ),
    ),
    Table(
        "attachments",
        "Metadata only. Content is not in the first build.",
        (
            Column("provider", "text", "GM", "", "", enum="provider"),
            Column("account", "text", "GM", "", ""),
            Column("message_id", "text", "GM", "", ""),
            Column("id", "text", "GM", "partId", "id"),
            Column("name", "text", "GM", "filename", "name"),
            Column("content_type", "text", "GM", "mimeType", "contentType"),
            Column("size_bytes", "bigint", "GM", "body.size", "size"),
            Column("is_inline", "boolean", "GM", "Content-Disposition", "isInline"),
            Column("content_id", "text", "GM", "Content-ID header", "contentId (file attachments)"),
            Column(
                "kind", "text", "GM", "`file`", "attachment type, see enum", enum="attachment_kind"
            ),
            Column("updated_at", "timestamp", "M", "NULL", "lastModifiedDateTime"),
        ),
    ),
    Table(
        "calendars",
        "",
        (
            Column("provider", "text", "GM", "", "", enum="provider"),
            Column("account", "text", "GM", "", ""),
            Column("id", "text", "GM", "id", "id"),
            Column("name", "text", "GM", "summary", "name"),
            Column("description", "text", "G", "description", "NULL"),
            Column("time_zone", "text", "G", "timeZone", "NULL (a calendar has none)"),
            Column("is_primary", "boolean", "GM", "primary", "isDefaultCalendar"),
            Column(
                "can_edit",
                "boolean",
                "GM",
                "accessRole is writer, writerWithoutPrivateAccess or owner",
                "canEdit",
            ),
            Column(
                "access_role",
                "text",
                "G",
                "accessRole",
                "NULL (v1.0 states no role on the calendar)",
                enum="calendar_access_role",
            ),
            Column("owner_address", "text", "GM", "dataOwner, NULL on primary", "owner.address"),
            Column("color", "text", "GM", "backgroundColor", "hexColor"),
            Column("change_key", "text", "GM", "etag", "changeKey"),
            Column("name_override", "text", "G", "summaryOverride", "NULL"),
            Column("is_hidden", "boolean", "G", "hidden", "NULL"),
            Column("is_selected", "boolean", "G", "selected", "NULL"),
            Column("location", "text", "G", "location", "NULL"),
            Column("color_text", "text", "G", "foregroundColor", "NULL"),
            Column("can_share", "boolean", "M", "NULL", "canShare"),
            Column("can_view_private_items", "boolean", "M", "NULL", "canViewPrivateItems"),
            Column("is_removable", "boolean", "M", "NULL", "isRemovable"),
            Column(
                "default_online_meeting_provider",
                "text",
                "M",
                "NULL",
                "defaultOnlineMeetingProvider",
            ),
            Column(
                "allowed_online_meeting_providers",
                "json",
                "M",
                "NULL",
                "allowedOnlineMeetingProviders",
                shape="text list",
            ),
        ),
    ),
    Table(
        "events",
        "Rows: single events, series masters and exceptions. Not every expanded occurrence. Google lists these with `singleEvents=false`. Microsoft's `/events` answers single events and series masters only; exceptions come from one further call per series master. `recurrence` has one encoding. The Microsoft source renders Graph's structured pattern as RFC 5545 lines; that renderer is part of the calendar build, and the column does not ship before it. A cancelled event is a row, for both providers: `is_cancelled` true and `status` `cancelled`.",
        (
            Column("provider", "text", "GM", "", "", enum="provider"),
            Column("account", "text", "GM", "", ""),
            Column("id", "text", "GM", "id", "id"),
            Column("calendar_id", "text", "GM", "calendar listed", "calendar listed"),
            Column("ical_uid", "text", "GM", "iCalUID", "iCalUId"),
            Column("title", "text", "GM", "summary", "subject"),
            Column("snippet", "text", "M", "NULL", "bodyPreview"),
            Column("body", "text", "GM", "description", "body.content"),
            Column(
                "body_format",
                "text",
                "M",
                "NULL (not stated)",
                "body.contentType",
                enum="body_format",
            ),
            Column("location", "text", "GM", "location", "location.displayName"),
            Column("is_all_day", "boolean", "GM", "start.date present", "isAllDay"),
            Column(
                "start_at",
                "timestamp",
                "GM",
                "start.dateTime; NULL when all-day",
                "start; NULL when all-day",
            ),
            Column(
                "end_at",
                "timestamp",
                "GM",
                "end.dateTime; NULL when all-day",
                "end; NULL when all-day",
            ),
            Column(
                "start_date",
                "date",
                "GM",
                "start.date; NULL unless all-day",
                "date of start; NULL unless all-day",
            ),
            Column("end_date", "date", "GM", "end.date (exclusive)", "date of end (exclusive)"),
            Column("start_time_zone", "text", "GM", "start.timeZone", "originalStartTimeZone"),
            Column("end_time_zone", "text", "GM", "end.timeZone", "originalEndTimeZone"),
            Column(
                "status",
                "text",
                "GM",
                "status",
                "`cancelled` when isCancelled, else NULL",
                enum="event_status",
            ),
            Column("is_cancelled", "boolean", "GM", "status = cancelled", "isCancelled"),
            Column("show_as", "text", "GM", "transparency", "showAs", enum="show_as"),
            Column(
                "visibility", "text", "GM", "visibility", "sensitivity", enum="event_visibility"
            ),
            Column("importance", "text", "M", "NULL", "importance", enum="importance"),
            Column("kind", "text", "GM", "derived, see enum", "type", enum="event_kind"),
            Column("series_id", "text", "GM", "recurringEventId", "seriesMasterId"),
            Column("original_start_at", "timestamp", "GM", "originalStartTime", "originalStart"),
            Column(
                "recurrence",
                "text",
                "GM",
                "RFC 5545 lines joined by newline",
                "the pattern rendered as RFC 5545 lines",
            ),
            Column(
                "organizer_address",
                "text",
                "GM",
                "organizer.email",
                "organizer.emailAddress.address",
            ),
            Column(
                "organizer_name",
                "text",
                "GM",
                "organizer.displayName",
                "organizer.emailAddress.name",
            ),
            Column("is_organizer", "boolean", "GM", "organizer.self", "isOrganizer"),
            Column("attendee_addresses", "json", "GM", "attendees", "attendees", shape="text list"),
            Column(
                "is_online_meeting",
                "boolean",
                "GM",
                "hangoutLink or conferenceData present",
                "isOnlineMeeting",
            ),
            Column("online_meeting_url", "text", "GM", "hangoutLink", "onlineMeeting.joinUrl"),
            Column("has_attachments", "boolean", "GM", "attachments present", "hasAttachments"),
            Column("categories", "json", "M", "NULL", "categories", shape="text list"),
            Column("created_at", "timestamp", "GM", "created", "createdDateTime"),
            Column("updated_at", "timestamp", "GM", "updated", "lastModifiedDateTime"),
            Column("change_key", "text", "GM", "etag", "changeKey"),
            Column("web_link", "text", "GM", "htmlLink", "webLink"),
            Column(
                "original_start_date",
                "date",
                "G",
                "originalStartTime.date (all-day series)",
                "NULL",
            ),
            Column("creator_address", "text", "G", "creator.email", "NULL"),
            Column("creator_name", "text", "G", "creator.displayName", "NULL"),
            Column("event_type", "text", "G", "eventType", "NULL", enum="event_type"),
            Column("sequence", "bigint", "G", "sequence", "NULL"),
            Column("is_end_unspecified", "boolean", "G", "endTimeUnspecified", "NULL"),
            Column("guests_can_modify", "boolean", "G", "guestsCanModify", "NULL"),
            Column("guests_can_invite_others", "boolean", "G", "guestsCanInviteOthers", "NULL"),
            Column(
                "guests_can_see_other_guests",
                "boolean",
                "GM",
                "guestsCanSeeOtherGuests",
                "not hideAttendees",
            ),
            Column("reminders", "json", "G", "reminders", "NULL"),
            Column("is_reminder_on", "boolean", "M", "NULL", "isReminderOn"),
            Column(
                "reminder_minutes_before_start", "bigint", "M", "NULL", "reminderMinutesBeforeStart"
            ),
            Column("conference", "json", "GM", "conferenceData", "onlineMeeting"),
            Column("online_meeting_provider", "text", "M", "NULL", "onlineMeetingProvider"),
            Column("attachments", "json", "G", "attachments", "NULL (a separate call per event)"),
            Column("color_id", "text", "G", "colorId", "NULL"),
            Column("locations", "json", "M", "NULL", "locations"),
            Column("is_draft", "boolean", "M", "NULL", "isDraft"),
            Column("is_response_requested", "boolean", "M", "NULL", "responseRequested"),
            Column("allow_new_time_proposals", "boolean", "M", "NULL", "allowNewTimeProposals"),
            Column(
                "my_response",
                "text",
                "GM",
                "the attendee marked self",
                "responseStatus.response",
                enum="attendee_response",
            ),
            Column(
                "cancelled_occurrences",
                "json",
                "M",
                "NULL",
                "cancelledOccurrences (series master)",
                shape="text list",
            ),
        ),
    ),
    Table(
        "event_attendees",
        "",
        (
            Column("provider", "text", "GM", "", "", enum="provider"),
            Column("account", "text", "GM", "", ""),
            Column("event_id", "text", "GM", "", ""),
            Column("position", "bigint", "GM", "", ""),
            Column("address", "text", "GM", "email", "emailAddress.address"),
            Column("name", "text", "GM", "displayName", "emailAddress.name"),
            Column("kind", "text", "GM", "optional, resource flags", "type", enum="attendee_kind"),
            Column(
                "response",
                "text",
                "GM",
                "responseStatus",
                "status.response",
                enum="attendee_response",
            ),
            Column("responded_at", "timestamp", "M", "NULL", "status.time"),
            Column("is_organizer", "boolean", "GM", "organizer", "status.response = organizer"),
            Column("comment", "text", "G", "comment", "NULL"),
            Column("additional_guests", "bigint", "G", "additionalGuests", "NULL"),
            Column("is_self", "boolean", "GM", "self", "address = account"),
        ),
    ),
    Table(
        "task_lists",
        "",
        (
            Column("provider", "text", "GM", "", "", enum="provider"),
            Column("account", "text", "GM", "", ""),
            Column("id", "text", "GM", "id", "id"),
            Column("name", "text", "GM", "title", "displayName"),
            Column("is_default", "boolean", "M", "NULL", "wellknownListName = defaultList"),
            Column("role", "text", "M", "NULL", "wellknownListName", enum="task_list_role"),
            Column("is_shared", "boolean", "M", "NULL", "isShared"),
            Column("updated_at", "timestamp", "G", "updated", "NULL"),
            Column("change_key", "text", "G", "etag", "NULL"),
        ),
    ),
    Table(
        "tasks",
        "",
        (
            Column("provider", "text", "GM", "", "", enum="provider"),
            Column("account", "text", "GM", "", ""),
            Column("id", "text", "GM", "id", "id"),
            Column("task_list_id", "text", "GM", "list read", "list read"),
            Column("parent_id", "text", "G", "parent", "NULL"),
            Column("position", "text", "G", "position", "NULL"),
            Column("title", "text", "GM", "title", "title"),
            Column("notes", "text", "GM", "notes", "body.content asked as text"),
            Column("status", "text", "GM", "status", "status", enum="task_status"),
            Column("vendor_status", "text", "GM", "status as given", "status as given"),
            Column("importance", "text", "M", "NULL", "importance", enum="importance"),
            Column("due_date", "date", "GM", "due (day only)", "date of dueDateTime"),
            Column("due_at", "timestamp", "M", "NULL", "dueDateTime"),
            Column("start_at", "timestamp", "M", "NULL", "startDateTime"),
            Column("completed_at", "timestamp", "GM", "completed", "completedDateTime"),
            Column("reminder_at", "timestamp", "M", "NULL", "reminderDateTime"),
            Column("recurrence", "text", "M", "NULL", "the pattern rendered as RFC 5545 lines"),
            Column("categories", "json", "M", "NULL", "categories", shape="text list"),
            Column("has_attachments", "boolean", "M", "NULL", "hasAttachments"),
            Column("is_deleted", "boolean", "G", "deleted", "NULL"),
            Column("is_hidden", "boolean", "G", "hidden", "NULL"),
            Column("created_at", "timestamp", "M", "NULL", "createdDateTime"),
            Column("updated_at", "timestamp", "GM", "updated", "lastModifiedDateTime"),
            Column("web_link", "text", "G", "webViewLink", "NULL"),
            Column("change_key", "text", "G", "etag", "NULL"),
            Column("links", "json", "G", "links", "NULL"),
            Column("assignment_info", "json", "G", "assignmentInfo", "NULL"),
            Column("is_reminder_on", "boolean", "M", "NULL", "isReminderOn"),
            Column("notes_updated_at", "timestamp", "M", "NULL", "bodyLastModifiedDateTime"),
        ),
    ),
)

TABLES: Mapping[str, Table] = MappingProxyType({t.name: t for t in _TABLES})


def ir_columns(table: str) -> list[tuple[str, str]]:
    """The landing columns of a canonical table, in order: (name, IR type) pairs, as
    ``CursorSource`` takes them."""
    return [(c.name, c.type) for c in TABLES[table].columns]


# --------------------------------------------------------------------------- enumerations


@dataclass(frozen=True)
class Derived:
    """A provider has no field holding this value: its loader works the canonical value out as
    ``how`` says and writes it, checked by :func:`canonical`."""

    how: str


@dataclass(frozen=True)
class Enum:
    values: tuple[str, ...]
    # Per provider: a table from the provider's values to canonical ones (None where the
    # provider's value means the column is NULL), a Derived, or absent where the provider never
    # populates the column.
    providers: Mapping[str, Mapping[str, str | None] | Derived] = field(default_factory=dict)
    # A provider value its table does not list takes this canonical value. The one enumeration
    # that has it is the folder role: both vendors' sets of system folders are open.
    catch_all: Mapping[str, str] = field(default_factory=dict)


def _same(*values: str) -> dict[str, str | None]:
    return {v: v for v in values}


_RESPONSE = ("needs_action", "accepted", "tentative", "declined")
_IMPORTANCE = ("low", "normal", "high")

ENUMS: Mapping[str, Enum] = MappingProxyType(
    {
        "provider": Enum(PROVIDERS),
        "flag_status": Enum(
            ("none", "flagged", "complete"),
            {
                MICROSOFT: {"notFlagged": "none", "flagged": "flagged", "complete": "complete"},
                GOOGLE: Derived("STARRED label present→flagged, else none"),
            },
        ),
        "importance": Enum(_IMPORTANCE, {MICROSOFT: _same(*_IMPORTANCE)}),
        "inference_classification": Enum(
            ("focused", "other"), {MICROSOFT: _same("focused", "other")}
        ),
        "recipient_kind": Enum(
            ("from", "sender", "to", "cc", "bcc", "reply_to"),
            {MICROSOFT: Derived("by property"), GOOGLE: Derived("by header")},
        ),
        "folder_kind": Enum(
            ("system", "user"),
            {
                MICROSOFT: Derived("role present→system, else user"),
                GOOGLE: _same("system", "user"),
            },
        ),
        "folder_role": Enum(
            (
                "inbox",
                "sent",
                "drafts",
                "trash",
                "spam",
                "archive",
                "outbox",
                "flagged",
                "important",
                "unread",
                "category",
                "other",
            ),
            {
                MICROSOFT: {
                    "inbox": "inbox",
                    "sentitems": "sent",
                    "drafts": "drafts",
                    "deleteditems": "trash",
                    "junkemail": "spam",
                    "archive": "archive",
                    "outbox": "outbox",
                },
                GOOGLE: {
                    "INBOX": "inbox",
                    "SENT": "sent",
                    "DRAFT": "drafts",
                    "TRASH": "trash",
                    "SPAM": "spam",
                    "STARRED": "flagged",
                    "IMPORTANT": "important",
                    "UNREAD": "unread",
                    "CATEGORY_PERSONAL": "category",
                    "CATEGORY_SOCIAL": "category",
                    "CATEGORY_PROMOTIONS": "category",
                    "CATEGORY_UPDATES": "category",
                    "CATEGORY_FORUMS": "category",
                },
            },
            catch_all={MICROSOFT: "other", GOOGLE: "other"},
        ),
        "label_list_visibility": Enum(
            ("show", "show_if_unread", "hide"),
            {
                GOOGLE: {
                    "labelShow": "show",
                    "labelShowIfUnread": "show_if_unread",
                    "labelHide": "hide",
                }
            },
        ),
        "message_list_visibility": Enum(("show", "hide"), {GOOGLE: _same("show", "hide")}),
        "attachment_kind": Enum(
            ("file", "item", "reference"),
            {
                MICROSOFT: {
                    "#microsoft.graph.fileAttachment": "file",
                    "#microsoft.graph.itemAttachment": "item",
                    "#microsoft.graph.referenceAttachment": "reference",
                },
                GOOGLE: Derived("file"),
            },
        ),
        "calendar_access_role": Enum(
            ("owner", "writer", "writer_without_private_access", "reader", "free_busy_reader"),
            {
                GOOGLE: {
                    "owner": "owner",
                    "writer": "writer",
                    "writerWithoutPrivateAccess": "writer_without_private_access",
                    "reader": "reader",
                    "freeBusyReader": "free_busy_reader",
                }
            },
        ),
        "body_format": Enum(("text", "html"), {MICROSOFT: _same("text", "html")}),
        "event_status": Enum(
            ("confirmed", "tentative", "cancelled"),
            {
                MICROSOFT: Derived("isCancelled→cancelled, else NULL"),
                GOOGLE: _same("confirmed", "tentative", "cancelled"),
            },
        ),
        "show_as": Enum(
            ("free", "busy", "tentative", "out_of_office", "working_elsewhere", "unknown"),
            {
                MICROSOFT: {
                    "free": "free",
                    "busy": "busy",
                    "tentative": "tentative",
                    "oof": "out_of_office",
                    "workingElsewhere": "working_elsewhere",
                    "unknown": "unknown",
                },
                GOOGLE: {"transparent": "free", "opaque": "busy"},
            },
        ),
        "event_visibility": Enum(
            ("default", "public", "personal", "private", "confidential"),
            {
                MICROSOFT: {
                    "normal": "default",
                    "personal": "personal",
                    "private": "private",
                    "confidential": "confidential",
                },
                GOOGLE: _same("default", "public", "private", "confidential"),
            },
        ),
        "event_kind": Enum(
            ("single", "series_master", "exception", "occurrence"),
            {
                MICROSOFT: {
                    "singleInstance": "single",
                    "seriesMaster": "series_master",
                    "exception": "exception",
                    "occurrence": "occurrence",
                },
                GOOGLE: Derived(
                    "recurrence present→series_master, recurringEventId present→exception, "
                    "else single"
                ),
            },
        ),
        "event_type": Enum(
            (
                "default",
                "birthday",
                "focus_time",
                "from_gmail",
                "out_of_office",
                "working_location",
            ),
            {
                GOOGLE: {
                    "default": "default",
                    "birthday": "birthday",
                    "focusTime": "focus_time",
                    "fromGmail": "from_gmail",
                    "outOfOffice": "out_of_office",
                    "workingLocation": "working_location",
                }
            },
        ),
        "attendee_kind": Enum(
            ("required", "optional", "resource"),
            {
                MICROSOFT: _same("required", "optional", "resource"),
                GOOGLE: Derived("resource flag→resource, optional flag→optional, else required"),
            },
        ),
        "attendee_response": Enum(
            _RESPONSE,
            {
                MICROSOFT: {
                    "none": "needs_action",
                    "notResponded": "needs_action",
                    "tentativelyAccepted": "tentative",
                    "accepted": "accepted",
                    "declined": "declined",
                    "organizer": None,  # is_organizer carries it
                },
                GOOGLE: {
                    "needsAction": "needs_action",
                    "accepted": "accepted",
                    "tentative": "tentative",
                    "declined": "declined",
                },
            },
        ),
        "task_list_role": Enum(
            ("default", "flagged_emails", "none"),
            {
                MICROSOFT: {
                    "defaultList": "default",
                    "flaggedEmails": "flagged_emails",
                    "none": "none",
                }
            },
        ),
        "task_status": Enum(
            ("open", "done"),
            {
                MICROSOFT: {
                    "notStarted": "open",
                    "inProgress": "open",
                    "waitingOnOthers": "open",
                    "deferred": "open",
                    "completed": "done",
                },
                GOOGLE: {"needsAction": "open", "completed": "done"},
            },
        ),
    }
)


class UnmappedValue(ValueError):
    """A provider gave an enumeration a value its translation table does not list."""

    def __init__(self, enum: str, provider: str, value: object) -> None:
        super().__init__(
            f"{provider} gave {value!r} for the enumeration {enum!r}, which its translation "
            f"table does not list; the canonical values are {', '.join(ENUMS[enum].values)}"
        )
        self.enum = enum
        self.provider = provider
        self.value = value


def translate(enum: str, provider: str, value: str | None) -> str | None:
    """The canonical value of ``value``, as ``provider`` gave it for the enumeration ``enum``.
    None stays None (the provider gave nothing). A value the provider's table does not list is
    refused by name, except where the enumeration declares a catch-all for that provider."""
    if value is None:
        return None
    spec = ENUMS[enum]
    table = spec.providers.get(provider)
    if not isinstance(table, Mapping):
        raise ValueError(
            f"{provider} has no translation table for the enumeration {enum!r}: its loader "
            "writes no value there, or works the canonical one out (canonical())"
        )
    if value in table:
        return table[value]
    if provider in spec.catch_all:
        return spec.catch_all[provider]
    raise UnmappedValue(enum, provider, value)


def canonical(enum: str, value: str | None) -> str | None:
    """``value`` as a canonical value of ``enum`` that a loader worked out itself: returned as
    given, refused by name when it is not one."""
    if value is not None and value not in ENUMS[enum].values:
        raise ValueError(
            f"{value!r} is not a canonical value of the enumeration {enum!r}: "
            f"{', '.join(ENUMS[enum].values)}"
        )
    return value


# ------------------------------------------------------------------------------ for the reader


def _type_cell(column: Column) -> str:
    cell = column.type + (" enum" if column.enum and column.name != "provider" else "")
    return cell if column.shape is None else f"{cell} ({column.shape})"


def render_tables() -> str:
    """The tables as the schema document states them."""
    out: list[str] = []
    for table in TABLES.values():
        out += [f"## {table.name}", ""]
        if table.note:
            out += [table.note, ""]
        out += ["| # | column | type | P | Google | Microsoft |", "|---|---|---|---|---|---|"]
        out += [
            f"| {n} | {c.name} | {_type_cell(c)} | {c.populated} | {c.google} | {c.microsoft} |"
            for n, c in enumerate(table.columns, 1)
        ]
        out.append("")
    return "\n".join(out)


def _provider_cell(spec: Enum, provider: str) -> str:
    table = spec.providers.get(provider)
    if table is None:
        return "none: NULL"
    if isinstance(table, Derived):
        return f"derived: {table.how}"
    parts = [k if v == k else f"{k}→{'NULL' if v is None else v}" for k, v in table.items()]
    if provider in spec.catch_all:
        parts.append(f"any other→{spec.catch_all[provider]} (declared catch-all)")
    return ", ".join(parts)


def render_enums() -> str:
    """The enumerations as the schema document states them: each one's canonical values, the
    columns that hold it, and each provider's translation table."""
    out = [
        "| enumeration | columns | canonical values | Microsoft | Google |",
        "|---|---|---|---|---|",
    ]
    for name, spec in ENUMS.items():
        if name == "provider":
            continue
        used = ", ".join(
            f"{t.name}.{c.name}" for t in TABLES.values() for c in t.columns if c.enum == name
        )
        out.append(
            f"| {name} | {used} | {', '.join(spec.values)} | "
            f"{_provider_cell(spec, MICROSOFT)} | {_provider_cell(spec, GOOGLE)} |"
        )
    return "\n".join(out) + "\n"
