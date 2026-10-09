# Copyright (c) 2026 Kenneth Stott
# Canary: 58611a9b-85cb-4542-a0ce-999ae1799f81
#
# This source code is licensed under the Business Source License 1.1
# found in the LICENSE file in the root directory of this source tree.
#
# NOTICE: Use of this software for training artificial intelligence or
# machine learning models is strictly prohibited without explicit written
# permission from the copyright holder.

"""A Gmail message, as Gmail answers it, read into the facts the canonical mail tables hold
(REQ-1923; docs/arch/canonical-mail-schema.md).

Gmail answers a message as an id, its label ids, and a tree of MIME parts whose headers are a
list of name and value. What a registrar reads as columns -- the subject, who it is from and to,
when it was sent, its text -- is in those headers and parts, so it is read out here, once, by
the rules of the mail standards and not by guessing:

- a header is found by its name without regard to case, and its first occurrence is taken;
- encoded words in a header (RFC 2047) are decoded; an address list is parsed as RFC 5322 says;
- the text of a message is its first ``text/plain`` part that is not an attachment, its HTML
  the first ``text/html`` one, each decoded by the charset its part declares (RFC 2045: a part
  that declares none is US-ASCII);
- an attachment is a part with a file name.

A fact Gmail did not answer is None, never an empty value standing in for it: a message read
as headers and labels only has no text, and says so by having none.

A part that is not what it declares (a charset that does not exist, bytes that are not that
charset) leaves the message's row with no text and every other column filled, and the message
is marked (``MessageFacts.unreadable``) so that the read can count and name it.
"""

# Requirements: REQ-1923
from __future__ import annotations

import base64
import datetime as dt
from dataclasses import dataclass, field
from email.header import decode_header, make_header
from email.message import Message
from email.utils import getaddresses, parsedate_to_datetime
from typing import Any

PROVIDER = "google"
_RECIPIENT_HEADERS = (
    ("from", "From"),
    ("sender", "Sender"),
    ("to", "To"),
    ("cc", "Cc"),
    ("bcc", "Bcc"),
    ("reply_to", "Reply-To"),
)


class UnreadableMessage(ValueError):
    """A message a part of which cannot be read as it declares itself."""

    def __init__(self, message_id: str, why: str) -> None:
        super().__init__(f"message {message_id}: {why}")
        self.message_id = message_id


@dataclass(frozen=True)
class MessageFacts:
    """One message's rows: its own, and those of its recipients, folders and attachments."""

    message: dict[str, Any]
    recipients: list[dict[str, Any]] = field(default_factory=list)
    folders: list[dict[str, Any]] = field(default_factory=list)
    attachments: list[dict[str, Any]] = field(default_factory=list)
    #: Why the message's text could not be read as its part declares it, when it could not: the
    #: row is kept with no text, and whoever reads the mailbox counts and names such messages.
    unreadable: str | None = None


def _header(headers: list[dict], name: str) -> str | None:
    wanted = name.lower()
    for header in headers:
        if header.get("name", "").lower() == wanted:
            return header.get("value")
    return None


def _decoded(value: str | None) -> str | None:
    """A header's text with its encoded words (RFC 2047) decoded."""
    if value is None:
        return None
    return str(make_header(decode_header(value)))


def _addresses(value: str | None) -> list[tuple[str | None, str]]:
    """The (name, address) pairs of an address-list header, in its order."""
    if not value:
        return []
    return [(_decoded(name) or None, address) for name, address in getaddresses([value]) if address]


def _utc(moment: dt.datetime) -> dt.datetime:
    """``moment`` in UTC without its zone, as a timestamp column holds one."""
    return moment.astimezone(dt.timezone.utc).replace(tzinfo=None)


def _sent_at(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None  # the schema: NULL when the Date header does not parse
    if moment.tzinfo is None:  # RFC 5322 "-0000": a time in UTC whose zone is not stated
        moment = moment.replace(tzinfo=dt.timezone.utc)
    return _utc(moment)


def _received_at(internal_date: str | None) -> dt.datetime | None:
    if internal_date is None:
        return None
    return _utc(dt.datetime.fromtimestamp(int(internal_date) / 1000, dt.timezone.utc))


def _parts(part: dict) -> list[dict]:
    """``part`` and every part under it, depth first, in the message's order."""
    found = [part]
    for child in part.get("parts") or []:
        found.extend(_parts(child))
    return found


def _content_type(part: dict) -> Message:
    """The part's Content-Type header, parsed for its parameters."""
    parsed = Message()
    declared = _header(part.get("headers") or [], "Content-Type")
    if declared:
        parsed["Content-Type"] = declared
    return parsed


def _text(message_id: str, part: dict) -> str | None:
    data = (part.get("body") or {}).get("data")
    if data is None:
        return None
    raw = base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))
    charset = _content_type(part).get_content_charset() or "us-ascii"  # RFC 2045 section 5.2
    try:
        return raw.decode(charset)
    except LookupError:
        raise UnreadableMessage(message_id, f"a part declares charset {charset!r}") from None
    except UnicodeDecodeError:
        raise UnreadableMessage(
            message_id, f"a part declared as {charset} is not {charset}"
        ) from None


def _is_inline(part: dict) -> bool:
    disposition = _header(part.get("headers") or [], "Content-Disposition") or ""
    return disposition.split(";")[0].strip().lower() == "inline"


def _attachment(part: dict) -> dict[str, Any]:
    content_id = _header(part.get("headers") or [], "Content-ID")
    return {
        "id": part.get("partId"),
        "name": part["filename"],
        "content_type": part.get("mimeType"),
        "size_bytes": (part.get("body") or {}).get("size"),
        "is_inline": _is_inline(part),
        "content_id": content_id.strip().strip("<>") if content_id else None,
        "kind": "file",
        "updated_at": None,
    }


def message_facts(account: str, answered: dict) -> MessageFacts:
    """The rows of the message Gmail ``answered`` (messages.get) for ``account``."""
    message_id = answered["id"]
    payload = answered.get("payload") or {}
    headers = payload.get("headers") or []
    parts = _parts(payload) if payload else []
    labels: list[str] = list(answered.get("labelIds") or [])
    key = {"provider": PROVIDER, "account": account}

    people = {kind: _addresses(_header(headers, name)) for kind, name in _RECIPIENT_HEADERS}
    files = [_attachment(p) for p in parts if p.get("filename")]
    bodies = [p for p in parts if not p.get("filename")]

    unreadable: list[str] = []

    def body(mime_type: str) -> str | None:
        for part in bodies:
            if part.get("mimeType") == mime_type:
                try:
                    return _text(message_id, part)
                except UnreadableMessage as why:
                    unreadable.append(str(why))
                    return None
        return None

    def first(kind: str, index: int) -> str | None:
        return people[kind][0][index] if people[kind] else None

    def listed(kind: str) -> list[str]:
        return [address for _, address in people[kind]]

    references = _header(headers, "References")
    flagged = "STARRED" in labels
    message = {
        **key,
        "id": message_id,
        "thread_id": answered.get("threadId"),
        "internet_message_id": _header(headers, "Message-ID"),
        "in_reply_to": _header(headers, "In-Reply-To"),
        "reference_ids": references.split() if references else [],
        "subject": _decoded(_header(headers, "Subject")),
        "snippet": answered.get("snippet"),
        "from_address": first("from", 1),
        "from_name": first("from", 0),
        "sender_address": first("sender", 1),
        "to_addresses": listed("to"),
        "cc_addresses": listed("cc"),
        "bcc_addresses": listed("bcc"),
        "reply_to_addresses": listed("reply_to"),
        "sent_at": _sent_at(_header(headers, "Date")),
        "received_at": _received_at(answered.get("internalDate")),
        "created_at": None,
        "updated_at": None,
        "is_read": "UNREAD" not in labels,
        "is_draft": "DRAFT" in labels,
        "is_flagged": flagged,
        "flag_status": "flagged" if flagged else "none",
        "importance": None,
        "has_attachments": any(not f["is_inline"] for f in files),
        "size_bytes": answered.get("sizeEstimate"),
        "folder_ids": labels,
        "categories": None,
        "inference_classification": None,
        "is_read_receipt_requested": None,
        "is_delivery_receipt_requested": None,
        "body_text": body("text/plain"),
        "body_html": body("text/html"),
        "headers": [{"name": h.get("name"), "value": h.get("value")} for h in headers],
        "change_key": answered.get("historyId"),
        "web_link": None,
        "classification_labels": answered.get("classificationLabelValues"),
        "conversation_index": None,
        "flag_start_at": None,
        "flag_due_at": None,
        "flag_completed_at": None,
    }
    recipients = [
        {
            **key,
            "message_id": message_id,
            "kind": kind,
            "position": position,
            "address": address,
            "name": name,
        }
        for kind, _ in _RECIPIENT_HEADERS
        for position, (name, address) in enumerate(people[kind])
    ]
    folders = [{**key, "message_id": message_id, "folder_id": label} for label in labels]
    attachments = [{**key, "message_id": message_id, **f} for f in files]
    return MessageFacts(message, recipients, folders, attachments, "; ".join(unreadable) or None)
