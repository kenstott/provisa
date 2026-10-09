# Canonical mail, calendar and task tables

Status: agreed. Written by the Exchange teammate, co-signed by the Gmail teammate on 2026-10-09.
Awaiting the maintainer.

The Google Workspace source and the Microsoft 365 source each produce every table below with the
same column names, types, order and meaning, so `UNION ALL` of the two needs no cast or rename.

## Rules

1. Every table starts with `provider` (`google` or `microsoft`) and `account` (the mailbox
   address, lower case). The key of every table is (`provider`, `account`, its id columns).
2. The column set is the superset of both providers. A column a provider has no value for is a
   NULL of the column's type. An empty string, zero or false never stands in for "not offered".
3. One fact, one column. No `gmail_x` beside `exchange_x`.
4. Ids are `text`, as the provider gives them. Microsoft ids are asked for as immutable ids
   (`Prefer: IdType="ImmutableId"`).
5. Both sources land their tables through the replica write face as (name, IR type) columns:
   `CursorSource` (`provisa/federation/replica_source.py:126`) builds its Arrow schema from
   them (`provisa/core/ir_arrow.py:67-71`). Neither goes through `api_endpoints`, whose five
   column types (`provisa/api_source/models.py:40-45`) have no timestamp or date.
6. Types are IR types, and these are the only ones used: `text` (Arrow string,
   `ir_arrow.py:39`), `bigint` (int64, `:38`), `boolean` (`:40`), `date` (date32, `:44`),
   `timestamp` (Arrow timestamp in microseconds, `:45`, `:62-63`), `json` (`:50`).
7. `*_at` is `timestamp`, the value in UTC. The IR has one timestamp: `timestamptz` is an alias
   of it (`provisa/core/ir_types.py:93`) and a value with a zone is converted to UTC and landed
   without one (`ir_arrow.py:91-95`). `*_date` is `date`. Where the provider states a zone it
   sits beside the value as `*_time_zone` (`text`, as the provider names it; not translated).
8. A list is `json`: a JSON array held as text (`ir_arrow.py:50`, `:81-82`). The IR has no array
   type; an array or list in a store's own vocabulary is collapsed to `text`
   (`ir_types.py:116`, `:151`). Where rows are needed for a join, a child table carries the
   same fact as rows (`message_folders`, `message_recipients`, `event_attendees`).
9. An enumeration column holds a canonical value. Each provider declares a table from its
   values to the canonical ones; a provider value the table does not list fails the build of
   that table by name. A row in a table below that maps to NULL is a declared mapping. Where a
   vendor says its own set is open, the table carries a declared catch-all and says so.

P = populated by: G Google, M Microsoft, GM both.

## messages

| # | column | type | P | Google | Microsoft |
|---|---|---|---|---|---|
| 1 | provider | text | GM | `google` | `microsoft` |
| 2 | account | text | GM | mailbox address | `{user-id}` of the path |
| 3 | id | text | GM | id | id |
| 4 | thread_id | text | GM | threadId | conversationId |
| 5 | internet_message_id | text | GM | Message-ID header | internetMessageId |
| 6 | in_reply_to | text | GM | In-Reply-To header | same header |
| 7 | reference_ids | json (text list) | GM | References header | same header |
| 8 | subject | text | GM | Subject header | subject |
| 9 | snippet | text | GM | snippet | bodyPreview (255 characters) |
| 10 | from_address | text | GM | From header | from.emailAddress.address |
| 11 | from_name | text | GM | From header | from.emailAddress.name |
| 12 | sender_address | text | GM | Sender header | sender.emailAddress.address |
| 13 | to_addresses | json (text list) | GM | To header | toRecipients |
| 14 | cc_addresses | json (text list) | GM | Cc header | ccRecipients |
| 15 | bcc_addresses | json (text list) | GM | Bcc header | bccRecipients |
| 16 | reply_to_addresses | json (text list) | GM | Reply-To header | replyTo |
| 17 | sent_at | timestamp | GM | Date header, NULL when unparsable | sentDateTime |
| 18 | received_at | timestamp | GM | internalDate | receivedDateTime |
| 19 | created_at | timestamp | M | NULL | createdDateTime |
| 20 | updated_at | timestamp | M | NULL | lastModifiedDateTime |
| 21 | is_read | boolean | GM | UNREAD label absent | isRead |
| 22 | is_draft | boolean | GM | DRAFT label present | isDraft |
| 23 | is_flagged | boolean | GM | STARRED label present | flag.flagStatus = flagged |
| 24 | flag_status | text enum | GM | see enum | flag.flagStatus |
| 25 | importance | text enum | M | NULL | importance |
| 26 | has_attachments | boolean | GM | a part with a filename that is not inline | hasAttachments (inline not counted) |
| 27 | size_bytes | bigint | G | sizeEstimate | NULL (v1.0 message has no size) |
| 28 | folder_ids | json (text list) | GM | labelIds | one element: parentFolderId |
| 29 | categories | json (text list) | M | NULL | categories |
| 30 | inference_classification | text enum | M | NULL | inferenceClassification |
| 31 | is_read_receipt_requested | boolean | M | NULL | isReadReceiptRequested |
| 32 | is_delivery_receipt_requested | boolean | M | NULL | isDeliveryReceiptRequested |
| 33 | body_text | text | GM | text/plain part, NULL when the message has none | body asked as text |
| 34 | body_html | text | GM | text/html part, NULL when the message has none | body asked as html |
| 35 | headers | json (list of {name, value}) | GM | payload.headers | internetMessageHeaders |
| 36 | change_key | text | GM | historyId | changeKey |
| 37 | web_link | text | M | NULL | webLink |
| 38 | classification_labels | json | G | classificationLabelValues | NULL |
| 39 | conversation_index | text | M | NULL | conversationIndex (base64) |
| 40 | flag_start_at | timestamp | M | NULL | flag.startDateTime |
| 41 | flag_due_at | timestamp | M | NULL | flag.dueDateTime |
| 42 | flag_completed_at | timestamp | M | NULL | flag.completedDateTime |

`folder_id` (single) is not a column: it is the same fact as `folder_ids`.

## message_recipients

Names beside addresses, as rows. `kind`: `from`, `sender`, `to`, `cc`, `bcc`, `reply_to`.

| # | column | type | P |
|---|---|---|---|
| 1 | provider | text | GM |
| 2 | account | text | GM |
| 3 | message_id | text | GM |
| 4 | kind | text enum | GM |
| 5 | position | bigint | GM |
| 6 | address | text | GM |
| 7 | name | text | GM |

## folders

A named container of messages: a Gmail label, an Exchange mail folder.

| # | column | type | P | Google | Microsoft |
|---|---|---|---|---|---|
| 1 | provider | text | GM | | |
| 2 | account | text | GM | | |
| 3 | id | text | GM | id | id |
| 4 | name | text | GM | name | displayName |
| 5 | parent_id | text | M | NULL | parentFolderId |
| 6 | kind | text enum | GM | type | `system` when it has a role, else `user` |
| 7 | role | text enum | GM | see enum | well-known folder name, see enum |
| 8 | is_hidden | boolean | GM | labelListVisibility = labelHide | isHidden |
| 9 | total_count | bigint | GM | messagesTotal | totalItemCount |
| 10 | unread_count | bigint | GM | messagesUnread | unreadItemCount |
| 11 | child_count | bigint | M | NULL | childFolderCount |
| 12 | threads_total | bigint | G | threadsTotal | NULL |
| 13 | threads_unread | bigint | G | threadsUnread | NULL |
| 14 | color_text | text | G | color.textColor | NULL |
| 15 | color_background | text | G | color.backgroundColor | NULL |
| 16 | message_list_visibility | text enum | G | messageListVisibility | NULL |
| 17 | label_list_visibility | text enum | G | labelListVisibility | NULL |

## message_folders

One row per message per container. Google: many per message. Microsoft: one.

| # | column | type | P |
|---|---|---|---|
| 1 | provider | text | GM |
| 2 | account | text | GM |
| 3 | message_id | text | GM |
| 4 | folder_id | text | GM |

## threads

Derived by both sources from the messages they hold, grouped by `thread_id`.

| # | column | type | P | meaning |
|---|---|---|---|---|
| 1 | provider | text | GM | |
| 2 | account | text | GM | |
| 3 | id | text | GM | thread_id |
| 4 | subject | text | GM | of the earliest message by received_at |
| 5 | snippet | text | GM | of the latest message |
| 6 | first_message_at | timestamp | GM | |
| 7 | last_message_at | timestamp | GM | |
| 8 | message_count | bigint | GM | |

## attachments

Metadata only. Content is not in the first build.

| # | column | type | P | Google | Microsoft |
|---|---|---|---|---|---|
| 1 | provider | text | GM | | |
| 2 | account | text | GM | | |
| 3 | message_id | text | GM | | |
| 4 | id | text | GM | partId | id |
| 5 | name | text | GM | filename | name |
| 6 | content_type | text | GM | mimeType | contentType |
| 7 | size_bytes | bigint | GM | body.size | size |
| 8 | is_inline | boolean | GM | Content-Disposition | isInline |
| 9 | content_id | text | GM | Content-ID header | contentId (file attachments) |
| 10 | kind | text enum | GM | `file` | attachment type, see enum |
| 11 | updated_at | timestamp | M | NULL | lastModifiedDateTime |

## calendars

| # | column | type | P | Google | Microsoft |
|---|---|---|---|---|---|
| 1 | provider | text | GM | | |
| 2 | account | text | GM | | |
| 3 | id | text | GM | id | id |
| 4 | name | text | GM | summary | name |
| 5 | description | text | G | description | NULL |
| 6 | time_zone | text | G | timeZone | NULL (a calendar has none) |
| 7 | is_primary | boolean | GM | primary | isDefaultCalendar |
| 8 | can_edit | boolean | GM | accessRole is writer, writerWithoutPrivateAccess or owner | canEdit |
| 9 | access_role | text enum | G | accessRole | NULL (v1.0 states no role on the calendar) |
| 10 | owner_address | text | GM | dataOwner, NULL on primary | owner.address |
| 11 | color | text | GM | backgroundColor | hexColor |
| 12 | change_key | text | GM | etag | changeKey |
| 13 | name_override | text | G | summaryOverride | NULL |
| 14 | is_hidden | boolean | G | hidden | NULL |
| 15 | is_selected | boolean | G | selected | NULL |
| 16 | location | text | G | location | NULL |
| 17 | color_text | text | G | foregroundColor | NULL |
| 18 | can_share | boolean | M | NULL | canShare |
| 19 | can_view_private_items | boolean | M | NULL | canViewPrivateItems |
| 20 | is_removable | boolean | M | NULL | isRemovable |
| 21 | default_online_meeting_provider | text | M | NULL | defaultOnlineMeetingProvider |
| 22 | allowed_online_meeting_providers | json (text list) | M | NULL | allowedOnlineMeetingProviders |

## events

Rows: single events, series masters and exceptions. Not every expanded occurrence.
Google lists these with `singleEvents=false`. Microsoft's `/events` answers single events and
series masters only; exceptions come from one further call per series master.

| # | column | type | P | Google | Microsoft |
|---|---|---|---|---|---|
| 1 | provider | text | GM | | |
| 2 | account | text | GM | | |
| 3 | id | text | GM | id | id |
| 4 | calendar_id | text | GM | calendar listed | calendar listed |
| 5 | ical_uid | text | GM | iCalUID | iCalUId |
| 6 | title | text | GM | summary | subject |
| 7 | snippet | text | M | NULL | bodyPreview |
| 8 | body | text | GM | description | body.content |
| 9 | body_format | text enum | M | NULL (not stated) | body.contentType |
| 10 | location | text | GM | location | location.displayName |
| 11 | is_all_day | boolean | GM | start.date present | isAllDay |
| 12 | start_at | timestamp | GM | start.dateTime; NULL when all-day | start; NULL when all-day |
| 13 | end_at | timestamp | GM | end.dateTime; NULL when all-day | end; NULL when all-day |
| 14 | start_date | date | GM | start.date; NULL unless all-day | date of start; NULL unless all-day |
| 15 | end_date | date | GM | end.date (exclusive) | date of end (exclusive) |
| 16 | start_time_zone | text | GM | start.timeZone | originalStartTimeZone |
| 17 | end_time_zone | text | GM | end.timeZone | originalEndTimeZone |
| 18 | status | text enum | GM | status | `cancelled` when isCancelled, else NULL |
| 19 | is_cancelled | boolean | GM | status = cancelled | isCancelled |
| 20 | show_as | text enum | GM | transparency | showAs |
| 21 | visibility | text enum | GM | visibility | sensitivity |
| 22 | importance | text enum | M | NULL | importance |
| 23 | kind | text enum | GM | derived, see enum | type |
| 24 | series_id | text | GM | recurringEventId | seriesMasterId |
| 25 | original_start_at | timestamp | GM | originalStartTime | originalStart |
| 26 | recurrence | text | GM | RFC 5545 lines joined by newline | the pattern rendered as RFC 5545 lines |
| 27 | organizer_address | text | GM | organizer.email | organizer.emailAddress.address |
| 28 | organizer_name | text | GM | organizer.displayName | organizer.emailAddress.name |
| 29 | is_organizer | boolean | GM | organizer.self | isOrganizer |
| 30 | attendee_addresses | json (text list) | GM | attendees | attendees |
| 31 | is_online_meeting | boolean | GM | hangoutLink or conferenceData present | isOnlineMeeting |
| 32 | online_meeting_url | text | GM | hangoutLink | onlineMeeting.joinUrl |
| 33 | has_attachments | boolean | GM | attachments present | hasAttachments |
| 34 | categories | json (text list) | M | NULL | categories |
| 35 | created_at | timestamp | GM | created | createdDateTime |
| 36 | updated_at | timestamp | GM | updated | lastModifiedDateTime |
| 37 | change_key | text | GM | etag | changeKey |
| 38 | web_link | text | GM | htmlLink | webLink |
| 39 | original_start_date | date | G | originalStartTime.date (all-day series) | NULL |
| 40 | creator_address | text | G | creator.email | NULL |
| 41 | creator_name | text | G | creator.displayName | NULL |
| 42 | event_type | text enum | G | eventType | NULL |
| 43 | sequence | bigint | G | sequence | NULL |
| 44 | is_end_unspecified | boolean | G | endTimeUnspecified | NULL |
| 45 | guests_can_modify | boolean | G | guestsCanModify | NULL |
| 46 | guests_can_invite_others | boolean | G | guestsCanInviteOthers | NULL |
| 47 | guests_can_see_other_guests | boolean | GM | guestsCanSeeOtherGuests | not hideAttendees |
| 48 | reminders | json | G | reminders | NULL |
| 49 | is_reminder_on | boolean | M | NULL | isReminderOn |
| 50 | reminder_minutes_before_start | bigint | M | NULL | reminderMinutesBeforeStart |
| 51 | conference | json | GM | conferenceData | onlineMeeting |
| 52 | online_meeting_provider | text | M | NULL | onlineMeetingProvider |
| 53 | attachments | json | G | attachments | NULL (a separate call per event) |
| 54 | color_id | text | G | colorId | NULL |
| 55 | locations | json | M | NULL | locations |
| 56 | is_draft | boolean | M | NULL | isDraft |
| 57 | is_response_requested | boolean | M | NULL | responseRequested |
| 58 | allow_new_time_proposals | boolean | M | NULL | allowNewTimeProposals |
| 59 | my_response | text enum | GM | the attendee marked self | responseStatus.response |
| 60 | cancelled_occurrences | json (text list) | M | NULL | cancelledOccurrences (series master) |

`recurrence` has one encoding. The Microsoft source renders Graph's structured pattern as
RFC 5545 lines; that renderer is part of the calendar build, and the column does not ship
before it.

## event_attendees

| # | column | type | P | Google | Microsoft |
|---|---|---|---|---|---|
| 1 | provider | text | GM | | |
| 2 | account | text | GM | | |
| 3 | event_id | text | GM | | |
| 4 | position | bigint | GM | | |
| 5 | address | text | GM | email | emailAddress.address |
| 6 | name | text | GM | displayName | emailAddress.name |
| 7 | kind | text enum | GM | optional, resource flags | type |
| 8 | response | text enum | GM | responseStatus | status.response |
| 9 | responded_at | timestamp | M | NULL | status.time |
| 10 | is_organizer | boolean | GM | organizer | status.response = organizer |
| 11 | comment | text | G | comment | NULL |
| 12 | additional_guests | bigint | G | additionalGuests | NULL |
| 13 | is_self | boolean | GM | self | address = account |

## task_lists

| # | column | type | P | Google | Microsoft |
|---|---|---|---|---|---|
| 1 | provider | text | GM | | |
| 2 | account | text | GM | | |
| 3 | id | text | GM | id | id |
| 4 | name | text | GM | title | displayName |
| 5 | is_default | boolean | M | NULL | wellknownListName = defaultList |
| 6 | role | text enum | M | NULL | wellknownListName |
| 7 | is_shared | boolean | M | NULL | isShared |
| 8 | updated_at | timestamp | G | updated | NULL |
| 9 | change_key | text | G | etag | NULL |

## tasks

| # | column | type | P | Google | Microsoft |
|---|---|---|---|---|---|
| 1 | provider | text | GM | | |
| 2 | account | text | GM | | |
| 3 | id | text | GM | id | id |
| 4 | task_list_id | text | GM | list read | list read |
| 5 | parent_id | text | G | parent | NULL |
| 6 | position | text | G | position | NULL |
| 7 | title | text | GM | title | title |
| 8 | notes | text | GM | notes | body.content asked as text |
| 9 | status | text enum | GM | status | status |
| 10 | vendor_status | text | GM | status as given | status as given |
| 11 | importance | text enum | M | NULL | importance |
| 12 | due_date | date | GM | due (day only) | date of dueDateTime |
| 13 | due_at | timestamp | M | NULL | dueDateTime |
| 14 | start_at | timestamp | M | NULL | startDateTime |
| 15 | completed_at | timestamp | GM | completed | completedDateTime |
| 16 | reminder_at | timestamp | M | NULL | reminderDateTime |
| 17 | recurrence | text | M | NULL | the pattern rendered as RFC 5545 lines |
| 18 | categories | json (text list) | M | NULL | categories |
| 19 | has_attachments | boolean | M | NULL | hasAttachments |
| 20 | is_deleted | boolean | G | deleted | NULL |
| 21 | is_hidden | boolean | G | hidden | NULL |
| 22 | created_at | timestamp | M | NULL | createdDateTime |
| 23 | updated_at | timestamp | GM | updated | lastModifiedDateTime |
| 24 | web_link | text | G | webViewLink | NULL |
| 25 | change_key | text | G | etag | NULL |
| 26 | links | json | G | links | NULL |
| 27 | assignment_info | json | G | assignmentInfo | NULL |
| 28 | is_reminder_on | boolean | M | NULL | isReminderOn |
| 29 | notes_updated_at | timestamp | M | NULL | bodyLastModifiedDateTime |

## Enumerations

Microsoft values are those of the Graph v1.0 description read on 2026-10-09. Google values are
the Gmail teammate's to confirm against the discovery documents.

| column | canonical values | Microsoft | Google |
|---|---|---|---|
| provider | google, microsoft | | |
| messages.flag_status | none, flagged, complete | notFlagged→none, flagged→flagged, complete→complete | STARRED→flagged, else none |
| importance (messages, events, tasks) | low, normal, high | low, normal, high | none: NULL |
| messages.inference_classification | focused, other | focused, other | none: NULL |
| message_recipients.kind | from, sender, to, cc, bcc, reply_to | by property | by header |
| folders.kind | system, user | role present→system, else user | system→system, user→user |
| folders.role | inbox, sent, drafts, trash, spam, archive, outbox, flagged, important, unread, category, other | inbox→inbox, sentitems→sent, drafts→drafts, deleteditems→trash, junkemail→spam, archive→archive, outbox→outbox, every other well-known name→other, no well-known name→NULL | INBOX→inbox, SENT→sent, DRAFT→drafts, TRASH→trash, SPAM→spam, STARRED→flagged, IMPORTANT→important, UNREAD→unread, CATEGORY_PERSONAL, CATEGORY_SOCIAL, CATEGORY_PROMOTIONS, CATEGORY_UPDATES, CATEGORY_FORUMS→category, any other label of type system→other (declared catch-all: Google states its list of reserved names is not exhaustive), type user→NULL |
| folders.label_list_visibility | show, show_if_unread, hide | none: NULL | labelShow→show, labelShowIfUnread→show_if_unread, labelHide→hide |
| folders.message_list_visibility | show, hide | none: NULL | show, hide |
| attachments.kind | file, item, reference | fileAttachment→file, itemAttachment→item, referenceAttachment→reference | file |
| calendars.access_role | owner, writer, writer_without_private_access, reader, free_busy_reader | none: NULL | owner, writer, writerWithoutPrivateAccess→writer_without_private_access, reader, freeBusyReader→free_busy_reader |
| events.body_format | text, html | text, html | none: NULL |
| events.status | confirmed, tentative, cancelled | isCancelled→cancelled, else NULL | confirmed, tentative, cancelled |
| events.show_as | free, busy, tentative, out_of_office, working_elsewhere, unknown | free, busy, tentative, oof→out_of_office, workingElsewhere→working_elsewhere, unknown | transparent→free, opaque→busy |
| events.visibility | default, public, personal, private, confidential | normal→default, personal, private, confidential | default, public, private, confidential |
| events.kind | single, series_master, exception, occurrence | singleInstance→single, seriesMaster→series_master, exception, occurrence | recurrence present→series_master, recurringEventId present→exception, else single |
| events.event_type | default, birthday, focus_time, from_gmail, out_of_office, working_location | none: NULL | default, birthday, focusTime→focus_time, fromGmail→from_gmail, outOfOffice→out_of_office, workingLocation→working_location |
| events.my_response | as event_attendees.response | as event_attendees.response | as event_attendees.response |
| event_attendees.kind | required, optional, resource | required, optional, resource | resource flag→resource, optional flag→optional, else required |
| event_attendees.response | needs_action, accepted, tentative, declined | none→needs_action, notResponded→needs_action, tentativelyAccepted→tentative, accepted, declined, organizer→NULL (is_organizer carries it) | needsAction→needs_action, accepted, tentative, declined |
| task_lists.role | default, flagged_emails, none | defaultList→default, flaggedEmails→flagged_emails, none→none, unknownFutureValue→refused | none: NULL |
| tasks.status | open, done | notStarted, inProgress, waitingOnOthers, deferred→open; completed→done | needsAction→open, completed→done |

## Open points

1. Microsoft `folders.role` needs one call per well-known folder name, since the v1.0 folder
   carries no such property. Its "every other well-known name→other" row is a declared
   catch-all like Google's.
2. Microsoft exceptions in `events` cost one call per series master.
3. Google's event list returns cancelled events only when asked to show deleted ones or on a
   sync; which rows exist is settled in the calendar build and must be the same for both.
4. Under Google's metadata-only scope `body_text` and `body_html` are NULL for every message.
   Google describes that format as returning "only email message ID, labels, and email
   headers", so whether `snippet`, `size_bytes` and `change_key` are returned under it is not
   yet known; each is NULL if not. Stated after the first live read.
