# Judgments under review

People make governance choices because those choices need judgment. Judgment can be wrong, or
dated, when the facts it rested on change. This design gives every governance judgment a record
of who made it, when, why and on what evidence. It flags the judgment when that evidence or its
stated basis changes, and puts wrong, dated and worth-a-look items in one review queue that a
data steward works through. It is a design: no code yet. Facts about today's code are cited at
`68d97785c`.

## What exists today

- **Tag assignments.** An assignment already carries `reason` and `expires_on`
  (`provisa/core/schema.sql:609,613`), governed by the tag's `reason_policy` and `expires_policy`
  (`hidden`/`optional`/`required`, :547-548; checked at `api/admin/schema_mutation.py:1183`).
  Expiry is reported by the `tag_expiry` view (`api/_meta_views.py:106-116`) and never acted on.
  No assignment records who set it or when.
- **Other judgments carry nothing.** Fakes (`table_columns.fake`, `fake_stable`, `synthetic_rule`,
  `schema.sql:236-239`), masks (`table_columns.masks`, :228) and write grants
  (`table_write_grants`, :509-520) have no actor, time or reason. Row rules and roles have a
  required `description` (`models.py:1782-1787`, REQ-2080/2081).
- **Who changed the model** is in the environment's git history: each request's model writes are
  committed with the request's actor (`core/model_change.py:175,241-252`). That trail is there,
  but it cannot be queried per decision.
- **Profiles are history.** Every profile run is kept, keyed by `run_id`
  (`profiler/schema.py:12-28`), and `compare.compare(current, previous)`
  (`profiler/compare.py:320`) compares any two runs, with drift flagged by its KS/PSI thresholds
  (`_flag`, :560-567).
- **Expiry sweeps** exist for role grants: `roles:sweep_grants`, every 15 minutes
  (`auth/role_grants.py:49-50,378-394`).
- **Rights.** `sensitive_data` covers revealing or hiding a sensitive column
  (`security/rights.py:93`). `_hiding_guard.rights_for` (`api/admin/_hiding_guard.py:67`) already
  maps each hiding field to the right that edits it.
- **Not in this code:** quasi-identifier choices, the adequacy checks and `sensitive_coverage`.
  The nearest to coverage is the environment data detail, which reports uncovered sensitive
  columns (`api/admin/environment_data_router.py:110-140`, `core/env_data.py:336`).

## Decisions

### 1. The record: one "why", the same shape everywhere

Every judgment carries an optional `why`:

| Field | Meaning |
|---|---|
| `reason` | The facts relied on, in a sentence. On a tag assignment this is the existing `reason` column, not a second one. |
| `purpose` | What the decision is for: the outcome the decider wanted. On a row rule and a role it is the required `description`, which already says what it is for; the Why? panel shows it rather than asking again. |
| `bases` | The named bases cited (section 3), by id. |
| `review_on` | An optional review-by date. On a tag assignment this is separate from `expires_on`: an expired assignment ends, while a review date only asks for a look. |
| `decided_by`, `decided_at` | Set by the server from the request's actor (the same actor `model_change` commits with) at each save that changes the decision or its `why`. Never typed. A YAML import records its importer. |
| `evidence` | Set by the server at save: the profile `run_id` the decision was made on, and a digest of that run for the decided column or table (`data_type`, `family`, `null_share`, `distinct_ratio`, `plausible_type` and its confidence, the top shape, the column list). The digest keeps the comparison possible after old runs are pruned. With no profile yet it is empty, and drift checks start from the first run after the decision. |

Where it lives (V1, no migrations: new columns are added to the SQLAlchemy metadata in
`core/schema_org.py` and created by `add_missing_columns`, `core/db.py:295,410`):

- `tag_assignments`: new columns `purpose`, `bases`, `review_on`, `decided_by`, `decided_at` and
  `evidence`, beside the existing `reason` and `expires_on`. This extends the tag store instead of
  adding a parallel one.
- `table_columns`: a `fake_why JSONB` column for the column's fake and synthetic rule.
- Each mask entry in `table_columns.masks` gains an optional `why` key in its JSON (`MaskEntry`).
- `rls_rules` and `table_write_grants`: a `why JSONB` column each.

In config YAML each carries the same optional `why:` block, so a model exports and imports with
its reasons. `decided_by`, `decided_at` and `evidence` are exported read-only and recomputed on
import.

### 2. Named bases: context the data cannot show

New leadership, a new strategy, a new regulation or an industry shift is invisible to Provisa.
A judgment can therefore cite **bases**: shared, named entries such as "GDPR", "Data retention
policy v3" or "EU expansion strategy". They are kept in a short list, never free text.

- **Table:** `judgment_bases` in the org model, with columns `id`, `name`, `description`,
  `owner`, `changed_at`, `changed_by` and `change_note`. It lives beside the glossary but stays
  separate from it: a basis is a policy or position, and it carries a change history that
  glossary terms do not.
- **Declaring a change:** someone holding `access_config` declares a basis changed, with a note.
  That sets `changed_at`. Every judgment citing the basis with `decided_at` before that time is
  flagged *dated by context* at once.
- **Review-by dates** catch everything a basis does not cover.

### 3. Staleness: what flags a judgment

All checks compare a judgment's `evidence` and `decided_at` with the model and profiles now.
Thresholds are the profiler's own, not new ones.

| Finding | Flagged when | Applies to |
|---|---|---|
| Profile drift | `compare.compare(latest run, evidence run)` flags drift on the decided column (the profiler's KS/PSI thresholds) | tags, fakes, masks on a column |
| Type changed | `plausible_type`, `family` or `data_type` differs from the evidence digest | the same |
| Column added | the decided table's column list has a column the evidence did not; in a table that has a sensitive tag or a fake, the new column is also *undecided* | table tags, row rules, write grants (and their `columns`), fakes of the table |
| Schema changed | a decided column was removed or retyped at registration | every judgment on it |
| Decider left | `decided_by` is no longer a member (`remove_from_org`, `core/org_membership.py:461`) | every judgment |
| Basis changed | a cited basis's `changed_at` is later than `decided_at` | every judgment citing it |
| Review date passed | `review_on` is on or before today; tag `expires_on` passed is listed too | every judgment |

A finding clears when the judgment is saved again: it is reaffirmed or changed, so `decided_at`
and `evidence` move, or the column it concerned is gone.

### 4. Findings and where the checks run

- **Table:** `judgment_findings` in the org model, one row per (object, judgment, finding kind).
  Its columns are `object_type`, `object_key`, `judgment` (tag, fake, mask, row_rule,
  write_grant, posture), `category` (`wrong`, `dated_by_data`, `dated_by_context`, `undecided`,
  `worth_a_look`), `finding`, `observed` (JSON: what changed, then and now), `first_seen`,
  `last_seen`, `resolved_at`, `resolved_by` and `resolution`.
- **One writer:** a function, `record_finding(...)`. It is idempotent per key and refreshes
  `last_seen`. The staleness checks, the adequacy checks and the posture checks all write through
  it, so the queue has one producer contract. The adequacy checks (leak rate, kind mismatch,
  synthetic copying) are ci-fix's work and write `wrong`. The existing refusals that are
  findings at heart, `refuse_copied_sensitive` (`synthetic/plan.py:252`) and
  `uncovered_sensitive` (`core/env_data.py:336`), also record what they refused.
- **When the checks run:**
  - a per-org scheduler job, `judgments:review`, shaped like `roles:sweep_grants`: hourly, with
    `max_instances=1` and `coalesce=True`;
  - at the end of each profile run, for the profiled table only, so drift is seen as soon as it
    is measured;
  - on a basis change, at once, for the judgments citing it.
- **Audit evidence:** an ops-domain view, `ops.judgment_review` (the REQ-1386 ops-view mechanism).
  It joins findings to their judgment's `why`, so audit evidence can be queried, under role and
  domain access, from every governed surface.

### 5. Rights

- **Seeing a finding:** reading a finding needs the right that reads the decision. A finding on a
  sensitive column needs `sensitive_data`; any other needs `view_governance`.
- **Resolving a finding:** reaffirm or change needs the right that edits that decision,
  `_hiding_guard.rights_for` for hiding fields and the existing editor's right otherwise.
- **Posture findings:** dismissing one with a note needs `access_config`.
- **New rights:** none.

### 6. UI

**"Why?" on every decision's editor.** A small, quiet text affordance labelled "Why?" sits next
to the decision control, inside the shared form layout as a `FormField` suffix.
- It opens a shared collapsible (`components/collapsible/Collapsible.tsx`) holding Reason,
  Purpose, Bases (a multi-select over `judgment_bases`) and Review by (a date).
- It is collapsed by default and shows no empty fields until opened. Nothing in it is required,
  except where a tag's own `reason_policy` already requires a reason, which stays as it is.
- A subtle dot on "Why?" shows that something is recorded.
- Read-only views (`FormRead`/`ReadRow`) show "Why?" only when it has content, with the decider
  and date beneath.

Where it appears:

| Decision | Editor | Placement |
|---|---|---|
| Tag on an object | `components/TagControl.tsx` (Tables, Table read view, Sources, Commands, Relationships) | after the tag chip's reason/expires drafts (:74-77), per assignment |
| Fake and synthetic rule | `pages/tables/testdata/ColumnFakeDialog.tsx`, via `FakePicker.tsx` | under the kind picker |
| Mask | `components/MaskListEditor.tsx` (`TableEditForm.tsx:961`, `ColumnsTable.tsx:262`) | per mask entry row, after the role and type |
| Row rule | `pages/security/SecurityRlsPage.tsx`, read in `RlsRuleRead.tsx` | after the predicate; Purpose shows the rule's description |
| Write grant | `pages/tables/WriteGrantsPanel.tsx` (`TableEditForm.tsx:980`, `TableReadView.tsx:761`) | per grant, after its operation |

**The review queue.** One list page, *Review*, in the Security nav group (`/security/review`), in
the shared list layout with a right-hand detail panel.
- **Rows:** one per open finding, grouped by category, in this order:
  - **Wrong:** adequacy.
  - **Dated by data:** drift, type, column or schema changes, and the decider leaving.
  - **Dated by context:** a basis changed, or the review date passed.
  - **Undecided.**
  - **Worth a look:** posture.
- **Detail panel:** the judgment's original Reason, Purpose, Bases, decider and date; what
  changed (`observed`: then and now); and the actions *Reaffirm* (re-records `why` with today's
  evidence), *Edit* (opens the decision's editor) and, for posture, *Dismiss with a note*.
- **Bases:** a list page, *Bases*, beside the Glossary in Admin, with the action *Declare changed*.

**Composing with coverage.** Sensitive coverage stays the count of sensitive columns with no
fake, as the environment data detail reports it. The queue lists each such column as one
*undecided* finding, fed by the same `uncovered_sensitive`, so the count and the list cannot
disagree.

### 7. Posture: observed, not enforced

Posture checks write `worth_a_look` findings with what was observed. The ones the code can
observe today:

1. **Plain-text source credential.** A source's credential field holds a value that is not a
   `${provider:ref}` reference (`core/secrets.py:34`). `sources.password_ref` says a typed literal
   is vaulted (`schema.sql:37-40`), but `schema_mutation.py:2023-2029` keeps a credential "as the
   operator entered it", so the check reads what is stored.
2. **Test environment reading prod.** A Test (fake) or Test (synthetic) environment source with
   the `copied` binding, which reads its parent's connection (`core/env_classes.py:190-195,
   234-243`), where that parent is prod. This is the operator's choice, made knowingly.
3. **Unsecured authentication.** No auth section, or `provider: none`
   (`auth/wiring.py:175-179,276-282`). The role header is then honoured as given.
4. **Unclaimed bootstrap administrator** (`api/auth_router.py:296-320`).
5. **A listener without TLS.** MCP falls back to plain HTTP (`api/mcp/tls.py:129`). The same check
   applies to each listener's TLS settings.
6. **Unbounded domain access.** A role with `*` or with empty `allowed_domains`
   (`security/rights.py:141-163,284`).
7. **Expired tag assignments still in force:** `expires_on` has passed, and nothing acts on it.

## Phases

1. **Sensitive tags and fakes:**
   - the `why` record on tag assignments and `fake_why`, with server-set decider, time and
     evidence;
   - `judgment_bases`, and *Declare changed*;
   - `judgment_findings` and `record_finding`;
   - the staleness checks and the `judgments:review` job;
   - the Why? affordance in `TagControl` and `ColumnFakeDialog`;
   - the Review page, the Bases page and `ops.judgment_review`;
   - undecided sensitive columns.
2. **Masks, row rules and write grants:** their `why`, Why? in their editors, and their staleness
   checks.
3. **Producers:** the adequacy findings (ci-fix) and the posture checks.
4. **Quasi-identifier choices,** once that model exists. Today there is none.
