# Environments

An environment is a named copy of an organization's governed model. The copy is physically a
separate PostgreSQL schema — not a discriminator column, not a prefix, a real schema — so every
existing repository query is correct inside an environment with nothing rewritten, and one
environment's rows cannot reach another's read by a forgotten predicate (REQ-1487, REQ-1488).
[tool-verified: `environments.py` module docstring; `org_schema()` at environments.py lines 86-96]

Every organization starts with one environment named `prod`. It cannot be deleted or renamed.
A request that names no environment is served by `prod`; a request naming a nonexistent environment
is refused. [tool-verified: `PROD = "prod"` at environments.py line 44; `select_environment()`
at env_routing.py lines 93-129]

Environments are available to organizations on a paid plan. [inferred: REQ-1507]

## Environment names

A name must match `[a-z][a-z0-9_]{1,31}` — two to thirty-two characters of lowercase letters,
digits, and underscores, starting with a letter. `prod` and names beginning with `pg_` are refused.
The maximum length for any one org depends on the org's own id: PostgreSQL truncates an identifier
over 63 bytes silently, and the longest schema name an environment derives is what the cap protects
against. [tool-verified: `ENV_NAME_PATTERN` at environments.py line 59; `validate_env_name()` at
environments.py lines 119-142; `max_env_name_length()` at environments.py lines 108-116]

## What a copy carries

Every table in the org schema falls in exactly one class (REQ-1489). The classification is an
allow-list, not an exclusion list: a table added later does not travel until someone names its
class here, so the failure mode for a forgotten table is a red test. [tool-verified: `CLASSIFIED`
constant and module docstring, env_classes.py lines 19-22]

| Class | Tables | What happens on copy |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Copied whole |
| IDENTITY_ONLY | sources, api_sources, kafka_sources, kafka_sinks | Identity and governance fields travel; connection values stay behind (see Bindings) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Copied only when an environment is first created; later merges leave them alone |
| PARTIAL | org_settings | Copied per key: governance settings travel, keys naming an external target or per-environment runtime stay behind |
| NEVER_SENSITIVE | org_secrets, user_directory | Never copied |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log, and others | Never copied |

[tool-verified: `CARRIED`, `IDENTITY_ONLY`, `SEEDED_AT_CREATION`, `PARTIAL`, `NEVER_SENSITIVE`,
`NEVER_RUNTIME` frozensets, env_classes.py lines 29-113]

`SEEDED_AT_CREATION` exists to solve one specific problem. A new environment needs roles and
assignments or it opens with nobody able to act. But a later merge that carried `prod`'s `developer`
row would overwrite the restricted version a restricted branch might need, making the review path
the escalation route. So roles and assignments travel once, at creation, and are afterwards each
environment's own answer. [tool-verified: env_classes.py lines 65-71; env_copy.py lines 41-44]

## Bindings

Bindings are the columns that say where a source actually points — `host`, `port`, `database`,
`username`, and the rest. They never travel in any copy. An environment that hasn't been bound is
marked `unbound` rather than left blank: an empty host is not an absent one, and the connection
builder would read it as `localhost:5432`. [tool-verified: `BOUND_COLUMN = "bound"` at
env_classes.py line 143; `BINDING_COLUMNS` dict at env_classes.py lines 155-172]

An environment's sources resolve in one of two ways.

**Base** — the environment carries its own credentials. An org_admin creates a base and then binds
each source explicitly. [tool-verified: `CreateEnvBody.inherit_connections = False` (default) at
environments_router.py line 227; "binding a base is an org_admin's act" comment at line 358]

**Branch** — the environment inherits the base's credentials by reference. Nothing is copied.
When a query needs a connection, resolution walks up the `branched_from` chain and stops at the
first environment whose row is bound. Rotating a credential on the base propagates to every branch
of it with no action required. Revoking it revokes for all of them at once. No secret is ever
materialized anywhere a branch, an export, or a repository could carry it away.
[tool-verified: `resolve()` at env_bindings.py lines 114-151; `lineage()` at env_bindings.py
lines 74-102; env_bindings.py module docstring lines 11-33]

To create a branch, set **Inherit connections** in the Environments panel. The default is off.
[tool-verified: `environmentsTab.json` key `inheritConnections`; `inheritHelp2` string]

## The git projection

Every write to the model commits the result to the environment's git branch. The repository is a
projection of the model, never its authority: Provisa reads and writes the control plane; the
repository is the record, not the source. Deploying a tree requires an explicit call — a merged
pull request on the git host does not deploy itself (REQ-1524, REQ-1526). [tool-verified:
deploy endpoint docstring at environments_router.py lines 777-791]

Each entity gets one file. The path is the REQ-1385 URI with scheme and org stripped:
`provisa://acme/sales/tables/Order` becomes `sales/tables/Order.yaml`. Sources land in `sources/`,
commands in `commands/`, metrics in `metrics/`. Child rows that cascade from a parent — columns,
relationships, RLS rules — are written inside the parent's file, not as files of their own.
[tool-verified: `table_path()` at env_files.py line 109-115; `kind_path()` at env_files.py
lines 118-120; `COMMANDS_DIR = "commands"` at env_project.py line 71; env_files.py module
docstring lines 17-24]

Commands and their tag assignments survive the round trip. A tag on a command is routed to the
command's own file (`commands/<name>.yaml`); a tag that belongs to no file disappears from the
projection and would be deleted on the next deploy of that tree. [tool-verified:
env_project.py lines 346-364; `owner_command_name` routing in `_assignments_for()` at
env_project.py lines 137-164]

No surrogate key reaches a file. `registered_tables.id` is an autoincrement integer — the same
model in two environments gets different integers, so a naive dump diffs against itself. Every
surrogate is dropped and every reference to one is written as the target's path.
[tool-verified: `STORAGE_COLUMNS` and `_model_columns()` at env_files.py lines 62-128;
env_project.py docstring lines 26-27]

Serialization is deterministic. Keys are emitted alphabetically, child collections sorted by
their address, and the YAML style is fixed. Two environments holding the same model produce
byte-identical trees. [tool-verified: `dump()` at env_files.py lines 131-143]

## Merge

Merging an environment's model into another one updates by identity: every object the source has
is created or updated in the target. Objects the source no longer has are removed only when the
caller explicitly requests removals. A merge that fails partway leaves the target as it was — one
transaction. [tool-verified: `copy_model()` at env_copy.py lines 216-234; REQ-1490 description]

Before applying, call the preview endpoint (`GET /{name}/merge-preview`) or pass `dry_run: true`.
The preview runs the same code path the merge uses; it is a `GET` endpoint so a CI script that
gets the flag wrong cannot accidentally apply the merge it meant to inspect. [tool-verified:
`preview_merge()` docstring at environments_router.py lines 1086-1095]

A merge leaves the target's bindings, roles, and secrets exactly as they were. A dev environment
does not lose its own database connections by taking a newer model from prod. Prod does not acquire
dev's grants. [tool-verified: env_copy.py lines 269-287; REQ-1490 scenario]

### What the report names

The merge report lists, by path, what was added, changed, removed, and left unchanged. It also
names any **conflicts** — objects both sides changed since they last shared a commit. A conflict
is reported and not resolved: the source wins, which is what a merge into a target means. Provisa
offers no conflict resolution, no merge markers, no per-object choosing. The value of the conflict
list is the signal — two people were editing the same object without knowing it (REQ-1555).
[tool-verified: `CopyReport.conflicts` at env_copy.py lines 151-165; `detect_conflicts()` called
at env_copy.py lines 261-263; REQ-1555 description]

An object both sides changed to the same value is agreement, not a conflict. When the two
environments share no ancestor at all, the base is `None` in the report and the empty conflicts
list means nothing was compared, not that nothing collided. [tool-verified: `CopyReport.compared`
property at env_copy.py lines 164-166; env_copy.py lines 255-264]

The merge lands as one squashed commit on the target's branch. The commit message is required
and must not be blank — it is the only account of the range of work the squash stands for. The
source's commits stay where they are and remain deployable by SHA afterwards.
[tool-verified: `_squash()` docstring at environments_router.py lines 663-680;
`MergeBody.message` comment at environments_router.py lines 258-260]

## Pull

Pulling takes what the remote holds for an environment and makes it the model. It does not fast-
forward the local branch directly; it applies the fetched tree through the ordinary deploy path,
so the same validation and audit that govern a manual deploy govern a pull.
[tool-verified: `pull_environment()` docstring at environments_router.py lines 1450-1462]

Like a merge, a pull reports what it overwrote — objects the incoming tree changed that the local
environment had also changed since the two lines last shared a commit. An uncommitted local change
is a drifted environment (see History below); a pull names it as an ordinary change in the report.
[tool-verified: REQ-1556 description; `pull_environment()` at environments_router.py
lines 1485-1519]

A pull is refused when the two lines have **diverged** — both hold commits the other does not.
The refusal carries the list of objects both sides touched, so the person who must now decide
whose work survives knows which objects to look at. [tool-verified: `state["diverged"]` check at
environments_router.py lines 1491-1503; `_collisions()` at environments_router.py
lines 1581-1602]

## History

Every deploy moves the environment's cursor forward in its own commit line. An undo steps back
one commit; a redo steps forward again toward the position the undo departed from. Neither
operation removes a commit — stepping back adds a position, it does not rewrite history.
[tool-verified: `_move()` docstring at environments_router.py lines 854-868]

A branch is seeded at the tip of the environment it was created from, so an undo stops at that
seeding point and does not walk onto the parent environment's commits. [tool-verified:
`origin_sha` comment at environments_router.py lines 428-448; `_move()` at
environments_router.py lines 907-916]

The `can_undo` and `can_redo` flags travel with the environments list response. Both report `false`
when the projection does not hold the commit the control plane names — a state the design admits,
called **drifted**. A node whose repository store never received a particular commit still lists
its environments; only the history answers change (REQ-1561). [tool-verified: `_with_history()`
at environments_router.py lines 316-344; REQ-1561 description]

## Authorization

Environments are governed by two rights. Neither is an analyst's by default (REQ-1573).
[tool-verified: REQ-1573 description; `MANAGE_CAPABILITY = "environment_management"` and
`SWITCH_CAPABILITY = "environment_switch"` at environments_router.py line 110 and
env_routing.py line 53]

| Right | Who holds it (seeded) | What it governs |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Creating and deleting environments |
| `environment_switch` | org_admin, developer | Being served by any environment other than prod |

`prod` needs no right — it is what a request naming nothing is served by, and refusing it would
refuse every request.

Enforcement is at the selection point, before any route is reached. A member who lacks
`environment_switch` is refused for every surface at once — HTTP, GraphQL, SQL, and the wire
protocols — because the environment is bound in the middleware, not in individual handlers.
[tool-verified: `select_environment()` at env_routing.py lines 93-129; env_routing.py
module docstring lines 28-34]

An analyst carrying no environment right can query `prod` and cannot see the environment switcher.
A contractor granted the analyst role sees no environments surface and cannot create or switch into
any environment other than production. [tool-verified: REQ-1573 use_case and scenario]

### Environment owner authority

Creating an environment is the only path by which a read-only member acquires model-editing rights
(REQ-1528). Inside the environment they created, the creator holds the `developer` role's
capabilities — minus the data rights (`write`, `full_results`, `usage`). Model-building rights,
not data rights. [tool-verified: `ENVIRONMENT_OWNER_CAPABILITIES` at env_authority.py lines 75-77;
`_DATA_RIGHTS` at env_authority.py lines 74-77; env_authority.py module docstring lines 14-38]

The grant is derived from `environments.created_by` at authorization time, never written into a
grant table. Deleting the environment removes it in the same act.
[tool-verified: env_authority.py module docstring lines 39-42; `environment_owner()` at
env_authority.py lines 84-98]

Domain membership still limits what the owner may change. Branching changes what a member may do;
it never changes which domains they may do it to (REQ-1530).
[tool-verified: `domains_within()` at env_authority.py lines 121-145]

## Protected environments (REQ-1504)

An environment can be protected. A merge or deploy into a protected environment is not applied
when requested; it is proposed, and someone other than the requester must approve it.

`prod` is protected automatically once the org has more than one member. A single-member org
cannot satisfy "someone other than the requester", so the rule is not applied there — it would
make `prod` unmergeable. Any environment can be marked protected by an org_admin.
[tool-verified: `is_protected()` at env_approvals.py lines 79-96; `protectedHelp2` UI string
in environmentsTab.json line 28]

A merge request is a row, not a confirmation dialog. The approver is by definition a different
person from the requester and is not present at the moment of the request; an ephemeral
confirmation would force approval inside the requester's session, which is the one arrangement the
requirement forbids. [tool-verified: env_approvals.py module docstring lines 11-17]

The request row carries the merge report alongside the requester's message. Staleness is derived
at read time, never stored: re-planning at read time and comparing against the stored report is
the only version that cannot be wrong. A stale request must be re-requested. The requester cannot
approve their own request. [tool-verified: `STALE` constant and `effective_state()` at
env_approvals.py lines 53, 215-243; `decide()` lines 265-268]

Request lifecycle states: `requested` → `approved`/`rejected` → `applied`. `stale` is derived.
[tool-verified: `REQUESTED`, `APPROVED`, `REJECTED`, `APPLIED`, `STALE` at env_approvals.py
lines 47-53]

The same door handles deploys from a repository ref: the request pins the SHA at proposal time.
If the ref moves between the proposal and the decision, the approver reads the report for the
pinned commit, not the new one. [tool-verified: `request_deploy()` at env_approvals.py lines
150-189; env_approvals.py docstring lines 26-27]

!!! note
    The merge-request UI is under the **Merge requests** tab in the Environments panel.
    The **Report** column shows what would change by count; the row expands to show per-object
    detail. [tool-verified: `environmentsTab.json` keys `requestsTitle`, `colReport`,
    `approve`, `reject`]

## The `env` CLI commands

`provisa env deploy` sends the model at a ref into an environment. It exits 0 when the deploy
applied or was a dry run, and 2 when the environment is protected and the deploy was only proposed
— a pipeline treating a pending approval as a released deploy would be wrong, and the exit code
says so. [tool-verified: `_cmd_env_deploy()` at cli.py lines 389-411]

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` brings the org's remote branches into the local repository. A deploy can then
name `origin/<branch>`. [tool-verified: `_cmd_env_fetch()` at cli.py lines 414-426]

```
provisa env fetch --org acme --api <url> --token <token>
```

Both commands accept `--api` (the Provisa API URL) and `--token` (a bearer token). Set
`PROVISA_API_URL` and `PROVISA_API_TOKEN` in the environment to avoid passing them on every call.
[inferred: shared `_api_call()` helper]

The typical CI pipeline for a repo-backed workflow:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## See also

- [Deployment](deployment.md) — how to stand up the control plane environments connect to
- [Commands](commands.md) — tracked functions and webhooks that appear in each environment's tree
