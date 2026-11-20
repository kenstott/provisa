# Environments

An environment is a named copy of an organization's governed model. Physically, the copy is a separate PostgreSQL schema, a real schema with no discriminator column and no prefix. So every existing repository query is correct inside an environment without being rewritten, and a forgotten predicate can't let one environment's rows be read from another.

Every organization starts with one environment named `prod`. It cannot be deleted or renamed.
A request that names no environment is served by `prod`; a request naming a nonexistent environment
is refused.

Environments are available to organizations on a paid plan.

To fill a non-production environment with data that is safe to use, see [Building test environments](test-data.md).

## Environment names

A name must match `[a-z][a-z0-9_]{1,31}`: two to thirty-two lowercase letters, digits and underscores, starting with a letter. `prod` and names beginning with `pg_` are refused. The maximum length for a given org depends on the org's own id. PostgreSQL silently truncates an identifier longer than 63 bytes, and the cap guards against the longest schema name an environment derives.

## Stages

Every environment declares one of four stages. They are production, preprod, test and development. `prod` starts as production, and a new environment starts as development. Any number of environments may share a stage, so you can run several production environments, one per region for example.

Use preprod for an environment that mirrors production. It holds the same model, and its sources are usually bound to production mirrors. Debug production incidents there.

The stage is a label for reporting and display. It never changes what anyone may do. That comes only from the roles assigned in the environment. You see the stage beside each environment's name in the environment switcher. Usage, query health, policy denial and cost reports can be filtered and grouped by it. The audit log and the catalog and lineage exports carry it too, so a downstream catalog can tell production assets from the rest.

Change a stage in the Stage column of the Environments page, or through the API:

```bash
curl -X PATCH https://provisa.example.com/admin/orgs/acme/environments/eu_prod \
  -H 'Content-Type: application/json' \
  -d '{"stage": "production"}'
```

The creator of an environment may change its stage, and so may an org admin. Each change is recorded in the audit log with the stage it replaced.

## What a copy carries

Every table in the org schema falls in exactly one class. The classification is an allow-list: a table added later doesn't travel until someone names its class here, so forgetting a table shows up as a red test.

| Class | Tables | What happens on copy |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Copied whole |
| IDENTITY_ONLY | sources, api_sources, kafka_sources | Identity and governance fields travel; connection values stay behind (see Bindings) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Copied only when an environment is first created; later merges leave them alone |
| PARTIAL | org_settings | Copied per key: governance settings travel, keys naming an external target or per-environment runtime stay behind |
| NEVER_SENSITIVE | org_secrets, user_directory | Never copied |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log, and others | Never copied |

`SEEDED_AT_CREATION` exists because a new environment needs roles and assignments, or it opens with nobody able to act. So roles and assignments travel once, at creation, and afterwards each environment keeps its own. A later merge never carries `prod`'s assignments into a branch, or a branch's into `prod`. Who holds which role in an environment stays that environment's decision.

## Bindings

Bindings are the columns that say where a source actually points: `host`, `port`, `database`, `username` and the rest. They never travel in a copy. An environment that hasn't been bound is marked `unbound` instead of left blank, because an empty host isn't the same as no host, and the connection builder would read it as `localhost:5432`.

An environment created as anything but Unbound copies each source's connection from its parent,
exactly as the parent wrote it. A `${secret:...}` or `${env:...}` reference is copied as that
reference, never resolved. After creation the copy is the environment's own row, editable on the
Sources page, and nothing is resolved through the parent when the environment's runtime is built.
Each source's `binding` says where its connection came from: `copied`, `own` (given in this
environment) or `unbound`. Editing a copied connection makes it `own`. **Re-copy from parent**
(`POST /admin/orgs/{org}/environments/{name}/sources/recopy`) copies the parent's connections
again. A merge or deploy carries no connections, so a source it introduces arrives `unbound`.

## The git projection

Every write to the model commits the result to the environment's git branch. Deploying a tree takes an explicit call. A merged pull request on the git host doesn't deploy itself.

Each entity gets one file. The path is the entity's `provisa://` address with the scheme and org stripped: `provisa://acme/sales/tables/Order` becomes `sales/tables/Order.yaml`. Sources land in `sources/`, commands in `commands/` and metrics in `metrics/`. Child rows that cascade from a parent (columns, relationships, RLS rules) are written inside the parent's file, without files of their own.

Commands and their tag assignments survive the round trip. A tag on a command is routed to the
command's own file (`commands/<name>.yaml`); a tag that belongs to no file disappears from the
projection and would be deleted on the next deploy of that tree.

No surrogate key reaches a file. `registered_tables.id` is an autoincrement integer, so the same model in two environments gets different integers, and a naive dump would diff against itself. Every surrogate is dropped, and every reference to one is written as the target's path.

Serialization is deterministic. Keys are emitted alphabetically, child collections sorted by
their address, and the YAML style is fixed. Two environments holding the same model produce
byte-identical trees.

## Merge

Merging one environment's model into another updates by identity: every object the source has is created or updated in the target. Objects the source no longer has are removed only when the caller explicitly asks for removals. A merge is one transaction, so if it fails partway the target is left as it was.

Before applying, call the preview endpoint (`GET /{name}/merge-preview`) or pass `dry_run: true`.
The preview runs the same code path the merge uses; it is a `GET` endpoint so a CI script that
gets the flag wrong cannot accidentally apply the merge it meant to inspect.

A merge leaves the target's bindings, roles, and secrets exactly as they were. A dev environment
does not lose its own database connections by taking a newer model from prod. Prod does not acquire
dev's grants.

### What the report names

The merge report lists, by path, what was added, changed, removed and left as it was. It also names any **conflicts**: objects both sides changed since they last shared a commit. A conflict is reported, and the source wins, which is what a merge into a target means. Provisa offers no conflict resolution, no merge markers and no choosing per object. The conflict list is valuable as a signal that two people were editing the same object without knowing it.

When both sides changed an object to the same value, they agree, and it's no conflict. When the two environments share no ancestor at all, the base is `None` in the report, and an empty conflicts list means nothing was compared; it doesn't mean nothing collided.

The merge lands as one squashed commit on the target's branch. The commit message is required and can't be blank, because it's the only account of the range of work the squash stands for. The source's commits stay where they are and can still be deployed by SHA afterwards.

## Pull

Pulling takes what the remote holds for an environment and makes it the model. It does not fast-
forward the local branch directly; it applies the fetched tree through the ordinary deploy path,
so the same validation and audit that govern a manual deploy govern a pull.

Like a merge, a pull reports what it overwrote: objects the incoming tree changed that the local environment had also changed since the two lines last shared a commit. A local change that was never committed makes the environment drifted (see History below), and a pull names it as an ordinary change in the report.

A pull is refused when the two lines have **diverged**, meaning each holds commits the other doesn't. The refusal lists the objects both sides touched, so whoever now has to decide whose work survives knows which objects to look at.

## History

Every deploy moves the environment's cursor forward along its own commit line. An undo steps back one commit, and a redo steps forward again toward where the undo started. Neither removes a commit. Stepping back adds a position and leaves history as it is.

A branch is seeded at the tip of the environment it was created from, so an undo stops at that
seeding point and does not walk onto the parent environment's commits.

The `can_undo` and `can_redo` flags come with the environments list response. Both report `false` when the projection doesn't hold the commit the model store names, a state the design allows and calls **drifted**. A node whose repository store never received a particular commit still lists its environments; only the history answers change.

## Authorization

What you may do in an environment comes only from the roles assigned to you there. Roles and their assignments live in each environment's own schema, so assigning someone Developer in `dev` gives them nothing in `prod`. No environment adds, removes or overrides a right. The standard roles mean the same thing everywhere.

Shape `prod` by assignment. A member who must not write data or change the model in production holds Analyst there and Developer in their own environments. A pipeline that writes to production holds Data writer in `prod` only. In `prod`, members, role and entitlement assignments, connections, credentials and business metadata are changed directly by whoever holds the right. Model structure and access policy usually arrive by merge or deploy, but a holder of the right may change them in `prod` too.

Four rights govern the environments themselves.

| Right | Who holds it (standard roles) | What it governs |
| --- | --- | --- |
| `environment_switch` | Viewer, and so every standard role built on it | Being served by an environment other than `prod` |
| `environment_owned_management` | Developer | Creating an environment, and deleting or changing the stage of one you created |
| `environment_management` | Org admin | Creating, deleting and protecting any environment |
| `environment_data` | Org admin | An environment's data choices, such as its data mode and source bindings |

`prod` needs no right. It serves every request that names no environment, and refusing it would refuse every request.

The check happens where the environment is selected, before any route is reached. Because the environment is bound in the middleware, one check covers HTTP, GraphQL, SQL and the wire protocols at once. A sign-in that holds no role in the environment it names is refused there, so the environments you can switch into are the ones you are assigned in.

### Environment owner authority

Inside an environment you created, you hold the Developer role's capabilities whatever roles you are assigned there, minus `usage`. That lets you build the model in your own branch. Querying, writing and full results come only from the roles assigned to you in that environment.

The grant is derived from `environments.created_by` at authorization time, never written into a
grant table. Deleting the environment removes it in the same act.

Domain membership still limits what the owner may change. Branching changes what a member may do;
it never changes which domains they may do it to.

## Protected environments

An environment can be protected. A merge or deploy into a protected environment is not applied
when requested; it is proposed, and someone other than the requester must approve it.

`prod` becomes protected automatically once the org has more than one member. A single-member org can't satisfy "someone other than the requester", so the rule doesn't apply there; it would make `prod` impossible to merge into. An org_admin can mark any environment protected.

A merge request is a stored row, and no confirmation dialog stands in for it. The approver is by definition someone other than the requester and isn't there at the moment of the request. A confirmation that vanishes would force approval inside the requester's session, which is exactly the arrangement the rule forbids.

The request row carries the merge report alongside the requester's message. Staleness is derived
at read time, never stored, because re-planning at read time and comparing against the stored report is
the only version that cannot be wrong. A stale request must be re-requested. The requester cannot
approve their own request.

Request lifecycle states: `requested` → `approved`/`rejected` → `applied`. `stale` is derived.

Deploys from a repository ref go through the same door, and the request pins the SHA when it's proposed. If the ref moves between the proposal and the decision, the approver reads the report for the pinned commit, not the new one.

!!! note
    The merge-request UI is under the **Merge requests** tab in the Environments panel.
    The **Report** column shows what would change by count; the row expands to show per-object
    detail.

## The `env` CLI commands

`provisa env deploy` sends the model at a ref into an environment. It exits 0 when the deploy was applied or was a dry run, and 2 when the environment is protected and the deploy was only proposed. A pipeline that treats a pending approval as a released deploy would be wrong, and the exit code tells it apart.

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` brings the org's remote branches into the local repository. A deploy can then
name `origin/<branch>`.

```
provisa env fetch --org acme --api <url> --token <token>
```

Both commands accept `--api` (the Provisa API URL) and `--token` (a bearer token). Set
`PROVISA_API_URL` and `PROVISA_API_TOKEN` in the environment to avoid passing them on every call.

The typical CI pipeline for a repo-backed workflow:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## See also

- [Deployment](deployment.md): how to stand up the control plane environments connect to
- [Commands](commands.md): tracked functions and webhooks that appear in each environment's tree
- [Building test environments](test-data.md): profile production, declare fakes, generate synthetic data for a non-production environment
- [Fake methods](fake-methods.md): every kind of fake and every method a column can declare
