# The CI, and the rules for changing it

Read this before you touch `.github/` or `scripts/ci/`.

Every construct described here has run on GitHub. The run that proved it is in the table at the
end; what has not run yet is listed there too, as unproven.

## The map

Three kinds of file. A change belongs in exactly one of them.

**Composite actions** (`.github/actions/<name>/action.yml`) — one kind of setup each. A job gets
its setup from the action and never repeats the commands.

| Action | What the job has afterwards | Inputs |
| --- | --- | --- |
| `setup-python` | uv, and the locked environment in `.venv` | `extras`, `client`, `cache`, `sync` (false: uv only) |
| `setup-ui` | Node, `provisa-ui/node_modules`, optionally Chromium | `node-version`, `install`, `playwright` (`none`, `chromium`, `chromium-with-deps`) |
| `trino-plugins` | the pinned Trino plugin jars, restored or saved under one key | `phase` (`restore`, `save`), `cache-hit`, `key-suffix` |
| `mssql-odbc` | ODBC Driver 18, registered | none |
| `docs-site-deps` | MkDocs and its plugins | none |
| `release-metadata` | the channel, the PEP 440 version, every asset name (18 outputs) | `version` |
| `ensure-release` | the draft GitHub release for a tag | `tag` |
| `apple-signing` | a keychain holding the Developer ID certificate | `p12-base64`, `p12-password` |
| `nixos-vm` | a booted NixOS guest with the checkout on its disk | none |

**The lane workflow** (`.github/workflows/lane.yml`) — one job with a fixed order of steps. Any
job that runs the container suite or Playwright is a call to it. The order: checkout, the setups
the caller asked for (`odbc`, `python-extras`, `ui`, `java`, `trino-plugins`, `image-cache`,
`splunk-cim-cache`), the caller's `env`, the sampler, `prepare`, `run`, `after`, `on-failure`
when the lane failed, the cache saves, the uploads. A lane's own logic lives in
`scripts/ci/lanes/`, not in the call.

**The release legs** (`release-*.yml`) — called by `build-dmg.yml`, which holds the triggers
and passes each leg the same `version` and `publish`.

## What starts what

| Trigger | Workflows |
| --- | --- |
| push to `main` | Unit Tests, UI Unit Tests, Lint, Requirements CI, CVE Audit, Integration suite, Deploy Site, NixOS, amd64-only engines, windows-container-smoke |
| push to `integration` | Integration suite, UI Unit Tests, NixOS |
| pull request | Unit Tests, UI Unit Tests, Lint, Requirements CI, CVE Audit, amd64-only engines, windows-container-smoke |
| tag `v*` | Build Provisa Packages (the release), Release Exports, both extension builds, the three UI e2e lanes |
| push to `release/**` | the three UI e2e lanes |
| nightly | the warehouse lane, CVE Audit, NixOS |
| push to `ci/**` touching `.github/` or `scripts/ci/` | CI leaf check |

The warehouse lane, the Salesforce lane and the core UI lane read live credentials. The core UI
lane also resumes a paid Fabric capacity. They run on the triggers above and on a dispatch.
Nowhere else.

## How do I

**Add a test lane.** Add the lane to `LANES` and `SUITE` in `scripts/ci/run_lane.py` (its paths,
its marker, its shard count and bound). The suite's matrix is generated from that table, so the
workflow does not change. For a lane outside the suite, add a job that `uses: ./.github/workflows/lane.yml`
with a `name`, a `run` command, and a `prepare` script under `scripts/ci/lanes/`.

**Add a setup step every lane needs.** Put it in the action that owns that kind of setup, or in
a new action. Then give `lane.yml` an input for it. Do not add the commands to a workflow.

**Give one lane a credential.** Declare the secret by name under `workflow_call.secrets` in
`lane.yml`, add it to the `&lane-env` block there, and pass it by name from the one caller that
needs it. The lane's `prepare`, `run` and `after` see it as an environment variable; no other
step does. A secret that is declared but not passed arrives empty, without an error — so a live
lane's first step checks its secrets and fails naming the empty ones.

**Add a source that needs a container.** The test declares the service with a `requires_*`
marker and `tests/conftest.py` starts it; CI has nothing per source. Check which lane's marker
expression in `scripts/ci/run_lane.py` now selects the test, and whether that lane's memory
still holds the new container.

**Add a release artifact.** Name it in `release-metadata`, build it in the leg for its platform,
and add it to the one file list in `release-publish.yml` ("Release assets present"). Not yet
proven on GitHub: see the table.

**Prove a change to a leaf.** Commit it on a `ci/**` branch and hand the hash to the lead, who
pushes. The push starts the CI leaf check, which runs each action and the lane workflow alone
and asserts what each leaves behind. Minutes, no secrets.

## Rules

Each rule is held by the test named beside it, in `tests/unit/`. A rule marked *no test* is held
by review alone.

1. A setup step exists once, in an action. A copy drifts, and the drift shows up as one lane
   failing for a reason the others do not have. — `test_no_workflow_step_carries_setup_an_action_owns`,
   `test_each_owned_setup_is_in_exactly_its_own_action`
2. A lane is a call to `lane.yml` plus scripts under `scripts/ci/lanes/`. Inline steps skip the
   uploads and the failure report. — `test_every_job_that_runs_the_container_suite_or_playwright_is_the_lane_workflow`
3. A lane script starts with `set -euo pipefail`, and no step pipes a command that can fail
   into another without `pipefail`. A masked exit status is a red lane reported green. —
   `test_a_lane_script_fails_loud`, `test_no_workflow_step_hides_a_failing_command_behind_a_pipe`
4. Secrets are declared and passed by name. Never `secrets: inherit`, never `toJSON(secrets)`.
   An action reads none; its caller passes values as inputs. —
   `test_no_workflow_inherits_secrets_or_serialises_them`, `test_an_action_reads_no_secret_itself`
5. No step prints its environment, and no secret goes on a command line or into `echo`. —
   `test_no_step_or_lane_script_prints_its_environment` (the command-line half: *no test*)
6. Nothing leaves a run before every build has succeeded. A release published beside a failed
   build has happened once. — `test_no_publication_is_reachable_without_every_build`,
   `test_every_step_of_the_release_that_leaves_the_run_waits_for_publish`
7. A test that did not execute is reported as not executed, by name. It is never counted as
   passed. — `test_a_test_that_did_not_run_is_named_and_counted_as_not_executed`
8. Never put `continue-on-error` on a job whose failure a later job's condition must see: the
   later job sees `success`. — `test_no_proof_job_fails_on_purpose_uncontained_and_no_needed_job_is_contained`
9. No workflow is red by design. People stop reading red. — same test as rule 8
10. Reusable workflows nest four deep at most; GitHub refuses a fifth. —
    `test_reusable_workflows_nest_no_deeper_than_github_allows`
11. A workflow that costs money or reads live credentials runs only on the triggers in the
    table above. — `test_the_salesforce_lane_runs_only_when_a_dispatch_asks_for_it`; the others: *no test*
12. A change to a leaf is proven on a `ci/**` branch before it reaches main. — *no test*
13. Teammates do not push, dispatch or re-run. The lead does, one suite at a time. — *no test*
14. Unit failures are found locally, not in CI. Run the tests for what you changed, the tests
    that import it, and the guards it could trip. — *no test*
15. A failure has a cause. Read the error and find it; do not re-run to make it pass. — *no test*
16. A defect that lets a caller exceed their rights, or that exposes a secret, goes to the lead
    for a private advisory. The public issue follows once the fix is on main. — *no test*

## What has been proven, and where

| Construct | Run |
| --- | --- |
| Every action except `apple-signing` and `nixos-vm`, each alone | 37985386268 |
| `trino-plugins`: miss on a fresh key, save, hit on a fresh runner | 37981549282 |
| `lane.yml` end to end; `matrix.*` in a call's `with:` and `secrets:`; boolean expressions as inputs; a folded `run`; `retention-days: 0`; the `&lane-env` alias | 37981549282 |
| A failed lane still runs `on-failure` and `after`, and uploads | 37981549282 |
| A lane contained with `failure-expected` does not fail its run | 37985386268 |
| A release job runs although a job it needs was skipped | 37981549282 |
| A release job does not run after a failed need | 37981549282 |
| A job failed under `continue-on-error` shows `success` to its dependant | 37983239912 |
| Real callers through `lane.yml`: amd64-only engines, the suite's neo4j lane, the UI Trino lane | 37985386268 |
| A callable workflow given a few of its declared secrets by name; `inputs.lanes` on a push | 37985386268 |

Unproven: the other suite lanes and the cluster job through `lane.yml`; the UI swap lane; the
core UI lane with its live credentials; the release legs (a dry run); the warehouse and
Salesforce jobs with secrets passed by name.
