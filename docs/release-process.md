# Release process

Provisa develops on `main` and releases from release branches. A change reaches users in three steps: it lands on `main`, a release branch takes it, and that branch converges until it can ship.

## Where things stand today

Provisa is in alpha, and for now every release is an alpha marked as the latest release. A release branch is cut from `main`, converges on the full suites, is packaged, and is published as an alpha. It carries the Releases page's latest mark, but only when it has a complete, successful set of release assets. If the assets fail, nothing is published and the previous alpha stays latest.

Beta and production releases are not used yet. The stage table below describes how they will work later.

## What each place promises

| Where | Who it is for | What it promises |
|---|---|---|
| `main` | Developers and contributors | It compiles, it comes up, and basic queries work |
| `release/<version>` | Anyone testing a candidate | No new features; bugs are being fixed |
| Tags and the Releases page | Users | What shipped, tested and packaged |

To run Provisa, install a release. To work on Provisa, clone `main`.

## Landing a change on main

A feature or fix starts from `main` and lands on `main`. It needs:

- Unit tests
- UI unit tests, when it touches the UI
- The tests written for the change
- The smoke check

The smoke check is what keeps `main` usable. It starts the default demo from a clean install on DuckDB, loads the UI and runs a query in it, runs one known query through each front door (SQL over HTTP and pgwire, GraphQL, Cypher over HTTP and Bolt, SPARQL, gRPC, Flight SQL, REST and MCP), reads each of the demo's sources plus a Parquet file and embedded PostgreSQL, and confirms that a restricted role gets less.

`main` does not run the integration or end-to-end suites. A branch that passed is merged without being tested again unless Git reports a conflict. When two changes conflict, the one landing second resolves it.

## Cutting a release branch

A release branch is named `release/<version>` and takes its content once, when it is created:

- **Everything:** the branch starts from a commit on `main`. Early in a product's life this is the usual case, because features are still changing.
- **A subset:** the branch starts from the last release, and the chosen landings are cherry-picked with `git cherry-pick -m 1`. `git log --first-parent main` lists them.

A wrong pick is not a special problem. The combination is tested on the branch, and anything broken is filed as a bug.

After it is cut, a release branch never takes a feature. That is why its bugs wind down.

## Stages

One branch goes through every stage, and tags mark them. Today only the alpha row is in use.

| Stage | Converging toward | What enters | Done when |
|---|---|---|---|
| Alpha | Beta: feature complete | Features, through `main` | The scope is complete and a release branch is cut |
| Beta | Release candidate: no known blockers | Fixes only | Full suites clean, packaged, manual testing finds no blockers |
| Release candidate | Final | Fixes for blockers only | It holds with nothing new found |
| Final | | Nothing | Tagged and published |

Tags look like `v0.1.0-alpha.12`, `v0.3.0-rc.1` and `v0.3.0`, and they sort by semantic version. A tag records a stage and publishes nothing. A release on the Releases page is a separate step, so a build can be tagged without being offered to users.

## Converging

A release branch runs the full suites: integration, end-to-end and conformance. Each failure becomes a bug-fix branch started from the release branch and landed on it.

Once the suites are clean, the branch is packaged. The whole release build runs as a dry run that publishes nothing, and the packages are installed. Packaging failures are fixed on the branch.

A packaged build is tested by hand as a beta. Manual testing can also find a defect in a requirement rather than in the code. The requirement is amended first. A small change can then go on the branch as a fix, and anything larger goes to `main` for a later release.

## Every fix travels

A fix is not finished until it is everywhere the bug is. It lands on its release branch, and that branch is merged into every newer open release and into `main`. A fix must carry a test that failed before it, because the test travels with the fix and shows whether the bug exists further along.

If the merge conflicts, or the test fails at a later stop, resolve it there. The usual cause is that newer code replaced what the fix changed. If the bug is gone from the new code, keep the new code. If it carried over, fix it there too. Git records the merge either way, so the question is not asked again.

A bug found on `main` that also affects an open release is fixed on `main` and cherry-picked to that release.

## Alpha releases

Every night a release branch is cut from `main` with everything on `main` at that point. The suites run and bugs are fixed for as long as the time box allows. The build is then packaged, and the alpha is tagged, published and marked latest, with the remaining failures listed in its notes. The branch is merged back to `main` and deleted.

Latest needs a successful set of release assets. An alpha that is not ready does not ship, and a missed day is fine. Nothing forces a release out to keep the schedule.

Later, beta and production releases will be cut deliberately, with no time box, and will ship with no known bugs.

## Overlapping releases

More than one release can be open. Fixes always flow forward through the open releases to `main`. Each open release runs its own suites and each merge forward can conflict, so few should be open at once, and betas should be kept short.
