# The hosted demo sandbox

The live demos page on provisa.dev opens each published demo in a sandbox on cloud.provisa.dev.
No account is needed. The sandbox is your own copy of the demo: its model, and its data landed
ahead of time, so the first query answers at once. Anything you add or change stays in your copy.

A "Try it Out" invitation opens the same kind of sandbox for a signed-in person. That sandbox is
yours from every device you sign in on, and nobody else can be admitted to it.

## What the sandbox is

It is a lightweight version of Provisa. The same product, with these limits:

<!-- The values below are the settings' shipped defaults; tests/unit/test_hosted_sandbox_doc.py
     fails when this table and provisa/core/settings_catalog_sandbox.py disagree. A deployment
     that changes a setting shows its own value in the sandbox's welcome notice. -->

| Limit | Value | Setting |
|---|---|---|
| Engine | A single-process DuckDB engine, not the engine production organizations use | |
| Memory | 256 MB for the sandbox's engine; a query that needs more is refused | `sandbox.memory_limit` |
| Disk | 512 MB for the sandbox's data and uploaded files together; a write past that is refused | `sandbox.disk_limit` |
| Inactivity | Deleted, with everything in it, after 1 hour without use | `sandbox.idle_ttl` |
| Open at once | 10 sandboxes; past that, a visitor is asked to try again shortly | `sandbox.max_concurrent` |
| Demos being prepared | 2 demos' templates are built at a time after the tier starts; a demo still waiting is built next when a visitor opens it, and the visitor is told it is still being prepared | `sandbox.template_builds_at_once` |

The sandbox also holds back what reaches past it:

- You can't invite people, confer roles, or switch or manage environments.
- The sample sources are read-only. What you write stays inside the sandbox.
- A source you register may reach only public internet addresses. A source that names a private,
  loopback or link-local address is refused by name, and so is one whose address could not be held
  to the one checked (a file read from a URL, for instance; upload the file instead).
- Changes made at the same moment from several devices can be refused as a conflict. Try again.

Every one of these is shown, with this deployment's own values, in the welcome notice the sandbox
opens with.

## For operators

The four settings above are on the Deployment settings page, under Limits. They take effect for
the next sandbox opened, and the memory limit for the next sandbox engine built.

The sandboxes run in a process of their own, the sandbox tier (`PROVISA_SANDBOX_TIER=true`, one
worker), beside the API that serves every other organization. The UI server sends a sandbox's
requests there by the sandbox's address; any other process refuses a sandbox by name. Each
published demo (`published: true` in its `demo/named/<name>/demo.yaml`) has a template the
sandbox tier builds when it starts, landing the demo's data once; a sandbox is opened as a copy of
it. See [named demos](named-demos.md) for how a demo is written.

On a deployment that holds many organizations, every organization's sources are held to public
addresses, not only a sandbox's; a sandbox's are also held to the address checked. See
[Sources on a shared deployment](security.md#sources-on-a-shared-deployment).

### AI in the sandbox

Polly and the AI features of a sandbox (column and table descriptions, relationship discovery,
natural-language SQL, table selection) all use one OpenAI-wire endpoint the operator names, with
one model. A sandbox has no embedding model, so catalog search is unavailable there; Polly browses
the catalog instead. The key is stored once, as a secret in the deployment organization, and
resolved there for every sandbox; a sandbox's own secrets never hold it.

| Setting | Value | Setting |
|---|---|---|
| Endpoint | Unset: no AI in sandboxes. OpenRouter's is `https://openrouter.ai/api/v1` | `sandbox.ai_base_url` |
| Model | `anthropic/claude-haiku-5.5`, for every role | `sandbox.ai_model` |
| Key | Unset: no AI in sandboxes. A `${secret:NAME}` reference to the deployment organization's secret | `sandbox.ai_api_key` |

