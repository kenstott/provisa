# provisa_cim_ta — a CIM technology add-on for the Splunk test fixture

The CIM add-on (`Splunk_SA_CIM`, vendored by `scripts/fetch-splunk-cim.sh`) ships the data
*models* and nothing else. A model is a search-time contract: it selects events by tag and reads
CIM-named fields off them. Neither half happens by itself — mapping raw events onto a model is the
job of a **technology add-on**, and every real Splunk deployment has one per data source. This is
that add-on for the test fixture, so the Splunk e2e exercises the arrangement a customer actually
runs rather than a synthetic model built to fit the events.

Three conf files do the whole mapping:

- `default/props.conf` — `KV_MODE = json` so the HEC-posted JSON event bodies extract as fields at
  search time. The events are already written with CIM field names (`action`, `user`, `src`,
  `dest`, `app`), which is why no `FIELDALIAS-*` stanza is needed; a source emitting `username`
  instead would alias it here.
- `default/eventtypes.conf` + `default/tags.conf` — the selection half. `Authentication`'s root
  constraint is `` (`cim_Authentication_indexes`) tag=authentication NOT (action=success user=*$) ``,
  so an event enters the model only once something tags it `authentication`. An eventtype matching
  the fixture's index/sourcetype carries that tag.
- `local/macros.conf` — the scope half. `Splunk_SA_CIM` ships every `cim_<Model>_indexes` macro
  defined as `()`, which expands to *no index restriction* but is the documented place a deployment
  narrows the model to the indexes that hold its data. Overriding it to the fixture index keeps
  each model reading only the fixture's events. It lives in `local/` because `local` beats any
  app's `default` regardless of app-directory ordering, and `Splunk_SA_CIM` defines the same macro
  in its own `default/`.

`metadata/default.meta` exports everything globally: the model search runs in the `search` app, not
here, and an unexported eventtype/tag/macro is invisible to it — the failure looks like an empty
model rather than a permission error.
