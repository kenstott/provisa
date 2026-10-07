# Building test environments

A test environment is a non-production [environment](environments.md) whose tables hold data you can use freely: shaped like production, carrying no personal data.

The main idea: **fake only the columns that identify a person, and keep everything else real.** Amounts, categories, dates and quantities stay as they are, so every real relationship, edge case and distribution survives. Nothing is copied, and the data is always as fresh as the source. Reach for synthetic data only when real rows must not exist in the environment, or when you need a different scale.

Everything here uses one example: a `customers` table and an `orders` table, with `orders.customer_id` pointing at `customers.id`.

## Faked on read, or synthetic?

| | Faked on read | Synthetic dataset |
| --- | --- | --- |
| Rows | The source's own rows | Generated rows |
| What changes | Only the columns you fake | Every column is generated |
| Real relationships, edge cases, distributions | All kept | Reproduced from profiles, at the scale you choose |
| Copy | None. Reads go to the source. | Generated into the environment's warehouse store |
| Freshness | Always current | As of the profile runs it was built from |
| Real rows in the environment | Real rows are read, with identifying columns faked | None |
| Scale | The source's | Any: ×0.5, ×10, or more |
| Needs profiles | No (a profile only helps you fill fakes) | Yes |

Choose faked on read when production's rows may be read but its people must not be seen. Choose synthetic when a real row must never exist in the environment, or when a test needs ten times the volume.

## How an environment gets its data

Every environment other than `prod` has one **data mode**, chosen when you create it and changeable afterwards. `prod` has no data mode: it is always real.

| Data mode | What its tables show |
| --- | --- |
| Inherit | The parent environment's real rows, through connections copied from the parent. |
| Unbound | Nothing until you bind a source to a database of the environment's own. The whole model is there, with no connections. |
| Test (fake) | The parent's real rows, with every column that declares a fake read through its fake, by every role. |
| Test (synthetic) | Rows generated from the parent's profile runs into the environment's own store. |

The environment an environment was created from is always recorded as its parent, whatever its mode, so you can switch any environment back to Inherit at any time.

When an environment is created as anything but Unbound, each source's connection is copied from the parent exactly as the parent wrote it. A reference to a secret or a variable is copied as that reference, so the copy names the same credential. From then on the environment's sources are its own: edit them on the Sources page like any other. A change in the parent no longer reaches them.

Each source says where its connection came from: **copied** from the parent, **own** (given in this environment), or **unbound** (none). Editing a copied source's connection makes it own. A password typed in an environment is stored under a name of that environment's own, so it never replaces the parent's. **Re-copy from parent** copies the parent's connections again, for one source or for all of them. Choosing Inherit re-copies every source; choosing Unbound clears every one; the Test modes leave each source as it is.

A model merged or deployed into an environment brings no connections: a source new to the environment arrives unbound until you re-copy it or give it a connection.

A fake applies only in a Test (fake) environment. `prod` never fakes, and neither do Inherit and Unbound. The engine computes the fakes as it serves the read, so ordering, filtering, grouping and joining work on the fakes the reader sees.

A synthetic table and a table reading real data are never read together. A statement joining them is refused, with both tables named: their keys share nothing, and the result would look plausible and mean nothing.

### Who may change an environment's data

Changing an environment's data choices (its data mode, its sources' bindings, its mutation handling, its synthetic settings) needs the `environment_data` right. `org_admin` holds it by default; no developer does. Creating an environment as anything other than Unbound with mutations refused needs it too.

Creating an environment as Inherit, or switching one to Inherit, also needs the right to read the parent's data. The change is audited with the real data it makes visible and how many members can see it.

### Sensitive columns

A tag definition has a **Sensitive data** option. A column carrying any tag with it set is a sensitive column. The built-in `pii` tag has the option set, and it cannot be cleared. You can set it on tags of your own, such as `mnpi`. Sensitivity is never passed on: a view's column or a calculated column derived from a sensitive column is sensitive only if you tag it.

The `sensitive_data` right governs every way a sensitive column's values are revealed or hidden, in every environment, `prod` included:

- adding or removing a sensitive tag on a column;
- setting or clearing the Sensitive data option on a tag;
- changing a sensitive column's role masks, fake, synthetic rule or column grants.

`org_admin` holds it by default; grant it to a data steward role. No developer holds it. Role masks on columns that are not sensitive need `masking_config`, and fakes and synthetic rules on them are open to anyone who may edit the table.

In a Test (synthetic) environment, a sensitive column may not take a rule that copies real values into the generated rows, such as `categories()` drawn from the profile, or `profile()`.

### Test (fake) and sensitive columns

A Test (fake) environment never shows a sensitive column unless that column declares a fake. While any such column has no fake, creating or switching to Test (fake) is refused, and so is every request into a Test (fake) environment. The refusal names the columns. A holder of the `sensitive_data` right can declare their fakes.

### Mutation handling

Mutation handling is chosen alongside the data mode and applies to any of them. A new environment's mutation handling is **Refused**.

- **Refused.** Every mutation is refused, naming the environment.
- **Direct.** Mutations change the data, but only data the environment owns: a source bound to a database of its own. A mutation through a connection copied from the parent is refused, so an environment never writes into its parent's real data.
- **Reversible.** Mutations are kept in the environment's own change log and the data underneath is never changed. Reads show the data with the log applied: the latest version of each row by its key, and a deleted row left out. **Reset mutations** on the environment's detail panel drops the log, returning the environment to its baseline: the parent's real rows, the generated rows, or a database of its own. Every table written this way needs a primary key. A MERGE is kept as what each of its clauses would write. In a Test (fake) environment a kept row is matched by its key as the environment reads it, and a written row is never faked again.

A `TRUNCATE` is a mutation like any other, and runs as itself; it is never rewritten to a `DELETE`. Because it cannot apply a row filter, it runs only for a role that holds the `write` right and has no row filter on the table; otherwise it is refused, naming the filter, and `DELETE` is the way to remove the rows the role can see. Under Reversible a `TRUNCATE` is kept as one entry: reads then show only what was written after it, and Reset mutations brings the rows back.

A change of data mode that changes row keys (to or from Test (synthetic), or regenerating it) discards the kept mutations. The edit asks you to confirm first. A change between Inherit and Test (fake) keeps them.

### API mutations

An API source's mutations are opaque: Provisa cannot know what they change. Under Refused and Reversible, an API mutation is refused. Under Direct, it is allowed only to an API bound to an address the environment owns, and it is never sent to production's API.

### Test (synthetic): generating the whole model

A Test (synthetic) environment generates its whole model, because implicit relationships make generating part of it unsafe. Every table is generated, each from a profile run in the parent or from a declared profile. The environment's synthetic plan lists every table with its profiles: the parent's runs, measured or declared, and the profiles declared in the environment itself. The parent's latest measured run is selected, or the latest declared profile where the table has no measured run. Generate is refused until every such table has one, and while any column has no profile fact, fake or synthetic rule, naming each such column.

A synthetic environment calls no source API:

- An API table that can be read in full is generated like any table.
- An API table that needs a required parameter (an OpenAPI path parameter, a remote GraphQL field's required argument, a remote gRPC method's input) has no full set of rows to measure. It is generated only from a declared profile. Without one it is not available in the environment: a read of it is refused, saying why and that declaring a profile generates it. Such a table does not hold Generate back.
- The commands backed by a generated API source are not defined in the environment. A call to one is refused, saying why.

A developer can restore them by hand, for example by standing up their own instance of the API that reads the synthetic tables and editing the source on the Sources page to point at it.

Generating takes two steps. **Generate** (`POST /admin/orgs/{org}/environments/{env}/synthetic`) generates nothing. It answers the Limitations of Synthetic Data warning: the limitations above, each table to be generated with its profile, scale and estimated rows, each source whose tables are generated, each table that will not be available and why, each command that will not be defined, grouped by its source, and the kept mutations that generating discards. The warning carries a digest. **Confirm** (`POST .../synthetic/confirm`) takes the same choices and that digest, and starts the generation. If the model or its profiles changed after the warning was shown, the confirmation is refused with the warning as it now stands.

Generation runs in the background. The environment shows Generating, then Ready, or Failed with the reason. Until it finishes, its tables read what they read before. Switching the environment out of Test (synthetic) drops the generated rows, and its tables read their sources again.

### Stores

An environment has two stores of its own. Its warehouse store holds all data that no source holds: every synthetic dataset, whatever its size, and any extract landed whole. Its relational store holds only the environment's own changes. A dataset is landed through the governed read path, so masking and row-level security apply before any row is written.

## Profile a table

A profile is a recorded description of a table's data: shapes, distributions, relationships between columns, and children per parent. Synthetic generation and fakes read profiles. Profiles are data, so they land as ordinary tables with cadence, lineage, governance and export.

### Create a profiler source

1. Open **Sources** and add a source of type **Data Profiler**.
2. Fill in the form. A profiler holds a name, a schedule and run defaults, and nothing else. It is never introspected, so it has no schema or table picker.

| Field | Meaning | Example |
| --- | --- | --- |
| Schedule (cron) | When the profiler runs | `0 3 * * *` for 03:00 daily |
| Sample above cells | Cells are rows times profiled columns. A table with more cells is profiled from a sample of about that many cells, so a wide table samples fewer rows. Empty profiles every row. | `5000000` |
| Low-cardinality threshold | A column with no more distinct values than this has every value counted, and may be inferred a category | `50` |
| Bounds on dependence work | The most numeric columns entered in the correlation matrix, and the most distinct values a category may have to enter a joint count | `30`, `50` |
| Drift window and season | How many previous runs a run is measured against, and whether the window follows a daily, weekly or monthly season | `8`, weekly |
| Drift thresholds | The distance, slope and distribution shift at which a measure is marked drifting | |

A profiler's detail view ends with a button that runs it at once for every member table.

### Add a table to the profiler

1. Open **Tables**, edit `customers`, and open the **Data Profiler** section.
2. Under **Add to Profiler**, pick a profiler. The list shows each profiler with its schedule.
3. Save the table.

A table belongs to at most one profiler. To move it, choose **Remove from Profiler**, save, and add it to the other one. Repeat for `orders`.

### Run a profile now

In the same section, **Run Profile Now** profiles the saved table immediately and reports the rows profiled. **Run Profile Now** and **View Profile Runs** stay disabled while a membership change is unsaved.

A run reads the table through the governed pipeline as the organization's administrator, in the region of the node that runs it. It describes exactly the rows and values that administrator may read there, with that administrator's row rules and masks applied. Each region profiles for itself, and no region's profile is copied to another.

### How a large table is sampled

A table within the cell budget is read whole. A larger one is sampled, by the first of these the source supports:

1. **Block sample.** A block-level sample taken at the source, so the source reads only a share of its blocks.
2. **Key ranges.** Several ranges of a single-column integer primary key, each answered from the source's index.
3. **Row filter.** A random row filter. This cuts the profile's work but not the read.

### View profile runs

**View Profile Runs** is offered in the table registry to those who may edit the table. It opens a modal with one row per run: run time, status, rows, rows profiled, method, duration and any error. Select a run to see its result tables, shown with its comparison to the previous run beside it, and each measure's history across the drift window with its drift marked.

Each run is safe for the person viewing it, worked out when it is shown from your roles and the profiled table's current rules. A column you may not see is left out. A column masked to you, or tagged `pii`, shows its shares, data type, length range and plausible type, and its values are left out: minimum, maximum, quantiles, histogram, most frequent values, shapes and fitted parameters. Every other column shows in full. The tabs are:

| Tab | What it holds |
| --- | --- |
| Columns | One row per column: type, row and null counts, distinct count, minimum, maximum, mean, standard deviation |
| Plausible types | What each column appears to hold (email, name, phone, category, free text and so on), with a confidence |
| Quantiles | A 101-point sketch of each numeric and temporal column |
| Histogram | Value counts by bucket |
| Top values | The most frequent values, and every value of a low-cardinality column |
| Value shapes | Text reduced to character classes (`A` upper, `a` lower, `9` digit), with frequencies |
| Fits | Closed-form distribution fits (normal, log-normal, exponential, uniform, gamma, and Poisson or negative binomial for integers) |
| Fit quality | How well each fit matches the sketch |
| Children per parent | For each relationship where the table is the parent, the distribution of child counts |
| Correlations, dependencies, joint counts | Rank correlations between columns, which columns tell most about which, and the counts they rest on |
| Drift | One row per measure per run, with its baseline, spread, distance, slope, distribution shift and drifting flag |

Each run also records duplicate rows, freshness (by the column the table names as its watermark), and a comparison with the previous run. Once the profile's columns table is registered, the table's read view shows its latest profile. The table view's Quick Profile, a sample computed in the browser and stored nowhere, is unchanged.

Lineage runs from the profiled table to its profile, derived from the table's membership of the profiler. Each profiled table has its own result tables; for an estate-wide view, build a materialized view over them.

### Register the result tables

A profile is invisible to every reader until you register its tables. Registration is the curation step, and nothing is exposed by default.

1. On **Tables**, choose **Register Table** and pick the Data Profiler source.
2. The form lists each member table's result tables, named `<table>_<id>_profile_<kind>`. For `customers` with table id 7, the columns result table is `customers_7_profile_columns`. Two profiled tables with the same name each get their own.
3. The form starts from the profiled table's rules: the roles that may read `customers` may read its result tables, and each described column's values are masked for the same roles as that column. Change anything before saving. Once saved, a result table's rules are its own and do not follow later changes to the profiled table.
4. Register.

A registered result table is an ordinary governed table. Its grants, masks and row rules decide who reads what, and you can query it:

```sql
SELECT run_time, column_name, null_count, distinct_count
FROM customers_7_profile_columns
ORDER BY run_time DESC
```

Every run is kept. Nothing is overwritten.

### Drift and exceptions

Each run measures how it stands against the trend of the table's recent runs. For each measure, freshness and row count included, it records the window's baseline (the median), the spread (the median absolute deviation, so one past outlier does not widen the band), how far the run lies from the baseline, and the slope over the window, so a gradual drift no single step reveals is caught. For each column's distribution it records the shift from the window's pooled distribution. A measure is marked drifting when its distance, slope or shift passes the profiler's thresholds. Until the table has as many previous successful runs as the window holds, the drift measures are null.

The profiler records drift and does not raise exceptions. A data-quality checker does:

1. Register the drift result table.
2. Point a Soda or Great Expectations checker source at it, with checks such as "no drifting measure in the latest run" or "a column's population stability index under 0.2".
3. The checker source's own schedule, history and notifications apply.

In **View Profile Runs**, **Create drift check** adds "the latest run has no drifting measure" to a checker source already scanning the registered drift table. It is refused by name where none does.

### Constraints

A profile proposes constraints its evidence supports: a column never null, a column unique, a column holding only its recorded values, a number within its observed range, one date never before another. Each shows its evidence and the share of rows it held for. Nothing is applied by proposing it. In the table editor you accept, edit or dismiss each one.

An accepted constraint becomes a check on the table's data quality, flagged when a later run or load breaks it, and it binds synthetic generation. Every run records, for each accepted constraint, the share of rows that met it and the number that broke it. You can export an accepted constraint as a check of a data-quality checker source that scans the table, so it runs on that source's schedule. Export is refused by name where no such source exists.

### Declared profiles

A declared profile holds the facts a profile run measures, written by hand. Use one to generate a table that has no data to profile, or to generate a what-if (ten times the orders, a new region's mix) without touching the source. It is stored as a profile run is, labeled declared, and generation reads it the same way. Drift, fakes measured from a profile and **Fill fakes from a profile** read measured runs only.

Declare a profile with `POST /admin/tables/{id}/declared-profiles` in the environment it belongs to:

```json
{
  "profile": {
    "rowCount": 5000,
    "columns": {
      "amount": {"nullShare": 0.02, "distinctCount": 900, "range": {"min": 1, "max": 900}},
      "placed": {"nullShare": 0, "distinctCount": 365, "range": {"min": "2026-01-01", "max": "2027-01-01"}},
      "region": {"nullShare": 0, "values": [{"value": "east", "weight": 3}, {"value": "west", "weight": 1}]}
    },
    "fanouts": {"lines": {"range": {"min": 0, "max": 6}}}
  }
}
```

| Fact | Meaning |
| --- | --- |
| `rowCount` | The table's rows |
| `nullShare` | The share of a column's rows that are null; required for every declared column |
| `distinctCount` | Distinct non-null values; taken from `values` for a category |
| `range` or `quantiles` | A number's or date's distribution: uniform between `min` and `max`, or 101 quantiles from 0 to 1 by 0.01 |
| `values` | A category's values and their weights |
| `shapes` | A text column's shapes (`A` upper, `a` lower, `9` digit) and their weights |
| `integerOnly` | Whether a number is whole; an integer column is |
| `fanouts` | Children per parent of each one-to-many relationship, as a range or 101 quantiles |
| `dependence` | Optional: a run's own correlation, dependency and joint-count rows |

A column the profile leaves out takes its fake or its synthetic rule. A key, or a column a relationship generates, takes its relationship's values. A column with none of these is refused by name.

To start from a run, read it with `GET /admin/tables/{id}/profile-runs/{run}/declared`, change what you need, and declare the result. The copy holds only what the run shows you: a column you see by its shape only is left out, so it generates by its fake or rule.

### External expectations

A run's measures can be checked against expectations produced outside Provisa: by a person, a spreadsheet or a forecasting model. Hold them in any registered table with these columns:

| Column | Meaning |
| --- | --- |
| `measure` | Any measure of a run, a constraint's pass share among them |
| `column` | The column it applies to, if any |
| `period start`, `period end` | When the expectation holds |
| `low`, `expected`, `high` | The band |
| `source` | Who produced it |

**Create expectation check** takes a registered table of that shape and adds, to a checker source already scanning the registered profile results, a check that the latest run's measures lie within `low` and `high` of the expectation whose period holds the run. It is refused by name where no such checker source exists or the table is not of the shape.

## Declare fakes

A fake is a rule that says what a column's values look like instead of its real ones. [Fake methods](fake-methods.md) defines every kind and method.

### Declare a fake on a column

Fake the identifying columns: those tagged `pii`, and names, emails, phones, addresses, government identifiers and account numbers. Leave the rest alone.

1. On **Tables**, edit `customers`. The column list has two modes: **Metadata**, the columns as described, typed, shown, masked and tagged, and **Test data**, the same rows showing each column's fake (blank where the real value is shown), its synthetic rule, a **Stable** switch and a sample value computed by the serving engine. A `pii` column with no fake is marked.
2. In **Test data** mode, edit a simple fake in its cell. To choose a kind, open the dialog for the column: it lists the kinds and methods by name and category with their arguments, the Stable switch, a preview of the values each gives, and the refusals the save would give as they arise.
3. Tick **Stable** if the fake must be the same on every engine, region and engine replacement.
4. Save.

| Column | Fake | Why |
| --- | --- | --- |
| `customers.name` | `name()` | A realistic full name |
| `customers.email` | `email()` | A realistic email address |
| `customers.phone` | `phone_number()` | A realistic phone number |

`orders.total`, `orders.status`, `orders.placed_at` and `orders.shipped_at` carry no fake. A read shows their real values, so order sizes, status mix and shipping delays are production's own.

The save is checked. A declaration is refused, with the column named, when:

- the kind is unknown, or its arguments are wrong (shares that do not sum to 1, a standard deviation of zero, points out of order);
- the kind cannot fill that column's type (`bool()` on a text column);
- a relative fake (`after`, `before`, `greater_than`, `less_than`, `sql`) names a column the table does not hold;
- the fakes of a table reference each other in a cycle;
- Stable is ticked with no fake, or on a method the portable definition does not cover;
- two columns joined by a relationship declare different fakes, or different stability, so the join would stop matching.

A synthetic dataset draws a faked column's generated values from the same declaration, so a column fakes alike whether it is shown as test data or generated.

**Columns your row rules read.** If a row-level security rule reads a column, either leave that column unfaked or give it a fake whose values still match the rule, such as `categories()`, which keeps the column's real values. Otherwise users in a Test (fake) environment, or on synthetic data, may see no rows, or rows that contradict the rule. The choice is the operator's. Provisa does not enforce it.

**Comparisons, ranges and sorts on faked columns.** They run over the faked values, which define their own order. A faked `customers.name` sorts as the fakes sort, not as the real names did. Where results must be the same on every engine and in every region, declare the fake stable. Where a column must compare or sort like production, leave it unfaked. The choice is the operator's.

### Fill fakes from a profile

**Fill from profile** in the column dialog fills one column. The same action on the column list fills every column at once, each still editable before you save. It proposes fakes only for identifying columns: those tagged `pii`, and those whose plausible type in the profile is a person's name, an email, a phone number, an address, a government identifier or an account number. Each gets its own method. A code-like identifier gets the `pattern` kind.

For the other columns it may propose synthetic rules (see below), not fakes. `categories`, listing the recorded values, is proposed only where the profile shows real repetition: few distinct values, each held by many rows. In a small table, where the evidence is thin, it proposes nothing and you decide. A `pii` column with no confident match is left undeclared and marked.

Columns already declared are left as they are. Nothing is saved by the action: the filled fields are the form's, which you tune and save as any edit.

## Generate a synthetic dataset

A synthetic dataset fills a non-production environment with generated data shaped like the profiled tables it names, at the scale you choose. In that environment its tables read the generated data instead of their sources.

### Define a dataset

1. Open **Environments**, then the **Synthetic data** tab.
2. Pick the environment to fill. `prod` is refused.
3. Under **Define a dataset**, set:

| Field | Meaning | Example |
| --- | --- | --- |
| Dataset name | Its identifier | `load-test` |
| Scale | A multiple or a fraction of the profiled row counts | `10` for ten times; `0.5` for half |
| Seed | The same seed and scale give the same data, so runs compare | `1` |
| Profiles from | The environment whose profile runs describe the tables, typically `prod` | `prod` |
| Run | One profile run for each table, so the dataset reproduces exactly | `2026-10-06 03:00 (120,000 rows)` |
| Table scale | Overrides the dataset scale for one table | `2` |
| Privacy budget ε | Makes the dataset private (see below) | `1.0` |
| Closeness threshold, draws | How near a real row a generated row may lie, and how many times it is redrawn | |

4. Choose **Save dataset**.

Name `customers` and `orders` together. A dataset that names a table but not the parent of one of its relationships is refused, naming the missing parent. A dataset may name profile runs held by another environment of the same organization.

### Generate

Before you generate, the dataset's form shows its estimated rows and size in the warehouse store. Choose **Generate** on the dataset's row. Status moves from Defined to Generating to Generated, or Failed with the reason. **Regenerate** reruns it; the same seed and scale produce the same data. Generation runs in the engine, in parallel partitions written through the bulk-load paths, so a dataset of billions of rows never passes through a client.

What generation keeps:

- **Row counts** follow the scale, with the ratio between tables kept: scale 10 gives `orders` ten times its profiled rows and `customers` ten times its own.
- **Key uniqueness** and density.
- **Fan-out.** The number of children per parent is drawn from each relationship's profiled distribution, at any scale, so join skew survives a reduction as well as a growth.
- **Distinct counts.** A column whose distinct count follows its rows scales with them. One that does not keeps its distinct count.
- **Value skew.** Hot keys stay hot.
- **Null and zero shares** and integer-only columns.
- **Dependence.** Numbers and categories are drawn jointly through the profile's correlation matrix, and a column the dependency network ties to others is drawn after them. A column with a declared fake keeps its fake.
- **Accepted constraints.** A column constrained unique has unique values; never null has none; an ordering between two dates is generated as `after` or `before`, and one number never exceeding another as `greater_than` or `less_than`.

How a value is chosen:

- A column's synthetic rule, where declared, decides its values. Otherwise its fake does. Otherwise its profile does.
- A column drawn from its profile: a number or date through its quantile sketch, text from its recorded shapes and lengths, so codes keep their format. A recorded value is never copied, except through a column's `categories` fake.
- A boolean column with no fake draws as `bool()`: true at its measured share.
- A column tagged `pii` with neither a fake nor a synthetic rule refuses the generation, listing every such column, until you declare one for each.

### Synthetic rules

A synthetic rule is declared beside a column's fake, in **Test data** mode, and applies only to generation. Use rules for the columns you left real on read, such as amounts and states, when generated rows need a particular shape:

| Column | Synthetic rule | Why |
| --- | --- | --- |
| `orders.total` | `lognormal(median=40, p95=300)` | Many small orders, a few large |
| `orders.status` | `categories((new, paid, shipped), (.2, .5, .3))` | Three states at stated shares |
| `orders.shipped_at` | `after(placed_at, 1 to 10 days)` | Never before the order was placed |

Any kind may be a rule. `sql_group` and `sequence`, which make sense only over a generated table, may be nothing else. [Fake methods](fake-methods.md) defines each.

### Conditional fan-out

A relationship may carry conditional child counts, each a condition on the parent row, written in the SQL subset of the fakes. The first condition a parent meets decides its count: a fixed number, a range drawn evenly, or the fan-out the profile measured for parents meeting it. A parent meeting none takes the relationship's measured fan-out. For example, no shipments for an order whose status is `cancelled`, or at least one reward for a gold member.

### Assertions

A dataset may carry assertions, each a statement over its generated tables that returns true or false, for example that every customer's amounts sum to zero. After generation each assertion runs, and the report shows its result beside the statement. An assertion never rejects, regenerates or alters the data.

### Not too close to a real row

A dataset may declare a closeness threshold and a number of draws, both or neither. With neither, closeness is not checked, and the report says so.

When it generates, the dataset reads up to 2,000 of each table's real rows as the organization's administrator and measures each generated row's distance to its nearest real row. The distance reads only the columns drawn from real values, so keys, foreign keys and columns with a declared fake or rule are left out. A number counts its difference over the sample's interquartile range; a date counts the same over seconds; any other value counts 0 if equal and 1 if not. The distance is the mean over the columns.

The threshold is a share of the real rows' median distance to their nearest other real row. A generated row nearer than that to any real row is drawn again, up to the number of draws, and dropped if no draw is far enough. A dropped row's children are dropped too. A draw changes a row's values only: its key, its parent and how many children it has stay the same, and so do the columns a child's conditional fan-out or dependence reads.

The real sample is held in the dataset's own store schema for the comparison only. It is never registered, so no role of the environment can read it, and it is removed once generation ends, whether generation succeeded or failed.

A private dataset cannot declare closeness, because the comparison reads real rows outside its ε.

### Differential privacy

Declare a dataset private with a privacy budget ε. A private dataset reads nothing from the values its profile runs recorded. Every statistic it is generated from is measured from the tables at generation time, with Laplace noise added, and charged against ε. That covers row and null counts, distinct counts, the shares of each column's values, each number's or date's distribution, each relationship's children per parent, and its hot parents. ε is split evenly across the kinds of statistic, and the charges add up to ε.

Nothing the noise could not hide reaches the generated rows. A distribution's range comes from a noised histogram, never from a recorded minimum or maximum, and values outside it are clipped. A column's shares are counts with no values attached. So in a private dataset:

- a text column must declare a fake or a synthetic rule, such as a method like `word()`, or `categories((a, b, c))` naming its values;
- `categories()` must name its values;
- `pattern()` cannot be used, because its shapes come from real values.

Any of these is refused by name when the dataset is generated. Faked reads are unaffected. A dataset not declared private carries no privacy guarantee, and its report says so.

#### Choosing ε

ε is how much the dataset may reveal about any one row. A smaller ε adds more noise. The noise does not shrink with the table, so on a small table it can swamp the statistics: at ε = 1, a table of a few hundred rows comes out mostly noise. The report flags any statistic whose noise is as large as its value. When that happens, raise ε, generate from a larger table, or give the column a synthetic rule that declares its distribution, such as `uniform(min=0, max=500)`. A column with a declared distribution spends nothing on measured bounds, which leaves more of ε for the rest. On a table of a hundred thousand rows, ε = 1 keeps distributions close to the real ones.

### Read the comparison report

Choose **Report** on a generated dataset. After generation the synthetic tables are profiled and compared with the source profiles:

| Column | Meaning |
| --- | --- |
| Table, Column | What was compared |
| Measure | Row counts; null share; the Kolmogorov-Smirnov distance between source and synthetic quantile sketches; skew; children-per-parent distances; correlation and dependency differences |
| Source, Synthetic, Difference | The two values and their gap |
| Note | Names every column generated with no declared fake, so you see what is undeclared |

The report also shows each conditional fan-out's parents and children, each assertion's result, the closeness threshold, the rows drawn again, dropped, and dropped with a parent, and the generated rows' distances to the nearest real row beside the real rows' own. For a private dataset it gives ε, the mechanism, how ε was split, the ε charged in total, what the noise dropped, and every statistic that is mostly noise. When closeness is checked it gives named privacy measures too: distance to closest record, the nearest-neighbour distance ratio, and a membership test, the chance that a generated row lies nearer the real rows than a real row lies to the others. A value of 0.5 means the generated rows sit no nearer than real rows do.

A small Kolmogorov-Smirnov distance means the synthetic column follows the source's distribution. A large one on `orders.total` says to declare a distribution, or to re-profile. The generated tables are marked as generated, and an environment filled this way holds no personal data.

### Drop a dataset

**Drop** removes the dataset and its schema. The environment's tables read their sources again.

### Seed a new environment

When you create an environment, the form offers **Start on synthetic data**. Name the tables, the environment whose profile runs describe them (typically `prod`), a scale and a seed. The new environment starts on synthetic data shaped like that estate, so development starts on plausible data without any production row reaching it.

## Walkthrough: a test environment for orders

The goal: a `dev` environment where developers see production's real order data, with no customer identified. A second pass then adds a ten-times synthetic dataset for load tests.

1. **Create the environment.** On **Environments**, create `dev` with the data mode **Test (fake)** and mutation handling **Reversible**, so tests can write without changing the parent's data.
2. **Fake the identifying columns.** Edit `customers`, switch the column list to **Test data** mode, and choose **Fill from profile** (profile `customers` first if it has no run). Review the proposals: `name()` on `name`, `email()` on `email`, `phone_number()` on `phone`. Tick **Stable** on each if results must match across engines and regions. Save.
3. **Leave the rest real.** `orders.total`, `orders.status`, `orders.placed_at` and `orders.shipped_at` carry no fake. If a row rule reads a column you faked, leave that column unfaked or give it `categories()`.
4. **Use it.** Switch to `dev`. Customers show fake names, emails and phones. Joins from orders to customers still match, order totals and delays are production's own, and the data is as fresh as production. Writes land in the environment's own changes and never reach the source.
5. **Profile production for the load test.** Add a Data Profiler source with schedule `0 3 * * *` and sample above `5000000` cells. Add `customers` and `orders` to it, save, and choose **Run Profile Now** on each. Open **View Profile Runs** to confirm both succeeded.
6. **Add a drift check.** Register the drift result table for `orders`, then choose **Create drift check** in View Profile Runs.
7. **Declare synthetic rules.** On `orders`: `lognormal(median=40, p95=300)` on `total`, `categories((new, paid, shipped), (.2, .5, .3))` on `status`, `after(placed_at, 1 to 10 days)` on `shipped_at`.
8. **Define and generate.** In a second environment, `load`, open **Synthetic data**. Name the dataset `load-test`, scale `10`, seed `1`, profiles from `prod`, and select `customers` and `orders` with their latest runs. Choose **Generate**.
9. **Check the report.** Row counts show `orders` and `customers` at ten times. The Kolmogorov-Smirnov distance on `orders.total` is small. Any column named in the Note column has neither rule nor fake; add one and regenerate.
10. **Test.** Run your suite against `load`. A statement joining its tables to a table still reading real data is refused, so generate every table the test joins.

## See also

- [Fake methods](fake-methods.md): every kind of fake and every method.
- [Environments](environments.md): copies of the governed model, bindings, merge and deploy.
