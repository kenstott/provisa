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

In any environment other than `prod`, each table reads from one of three places:

1. **Its source.** The table reads through the environment's bindings, under the environment's governance, as it does anywhere.
2. **Its source, faked on read.** The same rows, with every column that declares a fake showing a generated value in place of the real one. Nothing is stored. The engine computes the fakes as it serves the read, so ordering, filtering, grouping and joining work on the fakes the reader sees. This applies in a test-data environment.
3. **Its synthetic dataset.** Generated rows held in the environment's warehouse store. In that environment the table reads them instead of its source.

The Environments page shows each table's kind of data in the environment. Provisa generates each table's bindings when the environment is created and whenever its model changes, so the semantic SQL is the same in every environment and every feature works on it unchanged.

A table named by a synthetic dataset reads synthetic data whatever else is set. Fakes apply only to tables still reading their source, so synthetic data is never faked twice. A statement that joins a synthetic table to a table reading real data is refused, with both tables named: their keys share nothing, and the result would look plausible and mean nothing. The `prod` environment cannot hold a synthetic dataset.

An environment has two stores of its own. Its warehouse store holds all data that no source holds: every synthetic dataset, whatever its size, and any extract landed whole. Its relational store holds only the environment's own changes. A dataset is landed through the governed read path, so masking and row-level security apply before any row is written. The dev stores hold only what governance permitted out of prod and what the environment generated or wrote itself. A structure the environment's model declares and no source carries is created in its warehouse store. The relational store's size limit applies to change logs alone.

### Read and read-write environments

You create a non-production environment as **read** or **read-write**; the choice is on its form.

- **Read.** Takes no writes. Every write and every API mutation is refused by name.
- **Read-write.** Takes writes. The environment keeps its own changes, and a write never reaches a source. A table the environment has written reads as its original rows with those changes applied, the latest version of each row winning, and a deleted row left out. The cost follows the rows written, not the table's size, so a synthetic table of any size is writable.

You can switch an environment between read and read-write only while it holds no synthetic dataset, or as an explicit regeneration of its datasets under the new choice.

A table is writable in a read-write environment exactly where its source can take the write and the model enables it. A writable table needs a primary key; one without it is refused by name when the environment is created. A key you supply that the environment already shows is refused, because the environment owns a range of keys of its own for the rows it inserts.

For a table faked on read, the changes are applied over the faked rows, and written rows are never faked again, so a value a client reads and writes back reads back unchanged.

### API mutations

An API source's mutations are opaque: Provisa cannot know what they change. Outside production, a mutation runs only if the environment binds its own connection for that API, and then it runs against that API as in production. With no binding of its own, the mutation is refused by name. It is never sent to production's API. An environment backed by synthetic data treats API mutations no differently: it assumes they work and leaves their effect to whatever API it binds.

### Test-data mode

An org_admin with `environment_management` marks an environment as test data with the **Test data** switch on the Environments page.

In a test-data environment:

- Every column with a declared fake shows the fake to every role, including roles the column is otherwise unmasked to.
- Every column tagged `pii` with no declared fake shows `NULL`.
- A fake is stable unless declared otherwise.

No role setting changes this. Personal data never reaches a reader, whatever the environment's stores hold.

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

**Columns your row rules read.** If a row-level security rule reads a column, either leave that column unfaked or give it a fake whose values still match the rule, such as `categories()`, which keeps the column's real values. Otherwise users in a test-data environment, or on synthetic data, may see no rows, or rows that contradict the rule. The choice is the operator's. Provisa does not enforce it.

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

When it generates, a dataset reads a sample of each table's real rows as the organization's administrator and measures each generated row's distance to its nearest real row, over the columns with no declared fake. A row nearer than the closeness threshold, a share of the real rows' own typical distance to one another, is drawn again. A row still too near after the stated number of draws is dropped and counted.

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

The report also shows each conditional fan-out's parents and children, each assertion's result, the distribution of generated rows' distances to the nearest real row beside the real rows' own, and the rows redrawn and dropped. For a private dataset it gives ε, the mechanism, how ε was split, the ε charged in total, what the noise dropped, and every statistic that is mostly noise. It gives named privacy measures too: distance to closest record, the nearest-neighbour distance ratio, and the result of a membership-inference test.

A small Kolmogorov-Smirnov distance means the synthetic column follows the source's distribution. A large one on `orders.total` says to declare a distribution, or to re-profile. The generated tables are marked as generated, and an environment filled this way holds no personal data.

### Drop a dataset

**Drop** removes the dataset and its schema. The environment's tables read their sources again.

### Seed a new environment

When you create an environment, the form offers **Start on synthetic data**. Name the tables, the environment whose profile runs describe them (typically `prod`), a scale and a seed. The new environment starts on synthetic data shaped like that estate, so development starts on plausible data without any production row reaching it.

## Walkthrough: a test environment for orders

The goal: a `dev` environment where developers see production's real order data, with no customer identified. A second pass then adds a ten-times synthetic dataset for load tests.

1. **Create the environment.** On **Environments**, create `dev` as read-write, so tests can write. Turn on **Test data**.
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
