# Fake methods

A fake is a rule that says what a column's values look like in place of its real ones. Fake as little as you can: the identifying columns (names, emails, phones, addresses, identifiers). Every column you leave unfaked keeps its real values, so every real relationship, edge case and distribution survives. A column has a **fake** and, optionally, a **synthetic rule** laid over it. The fake is what a read shows in a Test (fake) environment; a column with none shows its real value. The synthetic rule applies only when a synthetic dataset is generated, and where declared it takes the place of the fake there. Every kind below may be either, except `sql_group` and `sequence`, which make sense only over a generated table: they are synthetic rules, and declaring one as a fake is refused by name.

A column declares one fake in the table editor's **Fake** field, as one call: `email()`, `categories((new, paid, shipped))`, `after(placed_at, 1 to 10 days)`. The [test-data guide](test-data.md) shows where fakes fit; this page defines each one.

A declaration is checked when saved and when loaded. It is refused, naming the column and the cause, if it cannot describe values: an unknown name, arguments the kind does not take, shares that do not sum to 1, a distribution whose points are out of order, or a kind that cannot fill the column's type. Where this page says a fake "shows" a value, it means a read of a table in a Test (fake) environment. A synthetic dataset draws the same values into generated rows.

## Writing a declaration

A declaration is `kind(arguments)`. Arguments are positional or named (`mean=50`). A list of values is written in parentheses: `(shoes, bra, sweater)`. A bare word or a quoted string is a value. A distance between two dates is written `3 days`, or as a range `1 to 10 days`.

```text
categories((shoes, bra, sweater), (.1, .6, .3))
normal(mean=50, sd=10, min=0)
after(created_at, 1 to 10 days)
sql(quantity * price)
```

Any name that is not one of Provisa's own kinds below names a [general fake method](#general-fake-methods), called with the arguments given.

## Properties every fake shares

**Consistency.** A fake that replaces a real value is a keyed function of that value: the same real value always shows the same fake, and different real values show different fakes. Joins, grouping and distinct counts through the column give the results they would give on the real values. The key is one platform key; without it a fake cannot be turned back into the value. A method's fake is drawn from the value's keyed digest mixed with a hash of the column's fake as declared (the method, its arguments, for a stable fake its definition version, and the column's type family: every integer type is one family and every text type another, a decimal keeps its precision and scale, and timestamps with and without a time zone differ), so two columns faked from the same value by different methods do not draw alike, and changing a column's fake changes all of its values.

**Where it is computed.** The engine of the region serving the read computes every fake at the read. Ordering, filtering, grouping, joining and aggregating through a faked column work on the fakes the reader sees, never on the real values.

**Stable fakes.** Tick **Stable** and a column's fake is computed from Provisa's portable definition, so it is the same on every engine, in every region and after an engine is replaced. A fake that is not stable is computed by the serving engine's own method and is consistent within its region only. In a Test (fake) environment a fake is stable unless declared otherwise, since test data is extracted, kept and compared over time. A stable fake must declare a fake. These methods can be stable, and no others:

`city`, `company`, `country`, `email`, `first_name`, `job`, `last_name`, `name`, `phone_number`, `postcode`, `sentence`, `state`, `street_address`, `user_name`, `uuid4`, `word`

Every distribution kind below, and `bucket`, `truncate`, `prefix`, `hash` and `encrypt`, is a pure function of its arguments and can be stable. A stable `profile()` must pin its run: `profile(run=<id>)`. A stable method takes no arguments. A stable fake cannot be one that reads what is measured where it is read: `pattern()`, `categories()` with no values, `bool()` with no share, or `after`, `before`, `greater_than` and `less_than` with no distance. A stable fake that names another column, such as `after(created_at, 1 day)` or `sql(quantity * price)`, needs each faked column it names to be stable too. Each of these is refused by name when saved.

A stable fake is pinned to the version of Provisa's portable definition it was saved under, and the column editor shows that version. A later release that adds a new version leaves the column on its pinned version, so its fakes do not change. The column moves to the newest version only when its fake is changed, or when it is made stable again after being made not stable.

**Joined columns agree.** Two columns joined by a relationship, both faked, must declare the same fake, the same stability and, when stable, the same definition version, and be of one type family, so the join still matches. A declaration that breaks this is refused, naming the relationship.

**Uniqueness.** Fakes of the identifier, email and phone kinds carry a short part derived from the keyed digest of the real value, so distinct real values give distinct fakes. Person names may collide, as real names do.

## Provisa's own kinds

### Values from a list

| Declaration | Meaning | Column type |
| --- | --- | --- |
| `categories((shoes, bra, sweater))` | A value from the list. Shares come from the column's measurement; a value with no measured share takes an even share of what remains. | Any; each value must fit the type |
| `categories((shoes, bra, sweater), (.1, .6, .3))` | The values and their shares. There must be one share per value, each between 0 and 1, summing to 1. | Any |
| `categories()` | Values and shares both from measurement. Refused on an empty table, which asks for the values to be declared. | Any |
| `bool()` | True at the measured share of true; even on an empty table | Boolean |
| `bool(.8)` | True 8 times in 10 | Boolean |

Example:

```text
categories((new, paid, shipped), (.2, .5, .3))
```

Measured values and shares come from the column's latest profile run. With no profile run, or no full value-frequency table, they come from the table itself, read as the organization's administrator through the governed pipeline, as a profile is.

A `categories` list is the one way a real value reaches a synthetic dataset.

### Numeric and temporal distributions

Each draws a uniform point and maps it through the distribution. Masking draws the point from the keyed digest of the real value, so a value always shows the same fake. A synthetic dataset draws it from the dataset's seed and the row. The result is cast to the column's type and scale, and an integer column stays integer. These fake a numeric column (integer or decimal), or a date or timestamp column when their points are dates, as in the second block below. A declaration with dates among its points and numbers among others is refused.

| Declaration | Meaning |
| --- | --- |
| `percentiles(min=0, p50=40, p95=300, max=900)` | Any three or more of `min`, `p5`, `p25`, `p50`, `p75`, `p95`, `max`, joined piecewise-linearly. Values must not decrease. |
| `normal(mean=50, sd=10)` | A normal distribution. `sd` must be above zero. Add `min` and `max` to bound it. |
| `lognormal(median=40, p95=300)` | A log-normal distribution given by its median and 95th percentile. Or `lognormal(mu=3.7, sigma=1.1)`. Optional `min`, `max`. |
| `uniform(min=1, max=100)` | Every value in the range equally likely |
| `triangular(min=0, mode=20, max=100)` | Most likely at the mode. `mode` must lie between `min` and `max`. |
| `poisson(mean=3)` | Integer counts. `mean` must be above zero. Integer column only. |
| `profile()` | The column's profiled distribution (numeric or temporal). `profile(run=<id>)` pins one profile run. A column with few distinct values draws from the profile's value frequencies instead of its sketch. A serving region with no run of the named profile refuses the read, naming it. |

An integer column that declares `lognormal(median=40, p95=300)` gets whole numbers; a decimal column gets values at its scale. Arguments that cannot describe a distribution (points out of order, a minimum above a maximum, a standard deviation not above zero) are refused by name.

Over dates and times, write each point as a quoted ISO date or timestamp and each spread as an interval:

```text
normal(mean='2026-01-01', sd=30 days)
uniform(min='2025-01-01', max='2026-01-01')
triangular(min='2025-01-01', mode='2025-06-01', max='2026-01-01')
percentiles(min='2025-01-01', p50='2025-06-01', max='2026-01-01')
lognormal(median='2025-03-01', p95='2025-12-01', min='2025-01-01')
```

A log-normal over dates needs `min`, the moment its times are measured from. A month counts as 30 days and a year as 365.25 days in a spread, which is a scale and not a calendar step. `poisson` and `bucket` do not take dates.

### Pattern

`pattern()` fills the value shapes a profile recorded for the column, so codes and references keep their format: a column whose values look like `AB-1234` gets values of the same shape. It fakes a text column and takes no arguments.

### Bucket, truncate, prefix

Each is a function of the value alone, so grouping, sorting and filtering work on the result. None can be reversed.

| Declaration | Meaning | Example | Column type |
| --- | --- | --- | --- |
| `bucket(10)` | The fixed-width range the value falls in | 37 shows as a label for the 30 to 40 range | Numeric |
| `bucket((0, 18, 65))` | The range between named edges, in ascending order | 37 shows as a label for the 18 to 65 range | Numeric |
| `truncate(month)` | The date cut to a unit: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second` | 2026-10-06 shows as 2026-10-01 | Date or timestamp. A date has no `hour`, `minute` or `second`. |
| `prefix(3)` | The first characters of a code | `GB84MYNB48764759382421` shows as `GB8` | Text |

The exact text of a bucket label is the engine's.

### Hash and encrypt

| Declaration | Meaning |
| --- | --- |
| `hash()` | A keyed digest of the value, under the platform key held inside the engine. The same value gives the same digest, so joins on it still match. Cannot be reversed. |
| `encrypt()` | A deterministic encryption under the same key that keeps the value's format: digits stay digits and the length is kept, so it passes the column's type and joins still match. Reversed only inside Provisa, for a reader granted the column unmasked. Never by a client. |

Both fake a text or integer column. A synthetic dataset applies them to generated values, never to real ones. Shuffling a column's values between rows is not a kind: it is not a function of the value and still shows the real ones.

### After, before, greater than, less than

A column can follow another column of the same table.

| Declaration | Column and the one it names | Value |
| --- | --- | --- |
| `after(placed_at, 3 days)` | Date or timestamp, both | The named column's value, later by 3 days |
| `after(placed_at, 1 to 10 days)` | | Later by a distance drawn evenly from the range |
| `before(delivered_at, 2 days)` | | Earlier by 2 days |
| `after(placed_at)` | | Later by the measured difference between the two columns |
| `greater_than(price, 5)` | Numeric, both | The named column's value plus 5 |
| `less_than(price, 1 to 3)` | | The named column's value minus a distance drawn evenly from 1 to 3 |
| `greater_than(price)` | | Plus the measured difference |

A distance below zero, a range that runs backwards, or a date distance with no unit (`days`, `hours`, `weeks` and so on) is refused. A synthetic dataset generates the named column first. A read computes the value from the named column's faked value. The order between the two holds in every row either way.

### sql

`sql(expression)` derives a value from other columns of the same row:

```text
sql(quantity * price)
sql(CASE WHEN status = 'cancelled' THEN 0 ELSE total END)
```

The expression is one scalar expression in the governed SQL dialect, translated for each engine: arithmetic, comparison, conditional, string, date and time operations, and a published list of functions (`coalesce`, `nullif`, `greatest`, `least`, `abs`, `round`, `floor`, `ceil`, `power`, `sqrt`, `ln`, `exp`, `sign`, `upper` and others). It allows no subquery, no other table, no aggregate or window, and nothing whose result could differ between two reads of the same row. An expression outside that subset, or naming a column the table does not hold, is refused when saved.

The columns it names are generated, or faked, first. The result is computed over those values.

### sql_group

`sql_group(expression)` computes a value across rows. It is a synthetic rule: it builds a rule that spans rows, where `sql` checks one row. A read creates no rows, and a window over a filtered read would see only the rows read, so a read shows the column's fake, or its real value, and never applies the rule.

```text
sql_group(SUM(amount) OVER (PARTITION BY customer_id ORDER BY id))
sql_group(SUM(lines.amount))
```

The expression is the `sql` subset plus window functions (`row_number`, `rank`, `dense_rank`, `lag`, `lead`, `first_value`, `last_value`) over partitions of the same table, and sums, counts and the like over a parent's children through a declared relationship (`SUM(lines.amount)`). Typical uses: the last row of each customer takes the remainder so that customer's amounts net to zero; a running balance; a sequence number within an order; an invoice total that sums its lines.

The expression may name `self`, the value the column's fake draws, or its profile draws where the column has no fake. A rule can then adjust only the rows it needs and keep the drawn value on the rest.

### sequence

`sequence((states), entity, order)` is a synthetic rule that gives an entity's rows their states in order:

```text
sequence((new, paid, shipped), order_id, line_no)
```

The three arguments are the states in order, the column that groups an entity's rows, and the column that orders them. Each entity's rows take the states in turn and never go back. An entity with fewer rows than states stops part-way, as an order not yet shipped does, and an entity is generated with no more rows than there are states. A read shows the column's fake, or its real value.

## Evaluation order and cycles

A row's fakes and rules are computed in this order:

1. **Independent fakes** first: every kind that names no other column.
2. **Relative fakes** next: `after`, `before`, `greater_than`, `less_than` and `sql`. Each is computed after every column it names, in the order the references require.
3. **Group rules** last, in synthetic generation only: `sql_group` and `sequence`, after the rows they span exist, again in reference order.

A set of fakes whose references form a cycle, a chain that names itself, is refused when any of them is saved, and the message names the columns in the cycle:

```text
orders: the fakes of a, b name one another in a cycle (a -> b -> a)
```

## General fake methods

Any other name is a general fake method: a realistic generated value of one kind (a name, an address, a phone number). A method is checked when declared. It must exist, take only the [arguments listed below](#method-arguments), and make values the column's type can hold: text, integer, decimal, boolean, date or timestamp.

Pass arguments by name: `date_between(start_date='-5y', end_date='today')`, `pyint(min_value=1, max_value=100)`. The example outputs below come from one run; yours differ unless the column is stable.

A method not listed under Stable fakes above is computed by the serving engine's own implementation, seeded by the keyed digest of the real value. It is consistent within its region. Declaring such a method stable is refused, naming the methods that can be.

**Now and today.** A method whose range starts or ends at now or today, by default or through an argument such as `end_date='now'` or `start_date='-30d'`, counts from one fixed reference instant, 15 July 2026 at 12:00 UTC, never from the clock. A value is the same at every read, on every engine and in every time zone. `date_time_this_month()` gives a time between 1 and 15 July 2026, and `date_time_between(start_date='-30d', end_date='now')` a time between 15 June and 15 July 2026.

### Names

Return text unless noted.

| Method | Makes | Example |
| --- | --- | --- |
| `name()` | A full name | `Joshua Wood` |
| `name_female()` | A full name from female first names | `Kimberly Wood` |
| `name_male()` | A full name from male first names | `Joshua Wood` |
| `name_nonbinary()` | A full name from nonbinary first names | `Amy Wood` |
| `first_name()` | A given name | `John` |
| `first_name_female()` | A female given name | `Cynthia` |
| `first_name_male()` | A male given name | `David` |
| `first_name_nonbinary()` | A nonbinary given name | `John` |
| `last_name()` | A family name | `Young` |
| `last_name_female()` | A family name from the female list | `Young` |
| `last_name_male()` | A family name from the male list | `Young` |
| `last_name_nonbinary()` | A family name from the nonbinary list | `Young` |
| `prefix()` | A title such as Dr. or Mx. | `Mr.` |
| `prefix_female()` | A female title | `Mrs.` |
| `prefix_male()` | A male title | `Mr.` |
| `prefix_nonbinary()` | A nonbinary title | `Mx.` |
| `suffix()` | A name suffix such as PhD | `II` |
| `suffix_female()` | A female suffix | `MD` |
| `suffix_male()` | A male suffix | `II` |
| `suffix_nonbinary()` | A nonbinary suffix | `II` |
| `language_name()` | The name of a language | `Haitian` |

### Addresses and places

Text unless noted.

| Method | Makes | Example |
| --- | --- | --- |
| `address()` | A full street address, city, state and postal code on two lines | `9791 Regina Mountains / Andreaborough, VT 62139` |
| `street_address()` | Building number, street and sometimes a unit | `825 Garrett Circles` |
| `street_name()` | A street name | `Brittany Curve` |
| `street_suffix()` | A street type such as Vista | `Freeway` |
| `building_number()` | A building number | `98259` |
| `secondary_address()` | A unit such as Suite 604 | `Apt. 982` |
| `city()` | A city name | `New Amy` |
| `city_prefix()` | A city name's first part such as Port | `East` |
| `city_suffix()` | A city name's last part | `bury` |
| `state()` | A state name | `Kansas` |
| `state_abbr()` | A two-letter state code | `IA` |
| `administrative_unit()` | A state or province name | `Kansas` |
| `postcode()` | A postal code | `31691` |
| `postalcode()` | A postal code (same as postcode) | `31691` |
| `zipcode()` | A ZIP code | `31691` |
| `postcode_in_state()` | A postal code inside a state | `52428` |
| `postalcode_in_state()` | A postal code inside a state | `52428` |
| `zipcode_in_state()` | A ZIP code inside a state | `52428` |
| `postalcode_plus4()` | A ZIP+4 code | `31691-9710` |
| `zipcode_plus4()` | A ZIP+4 code | `31691-9710` |
| `country()` | A country name | `Dominica` |
| `country_code()` | A two-letter country code | `GW` |
| `current_country()` | The default country's name | `United States` |
| `current_country_code()` | The default country's two-letter code | `US` |
| `military_dpo()` | A military diplomatic post office address | `Unit 3982 Box 5979` |
| `military_ship()` | A military ship prefix | `USNS` |
| `military_state()` | A military postal state code | `AE` |
| `locale()` | A language and region tag | `hi_IN` |
| `language_code()` | A two-letter language code | `hi` |

### Geography

Coordinates are numbers; the rest are text.

| Method | Makes | Example |
| --- | --- | --- |
| `latitude()` | A latitude in degrees | `-26.1218565` |
| `longitude()` | A longitude in degrees | `-52.243713` |

### Internet

Text unless noted.

| Method | Makes | Example |
| --- | --- | --- |
| `email()` | An email address | `brittanyanderson@example.net` |
| `safe_email()` | An email address on a reserved example domain | `brittanyanderson@example.net` |
| `free_email()` | An email address at a free provider | `brittanyanderson@hotmail.com` |
| `company_email()` | An email address at a company domain | `brittanyanderson@wells.info` |
| `ascii_email()` | An email address of ASCII letters only | `mitchellmunoz@carpenter.info` |
| `ascii_safe_email()` | An ASCII email address on a reserved example domain | `brittanyanderson@example.net` |
| `ascii_free_email()` | An ASCII email address at a free provider | `brittanyanderson@hotmail.com` |
| `ascii_company_email()` | An ASCII email address at a company domain | `brittanyanderson@wells.info` |
| `free_email_domain()` | A free provider's domain | `gmail.com` |
| `user_name()` | A login name | `brittanyanderson` |
| `domain_name()` | A domain name | `watkins.com` |
| `domain_word()` | The first label of a domain | `watkins` |
| `safe_domain_name()` | A reserved example domain | `example.org` |
| `tld()` | A top-level domain | `com` |
| `hostname()` | A host name | `srv-98.wood.info` |
| `dga()` | A machine-generated domain name | `gmjfulkjmkjgehahqffvkxvio.com` |
| `url()` | A URL | `http://www.moon-wells.org/` |
| `uri()` | A URI with a path | `http://www.smith.com/list/tagfaq.html` |
| `uri_path()` | A URI path | `posts` |
| `uri_page()` | A page name for a URI | `main` |
| `uri_extension()` | A file extension for a URI | `.htm` |
| `slug()` | A hyphenated URL slug | `economy-person-off` |
| `image_url()` | A placeholder image URL | `https://dummyimage.com/487x267` |
| `ipv4()` | An IPv4 address | `114.22.54.54` |
| `ipv4_private()` | A private-range IPv4 address | `10.66.198.198` |
| `ipv4_public()` | A public-range IPv4 address | `40.88.216.218` |
| `ipv4_network_class()` | An IPv4 network class letter | `a` |
| `ipv6()` | An IPv6 address | `2163:6369:8b52:9b4a:97b7:5093:3ceb:3ffd` |
| `mac_address()` | A hardware address | `9a:79:42:bd:f2:21` |
| `port_number()` | A port number (integer) | `31190` |
| `http_method()` | An HTTP method | `PUT` |
| `http_status_code()` | An HTTP status code (integer) | `221` |
| `iana_id()` | A registrar identifier | `3992384` |
| `nic_handle()` | A network information centre handle | `MA93682-FAKE` |
| `ripe_id()` | A regional registry identifier | `ORG-MA93682-RIPE` |

### Phone numbers

Text.

| Method | Makes | Example |
| --- | --- | --- |
| `phone_number()` | A phone number in a varying format | `298.925.9791` |
| `basic_phone_number()` | A phone number as ###-###-#### | `2989259791` |
| `msisdn()` | A mobile subscriber number | `9825979190748` |
| `country_calling_code()` | A calling code such as +687 | `+881 0` |

### Companies and jobs

Text.

| Method | Makes | Example |
| --- | --- | --- |
| `company()` | A company name | `Watkins and Sons` |
| `company_suffix()` | A company type such as Group | `and Sons` |
| `catch_phrase()` | A marketing tagline | `Front-line optimizing moratorium` |
| `bs()` | A business-speak phrase | `incentivize end-to-end relationships` |
| `job()` | A job title | `Field seismologist` |
| `job_female()` | A job title (the female list) | `Field seismologist` |
| `job_male()` | A job title (the male list) | `Field seismologist` |

### Finance

Text unless noted.

| Method | Makes | Example |
| --- | --- | --- |
| `credit_card_number()` | A credit card number that passes the check digit | `4597919074832` |
| `credit_card_provider()` | A card network name | `VISA 13 digit` |
| `credit_card_expire()` | An expiry date as MM/YY | `02/29` |
| `credit_card_security_code()` | A three-digit security code | `982` |
| `credit_card_full()` | Provider, holder, number, expiry and code on separate lines | `VISA 13 digit / Brittany Anderson / 491907483...` |
| `iban()` | An international bank account number | `GB46HSRE59791907483378` |
| `bban()` | A basic bank account number | `HSRE59791907483378` |
| `bank()` | A bank name | `Kroo Bank` |
| `bank_country()` | A bank's two-letter country code | `GB` |
| `aba()` | A nine-digit bank routing number | `049825974` |
| `swift()` | A BIC / SWIFT code (8 or 11 characters) | `SRELGB4E` |
| `swift8()` | An 8-character BIC | `HSREGBX4` |
| `swift11()` | An 11-character BIC | `HSREGBX4EA4` |
| `currency_code()` | A currency code such as MWK | `IDR` |
| `currency_name()` | A currency's name | `Indonesian rupiah` |
| `currency_symbol()` | A currency symbol | `Rp` |
| `cryptocurrency_code()` | A cryptocurrency ticker | `NMC` |
| `cryptocurrency_name()` | A cryptocurrency's name | `Namecoin` |
| `pricetag()` | A formatted price such as $7,604.87 | `$69.82` |

### Government and personal identifiers

Text.

| Method | Makes | Example |
| --- | --- | --- |
| `ssn()` | A US social security number | `244-76-8917` |
| `invalid_ssn()` | A social security number that is never issued | `243-69-0000` |
| `itin()` | A US individual taxpayer number | `930-87-9709` |
| `ein()` | A US employer identification number | `39-9942864` |
| `sbn9()` | A nine-digit standard book number | `398-25979-0` |
| `passport_number()` | A passport number | `S82597919` |
| `passport_gender()` | A passport gender letter | `F` |

### Identifiers and codes

Text.

| Method | Makes | Example |
| --- | --- | --- |
| `uuid4()` | A random UUID (version 4) | `21636369-8b52-4b4a-97b7-50923ceb3ffd` |
| `uuid1()` | A time-based UUID (version 1) | `b7adc38d-6c57-11d0-a2d4-97b73ceb3ffd` |
| `uuid7()` | A time-ordered UUID (version 7) | `012f3ceb-3ffd-78b5-97ad-586921636369` |
| `isbn10()` | A 10-digit book number | `0-597-91907-0` |
| `isbn13()` | A 13-digit book number | `978-0-597-91907-7` |
| `ean()` | A barcode (13 digits by default) | `3982597919071` |
| `ean13()` | A 13-digit barcode | `3982597919071` |
| `ean8()` | An 8-digit barcode | `39825971` |
| `localized_ean()` | A barcode with a local prefix | `0482597919079` |
| `localized_ean13()` | A 13-digit barcode with a local prefix | `0482597919079` |
| `localized_ean8()` | An 8-digit barcode with a local prefix | `10825976` |
| `upc_a()` | A 12-digit product code | `982597919074` |
| `upc_e()` | An 8-digit compressed product code | `13982591` |
| `vin()` | A vehicle identification number | `F9PX51XG6XS0E8337` |
| `license_plate()` | A vehicle licence plate | `2S 5979R` |
| `doi()` | A digital object identifier | `10.31940071/m9e8o25` |
| `md5()` | An MD5 hash string | `0303ec013e8b1aa62a1a2d06ca252d99` |
| `sha1()` | A SHA-1 hash string | `e3e4da706e7519080db352b21b6ff30b82219fd4` |
| `sha256()` | A SHA-256 hash string | `1c0ade1d880eca1bc84cad5919d50d9d18b84908cd1b9...` |
| `pystr(min_chars=8, max_chars=8)` | A random string of letters | `EgVyEFVy` |

### Dates and times

Dates are dates, date-times are timestamps, the rest are text unless noted.

| Method | Makes | Example |
| --- | --- | --- |
| `date()` | A date as text (YYYY-MM-DD) | `1997-12-22` |
| `date_object()` | A date | `1997-12-22` |
| `date_of_birth()` | A date of birth, 0 to 115 years ago | `1967-12-07` |
| `date_this_year()` | A date this year | `2026-04-07` |
| `date_this_month()` | A date this month | `2026-07-07` |
| `date_this_decade()` | A date this decade | `2023-03-27` |
| `date_this_century()` | A date this century | `2013-02-16` |
| `date_between(start_date='-5y', end_date='today')` | A date between two bounds | `2024-01-04` |
| `past_date()` | A date in the last 30 days | `2026-06-29` |
| `future_date()` | A date in the next 30 days | `2026-07-30` |
| `date_time()` | A timestamp since 1970 | `1997-12-22 09:26:17` |
| `date_time_ad()` | A timestamp since year 1 | `1003-03-29 11:17:12` |
| `date_time_this_year()` | A timestamp this year | `2026-04-07 17:36:22` |
| `date_time_this_month()` | A timestamp this month | `2026-07-08 04:11:26` |
| `date_time_this_decade()` | A timestamp this decade | `2023-03-27 08:05:50` |
| `date_time_this_century()` | A timestamp this century | `2013-02-16 20:36:31` |
| `date_time_between(start_date='-30d', end_date='now')` | A timestamp between two bounds | `2026-06-30 08:15:24` |
| `past_datetime()` | A timestamp in the last 30 days | `2026-06-30 08:15:23` |
| `future_datetime()` | A timestamp in the next 30 days | `2026-07-30 08:15:24` |
| `iso8601()` | A timestamp as ISO 8601 text | `1997-12-22T09:26:17.410707` |
| `unix_time()` | Seconds since 1970 (number) | `8.828e+08` |
| `time()` | A time of day as text (HH:MM:SS) | `09:26:17` |
| `year()` | A year as text | `1983` |
| `month()` | A month number as text | `07` |
| `month_name()` | A month name | `July` |
| `day_of_month()` | A day of the month as text | `05` |
| `day_of_week()` | A weekday name | `Tuesday` |
| `am_pm()` | AM or PM | `PM` |
| `century()` | A century in Roman numerals | `VIII` |
| `timezone()` | A time zone name | `Africa/Bissau` |

### Text

Text.

| Method | Makes | Example |
| --- | --- | --- |
| `word()` | One word | `economy` |
| `sentence()` | One sentence | `Or candidate trouble listen ok.` |
| `paragraph()` | One paragraph | `Name have page personal assume actually study...` |
| `text(max_nb_chars=100)` | A block of text up to a given length | `Name have page personal assume actually study...` |

### Numbers and booleans

Numeric methods take bounds as arguments.

| Method | Makes | Example |
| --- | --- | --- |
| `pyint(min_value=1, max_value=100)` | A whole number | `31` |
| `random_int(min=1, max=100)` | A whole number | `31` |
| `random_number(digits=4)` | A whole number with up to a number of digits | `3898` |
| `pyfloat(min_value=0, max_value=100)` | A floating-point number | `75.89` |
| `pydecimal(left_digits=4, right_digits=2, positive=True)` | A decimal number | `4898.98` |
| `pybool()` | True or false (boolean) | `True` |
| `boolean()` | True or false (boolean) | `True` |
| `null_boolean()` | True, false or null (boolean) | `False` |
| `random_digit()` | One digit, 0 to 9 | `3` |
| `random_digit_not_null()` | One digit, 1 to 9 | `4` |
| `random_digit_above_two()` | One digit, 2 to 9 | `5` |
| `random_digit_or_empty()` | One digit, or an empty string | `` |
| `random_digit_not_null_or_empty()` | One digit 1 to 9, or an empty string | `` |

### Patterns

Fill a template. # becomes a digit and ? a letter.

| Method | Makes | Example |
| --- | --- | --- |
| `numerify(text='###-####')` | Replaces each # in a template with a digit | `398-2597` |
| `lexify(text='??-??')` | Replaces each ? in a template with a letter | `pL-Ii` |
| `bothify(text='##-??')` | Replaces # with digits and ? with letters | `39-Ii` |
| `hexify(text='^^^^')` | Replaces each ^ in a template with a hexadecimal digit | `74bf` |
| `random_letter()` | One letter | `p` |
| `random_lowercase_letter()` | One lowercase letter | `h` |
| `random_uppercase_letter()` | One uppercase letter | `H` |

### Colours

Methods that make a colour as a triple of numbers cannot be declared; see below.

| Method | Makes | Example |
| --- | --- | --- |
| `color()` | A colour as #RRGGBB | `#017c03` |
| `hex_color()` | A colour as #rrggbb | `#3ceb40` |
| `safe_hex_color()` | A web-safe colour as #rrggbb | `#7744bb` |
| `color_name()` | A colour name | `LavenderBlush` |
| `safe_color_name()` | A basic colour name | `navy` |
| `rgb_color()` | A colour as r,g,b text | `121,66,189` |
| `rgb_css_color()` | A colour as rgb(r,g,b) text | `rgb(121,66,189)` |

### Files and devices

Text.

| Method | Makes | Example |
| --- | --- | --- |
| `file_name()` | A file name | `off.png` |
| `file_extension()` | A file extension | `png` |
| `file_path()` | A file path | `/case/off.png` |
| `mime_type()` | A media type | `message/imdn+xml` |
| `unix_device()` | A device path | `/dev/sds` |
| `unix_partition()` | A partition path | `/dev/sds8` |

### User agents

Text.

| Method | Makes | Example |
| --- | --- | --- |
| `user_agent()` | A browser user-agent string | `Mozilla/5.0 (Android 1.6; Mobile; rv:39.0) Ge...` |
| `chrome()` | A Chrome user-agent | `Mozilla/5.0 (iPhone; CPU iPhone OS 11_4_1 lik...` |
| `firefox()` | A Firefox user-agent | `Mozilla/5.0 (Android 1.6; Mobile; rv:39.0) Ge...` |
| `safari()` | A Safari user-agent | `Mozilla/5.0 (iPod; U; CPU iPhone OS 3_1 like ...` |
| `opera()` | An Opera user-agent | `Opera/9.90.(Windows NT 6.0; si-LK) Presto/2.9...` |
| `internet_explorer()` | An Internet Explorer user-agent | `Mozilla/5.0 (compatible; MSIE 6.0; Windows NT...` |
| `android_platform_token()` | An Android platform token | `Android 4.0.2` |
| `ios_platform_token()` | An iOS platform token | `iPhone; CPU iPhone OS 17_4_1 like Mac OS X` |
| `linux_platform_token()` | A Linux platform token | `X11; Linux i686` |
| `mac_platform_token()` | A Mac platform token | `Macintosh; PPC Mac OS X 10_7_5` |
| `windows_platform_token()` | A Windows platform token | `Windows CE` |
| `linux_processor()` | A Linux processor name | `i686` |
| `mac_processor()` | A Mac processor name | `PPC` |

### Symbols

Text.

| Method | Makes | Example |
| --- | --- | --- |
| `emoji()` | An emoji | `🧑🏽‍🎨` |

### Other single values

Text unless noted.

| Method | Makes | Example |
| --- | --- | --- |
| `coordinate()` | One coordinate in degrees (number) | `-52.243713` |
| `military_apo()` | A military post office address | `PSC 3982, Box 5979` |
| `passport_dob()` | A date of birth for a passport (date) | `1938-05-14` |
| `passport_full()` | A passport's fields on separate lines | `Jacqueline / Munoz / F / 14 May 1938 / 17 Mar...` |
| `password(length=12)` | A random password | `FWe$!n759MRN` |
| `pystr_format()` | A string of letters and digits in a fixed shape | `E8-2593898L` |
| `random_element(elements=('new', 'paid', 'shipped'))` | One value from a list you give | `new` |
| `date_between_dates(date_start=datetime.date(2024,1,1), date_end=datetime.date(2024,12,31))` | A date between two dates you give | `2024-03-27` |
| `date_time_between_dates(datetime_start=datetime.datetime(2024,1,1), datetime_end=datetime.datetime(2024,12,31))` | A timestamp between two timestamps you give | `2024-03-27 20:34:12` |
| `json()` | A JSON document of sample records as text | `[{"name": "Joshua Wood", "residency": "074 Ja...` |
| `csv()` | Sample records as comma-separated text | `"Joshua Wood","074 James Stravenue / Weaversi...` |
| `tsv()` | Sample records as tab-separated text | `"Joshua Wood"	"074 James Stravenue / Weaversi...` |
| `psv()` | Sample records as pipe-separated text | `"Joshua Wood"|"074 James Stravenue / Weaversi...` |
| `dsv()` | Sample records as delimiter-separated text | `"Joshua Wood","074 James Stravenue / Weaversi` |
| `fixed_width()` | Sample records in fixed-width columns as text | `Joshua Wood         19  / Kevin Jacobs       ...` |
### Methods that make several values

These methods make a list or a pair. Provisa shows the list as one text, joined by line for `paragraphs` and `texts`, by space for the others, and with no separator for `random_letters`. A `currency` or `cryptocurrency` shows its code.

| Method | Shows | Example |
| --- | --- | --- |
| `cryptocurrency()` | A cryptocurrency's code | `NMC` |
| `currency()` | A currency's code | `IDR` |
| `get_words_list()` | The whole word list, joined by spaces | `a ability able about above accept according account acros...` |
| `nic_handles()` | Network information centre handles, joined by spaces | `MA93682-EQJO` |
| `paragraphs()` | Paragraphs, one per line | `Name have page personal assume actually study else. Court...` |
| `random_choices(elements=('a', 'b', 'c'))` | Elements drawn with repeats, joined by spaces | `c` |
| `random_elements(elements=('a', 'b', 'c'))` | Elements drawn, joined by spaces | `c` |
| `random_letters()` | Random letters run together | `mCtFGdaRnmZyRyHh` |
| `random_sample(elements=('a', 'b', 'c'))` | A sample of elements, joined by spaces | `c` |
| `sentences()` | Sentences, joined by spaces | `Or candidate trouble listen ok. Actually study else docto...` |
| `texts()` | Blocks of text, one per line | `Name have page personal assume actually study else. Court...` |
| `words()` | Words, joined by spaces | `draw name have` |

### Methods no column can declare

These 29 methods make bytes, several values at once (a coordinate pair is two columns, and Provisa has no point type), structures of mixed values, objects or generators, or need a class argument. Declaring one is refused by name, on every engine: `binary`, `color_hsl`, `color_hsv`, `color_rgb`, `color_rgb_float`, `enum`, `image`, `json_bytes`, `latlng`, `local_latlng`, `location_on_land`, `passport_dates`, `passport_owner`, `profile`, `pydict`, `pyiterable`, `pylist`, `pyobject`, `pyset`, `pystruct`, `pytimezone`, `pytuple`, `simple_profile`, `tar`, `time_delta`, `time_object`, `time_series`, `xml`, `zip`.

For a single value from a pair, use the single-value method: `latitude` or `longitude`; `color` or `hex_color`; `first_name` or `last_name`.

## Method arguments

A method takes the arguments below, by name, and no others. The same arguments work on every engine, which is why any other is refused by name, with the list the method does take. A method not listed takes none.

| Method | Arguments |
| --- | --- |
| `bothify` | `text`, `letters` |
| `numerify` | `text` |
| `lexify` | `text`, `letters` |
| `hexify` | `text`, `upper` |
| `pyint` | `min_value`, `max_value`, `step` |
| `random_int` | `min`, `max`, `step` |
| `random_number` | `digits`, `fix_len` |
| `pyfloat` | `left_digits`, `right_digits`, `positive`, `min_value`, `max_value` |
| `pydecimal` | `left_digits`, `right_digits`, `positive`, `min_value`, `max_value` |
| `pystr` | `min_chars`, `max_chars`, `prefix`, `suffix` |
| `password` | `length`, `special_chars`, `digits`, `upper_case`, `lower_case` |
| `nic_handle` | `suffix` |
| `nic_handles` | `count`, `suffix` |
| `date` | `pattern`, `end_datetime` |
| `time` | `pattern`, `end_datetime` |
| `date_object` | `end_datetime` |
| `date_time` | `end_datetime` |
| `date_time_ad` | `start_datetime`, `end_datetime` |
| `iso8601` | `end_datetime`, `sep` |
| `boolean` | `chance_of_getting_true` |
| `pybool` | `truth_probability` |
| `random_element` | `elements` |
| `date_between` | `start_date`, `end_date` |
| `date_time_between` | `start_date`, `end_date` |
| `date_between_dates` | `date_start`, `date_end` |
| `date_time_between_dates` | `datetime_start`, `datetime_end` |
| `future_date` | `end_date` |
| `future_datetime` | `end_date` |
| `past_date` | `start_date` |
| `past_datetime` | `start_date` |
| `date_of_birth` | `minimum_age`, `maximum_age` |
| `date_this_century` | `before_today`, `after_today` |
| `date_this_decade` | `before_today`, `after_today` |
| `date_this_year` | `before_today`, `after_today` |
| `date_this_month` | `before_today`, `after_today` |
| `date_time_this_century` | `before_now`, `after_now` |
| `date_time_this_decade` | `before_now`, `after_now` |
| `date_time_this_year` | `before_now`, `after_now` |
| `date_time_this_month` | `before_now`, `after_now` |
| `unix_time` | `start_datetime`, `end_datetime` |
| `words` | `nb`, `unique` |
| `sentences` | `nb` |
| `paragraphs` | `nb` |
| `texts` | `nb_texts`, `max_nb_chars` |
| `sentence` | `nb_words` |
| `paragraph` | `nb_sentences` |
| `text` | `max_nb_chars` |
| `random_letters` | `length` |
| `random_choices` | `elements`, `length` |
| `random_elements` | `elements`, `length`, `unique` |
| `random_sample` | `elements`, `length` |

## See also

- [Building test environments](test-data.md)
- [Environments](environments.md)
