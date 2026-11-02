# Rust pgwire DataRow encoder: differential test and benchmark

**Wired (J492e, REQ-2194).** pgwire's `send_data_rows` writes every result through this crate
(`provisa._encoders.pgwire`), straight from the engine's Arrow batches; the Python DataRow writers
it was compared against are gone from the product. They survive only as the differential's frozen
reference, `tests/unit/pgwire_encoder/oracle.py`, which `pg_reference.pg17()` corrects to
PostgreSQL 17. The differential (`tests/unit/pgwire_encoder/test_differential.py`) now drives the
product's wired send path, not the crate alone; `test_row_boundary.py` holds a row engine's driver
values through the fetch boundary (`ir_arrow.row_batch`, docs/arch/fetch-paths.md) to the same
answers. The sections below record the experiment (J483, J492a, J492b) as it was run; its file
names were `experiments/pgwire_encoder_rs/{test_compare,python_ref,pg_reference,bench}.py`.

Since the wiring the encoder also reads DuckDB's lossless Arrow export (opaque `hugeint`,
`uhugeint`, `varint`, `bit`, `time_tz` from their own bytes), an interval at any depth, a time of
24:00:00 (which PostgreSQL holds and `datetime.time` cannot), and the enum array coercer
(`pg_enum.array_text`). Two of the product defects below were corrected on main before the wiring
(68bdf0150f: `interval_days`, and the 28-digit half of `numeric_binary`); the rest are corrected
on the wire by the wiring itself.

## What is compared

**Python (the product's path, run unmodified):** Arrow `RecordBatch` →
`provisa.federation.runtime_support.stream_rows_from_arrow` (`column_values` per column, `zip` into
row tuples) → `provisa.pgwire.server.ProvisaQueryResult` (column types from the engine's type names)
→ `buenavista.postgres.BuenaVistaHandler.send_data_rows` (the `BVTYPE_TO_PGTYPE` converters, DataRow
framing, 500-row chunked writes). `python_ref.py` drives it into an in-memory writer. Two
substitutions: a timestamp column in a named zone takes its local times from zoneinfo, not pytz
(`python_ref.named_zones`; see Named time zones), and **`pg_reference.pg17()`** replaces each
product helper whose answer is not PostgreSQL 17's with one that gives PostgreSQL's (Product
defects, below). That reference is what the encoder is held to; everything else is the product's
own code. `pg_types_check.py` holds the corrected answers to a live server.

**Rust (`src/`):** `pgwire_encoder_rs.encode_batch(batch, bvtypes, formats, coercers)` takes the
same Arrow batch zero-copy through the Arrow C data interface (arrow-rs `pyarrow` feature), the
BVType `ProvisaQueryResult` resolved for each column (its member name, or `"xml[]"`), the
per-column result formats, and each column's coercers by name (`python_ref.coercer_names`, read off
`ProvisaQueryResult._coerce`), and returns the DataRow message bytes.
`encode_stream(batches, bvtypes, write, formats, coercers)` streams a batch iterator and hands each
batch's bytes to `write`, the shape `send_data_rows` has. `forward_raw(rows, write)` is the
`RawDataRowBytes` passthrough. `register_catalog_names(procs, types)` hands the reg* writers the
catalog names once (`python_ref.register_catalog_names`). The GIL is released while encoding.

**How a column is written.** A column whose BVType and Arrow type have a direct writer (the hot
types: ints, floats, numeric, text, bytea, dates, timestamps, times, intervals, JSON, UUID and the
scalar arrays) and no coercer is written straight from the Arrow buffers, as before. Every other
column (each other BVType, an Arrow type the direct writer does not take, or a coerced column) is
written from its cell values: `src/pyval.rs` reads a cell as `column_values` hands it on (to_pylist,
the IR's intervals and infinities) into a Rust value, never a Python object; `src/coerce.rs`
applies the column's coercers; `src/pgtypes.rs` writes it as its BVType's converter does. Python's
`str()`, `repr()`, `int()`, `float()` and `Decimal()` of a value are ported where a converter uses
them (repr's printable characters from Python 3.12's Unicode 15.0.0 table, `gen_unicode.py`). No
type raises `NotImplementedError` (`test_no_type_is_unported`).

## Differential test (`test_compare.py`)

**1818 passed, none skipped** (1444 before this job). Every cell's bytes are identical to the
reference's (the product's, with PostgreSQL's answer for each product defect below), or both
raise, except a float4/float8
cell or array element. That one is compared by its decoded value, within one part in 10^14
(`REL_TOL`), with NaN equal to NaN and the sign of zero kept. A float4 text cell decodes to the float
`strtof` reads, correctly rounded as PostgreSQL's `float4in` and JDBC read it. A float4 text cell is
held to the float the product sends in **binary** for the same value, because the product's own text
can name another float (product defect 4). The run covers 113 single-column cases in text and binary,
the mixed and randomized tests listed below, a fixed table of PostgreSQL float forms, and the
time-zone proof (1194 zone tests and 2 year-limit tests).

- **bool**; **int2/int4/int8/oid** from every Arrow int width, with range overflow in binary.
- **float8**: NaN, ±Infinity, −0, subnormals, max, the fixed/exponent boundary, and 300,000 random
  bit patterns. 60 of those 300,000 text cells differ in bytes from the product's `repr` digits (the
  boundary and tie cases below), and all 60 are equal in value.
- **float4**: 300,000 random bit patterns (431 text cells differ in bytes from the product, all
  equal in value), a float64 column described as REAL (100,000 random values), and overflow.
- **PostgreSQL float text** (`test_postgres_float_text`): 23 float8 and 14 float4 values against
  the exact text PostgreSQL writes, including `1.2188652062118339e+17` and `-738171652909232.2`.
- **JSON floats** (`test_random_json_floats`): 300,005 doubles in `json.dumps`, byte for byte. This
  is Python `repr`, so it uses the stock ryu crate.
- **numeric**, **text**, **bytea**, **json/jsonb**, **interval**, **arrays**, **mixed batches**: as
  before (decimal128/256 at many scales, every `str.isspace()` code point, month-day-nano and
  duration intervals, 2-D arrays, 1-/700-/533-row batch splits).
- **date**: date32 with years 1 and 9999, ±infinity, out of range. **date64**: millisecond values
  floored to the day (as `to_pylist` gives them), years 1 and 9999, before year 1, after 9999, and the
  date32 infinity marker, which is a plain (out-of-range) day count in a date64.
- **timestamp / timestamptz**: s, ms, µs and **ns** units. A ns value is a pandas `Timestamp` in
  `to_pylist`, so its microseconds are floored, NaT (`int64` minimum) raises, and a zoned value whose
  local time leaves pandas' int64 range raises (`OutOfBoundsDatetime`). Covered: UTC, `+05:30`,
  `-03:00`, named zones (New York ns, Kolkata ms, Lord Howe s), an unknown zone (both raise), ±infinity,
  years 1 and 9999, and 200,000 random values.
- **time**: s, ms, µs and **ns**. A ns time is `pandas.Timestamp(v, unit="ns").time()`: the time of
  day of that instant, so `86400·10^9` is `00:00:00` and `-1` is `23:59:59.999999`. NaT raises.
- **interval** from **duration[ns]** (pandas `Timedelta`, floored to µs, NaT raises) and
  duration[ms], beside the month-day-nano and duration[s/µs] cases.
- **uuid**: the hyphenated, upper-case, braced, `urn:uuid:` and bare spellings, plus every spelling
  `int(hex, 16)` takes inside `uuid.UUID`'s 32 characters. These are ASCII and Unicode whitespace
  around the digits, `+`, `0x`/`0X` followed by one optional `_`, single `_` between digits, and
  Unicode decimal digits (Arabic-Indic, full-width, mathematical, Adlam). 15 refused spellings each
  have their own case. Also covered: large_string, an `arrow.uuid` extension column, and plain
  `fixed_size_binary(16)` (`test_uuid_fixed_size_binary`, product defect 5).

### Floats: PostgreSQL's digits

`float8_text` and `float4_text` now write PostgreSQL 17's `float8out` and `float4out` with
`extra_float_digits` at its default of 1, which selects the shortest-exact mode. The source is
REL_17_STABLE `src/backend/utils/adt/float.c`, `src/common/d2s.c`, `f2s.c` and `ryu_common.h`:

- NaN (any sign, any payload) is `NaN`, then `Infinity`, `-Infinity`, and `-0` for negative zero.
- The digits are Ryu's, but PostgreSQL builds Ryu with `STRICTLY_SHORTEST 0`
  (`acceptBounds = false`). A decimal lying exactly on the boundary of the rounding interval is never
  taken, so a few values get one digit more than the shortest: `1.2188652062118339e+17` where
  `repr` writes `1.218865206211834e+17`. Exact ties round to even. The ryu crate takes boundaries,
  which matches `repr`. `third_party/ryu-pg` is ryu 1.0.23 with that one flag set as PostgreSQL sets
  it, in `d2s.rs` and `f2s.rs`. JSON keeps the stock crate.
- The output is in fixed form when the first digit's decimal exponent is in [-4, 15) for float8 and
  [-4, 6) for float4. Otherwise it is `d.ddde±XX`, with at least two exponent digits.

The nine-candidate float4 search and the float8 tie fix-up are gone. **Live check**
(`pg_float_check.py`): PostgreSQL 18.4 (Homebrew) on a throwaway cluster, over 300,016 float8 and
300,011 float4 values (random bit patterns plus edges), gave **0 differences** from the encoder's
text. PostgreSQL 18's `d2s.c`, `f2s.c`, `ryu_common.h`, `float8out_internal` and `float4out` differ
from 17's in their copyright lines only. No PostgreSQL 17 binary is installed on this machine.

### Named time zones

DuckDB's Arrow timestamptz carries the session `TimeZone`, and the encoder now takes any IANA name.
It looks up the offset at each instant in **chrono-tz 0.10.4**, rebuilt from the IANA release
vendored in `third_party/chrono-tz/tz` (with `filter-by-regex` and no filter set, the tables are
generated from those files at build time). chrono-tz expands a zone's rules only for 1800–2099
(`parse-zoneinfo`). From 2100 on, the encoder applies the zone's POSIX TZ rule (the TZif footer),
which is what zoneinfo and PostgreSQL apply after a zone's last explicit transition. `src/tz.rs`
reads `M` rules. `gen_tz_rules.py` writes `src/tz_rules.rs` by running `zic` over the same vendored
files, and fails on any other rule form. zic's footers from the vendored source equal the tzdata
package's for all 597 zones. Text offsets keep their seconds (`-04:56:02`), as PostgreSQL writes
them.

**What pyarrow uses here: pytz, not zoneinfo.** `to_pylist` of a zoned timestamp calls
`string_to_tzinfo`, which takes pytz when it can import it. pytz 2026.2 is installed (pandas requires
it). pytz reads only a TZif file's 32-bit data and rounds offsets to whole minutes. Over the
differential's instants, `tz_pytz_diff.py` finds the product's own path differing from zoneinfo in
536 of 597 zones:

| instants | cells that differ |
|---|---|
| after 2037 (no DST at all: New York in July 2050 is `-05`) | 25,487 |
| before 1901 (no transitions: LMT stays in force) | 23,390 |
| 1901–2037 (LMT offsets rounded to the minute: `-04:56`) | 843 |

PostgreSQL 17 agrees with zoneinfo on all three. The differential therefore takes a named zone's
local time from **zoneinfo**, over the `tzdata` package. `python_ref.named_zones` moves the product's
own `column_values` of the column in UTC into the zone, so infinities, NULLs and the product's
formatters are unchanged. Fixed offsets and UTC still go through the unmodified path.

**Proof** (`test_named_zone`, `test_named_zone_year_limits`): every zone in the tzdata package's list
except `Factory` (597 zones, links included), in text and binary. Each zone is tested on:

- 1 µs either side of, and at, every transition in its TZif file;
- every transition zoneinfo makes in 2037, 2038, 2050, 2099, 2100, 2101, 2400, 5000, 9998 and 9999
  (a day scan, then bisection to the second);
- mid-month instants in years 1, 2, 9998 and 9999;
- 200 random instants, plus ±infinity and NULL;
- the same transitions in nanoseconds where they fall in the ns range;
- each zone alone at the first and last instant of years 1..9999, where both encoders refuse a local
  time outside those years.

Every case encodes on the Python side (the test asserts it) and agrees. `Factory`, the placeholder
zone of tzdata's `factory` file, is not among the files chrono-tz builds, so it is not served.

**Pinned version and how it stays in step.** chrono-tz's tables, `tz_rules.rs` and the Python
reference are all IANA **2026b**. The vendored `tz/` is `tzdata2026b.tar.gz` from data.iana.org,
sha256 `114543d9f19a6bfeb5bca43686aea173d38755a3db1f2eec112647ae92c6f544`. The signature could not be
checked: the keyserver was unreachable. pytz 2026.2 and the `tzdata` package (uv.lock) are 2026b too.
The release's NEWS file labels its top entry "2026a" while its `version` file says 2026b, and
chrono-tz takes its version from NEWS, so line 3 is corrected to 2026b. `test_tz_version` asserts
`pgwire_encoder_rs.TZDB_VERSION` (chrono-tz) and `TZ_RULES_VERSION` (`tz_rules.rs`) both equal
`tzdata.IANA_VERSION`, so a bump of either side fails until the other follows. To bump: replace
`third_party/chrono-tz/tz` with the new release's files, rerun `gen_tz_rules.py`, and bump `tzdata`
in uv.lock to the same release. PostgreSQL itself takes zones from its own build's tz data (or the
system's), which a deployment pins separately.

### Product behaviors the port reproduces, and product defects

1. **Binary numeric rounds past 28 significant digits** (`abs(d)` under the default decimal
   context). *Now a defect corrected* (`numeric_binary` below): PostgreSQL keeps every digit.
2. *(Gone.)* The float8 tie rule no longer needs porting: both sides round exact ties to even, and
   floats compare by value.
3. **Interval days are split with float division** (`_interval_parts`). *Now a defect corrected*
   (`interval_days` below).
4. **New: float4 text of a double described as REAL can name another float.** `_float4_text`
   searches the *double's* 1..9-digit roundings. When none reads back, it writes the double's 9 digits,
   and that text can sit across a float midpoint: `-3.5713408890908175e-09` is sent as
   `-3.57134089e-09`, which `strtof` reads as a different float from the one the product sends in
   binary (`-3.5713408e-09`, PostgreSQL's text for that float). This happens for 293 of 100,000
   random values. The encoder writes the float's own digits.
5. **New: a plain `fixed_size_binary(16)` UUID column is written as `str(bytes)`** in text
   (`b'\xa0\xee...'`) and refused in binary. The encoder writes the UUID, as the product does for the
   same bytes under the `arrow.uuid` extension.
6. **New: named zones come from pytz** (above). The encoder answers as zoneinfo and PostgreSQL do.
7. **New, ported: a zoned ns value whose local time leaves pandas' int64 range is refused**
   (`OutOfBoundsDatetime`, within hours of 1677-09-21 and 2262-04-11). PostgreSQL would write it.

### Every type pgwire sends (J492b)

Every BVType in `BVTYPE_TO_PGTYPE` and xml[] is written, in text and binary, from every Arrow type a
column may reach it as: inet, cidr, macaddr, macaddr8, money, point, line, lseg, box, path, polygon,
circle, geometry and geography (text only, as the product), the six ranges and six multiranges, bit,
varbit, tsvector, tsquery, xml, timetz, "char", name, tid, xid, cid, xid8, txid_snapshot,
pg_snapshot, jsonpath, pg_lsn, regproc, regtype, regclass, regnamespace, the text-only catalog types
(aclitem, regprocedure, regoper, regoperator, regconfig, regdictionary, regrole, regcollation,
pg_ndistinct, pg_dependencies, pg_mcv_list, gtsvector, pg_brin_*: binary refused as the product
refuses it), oidvector, int2vector, every array type (int2/int4/int8/oid/float4/float8/numeric/bool/
text/"char"/name/money/interval/regtype/xml/aclitem), BVType.ARRAY, UNKNOWN and NULL. The row
coercers are ported: `_SHAPE_COERCERS` (int, float, `_to_decimal`, `_to_date`, `_to_timestamp`,
`_to_list`, `xml_out`), every `CastType.coerce` (`_to_int`, `_to_bool`, `_to_float`, `_to_decimal`,
`_padded(n)`, `jsonb_out`, `record_text`, `_clock_of`, `_of_no_zone`, `xml_out`), `_null_alone` and
`_duckdb_inet`. `RawDataRowBytes` rows pass through `forward_raw` in 500-row writes, byte for byte
(each is checked to be one DataRow message; the product forwards anything).

The new tests (`test_compare.py`):

- `test_every_type_from_every_arrow_type`: every BVType (101) in text and binary, each over 110
  columns: 54 Arrow types (every int and float width, decimal128/256, binary and
  fixed_size_binary/arrow.uuid, date32/64 with infinities, timestamps in every unit and zone kind,
  times, durations, month-day-nano, lists of every element kind, nested lists, structs, maps,
  dictionaries, null) and 56 one-value text columns (PostgreSQL's text of each type held as text,
  and what `int()`, `float()` and `Decimal()` read). Same bytes, or both raise.
- `test_every_coercer`: each of the 17 coercers with each BVType it brings values to (36 pairs),
  over the same 110 columns plus JSON, XML and padded texts and ClickHouse's Time64 bytes.
- `test_described`: 40 columns as the product describes them (engine type names, a Describe's
  shape, a read shape's subtypes, a result cast), asserting the coercers the product applies.
- `test_tsquery_binary`, `test_forward_raw`, `test_encode_stream_coerced`, `test_product_defect`
  (each defect below on a witness: the product's answer differs, the encoder gives PostgreSQL's),
  `test_unicode_version`, `test_no_type_is_unported`.

### Product defects (PostgreSQL 17's answer ported; `pg_reference.DEFECTS`)

| key | the product | PostgreSQL 17 (the encoder) |
|---|---|---|
| `numeric_text` | `Decimal.__str__`: `1E-7`, `-0.00`, `1E+16` | `numeric_out`: `0.0000001`, `0.00`, `10000000000000000` |
| `numeric_binary` | rounds past 28 digits under the default context; `Infinity` raises | every digit; NaN `0xC000`, ±Infinity `0xD000`/`0xF000` with dscale 32 (as `numeric_send` reads it) |
| `empty_array` | `{{},{}}` and binary ndim 2 with a zero dimension | `{}`, binary ndim 0 (`array_send` of an empty array) |
| `array_element_list` | a NULL first sub-list: the next sub-lists written as `str(list)` elements | refused: no array holds a sub-array as an element |
| `vector_lower_bound` | oidvector/int2vector binary with lower bound 1 | lower bound 0 (`oidvectorsend`) |
| `inet_text` | an IPv6 address holding an IPv4 one in hextets (`::ffff:102:304`) | dotted quad (`::ffff:1.2.3.4`, `::1.2.3.4`) |
| `duckdb_inet` | DuckDB's INET reaches Arrow with a `decimal128(38, 0)` address; `_duckdb_inet` raises on it, so no DuckDB inet column can be sent | the address it is |
| `range_bounds` | a date or timestamp bound `infinity`, `BC` or past 9999 raises (`fromisoformat`) | written (`range_send`) |
| `tsquery_binary` | binary refused | `tsquerysend` of `tsquery_in(text)` |
| `money_range` | text of a value past int8 cents written | refused (`money out of range`) |
| `interval_days` | days by float division: a day too many within ~10 ms of the next near a billion days | exact |
| `coerced_infinity` | `_to_date`, `_to_timestamp` and `_of_no_zone` turn ±infinity into 9999-12-31 / 0001-01-01 | infinity kept |
| `uuid_bytes` | (defect 5) a plain `fixed_size_binary(16)` uuid as `str(bytes)`; binary refused | the UUID |

Kept as the product has them (no PostgreSQL answer to port, or one the value cannot give): geometry
and geography in text only (PostGIS sends EWKB; Provisa holds WKT); the text-only catalog types,
whose binary is an oid or an internal structure the text does not carry; a TEXT column's `str()` of
any value; tsvector positions past 16383 (PostgreSQL never writes one).

### PostgreSQL check (`pg_types_check.py`)

A throwaway PostgreSQL 18.4 (Homebrew) cluster on a Unix socket: for each type's case, the
encoder's text and binary cell against the server's output function (`format('%s', v)`) and send
function (`encode(<type>_send(v), 'hex')`). The cases cover every geometric type, every range and
multirange (with infinity, BC and year-12000 bounds), tsvector, 14 tsqueries, inet and cidr from text
and from DuckDB's Arrow INET, macaddr/8, bit/varbit, tid, xid, cid, xid8, both snapshots, pg_lsn,
jsonpath, xml (with `xml_out`), uuid, timetz, name, "char", money from numeric (rounding and both
range ends), numeric at scale 10, at 38 digits and NaN/±Infinity, empty and 2-D arrays, numeric and
money arrays, oidvector and int2vector. **280 cells checked, 0 differ.** PostgreSQL 18's send and
output functions for these types are 17's.

## Benchmark (`bench.py`)

**Machine:** Apple M3 Pro, 12 cores, 18 GiB RAM, macOS 26.7.1. Python 3.12.0, pyarrow 24.0.0,
numpy 2.4.6. rustc 1.95.0, arrow-rs 56.2.1, pyo3 0.25.1. Release build with fat LTO,
`codegen-units=1` and `target-cpu=native`. Single-threaded, run through `heavy.sh` (the machine's
one job slot).

**Dataset:** 5,000,000 rows in 77 batches of 65,536 rows (DuckDB's Arrow stream batch size),
574.3 MB of Arrow, generated in memory with numpy from seed 483 and held once. Twelve columns:

| column | Arrow type | described as | NULLs |
|---|---|---|---|
| id | int64 | BIGINT | |
| qty | int32 | INTEGER | |
| price | float64 (normal, mean 1000) | DOUBLE | |
| ratio | float32 [0,1) | FLOAT (float4) | |
| amount | decimal128(18,2) | DECIMAL(18,2) | |
| name | utf8, word + number, some non-ASCII | VARCHAR | |
| created | timestamp[us, UTC], 2000–2030 | TIMESTAMP WITH TIME ZONE | |
| day | date32 | DATE | |
| active | bool | BOOLEAN | |
| payload | utf8 JSON text | JSON | |
| note | utf8 | VARCHAR | 30% |
| parent_id | int64 | BIGINT | 20% |

**Method:** each run is end to end, Arrow batches in and DataRow bytes out, with the output counted
and not kept. Before timing, the first two batches are checked to agree as the differential holds
them: every cell's bytes, and a float cell's value within 10^-14. The row counts agree. So do the
byte totals on this data (1,022,938,083 text, 793,313,142 binary), although float text may differ in
length. Best of 3 runs for Python and best of 5 for Rust. CPU time is `process_time`. Free memory
stayed at about 46% (the guard aborts under 25%).

**After (this job, J492a: PostgreSQL float digits by Ryu, named zones and the new units):**

| format | rows | DataRow bytes | Python wall (cpu) | Rust wall (cpu) | Python | Rust | speedup |
|---|---|---|---|---|---|---|---|
| text | 5,000,000 | 1,022.9 MB | 69.77 s (69.53 s) | 1.461 s (1.448 s) | 0.072 Mrow/s, 14.7 MB/s | 3.42 Mrow/s, 699.9 MB/s | **47.7×** |
| binary | 5,000,000 | 793.3 MB | 50.98 s (50.87 s) | 1.242 s (1.229 s) | 0.098 Mrow/s, 15.6 MB/s | 4.03 Mrow/s, 638.6 MB/s | **41.0×** |

Run-to-run spread: Python 69.77–70.15 s (text) and 50.98–51.38 s (binary); Rust 1.461–1.512 s
(text) and 1.242–1.299 s (binary). The full output is in `bench-5m.log`.

**Before (J483, b7b76b854: the nine-candidate float4 search, core's float8 formatter):**

| format | Python wall (cpu) | Rust wall (cpu) | speedup |
|---|---|---|---|
| text | 70.25 s (69.98 s) | 4.830 s (4.804 s) | 14.5× |
| binary | 51.14 s (50.98 s) | 1.254 s (1.239 s) | 40.8× |

Rust text went from 4.830 s to 1.461 s (3.3× faster). Binary is unchanged within the run-to-run
spread, since binary floats were never formatted. Python's times are the same path, measured again.

### Where the Python time goes

- **Arrow → Python rows: 24.2 s of the 5M-row run** (`stream_rows_from_arrow` plus
  `ProvisaQueryResult.rows()` alone, measured separately). That is about 34% of the text run and 47%
  of the binary run. Within it, `column_values` → `to_pylist` builds about 60M Python objects. The
  date32 and µs-timestamp columns each pay for `_with_infinities`: a cast, two compares, an `or`
  and an `any` per batch, and when the batch is free of infinities (always here) a second
  `to_pylist`.
- **The per-cell loop in `send_data_rows`.** cProfile over 458,752 rows (7 batches), with the
  profiler inflating Python time about 2×:
  - The loop body itself (`tottime`) is 2.9 s of 13.4 s: twelve indexed reads, a None check,
    `str.encode` and a `struct.pack` per cell.
  - Text format: `_float4_text` costs 3.2 s cumulative. Its search formats and parses up to nine
    candidate strings per value, so the one float4 column costs more than any other. `_float8_text`
    costs 1.3 s (`repr` → `Decimal` → `format` → `_shortest_text`). `_timestamp_text` costs 1.6 s
    (`_pg_clock`, `_pg_offset`, `utcoffset`). The rest is about 5.3M `str.encode` and
    `Struct.pack` calls.
  - Binary format: `_numeric_to_pg_binary` costs 3.0 s, by string slicing of `format(d, "f")`.
    `_timestamp_binary` costs 1.3 s (tz `replace` plus datetime subtraction per value). The int
    columns cost about 2.2M `int.to_bytes` calls.
- **What is left in Rust.** Text now costs 1.18× binary (1.46 s against 1.24 s). The float4 digit
  search is gone: one Ryu call gives a float's digits, and float8 takes one Ryu call where it used
  core's formatter plus a second correctly rounded format at 15+ digits.

## Files

- `Cargo.toml`, `Cargo.lock`, `.cargo/config.toml`, `rustfmt.toml`: the cdylib (pyo3 + arrow-rs);
  macOS link flags; 120-column formatting.
- `src/lib.rs`: kinds, Arrow access, per-type writers (UUID parsing as `int(hex, 16)` reads it),
  arrays, DataRow framing, Python entry points, `TZDB_VERSION` and `TZ_RULES_VERSION`.
- `src/fmt_num.rs`: float8/float4 text (PostgreSQL's digits), float repr, `Decimal.__str__`,
  numeric text and binary (`numeric_out`, `numeric_send`).
- `src/temporal.rs`: date, timestamp, time and interval text and parts; time units; zones.
- `src/tz.rs`: a named zone's offset (chrono-tz, then the POSIX rule from 2100).
  `src/tz_rules.rs`: each zone's POSIX rule, generated by `gen_tz_rules.py`.
- `src/json.rs`: `json.dumps` of Arrow nested values.
- `third_party/chrono-tz`: chrono-tz 0.10.4's source (MIT OR Apache-2.0) with `tz/` replaced by IANA
  tzdata 2026b. `third_party/ryu-pg`: ryu 1.0.23 (Apache-2.0 OR BSL-1.0) with acceptBounds false.
  These are path dependencies of this crate only; packaging them is J492d's call.
- `build.sh`: `cargo build --release`, then copies the module to `build/`. Nothing is installed into
  any venv.
- `src/pyval.rs`: a cell's value (to_pylist's, held in Rust), Python's `str()`, `repr()`, `int()`,
  `float()` and `Decimal()`. `src/unicode_tables.rs`: the characters `repr` escapes, generated by
  `gen_unicode.py`. `src/pgtypes.rs`: every BVType's text and binary writer over a value.
  `src/coerce.rs`: the row coercers, `json.loads`, `jsonb_out`, `xml_out`. `src/tsearch.rs`:
  tsvector and tsquery binary.
- `python_ref.py`: drives the product's Python path, names each column's coercers, registers the
  catalog names. `pg_reference.py`: the product's path with PostgreSQL's answer where it differs.
  `pg_types_check.py`: the live PostgreSQL check of the types ported here.
  `test_compare.py`: the differential test.
  `bench.py`: the benchmark. `bench-5m.log`: the benchmark's output. `pg_float_check.py`: the live
  PostgreSQL float text check. `tz_pytz_diff.py`: the product's pytz path against zoneinfo.
