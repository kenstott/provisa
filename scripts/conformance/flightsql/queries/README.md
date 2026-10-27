Query overrides for the ADBC validation suite, merged over its base queries part by part.

The ten timestamp and ten timestamptz cases of `type/select` share two table names with
different contents, so each case's table is named for the case in the job's fixture model
(`../fixtures.py`). Each case's query is overridden here to read it (`type/select/<case>.sql`),
and its metadata to name it (`type/select/<case>.toml`, `[setup] drop`, which the suite reads the
case's table schema from). `fixtures.py seed` checks each override against the suite's case with
the table renamed. No other case is overridden.
