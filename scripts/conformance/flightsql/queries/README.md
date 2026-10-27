Query overrides for the ADBC validation suite, merged over its base queries part by part.

The ten timestamp and ten timestamptz cases of `type/select` share two table names with
different contents, so each case's table is named for the case in the job's fixture model
(`../fixtures.py`), and each case's query is overridden here to read it
(`type/select/<case>.sql`). `fixtures.py seed` checks each override against the suite's query
with the table renamed. No other query is overridden.
