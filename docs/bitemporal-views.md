# Create a bitemporal view

A bitemporal view keeps both when something happened and when it was recorded. You can then read it as of a date, or as it was recorded on a date, and a correction made later never hides what was known before it.

You make a view bitemporal by mapping three of its columns:

- **Business key**: the column, or columns, that identify the same record across versions, such as `transaction_id`. A corrected payment is still the same payment, so both versions carry the same key.
- **Valid time**: the column holding the date your business gives the record, such as when it was agreed, booked or took effect.
- **Recorded time**: the column holding when this version was entered or changed, such as `updated_at`.

Recorded time, called transaction time in the temporal-database literature and system time in SQL:2011; valid time, called application time in SQL:2011 or effective date.

The three values map columns your source already has. Provisa decides how the versions are kept; there is no storage mode to choose.

## Three examples

| Example | Valid time | Recorded time |
| ------- | ---------- | ------------- |
| A payment of 100 for 3 Oct is corrected to 120 on 5 Oct. | 3 Oct, kept by the correction. | 3 Oct for the first version; 5 Oct for the correction. |
| A field measurement taken 3 Oct 14:10 is keyed in on 6 Oct. A typo in it is fixed on 7 Oct. | 3 Oct 14:10, kept by the fix. | 6 Oct; the fix gets 7 Oct. |
| A trade is booked at execution, trade date 3 Oct, at 10:02. On 4 Oct its quantity is found wrong and amended. | Trade date 3 Oct, kept by the amendment. | 3 Oct 10:02; the amendment gets 4 Oct. |

Read as reported on 3 Oct, the trade shows the wrong quantity. Read as known now, it shows the corrected one.

## Create the view

A view that keeps history is materialized, so its versions are stored.

1. Open **Views** and click **+ View**.
2. Define the view on either tab:
    - **Metrics**: name the view, pick its domain, metrics and dimensions, then click **Continue**.
    - **SQL**: open the SQL editor, write and run the `SELECT`, click **+ View** in the editor, name the view and click **Continue**.
3. The view's settings open in the same sections as its editor. On **Load and timeliness**, tick **Materialized View**. The **Time Travel** section appears.
4. Set **Keep history** to **Bitemporal**.
5. Pick the **Business key**, **Valid time** and **Recorded time** columns.
6. Set anything else on the other sections, then click **Save**. Nothing is registered until then.

The three are declared together or not at all. Provisa refuses a view that names only some of them, and says which are missing.

A table takes the same sections when you register it: pick it under **+ Table**, click **Continue**, and its Time Travel section is on **Load and timeliness**. A model takes the section when you create one (**+ Model**). For a model, its key is the business key, so the section asks only for the two times.

## A table updated in place

Some tables keep every version as its own row, so a correction adds a row with the same business key. Others hold one row per key and overwrite it, so their business key is their primary key. For a table of the second kind, Provisa keeps the versions itself in a version log:

- Each capture reads the rows recorded since the latest recorded time the log holds, and adds those that changed.
- The log's allowed lateness widens that window, so a row committed late with an earlier recorded time is still read.
- A capture cannot see a row the source deleted. **Full capture** reads the whole table again and records each deleted row as gone.
- The log begins at its first capture: before it, each row has only the version it had then.

The log is listed under **Cache**, on the **Materialized Store** tab, as **Version log of** the table. Its **Settings** set how often it is captured and its allowed lateness; **Full capture** is beside them. A plain read of the table reads the table; a read as of a time, or as recorded at a time, reads its log.

## Read it

The view appears in the **Views** list with a **HISTORY** badge. Its detail panel says it keeps history and links to the SQL explorer with the view selected. Set the explorer's **As of** and **As recorded at** pickers to the times you want, then run the query.

The same times reach every other surface. A pgwire session sets them once for every statement it runs:

```sql
SET provisa.as_of = '2026-10-03T12:00:00';
SET provisa.as_recorded = '2026-10-03T23:59:59';
```

The HTTP headers, `resolve` arguments and REST parameters are listed in [Views with history](configuration.md#views-with-history-time-travel).

## Change it later

The editor shows the Time Travel section on the view's **Load and timeliness** tab. Once the view has been built, its versions are recorded under its business key and valid time, so the editor no longer offers those two, or the switch that turns Time Travel off. The recorded time can still change. To key the history differently, create a new view.
