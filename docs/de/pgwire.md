# Provisa-pgwire-Server

Provisa stellt einen Endpunkt für das PostgreSQL-Wire-Protokoll (pgwire) bereit. Jedes Tool, das das PostgreSQL-Client-Protokoll spricht — psycopg2, asyncpg, DBeaver, Tableau, JDBC —, kann sich verbinden und Provisa-Daten über dieselbe Governance-Pipeline abfragen, die auch die HTTP-API steuert. (REQ-266)

Abfragen durchlaufen den vollständigen Governance-Stack: Durchsetzung der Sicherheit auf Zeilenebene, Maskierungsregeln, Beziehungsschutz, Domänenzugriffsprüfungen. (REQ-001, REQ-002, REQ-263) Die pgwire-Schnittstelle ist kein Umgehungsweg. (REQ-002, REQ-266)

---

## Verbindungsdetails {: #connection-details }

Der Server startet, wenn `PROVISA_PGWIRE_PORT` auf eine Ganzzahl ungleich null gesetzt ist. Standardmäßig ist er deaktiviert. (REQ-527) [tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Setzen Sie `PROVISA_PGWIRE_CERT` und `PROVISA_PGWIRE_KEY` auf die Pfade eines PEM-Zertifikats und -Schlüssels. Sind beide vorhanden, verpackt der Server eingehende Verbindungen in TLS. Fehlen sie, ist TLS deaktiviert, und der Server antwortet auf SSL-Verhandlungsanfragen mit `N`. (REQ-530) [tool-verified: `server.py:1746-1750`]

**Gemeldete Serverversion.** Clients sehen `14.0.provisa`. Tools, die Funktionen anhand der Versionsnummer freischalten, verhalten sich möglicherweise so, als wären sie mit PostgreSQL 14 verbunden. (REQ-579) [tool-verified: `server.py:208`]

---

## Authentifizierung {: #authentication }

Das Startup-Paket führt einen Benutzernamen und ein einzelnes Geheimnisfeld mit, aber kein Schema, das angibt, was dieses Geheimnis ist. Provisa entscheidet anhand des Geheimnisses selbst, sodass ein Client außer `user` und `password` keine weitere Konfiguration benötigt:

| Das Geheimnis ist | Erkannt an | Wird aufgelöst zu |
| --------------- | --------------- | ------------- |
| Ein Personal Access Token | seinem Präfix `provisa_pat_` | Eigentümer und Rolle des Tokens (REQ-1263) |
| Ein OIDC-/Provider-Bearer-Token | daran, dass der konfigurierte Provider ein Token-Provider ist | die Identität, die das Token behauptet (REQ-890) |
| Ein Passwort | allem anderen | das Konto im konfigurierten Provider (`basic` oder `simple`) |

Die Entscheidung fällt einmal. Ein Credential, das der gewählte Validator ablehnt, wird nicht erneut gegen einen anderen geprüft — aus einer Ablehnung wird also kein zweiter Versuch.

Der Trust-Modus (`provider: none` oder inaktive Auth-Middleware) ist die Ausnahme: Der Benutzername wird direkt als `role_id` verwendet und das Geheimnis wird ignoriert. Verwenden Sie ihn nicht über eine unverschlüsselte Verbindung.

**SCRAM-SHA-256.** Unter `provider: basic` mit `auth.scram: true` kündigt der Server SASL (Authentifizierungscode 10) mit `SCRAM-SHA-256` an, und das Passwort wird bewiesen statt gesendet. (REQ-1394) `SCRAM-SHA-256-PLUS` wird nicht angeboten. Einem Benutzer, für den noch kein Verifier geschrieben wurde — Verifier lassen sich nicht aus bcrypt-Hashes ableiten —, antwortet der Server mit einem Schein-Austausch, damit auf der Leitung nicht erkennbar wird, wer bereits migriert ist; dieser Benutzer authentifiziert sich per Klartextpasswort über TLS, bis seine nächste Passworteingabe einen Verifier schreibt. Ist `auth.scram` aus, verwendet der Server den PG-Auth-Typ 3 (Klartextpasswort). MD5 wird in keinem der beiden Fälle unterstützt.

**Client-Zertifikate.** Setzen Sie `PROVISA_MTLS_CLIENT_CA`, und der Server prüft während des Handshakes ein Client-Zertifikat, bevor irgendein Credential betrachtet wird. (REQ-1228) Mit `PROVISA_MTLS_BIND_PRINCIPAL` muss der Common Name des Zertifikats dem `user` entsprechen, als den sich die Verbindung anschließend authentifiziert. Siehe [Konfiguration](configuration.md#mutual-tls).

**Fehlversuche werden gezählt.** Fünf Fehlversuche in fünf Minuten sperren das Konto für fünfzehn Minuten, und der Zähler wird mit HTTP und Bolt geteilt — eine auf einer Oberfläche erwirkte Sperre gilt auf allen. (REQ-1393)

**Auswahl einer Organisation.** In einem Multi-Org-Deployment verbinden Sie sich mit `<org>.<your-domain>`, und pgwire liest die Organisation aus dem Hostnamen im TLS-ClientHello — so, wie HTTP sie aus dem `Host`-Header liest. (REQ-1234) Der Hostname fordert eine Organisation an; er gewährt sie nicht, und ein Principal ohne Mitgliedschaft dort wird abgewiesen. Eine Verbindung über eine IP-Adresse fordert keine Organisation an.

---

## Was funktioniert {: #what-works }

### SELECT

Alle SELECT-Anweisungen durchlaufen die Governance-Pipeline (`_pipeline.py`). (REQ-001, REQ-262, REQ-266) Die Pipeline:

1. Schreibt semantisches SQL in physisches SQL um (`rewrite_semantic_to_physical`)
2. Wendet Governance an (Sicherheit auf Zeilenebene, Maskierung, Domänenzugriff) (REQ-263)
3. Validiert gegen das registrierte Schema (REQ-011)
4. Routet zu Trino oder direkt zum Quellen-Pool (REQ-027, REQ-028)

Einfache Abfragen mit mehreren Anweisungen werden unterstützt. Durch Semikolon getrennte Anweisungen werden aufgeteilt und der Reihe nach ausgeführt. (REQ-580) [tool-verified: `server.py:318-381`]

Parametrisierte Abfragen (`$1`, `$2`, ...) werden sowohl im Simple-Query- als auch im Extended-Query-Modus (Bind/Execute) unterstützt. Parameter werden vor der Ausführung als Literale eingesetzt. (REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` und `SELECT fn(args)` — wobei `fn` eine registrierte, nachverfolgte Funktion benennt — werden vor der Governance-Pipeline abgefangen und über den einen governance-gesteuerten Executor (`invoke_tracked_function`) geroutet. Das Ergebnis ist eine typisierte Zeilenmenge, identisch zu dem, was jede andere Oberfläche für diesen Befehl liefert. Die Befehlszuweisung (`visible_to`), das Recht `write` für Mutationen und die Governance-Regeln werden innerhalb des Executors durchgesetzt. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### Cursor (DECLARE / FETCH / MOVE / CLOSE) {: #cursors-declare-fetch-move-close }

Provisa verarbeitet textuelle SQL-Cursor-Syntax über das Simple-Query-Protokoll. (REQ-1862) Clients, die `DECLARE`/`FETCH`/`MOVE`/`CLOSE` als einfache SQL-Anweisungen senden, erhalten echtes serverseitiges Streaming statt eines Protokollfehlers oder eines vollständigen, in den Client-Speicher geladenen Ergebnispuffers.

**Das ist etwas anderes als Portal-basiertes Streaming.** Das Extended-Query-Protokoll — Bind/Execute mit einem Zeilenlimit-Feld — ist ein anderer Mechanismus auf dem Wire-Protokoll. asyncpg und die meisten modernen PostgreSQL-Treiber nutzen es automatisch, ganz ohne Cursor-SQL. Wenn Ihr Treiber bereits Portale verwendet, brauchen Sie diese Syntax nicht. `DECLARE CURSOR` ist für Clients gedacht, die Cursor-SQL wörtlich senden: die benannten/serverseitigen Cursor von psycopg2 (`conn.cursor(name="...")`), bestimmte BI-Tools und einige ältere JDBC-Treiber.

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```
Das innere SELECT läuft durch die normale regulierte Pipeline von Provisa — dieselbe RLS-Durchsetzung, Maskierung und Domänenzugriffsprüfung wie bei jedem eigenständigen SELECT. [tool-verified: `server.py:1367` — `ctx.execute_sql(inner_sql)`, the same call path as a plain SELECT] Einen Cursor zu deklarieren umgeht die Governance nicht.

Das Schlüsselwort `SCROLL` bestimmt sowohl, welche Navigation erlaubt ist, als auch, wie viel Speicher der Cursor belegt:

| Deklaration | Rückwärtsnavigation | Speicherbedarf |
|---|---|---|
| `DECLARE c CURSOR FOR ...` (Standard) | Nicht erlaubt | Begrenzt auf einen FETCH-Batch |
| `DECLARE c NO SCROLL CURSOR FOR ...` | Nicht erlaubt | Begrenzt auf einen FETCH-Batch |
| `DECLARE c SCROLL CURSOR FOR ...` | Erlaubt | Vollständiges Ergebnis im Python-Speicher gepuffert |

Steht weder `SCROLL` noch `NO SCROLL`, ist der Standard `NO SCROLL` — wie PostgreSQLs eigener Standard. [tool-verified: `server.py:1361-1362`] Ein `NO SCROLL`-Cursor entfernt verbrauchte Zeilen nach jedem Vorwärtsschritt aus seinem internen Puffer, sodass ein `FETCH FORWARD`-Durchlauf durch ein großes Ergebnis nie mehr als ungefähr einen Batch auf einmal im Python-Speicher hält. [tool-verified: `server.py:443-447`] Ein `SCROLL`-Cursor behält jede Zeile, die er je geholt hat — das ist der Preis für `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` und `LAST`. Bevor Sie einen `SCROLL`-Cursor über ein großes Ergebnis deklarieren, planen Sie die volle Ergebnisgröße als Python-Heap ein. [tool-verified: `server.py:383-389`]

Wird ein bereits offener Cursorname erneut deklariert, wird zuerst der vorherige Cursor geschlossen. [tool-verified: `server.py:1369-1371`] `WITH HOLD` und `WITHOUT HOLD` werden in der Syntax akzeptiert; siehe Cursor-Lebensdauer unten.

**FETCH und MOVE**

```sql
FETCH 100 FROM c1            -- next 100 rows
FETCH FORWARD 100 FROM c1    -- same
FETCH ALL FROM c1            -- all remaining rows
FETCH BACKWARD 10 FROM c1    -- previous 10 rows (SCROLL cursors only)
FETCH ABSOLUTE 50 FROM c1    -- the row at absolute position 50 (SCROLL cursors only)
FETCH RELATIVE -5 FROM c1    -- 5 positions back (SCROLL cursors only)
FETCH FIRST FROM c1          -- row 1 (SCROLL cursors only)
FETCH LAST FROM c1           -- the final row (SCROLL cursors only)
MOVE 100 FROM c1             -- advance position without returning rows
```
`FETCH` liefert Zeilen und sendet den Command-Tag `FETCH <n>`, wobei `n` die tatsächlich gelieferte Anzahl ist — die kleiner als angefordert sein kann, wenn der Cursor das Ende erreicht. `MOVE` rückt die Position vor, ohne Zeilen zu liefern, und sendet `MOVE <n>`. [tool-verified: `server.py:1417`, `server.py:1419`]

`FROM` und `IN` sind als Einleitung des Cursornamens austauschbar. Cursornamen ohne Anführungszeichen werden in Kleinbuchstaben umgewandelt; Namen in Anführungszeichen (`"CaseSensitive"`) behalten ihre Schreibweise. [tool-verified: `test_cursor.py:74-79`]

Rückwärtsnavigation (`BACKWARD`, `ABSOLUTE`, `FIRST`, `LAST`, negatives `RELATIVE`) auf einem `NO SCROLL`-Cursor liefert SQLSTATE 55000 (`object_not_in_prerequisite_state`), wie die Ablehnung durch echtes PostgreSQL. [tool-verified: `server.py:1402-1406`] Ein FETCH oder MOVE auf einen nie deklarierten Cursor liefert SQLSTATE 34000 (`invalid_cursor_name`). [tool-verified: `server.py:1398-1400`]

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```
`CLOSE` gibt den zugrunde liegenden Ergebnisstream frei und entfernt den Cursor aus der Sitzung. [tool-verified: `server.py:1421-1436`] Die Verbindung ohne `CLOSE` zu trennen ist sicher: `ProvisaSession.close()` gibt beim Trennen alle noch offenen Cursor automatisch frei. [tool-verified: `test_cursor.py:335-346`]

**Cursor-Lebensdauer.** Der pgwire-Server von Provisa hat keinen Transaktionszustandsautomaten (`ProvisaSession.in_transaction()` ist fest auf `False` codiert). Jeder Cursor verhält sich daher, als wäre er mit `WITH HOLD` deklariert: Er lebt für die Dauer der Verbindung, nicht bis `COMMIT`. [tool-verified: `server.py:200` — `in_transaction()` always returns `False`] Die Schlüsselwörter `WITH HOLD` und `WITHOUT HOLD` werden akzeptiert und stillschweigend ignoriert — es gibt keinen Cursor-Lebenszyklus pro Transaktion, der durchzusetzen wäre.

### Definitionen (CREATE, ALTER, DROP) {: #definitions-create-alter-drop }

Über pgwire wird nichts definiert. Eine Anweisung, die ein Objekt anlegt, ändert oder entfernt (`CREATE TABLE`, `CREATE TABLE … AS SELECT`, `CREATE VIEW`, `ALTER`, `DROP`, ein Index, eine Sequenz oder ein Schema), wird mit SQLSTATE 0A000 abgelehnt, was immer die Rolle besitzt; die Ausnahme ist die eigene temporäre Tabelle einer Sitzung (siehe [Temporäre Tabellen](#temporary-tables)). Eine Tabelle oder View ist ein Modellobjekt: Legen Sie sie im Modell an (Admin-Seiten, Admin-API oder Konfiguration) oder in der Datenquelle und nehmen Sie sie ins Modell auf. `TRUNCATE` definiert nichts: Es ist ein Datenschreibvorgang (siehe [Datenschreibvorgänge](#data-writes-insert-update-delete-merge-copy-from-stdin)). [tool-verified: `provisa/compiler/definitions.py`]

Die Ablehnung erfolgt einmal, in der Pipeline, die jede SQL-Oberfläche durchläuft, sodass SQL über HTTP, Flight und MCP dieselben Anweisungen mit derselben Meldung ablehnen.

### Temporäre Tabellen {: #temporary-tables }

Eine Sitzung kann eigene temporäre Tabellen anlegen, beschreiben, lesen und löschen:

```sql
CREATE TEMP TABLE busy AS SELECT customer_id, COUNT(*) AS n FROM sales.orders GROUP BY customer_id;
INSERT INTO busy VALUES (0, 0);
UPDATE busy SET n = n + 1 WHERE customer_id = 0;
DELETE FROM busy WHERE n < 5;
SELECT c.name, b.n FROM sales.customers c JOIN busy b ON b.customer_id = c.id;
DROP TABLE busy;
```
- **Anlegen** mit `CREATE TEMP TABLE name (column type, ...)` oder `CREATE TEMP TABLE name AS SELECT ...`. Der Name nimmt kein Schema. Ein `SELECT`, aus dem sie angelegt wird, ist ein Lesevorgang wie jeder andere: Zeilenregeln, Masken, Fakes und Zeilenlimit der Rolle gelten für die Zeilen, die er nimmt.
- **Spaltentypen** sind `smallint`, `integer`, `bigint`, `real`, `double precision`, `boolean`, `date`, `timestamp`, `text`, `varchar` und `DECIMAL(precision, scale)`. Ein Dezimalwert behält Genauigkeit und Skala. Jeder andere Typ wird abgelehnt, mit Spalte und Typ: Ein Wert wird nie als ein anderer Typ als sein eigener gespeichert. Eine aus einem `SELECT` angelegte Spalte nimmt den Typ ihrer Werte an; eine ohne Wert, an dem sich ein Typ erkennen ließe, wird abgelehnt, also wenden Sie im `SELECT` ein `CAST` an.
- **Zeilenlimit.** Die Zeilen, die ein `CREATE ... AS SELECT` oder ein `INSERT ... SELECT` nimmt, werden gelesen, wie die Rolle alles liest, also gilt das Zeilenlimit der Rolle. Eine temporäre Tabelle, die aus einem vom Limit gekürzten Lesevorgang gefüllt wurde, trägt dieselbe Anweisungswarnung wie jeder gekürzte Lesevorgang (`statement.rows_cut`), mit dem Limit.
- **Schreiben** mit `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE` und `DELETE`, ohne `RETURNING` und ohne `FROM`. Ein Schreibvorgang in eine temporäre Tabelle ist keine Mutation: Er erreicht keine Quelle und keine Daten der Umgebung, funktioniert also unabhängig vom Mutationsverhalten der Umgebung und braucht kein Schreibrecht.
- **Lesen** über ihren Namen, allein oder mit Tabellen des Modells gejoint. Die Engine berechnet den Lesevorgang.
- **Lebensdauer.** Eine temporäre Tabelle ist kein Modellobjekt, keine andere Sitzung sieht sie, und sie endet mit der Sitzung. Bei pgwire ist eine Sitzung die Verbindung. Auf einer Oberfläche ohne eigene Verbindung (SQL über HTTP, Flight, MCP) ist eine Sitzung eine Anfrage, sodass eine temporäre Tabelle für die gemeinsam in dieser Anfrage gesendeten Anweisungen lebt.
- **Speicherung.** Die Zeilen liegen in einem eigenen Schema der Sitzung im Store der Engine und werden gelöscht, wenn die Sitzung endet. Ein `INSERT`, `UPDATE` oder `DELETE` schreibt die ganze Tabelle über die Engine neu, sodass eine temporäre Tabelle für Arbeitsdaten taugt, nicht für große, zeilenweise geschriebene Tabellen. Ein Prozess, der anhält, ohne seine Sitzungen zu beenden, lässt deren Schemas im Store zurück; sie werden entfernt, wenn die Umgebung außer Betrieb geht.

Dieselben Anweisungen funktionieren auf jeder SQL-Oberfläche: Sie werden in der Pipeline behandelt, die alle teilen. [inferred: verified by `tests/integration/test_session_temp_tables.py` over HTTP; no test covers the pgwire connection-long session]

### Datenschreibvorgänge (INSERT, UPDATE, DELETE, MERGE, COPY FROM STDIN) {: #data-writes-insert-update-delete-merge-copy-from-stdin }

Zeilen einer bestehenden registrierten Tabelle werden über pgwire wie auf jeder anderen SQL-Oberfläche geschrieben, über die eine Schreibzulassung (`provisa/compiler/write_admission.py`):

1. **Das Recht.** Die Rolle hält `write`. (REQ-868)
2. **Die Berechtigung.** Die Rolle besitzt eine Schreibberechtigung auf der Tabelle für die Operation: Ein `INSERT` braucht eine `insert`-Berechtigung, ein `UPDATE` eine `update`-Berechtigung, ein `DELETE` eine `delete`-Berechtigung. Ein `MERGE` braucht eine `insert`- und eine `update`-Berechtigung über jede Zeile, ein `INSERT ... ON CONFLICT DO UPDATE` eine `update`-Berechtigung über jede Zeile. (REQ-2074)
3. **Die Spalten.** Jede Spalte, die die Anweisung schreibt, wird von der Berechtigung benannt. Eine Spalte, die die Berechtigung als Preset setzt, ist Sache der Berechtigung: Die Anweisung darf sie nicht liefern, und der Wert der Berechtigung (eine Sitzungsvariable, ein Literal oder der Zeitpunkt des Schreibvorgangs) wird hinzugefügt. (REQ-663, REQ-2074)
4. **Die Zeilen, die sie berühren darf.** Das Prädikat `rows` der Berechtigung wird dem `WHERE` eines `UPDATE` oder `DELETE` hinzugefügt. Gibt die Berechtigung keines an, übernimmt sie den Zeilenfilter der Rolle, dasselbe Prädikat, das ihre Lesezugriffe tragen.
5. **Die Zeilen, die sie hinterlässt.** Die Zeilen eines `INSERT` und die neuen Werte eines `UPDATE`, das eine Spalte setzt, die das `check` der Berechtigung liest, werden gegen das `check` geprüft, bevor etwas an die Quelle gesendet wird. Gibt die Berechtigung kein `check` an, übernimmt sie den Zeilenfilter der Rolle.

`TRUNCATE` ist ebenfalls ein Datenschreibvorgang, wird als solcher ausgeführt und nie in ein `DELETE` umgeschrieben. Es braucht das Recht `write`, eine `delete`-Berechtigung über jede Zeile der Tabelle und eine Quelle, die Deletes unterstützt. Es leert jeweils eine Tabelle, und eine Rolle, deren Berechtigung die Zeilen einschränkt (ein Prädikat `rows` oder der Zeilenfilter der Rolle, wo die Berechtigung keines angibt), wird abgelehnt, da ein `TRUNCATE` das Prädikat nicht anwenden kann: Verwenden Sie `DELETE`, das es anwendet. (REQ-1942) [tool-verified: `provisa/compiler/write_admission.py` `admit_write`]

Was nicht entschieden werden kann, wird namentlich abgelehnt statt im Zweifel zugelassen (ein `INSERT … SELECT`, ein Wert, der weder Literal noch gebundener Parameter ist, ein Filter, der eine Spalte liest, die die Anweisung nicht liefert). [tool-verified: `provisa/compiler/write_admission.py`, `provisa/pgwire/copy_handler.py`]

### COPY

`COPY ... TO STDOUT` und `COPY ... FROM STDIN` werden beide unterstützt. (REQ-585)

**COPY TO STDOUT** — exportiert Abfrageergebnisse im PG-COPY-Wire-Format. Zwei Formen funktionieren:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```
Unterstützte Formate: `text` (tabulatorgetrennt, Standard), `csv` und `binary`, das binäre COPY-Format von PostgreSQL, mit dem die postgres-Erweiterung von DuckDB liest. (REQ-883) [tool-verified: `provisa/pgwire/copy_handler.py`, `provisa/pgwire/copy_binary.py`]

**COPY FROM STDIN** ist ein Datenschreibvorgang in eine bestehende registrierte Tabelle: Er wird durch dieselbe Schreibzulassung zugelassen wie ein `INSERT` derselben Zeilen, mit der zusätzlichen Bedingung, dass die `insert`-Berechtigung keine Presets hat (siehe [Datenschreibvorgänge](#data-writes-insert-update-delete-merge-copy-from-stdin)). Er liest `text` (Standard) und `csv`. Jedes andere Format wird mit SQLSTATE 0A000 unter Nennung des Formats abgelehnt, bevor Daten gesendet werden. [tool-verified: `_parse_copy_data`, `provisa/pgwire/copy_handler.py`]

### Transaktionen und Sitzungsbefehle {: #transactions-and-session-commands }

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET und DEALLOCATE werden abgefangen und liefern eine leere Erfolgsantwort. (REQ-587) Der Server ist hinsichtlich Transaktionen zustandslos — es gibt weder Transaktionsisolation noch Rollback-Unterstützung. (REQ-587) [tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## Katalog-Interception {: #catalog-intercept }

Abfragen gegen `information_schema` und `pg_catalog` werden lokal beantwortet, ohne einen Trino-Roundtrip. (REQ-532) Die Interception-Schicht baut pro Anfrage eine In-Memory-DuckDB-Datenbank auf, die aus dem Kompilierungskontext der Rolle befüllt wird. (REQ-532) [tool-verified: `catalog.py:210-213`]

Abgefangene Tabellen:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`, `pg_extension`, `pg_enum`, `pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` wird mit echten PK- und FK-Daten befüllt, die aus den Feldern `pk_columns` und `joins` des Domänenmodells abgeleitet werden. (REQ-392, REQ-399) BI-Tools, die Fremdschlüsselbeziehungen untersuchen (Tableau, DBeaver usw.), sehen den Join-Graphen, den Provisa kennt. [tool-verified: `catalog.py:551-632`] Einspaltige Joins zwischen demselben Quelle/Ziel-Paar, deren Zielspalten zusammen den zusammengesetzten Primärschlüssel des Ziels bilden, werden zu einer einzigen FK-Zeile mit mehrelementigen `conkey`/`confkey`-Arrays zusammengefasst. (REQ-1094) [tool-verified: `catalog_constraints.py`]

Eine Junction-gestützte Beziehung (REQ-1586) erzeugt keine FK-Zeile. Sie ist eine Kante über eine Zuordnungstabelle, kein Spaltenpaar, und `pg_constraint` hat keine Form für zwei Sprünge — deshalb lässt das Domänenmodell sie aus `joins` heraus, und die Junction-Tabelle erscheint als gewöhnliche Tabelle mit eigenen Fremdschlüsseln auf beide Endpunkte. SQL-Clients erreichen sie, indem sie diese Tabelle joinen; Cypher-Clients traversieren sie als eine einzige Beziehung. [tool-verified: `provisa/compiler/schema_gen.py:302-306`]

`pg_index` wird mit einer Zeile pro Primärschlüssel- und UNIQUE-Constraint befüllt (`indrelid` = OID der Tabelle, `indkey` = geordnete Schlüssel-Attnums, `indisprimary`/`indisunique` gesetzt). Clients, die Schlüsselspalten über `pg_index.indkey` statt über `pg_constraint` auflösen — zum Beispiel DataGrip —, ermitteln die korrekten Spalten über den Standard-Join `pg_index` → `pg_attribute`. (REQ-1095) [tool-verified: `catalog_constraints.py:340-384`]

Zusätzlich werden folgende skalare Ausdrücke abgefangen: (REQ-588)

- `current_user`, `session_user` → die authentifizierte `role_id`
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → liefert einen Wert aus einer festen Einstellungstabelle
- `SHOW <setting>` → liefert einen Wert aus derselben Einstellungstabelle

[tool-verified: `catalog.py:168-207`, `catalog.py:1076-1120`]

---

## Binäre Parameterkodierung {: #binary-parameter-encoding }

Das Extended-Query-Protokoll (Bind/Execute) unterstützt binär kodierte Parameter. (REQ-589) Die folgenden Typ-OIDs werden aus dem Binärformat dekodiert: [tool-verified: `postgres.py:69-97`]

| OID | PG-Typ | Python-Typ |
| ----- | --------- | ------------- |
| 16 | bool | bool |
| 17 | bytea | bytes |
| 20 | int8 | int |
| 21 | int2 | int |
| 23 | int4 | int |
| 25 | text | str |
| 700 | float4 | float |
| 701 | float8 | float |
| 1043 | varchar | str |
| 1082 | date | datetime.date |
| 1114 | timestamp | datetime.datetime |
| 1184 | timestamptz | datetime.datetime (UTC) |
| 1700 | numeric | decimal.Decimal |
| 2950 | uuid | str |

Jede OID, die nicht in dieser Tabelle steht, löst `"Unsupported binary parameter type: <oid>"` aus. (REQ-589) [tool-verified: `postgres.py:579`]

Ergebnisspalten werden ebenfalls binär gesendet, wenn der Client dies anfordert, für denselben Typsatz plus ARRAY, JSON, INTERVAL und BIGINT. (REQ-589) [tool-verified: `postgres.py:191-244`]

---

## Treiberempfehlungen {: #driver-recommendations }

**Native Python-Treiber (psycopg2, asyncpg).** Diese handeln standardmäßig das Extended-Query-Protokoll aus und verwenden für die meisten Typen binäre Kodierung. Die Typtreue ist hier am höchsten — `NUMERIC`-Spalten kommen als `Decimal`, `TIMESTAMP` als `datetime` an, und so weiter. Verwenden Sie diese für Python-basierte ETL, Skripte oder direkte Integration.

**JDBC (PostgreSQL-JDBC-Treiber).** Verwenden Sie diesen für Java-Ökosystem-Tools: DBeaver, Tableau, Power BI, Metabase, JDBC-Operatoren von Airflow. JDBC verwendet standardmäßig das Simple-Query-Protokoll, was Komplikationen durch Binärkodierung vermeidet. Verbindungszeichenfolge:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Manche JDBC-basierten BI-Tools senden beim Verbindungsaufbau eine Reihe von Abfragen an `information_schema` und `pg_catalog`, um ihren Schema-Browser zu befüllen. Diese werden alle von der Katalog-Interception-Schicht beantwortet — bei der Schemaprüfung entsteht kein Trino-Verkehr. (REQ-532)

**Wann welcher Treiber vorzuziehen ist.** Ist der Client in Python geschrieben, verwenden Sie psycopg2 oder asyncpg für eine bessere Typbehandlung. Ist der Client ein BI-Tool oder eine beliebige JVM-Anwendung, verwenden Sie JDBC. Vermeiden Sie es, binäre und textbasierte Protokollerwartungen in derselben Verbindung zu mischen, wenn Sie überraschende Typkonvertierungen beobachten — das Textmodus-Verhalten von JDBC ist einfacher nachzuvollziehen.

---

## Einschränkungen und Randbedingungen {: #caveats-and-constraints }

**Nur SQL.** Der pgwire-Listener analysiert und führt ausschließlich SQL aus — GraphQL- und Cypher-Zeichenketten werden nicht akzeptiert. (REQ-614) Datenschreibvorgänge in bestehende Tabellen werden nach den Rechten der Rolle, ihren Spaltenfreigaben und ihrem Zeilenfilter zugelassen; siehe [Datenschreibvorgänge](#data-writes-insert-update-delete-merge-copy-from-stdin).

**Keine Definitionen.** `CREATE`, `ALTER` und `DROP` werden mit SQLSTATE 0A000 abgelehnt, was immer die Rolle besitzt, außer der eigenen temporären Tabelle einer Sitzung; siehe [Temporäre Tabellen](#temporary-tables). `TRUNCATE` ist ein Datenschreibvorgang. (REQ-615, REQ-616)

**Keine echte Transaktionsunterstützung.** BEGIN/COMMIT/ROLLBACK werden akzeptiert und stillschweigend ignoriert. Jede Anweisung wird unabhängig ausgeführt. (REQ-587) [tool-verified: `server.py:146-158` — `in_transaction()` liefert stets `False`]

**Abfrage-Timeout.** Eine Anweisung darf das eigene Anfrage-Timeout von pgwire nutzen, `limits.request_timeouts.pgwire`, oder das gemeinsame `limits.request_timeout`, wenn jenes nicht gesetzt ist; beide sind Betreibereinstellungen. (REQ-590, REQ-1905) [tool-verified: `provisa/core/limits.py`, `server.py`]

**COPY-Eingabe ist text oder csv.** `COPY ... TO STDOUT` schreibt `text`, `csv` und `binary`; `COPY ... FROM STDIN` liest `text` und `csv`. [tool-verified: `_parse_copy_data`, `provisa/pgwire/copy_handler.py`]

**Die Parametersubstitution erfolgt literal.** Die Parameter `$1`, `$2`, ... werden vor der Ausführung als SQL-Literale eingesetzt, nicht als Bind-Parameter an die zugrunde liegende Engine gesendet. Das bedeutet, die zugrunde liegende Engine sieht niemals eine vorbereitete Anweisung. Für Trino hat dies keine praktische Auswirkung; bei Direct-Pool-Quellen umgeht es das Caching vorbereiteter Anweisungen. (REQ-581) [tool-verified: `server.py:78-85`]

**DECLARE SCROLL puffert das vollständige Ergebnis.** Ein `SCROLL`-Cursor sammelt jede Zeile, die er von der Engine holt, im Python-Heap, damit `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` und `LAST` bei Bedarf bedient werden können. Bei einem großen Ergebnis liegt damit das ganze Ergebnis im Speicher, solange der Cursor offen ist. Nutzen Sie den Standard `NO SCROLL` für reines Vorwärts-Streaming. (REQ-1862) [tool-verified: `server.py:383-389`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** Diese Tabellen existieren in der Katalogschicht, sind aber leere Stubs. Monitoring-Tools, die sie abfragen, erhalten null Zeilen statt Fehler. (REQ-532) [tool-verified: `catalog.py:519-535`, `catalog.py:639-934`] (`pg_index` ist befüllt — siehe Katalog-Interception.)
