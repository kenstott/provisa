# Provisa-pgwire-Server

Provisa stellt einen Endpunkt für das PostgreSQL-Wire-Protokoll (pgwire) bereit. Jedes Tool, das das PostgreSQL-Client-Protokoll spricht — psycopg2, asyncpg, DBeaver, Tableau, JDBC —, kann sich verbinden und Provisa-Daten über dieselbe Governance-Pipeline abfragen, die auch die HTTP-API steuert. (REQ-266)

Abfragen durchlaufen den vollständigen Governance-Stack: Durchsetzung der Sicherheit auf Zeilenebene, Maskierungsregeln, Beziehungsschutz, Domänenzugriffsprüfungen. (REQ-001, REQ-002, REQ-263) Die pgwire-Schnittstelle ist kein Umgehungsweg. (REQ-002, REQ-266)

---

## Verbindungsdetails

Der Server startet, wenn `PROVISA_PGWIRE_PORT` auf eine Ganzzahl ungleich null gesetzt ist. Standardmäßig ist er deaktiviert. (REQ-527) [tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Setzen Sie `PROVISA_PGWIRE_CERT` und `PROVISA_PGWIRE_KEY` auf die Pfade eines PEM-Zertifikats und -Schlüssels. Sind beide vorhanden, verpackt der Server eingehende Verbindungen in TLS. Fehlen sie, ist TLS deaktiviert, und der Server antwortet auf SSL-Verhandlungsanfragen mit `N`. (REQ-530) [tool-verified: `server.py:1746-1750`]

**Gemeldete Serverversion.** Clients sehen `14.0.provisa`. Tools, die Funktionen anhand der Versionsnummer freischalten, verhalten sich möglicherweise so, als wären sie mit PostgreSQL 14 verbunden. (REQ-579) [tool-verified: `server.py:208`]

---

## Authentifizierung

Zwei Modi, gesteuert über den Schlüssel `provider` in `auth_config`:

| Modus | Wert von `provider` | Verhalten |
| ------ | ----------------- | ----------- |
| Trust | `none` (oder Auth-Middleware inaktiv) | Der vom Client gesendete Benutzername wird direkt als `role_id` verwendet. Das Passwort wird ignoriert. |
| Simple | `simple` | Das Passwort wird gegen den Auth-Provider `simple` geprüft (bcrypt). Der Benutzername wird bei Erfolg zur `role_id`. (REQ-124) |

Jeder andere `provider`-Wert liefert bei der Anmeldung einen FATAL-Fehler. (REQ-529) Das Protokoll verwendet stets den PG-Auth-Typ 3 (Klartextpasswort). (REQ-529) Verwenden Sie den Trust-Modus nicht über eine unverschlüsselte Verbindung. [tool-verified: `server.py:282-311`]

---

## Was funktioniert

### SELECT

Alle SELECT-Anweisungen durchlaufen die Governance-Pipeline (`_pipeline.py`). (REQ-001, REQ-262, REQ-266) Die Pipeline:

1. Schreibt semantisches SQL in physisches SQL um (`rewrite_semantic_to_physical`)
2. Wendet Governance an (Sicherheit auf Zeilenebene, Maskierung, Domänenzugriff) (REQ-263)
3. Validiert gegen das registrierte Schema (REQ-011)
4. Routet zu Trino oder direkt zum Quellen-Pool (REQ-027, REQ-028)

Einfache Abfragen mit mehreren Anweisungen werden unterstützt. Durch Semikolon getrennte Anweisungen werden aufgeteilt und der Reihe nach ausgeführt. (REQ-580) [tool-verified: `server.py:318-381`]

Parametrisierte Abfragen (`$1`, `$2`, ...) werden sowohl im Simple-Query- als auch im Extended-Query-Modus (Bind/Execute) unterstützt. Parameter werden vor der Ausführung als Literale eingesetzt. (REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` und `SELECT fn(args)` — wobei `fn` eine registrierte, nachverfolgte Funktion benennt — werden vor der Governance-Pipeline abgefangen und über den einen governance-gesteuerten Executor (`invoke_tracked_function`) geroutet. Das Ergebnis ist eine typisierte Zeilenmenge, identisch zu dem, was jede andere Oberfläche für diesen Befehl liefert. `writable_by` und die Governance-Regeln werden innerhalb des Executors durchgesetzt. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### DDL

DDL-Anweisungen werden über den regulären Ausdruck in `server.py` erkannt und an `DdlHandler` weitergeleitet. Die Rolle muss die Capability `"ddl"` besitzen. (REQ-042) Ohne sie wird die Anweisung mit SQLSTATE 42501 abgelehnt. [tool-verified: `ddl_handler.py:82-83`]

Die erkannten DDL-Formen sind:

```sql
CREATE TABLE / VIEW / INDEX / UNIQUE INDEX / SEQUENCE / SCHEMA
ALTER TABLE / INDEX / SEQUENCE / VIEW
DROP TABLE / VIEW / INDEX / SEQUENCE / SCHEMA
```

[tool-verified: `server.py:56-61`]

Abhängig von `ddl_catalog` gibt es zwei Ausführungspfade: (REQ-582)

**Trino-Pfad** — wird verwendet, wenn `ddl_catalog` ein Iceberg-, Hive- oder anderer nicht registrierter Trino-Katalog ist (z. B. `iceberg`, `hive`, `otel`, `results`). Auf diesem Pfad werden nur `CREATE TABLE` und `CREATE VIEW` unterstützt. Der Versuch von `ALTER`, `DROP` oder `CREATE INDEX` löst einen Fehler aus. Der Tabellenname wird vollständig qualifiziert als `catalog.schema.table`. [tool-verified: `ddl_handler.py:92-100`]

**Direkter Pfad** — wird verwendet, wenn `ddl_catalog` einer registrierten Quellen-ID entspricht. Vollständiges DDL wird unterstützt: CREATE, ALTER, DROP, Indizes, Sequenzen. `CREATE TABLE` und `CREATE VIEW` werden schemaqualifiziert als `schema.table`. Alles andere DDL (ALTER, DROP, CREATE INDEX) wird nach dem Setzen des Schemakontexts unverändert durchgereicht. Bei PostgreSQL- und SQLite-Quellen wird der Kontext mit `SET search_path TO schema` gesetzt. Bei MySQL und MariaDB mit `USE schema`. [tool-verified: `ddl_handler.py:139-170`, `ddl_handler.py:207-213`]

Nach dem DDL wird auf beiden Pfaden die neue Tabelle in den Kompilierungskontext der Rolle aufgenommen, sodass sie sofort abfragbar ist. (REQ-583) [tool-verified: `ddl_handler.py:216-250`]

**Auflösung des Schreibziels.** Katalog und Schema für DDL stammen aus den Feldern `ddl_catalog` und `ddl_schema` der Domäne. Ist `ddl_catalog` nicht gesetzt, verwendet das System standardmäßig den Iceberg-Katalog. Ist `ddl_schema` nicht gesetzt, wird standardmäßig die Domänen-ID verwendet. Die Domäne wird über die `domain_access`-Liste der Rolle aufgelöst. (REQ-584) [tool-verified: `app.py:804-811`, `ddl_handler.py:104-115`]

### COPY

`COPY ... TO STDOUT` und `COPY ... FROM STDIN` werden beide unterstützt. (REQ-585) [tool-verified: `copy_handler.py:231-257`]

**COPY TO STDOUT** — exportiert Abfrageergebnisse im PG-COPY-Wire-Format. Zwei Formen funktionieren:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

Unterstützte Formate: `text` (tabulatorgetrennt, Standard) und `csv`. Das Binärformat wird bei der COPY-Ausgabe nicht unterstützt. [tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** — fügt Zeilen in eine Zieltabelle ein. Beschränkt auf Quellen der Typen `postgresql`, `mysql`, `sqlite` oder `mariadb`. (REQ-586) Der Versuch von COPY FROM gegen eine reine Trino-Quelle (z. B. Iceberg) löst einen Berechtigungsfehler aus. [tool-verified: `copy_handler.py:65`, `copy_handler.py:351-356`]

```sql
COPY my_table (col1, col2) FROM STDIN WITH (FORMAT text)
```

Wird keine Spaltenliste angegeben, werden die Spalten aus dem registrierten Schema abgeleitet. [tool-verified: `copy_handler.py:357`]

### Transaktionen und Sitzungsbefehle

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET und DEALLOCATE werden abgefangen und liefern eine leere Erfolgsantwort. (REQ-587) Der Server ist hinsichtlich Transaktionen zustandslos — es gibt weder Transaktionsisolation noch Rollback-Unterstützung. (REQ-587) [tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## Katalog-Interception

Abfragen gegen `information_schema` und `pg_catalog` werden lokal beantwortet, ohne einen Trino-Roundtrip. (REQ-532) Die Interception-Schicht baut pro Anfrage eine In-Memory-DuckDB-Datenbank auf, die aus dem Kompilierungskontext der Rolle befüllt wird. (REQ-532) [tool-verified: `catalog.py:210-213`]

Abgefangene Tabellen:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`, `pg_extension`, `pg_enum`, `pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` wird mit echten PK- und FK-Daten befüllt, die aus den Feldern `pk_columns` und `joins` des Domänenmodells abgeleitet werden. (REQ-392, REQ-399) BI-Tools, die Fremdschlüsselbeziehungen untersuchen (Tableau, DBeaver usw.), sehen den Join-Graphen, den Provisa kennt. [tool-verified: `catalog.py:551-632`] Einspaltige Joins zwischen demselben Quelle/Ziel-Paar, deren Zielspalten zusammen den zusammengesetzten Primärschlüssel des Ziels bilden, werden zu einer einzigen FK-Zeile mit mehrelementigen `conkey`/`confkey`-Arrays zusammengefasst. (REQ-1094) [tool-verified: `catalog_constraints.py`]

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

## Binäre Parameterkodierung

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

## Treiberempfehlungen

**Native Python-Treiber (psycopg2, asyncpg).** Diese handeln standardmäßig das Extended-Query-Protokoll aus und verwenden für die meisten Typen binäre Kodierung. Die Typtreue ist hier am höchsten — `NUMERIC`-Spalten kommen als `Decimal`, `TIMESTAMP` als `datetime` an, und so weiter. Verwenden Sie diese für Python-basierte ETL, Skripte oder direkte Integration.

**JDBC (PostgreSQL-JDBC-Treiber).** Verwenden Sie diesen für Java-Ökosystem-Tools: DBeaver, Tableau, Power BI, Metabase, JDBC-Operatoren von Airflow. JDBC verwendet standardmäßig das Simple-Query-Protokoll, was Komplikationen durch Binärkodierung vermeidet. Verbindungszeichenfolge:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Manche JDBC-basierten BI-Tools senden beim Verbindungsaufbau eine Reihe von Abfragen an `information_schema` und `pg_catalog`, um ihren Schema-Browser zu befüllen. Diese werden alle von der Katalog-Interception-Schicht beantwortet — bei der Schemaprüfung entsteht kein Trino-Verkehr. (REQ-532)

**Wann welcher Treiber vorzuziehen ist.** Ist der Client in Python geschrieben, verwenden Sie psycopg2 oder asyncpg für eine bessere Typbehandlung. Ist der Client ein BI-Tool oder eine beliebige JVM-Anwendung, verwenden Sie JDBC. Vermeiden Sie es, binäre und textbasierte Protokollerwartungen in derselben Verbindung zu mischen, wenn Sie überraschende Typkonvertierungen beobachten — das Textmodus-Verhalten von JDBC ist einfacher nachzuvollziehen.

---

## Einschränkungen und Randbedingungen

**Nur SQL; keine DML-Mutationen.** Der pgwire-Listener analysiert und führt ausschließlich SQL aus — GraphQL- und Cypher-Zeichenketten werden nicht akzeptiert. (REQ-614) Einfaches `INSERT`, `UPDATE` und `DELETE` wird nicht auf einen Schreibpfad geroutet. (REQ-615) Schreiben Sie Daten über `COPY FROM STDIN` (beschreibbare Quellen) oder `CREATE TABLE AS`; zeilenbasierte Mutationen laufen stattdessen über die GraphQL-, Cypher- oder Trino-Schreibpfade.

**COPY und DDL erfordern die Capability `ddl`.** Sowohl `COPY` (in beide Richtungen) als auch DDL sind an die Capability `ddl` der Rolle gebunden; Rollen ohne diese erhalten SQLSTATE 42501. (REQ-616)

**Keine echte Transaktionsunterstützung.** BEGIN/COMMIT/ROLLBACK werden akzeptiert und stillschweigend ignoriert. Jede Anweisung wird unabhängig ausgeführt. (REQ-587) [tool-verified: `server.py:146-158` — `in_transaction()` liefert stets `False`]

**60-Sekunden-Timeout für DDL, 120-Sekunden-Timeout für Abfragen.** Diese Werte sind in den Handler-Threads fest codiert. (REQ-590) Lang laufendes DDL gegen entfernte Quellen (Schemaänderungen an großen Tabellen) kann in ein Timeout laufen. [tool-verified: `ddl_handler.py:136`, `server.py:186`]

**COPY FROM funktioniert nur mit beschreibbaren Quellen.** Iceberg, Hive, reine Trino-Quellen und schreibgeschützte Quellentypen akzeptieren kein COPY FROM. Der Fehler ist SQLSTATE 42501. (REQ-586) [tool-verified: `copy_handler.py:65`]

**Das COPY-Ausgabeformat ist text oder csv.** Das binäre PG-COPY-Format (`FORMAT binary`) ist nicht implementiert. [inferred: In `_rows_to_copy_text` / `_rows_to_copy_csv` existieren nur die Zweige `text` und `csv`]

**DDL auf dem Trino-Pfad ist auf CREATE beschränkt.** ALTER, DROP und CREATE INDEX gegen Iceberg- oder Hive-Kataloge werden nicht unterstützt. Verwenden Sie eine registrierte SQL-Quelle als `ddl_catalog`, wenn Sie vollständiges DDL benötigen. (REQ-582) [tool-verified: `ddl_handler.py:92-100`]

**Die Parametersubstitution erfolgt literal.** Die Parameter `$1`, `$2`, ... werden vor der Ausführung als SQL-Literale eingesetzt, nicht als Bind-Parameter an die zugrunde liegende Engine gesendet. Das bedeutet, die zugrunde liegende Engine sieht niemals eine vorbereitete Anweisung. Für Trino hat dies keine praktische Auswirkung; bei Direct-Pool-Quellen umgeht es das Caching vorbereiteter Anweisungen. (REQ-581) [tool-verified: `server.py:78-85`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** Diese Tabellen existieren in der Katalogschicht, sind aber leere Stubs. Monitoring-Tools, die sie abfragen, erhalten null Zeilen statt Fehler. (REQ-532) [tool-verified: `catalog.py:519-535`, `catalog.py:639-934`] (`pg_index` ist befüllt — siehe Katalog-Interception.)
