# Replikate {: #replicas }

Ein Replikat ist eine Kopie einer Quelltabelle, die Provisa im Materialisierungs-Store der Engine hält und anstelle der Quelle liest. Diese Seite behandelt, worauf ein Replikat indiziert wird, wo Replikate abzulegen sind und wie aktuell sie sind. Ob eine Tabelle überhaupt repliziert wird, legt [`replicate`](configuration.md#the-replicate-setting) fest, zusammen mit dem, was die Engine erreichen kann; siehe [Was eine Engine ändert](engines.md#what-an-engine-changes).

## Replikate sind indiziert {: #replicas-are-indexed }

Provisa indiziert ein Replikat so, wie das Modell seine Tabelle verwendet.

- **Der deklarierte Primärschlüssel**, als Schlüssel durchgesetzt.
- **Die Spalten, über die deklarierte Beziehungen joinen**:
  - der Fremdschlüssel jeder Beziehung, deren Quelle diese Tabelle ist;
  - die Zielspalte jeder Beziehung, deren Ziel diese Tabelle ist, sofern diese Spalte nicht bereits der Schlüssel ist;
  - beide Fremdschlüssel einer Verbindungstabelle, die eine Beziehung durchläuft.

Nehmen Sie diese Beziehung:

```yaml
relationships:
  - id: orders-to-customers
    source_table_id: orders
    target_table_id: customers
    source_column: customer_id
    target_column: id
    cardinality: many-to-one
```

Das Replikat von `orders` erhält seinen Schlüssel und einen Index auf `customer_id`. Das Replikat von `customers` erhält seinen Schlüssel, und wenn `id` dieser Schlüssel ist, braucht es nichts weiter. Ein Join der beiden liest dann auf beiden Seiten einen Index, in einem Store, der Indizes für Joins verwendet (siehe [Was jeder Store aufbaut](#what-each-store-builds)). Eine Beziehung über eine berechnete Spalte, die das Replikat nicht enthält, erhält keinen Index.

Deklarieren Sie den Schlüssel mit `is_primary_key: true` an der Spalte. Ein Replikat ohne deklarierten Schlüssel erhält nur seine Join-Indizes.

### Zuerst lesbar, danach indiziert {: #readable-first-indexed-after }

Ein Landen wartet nie auf Indizes. Das Replikat wird ab dem Moment gelesen, in dem es eingetauscht wird; der Replikat-Runner baut seine Indizes danach einzeln in einem getrennten Durchlauf auf, unter einer Index-Sperre pro Replikat. Rechnen Sie damit, dass die ersten Abfragen nach einem Landen ohne sie laufen.

Jedes Landen hält die Indizes, die es angefordert hat, die aufgebauten und die fehlgeschlagenen, namentlich im Datensatz des Replikats in der Control Plane fest (`replica_state.index_state`).

- **Ein fehlgeschlagener Index lässt das Landen nie scheitern.** Er wird mit seiner Ursache protokolliert und vom nächsten Landen erneut angefordert. Die Protokollzeile lautet `replica of <source>.<schema>.<table>: index <name> on (<columns>) failed: <cause>`.
- **Zeilen, die einen deklarierten Schlüssel wiederholen, sind ein eigener Fehler**, Code `replica.index_duplicate_key`, der die Spalten des Schlüssels nennt. Die Quelle enthält Duplikate einer Spaltenmenge, die das Modell als eindeutig deklariert. Eine Wiederholung behebt das nicht: Korrigieren Sie die Daten an der Quelle oder den deklarierten Schlüssel.
- **Ein Landen, das die Tabelle mitten im Aufbau ersetzt**, lässt die früheren Aufbauten zurück; sie brechen ab und halten nichts fest, und die neue Tabelle erhält ihre eigenen. Bei einem PostgreSQL-Store bricht der Tausch einen Aufbau ab, der noch auf der ersetzten Tabelle läuft.
- **Der Plan folgt bei jedem Landen dem Modell.** Eine später hinzugefügte Beziehung erhält ihren Index beim nächsten Landen des Replikats.

Die Admin-UI zeigt den Index-Datensatz nicht an. Lesen Sie das Server-Protokoll oder die Spalte `index_state` in der Zeile des Replikats.

### Was jeder Store aufbaut {: #what-each-store-builds }

| Store | Was aufgebaut wird | Wie |
| --- | --- | --- |
| PostgreSQL: die eigene Datenbank der `pg`-Engine, der PostgreSQL-Store von Trino oder DuckDB, die `sqlalchemy`-Engine auf PostgreSQL | Schlüssel- und Join-Indizes | `CREATE INDEX CONCURRENTLY` auf einer eigenen Verbindung, sodass Leser und Schreiber nicht blockiert werden. Der Schlüssel wird als eindeutiger Index aufgebaut und dann mit `ADD CONSTRAINT ... PRIMARY KEY USING INDEX` zum Primärschlüssel gemacht. Ein ungültiger Index, den ein abgebrochener Aufbau hinterlassen hat, wird verworfen und neu aufgebaut. |
| Andere Datenbanken hinter einer relationalen Engine (`mysql`, `oracle`, `mssql` und die übrigen) | Schlüssel- und Join-Indizes | Das eigene DDL des Dialekts. Eine Datenbank, die das DDL ablehnt, hält den Index als fehlgeschlagen fest. |
| Eingebettete DuckDB-Datei, MotherDuck | Schlüssel- und Join-Indizes | Über den Store-Broker: `ALTER TABLE ... ADD PRIMARY KEY`, `CREATE INDEX IF NOT EXISTS`. |
| ClickHouse | Nur der Sortierschlüssel | Ein ClickHouse-Replikat wird nach seinem deklarierten Schlüssel geordnet (`ORDER BY`), der beim Landen gesetzt wird. ClickHouse hat keinen sekundären B-Tree-Index, den es aufzubauen gäbe. |
| Snowflake, BigQuery, Databricks, Fabric, Synapse | Nichts | Das Warehouse organisiert die Tabelle selbst. |

Ein Index hilft nur einem Join, der dort läuft, wo der Index liegt. Zwei Engines lesen ihren Store, ohne dessen Indizes für Joins zu nutzen:

- **DuckDB mit seiner eingebetteten Datei.** Die Engine hängt die Datei nie an. Jede Anweisung liest ihre eigene lokale Kopie jeder Store-Tabelle, die sie nennt, die neu erstellt wird, sobald sich der Store geändert hat. Joins laufen als Hash-Joins über diese Kopie, daher beschleunigen Join-Indizes sie nicht; die Indizes der Datei erzwingen den Schlüssel und bedienen alles, was die Datei an Ort und Stelle liest.
- **DuckDB mit einem PostgreSQL-Store.** DuckDB liest den Store über seine Erweiterung `postgres` mit abgeschaltetem Filter-Pushdown (Upstream-Fehler duckdb/duckdb-postgres#564), scannt also das Replikat und filtert und joint in DuckDB. Die Indizes des Stores werden aufgebaut, aber diese Lesezugriffe nutzen sie nicht.

Ein MotherDuck-Store wird angehängt und an Ort und Stelle gelesen.

## Eine relationale Datenbank als Replikat-Store verwenden, wo die Engine SQL darin ausführt {: #use-a-relational-database-as-the-replica-store-where-the-engine-runs-sql-in-it }

Führt die Engine ihre Joins innerhalb einer relationalen Datenbank aus, halten Sie die Replikate dort. Die Joins lesen dann die oben genannten Indizes. Drei Setups tun das:

- **Die `pg`-Engine.** Replikate landen in der eigenen PostgreSQL-Datenbank der Engine, und jede Anweisung läuft dort, über ihre Indizes.
- **Eine relationale Engine** (`mysql`, `oracle`, `mssql`, der `sqlalchemy`-Sammler und die übrigen). Jede Quelle landet in dieser Datenbank und wird dort abgefragt.
- **Trino mit seinem PostgreSQL-Store.** PostgreSQL ist der einzige Store, den Trino unterstützt; Trino liest Replikate über seinen PostgreSQL-Connector. Ein Filter oder Join, den Trino an PostgreSQL weiterreicht, nutzt die Indizes; Arbeit, die Trino selbst erledigt, nicht.

Richten Sie eine Engine mit `PROVISA_MATERIALIZE_URL` oder `materialize_store_url` auf einen Store aus; ein von der Org selbst registrierter Store hat Vorrang vor beiden.

```bash
PROVISA_MATERIALIZE_URL="postgresql://provisa:secret@replicas.internal:5432/replicas"
```

Das ist eine allgemeine Empfehlung, keine Regel. Ob sie sich lohnt, hängt von der Engine, den Quellen, dem Store und den Tabellengrößen ab. Wägen Sie Folgendes ab:

- **Reichweite pro Abfrage.** Ein Store, den die Engine über das Netzwerk erreicht, fügt jedem Lesezugriff einen Roundtrip und eine Übertragung hinzu. Die eingebettete DuckDB-Datei ist lokal und spaltenorientiert und scannt schnell.
- **Kosten des Landens.** Ein Replikat in eine relationale Datenbank zu schreiben und dann seine Indizes aufzubauen, kostet mehr, als eine lokale Datei zu schreiben.
- **Die eigene Last der Datenbank.** Replikat-Lesezugriffe, Landungen und Index-Aufbauten laufen alle auf dieser Datenbank. Dimensionieren Sie sie dafür.
- **Große Scans.** Ein Index nützt einer Abfrage nichts, die den Großteil einer großen Tabelle liest. Ein spaltenorientierter Store scannt sie schneller als ein zeilenorientierter.

## Wo Replikate liegen sollen {: #choosing-where-replicas-live }

Der Store ist ein Hebel unter mehreren, und sie überschneiden sich. Klären Sie zuerst die anderen; der Store entscheidet nur über das, was sie übrig lassen.

| Hebel | Was er bewirkt | Wo er hilft |
| --- | --- | --- |
| Hot Tables | Eine kleine Nachschlagetabelle wird in Redis gehalten und in jede Abfrage, die sie liest, als Inline-`VALUES`-CTE (Common Table Expression) eingefügt. | Joins auf kleine Nachschlagetabellen, auf jedem Store: Die Zeilen reisen innerhalb der Abfrage. Das Ziel einer Many-to-One-Beziehung oder eine Tabelle mit höchstens `hot_tables.auto_threshold` Zeilen (standardmäßig 1.000) wird beim ersten Lesen zur Hot Table. `hot: true` erzwingt die Aufnahme einer Tabelle und `hot: false` hält sie draußen. |
| Von Ihnen definierte materialisierte Sichten | Eine Sicht mit `materialize: true` wird gespeichert und aktualisiert. Eine Abfrage, die die Sicht liest, wird aus den gespeicherten Zeilen beantwortet, wenn sie aktuell ist und der Leser sie vollständig sieht: keine Eingabe trägt eine Zeilenregel, die enger ist als alles. | Aggregate über große Tabellen. Sie entscheiden, welche existieren; keine wird für Sie angelegt. Siehe [Sichten](configuration.md#views-governed-computed-datasets). |
| Beziehung mit `materialize: true` | Eine quellenübergreifende Beziehung erhält eine gespeicherte, vorab gejointe Eltern-Kind-Sicht. Eine Anfrage, deren Join ganz oder teilweise dem Muster entspricht, wird umgeschrieben, damit sie sie liest, ob sie als SQL ankommt oder als vom Compiler gebaute Abfrage. | Joins auf einem Store, der keine Join-Indizes aufbaut. Siehe [Auto-Materialisierung](configuration.md#auto-materialization). |
| Platzierung | Quellen, die die Engine live lesen kann, werden live gelesen; der Rest wird repliziert. Was im Store landet, folgt aus der gewählten Engine. | Die Wahl der Engine entscheidet über den größten Teil dessen, was der Store halten muss. |

Beziehungsmaterialisierung und indizierte Joins beantworten denselben Bedarf auf zwei Wegen. Auf einem spaltenorientierten Store oder Warehouse-Store (die DuckDB-Datei, ClickHouse, Snowflake, BigQuery, Databricks, Fabric, Synapse) wird kein Join-Index aufgebaut; eine vorab gejointe Sicht ist dort der Weg, einen häufigen Join günstig zu machen. Auf einem relationalen Store sind die Join-Spalten bereits indiziert, und dieselben Joins können schnell laufen, ohne sie zu materialisieren.

Was weiterhin vom Store abhängt: Ad-hoc-Scans und -Joins über große Tabellen, die keine materialisierte Sicht abdeckt und keine Hot Table bedient. Diese laufen in einer relationalen Datenbank auf den Indizes des Stores, in einer spaltenorientierten auf seiner Scan-Geschwindigkeit.

Die Ergebnisse hängen von der Last ab. Messen Sie vor und nach einer Änderung mit dem [Dimensionierungswerkzeug](sizing.md); seine [Live-oder-Replikat-Läufe](sizing.md#live-or-replica-reads-and-the-ttl) geben die Kosten eines Replikat-Lesezugriffs im Vergleich zu einem Live-Lesezugriff in Ihrem Deployment an.

## Aktualität {: #freshness }

Machen Sie die Aktualität neben der Geschwindigkeit zum Teil der Entscheidung. Wie alt eine Antwort sein darf und was ihre Aktualisierung die Quelle kostet, entscheiden darüber, ob eine Tabelle live oder aus einem Replikat gelesen wird und damit, was der Store halten muss.

Unternehmensquellen unterstützen selten Echtzeit. Daten kommen per Batch-Ladung oder nächtlichem ETL, Change-Feeds hinken hinterher, und viele Systeme vertragen keine Live-Lesezugriffe in Abfragevolumen. Gehen Sie von der Aktualität aus, die jede Tabelle braucht, und wählen Sie dann die günstigste Strategie, die sie erfüllt. Die Regeln auf der Betreiberseite stehen unter [Aktualität und Last: wer entscheidet](configuration.md#freshness-and-load-who-decides).

| Strategie | Gesetzt mit | Was sie garantiert | Was sie die Quelle kostet |
| --- | --- | --- | --- |
| Live-Lesezugriff | `replicate: -1` | Aktuell zum Zeitpunkt der Abfrage | Jede Abfrage liest die Quelle |
| TTL | `change_signal: ttl`, `cache_ttl` | Ein Replikat, das nicht älter als `cache_ttl` ist | Ein Neuaufbau pro `cache_ttl`; mit einer `watermark_column` nur die Zeilen jenseits des Watermarks |
| Änderungs-Probe | `change_signal: probe` oder `ttl_probe`, `cache_ttl` | Wird erst neu aufgebaut, nachdem die Probe eine Änderung meldet; die Probe läuft alle `cache_ttl` | Eine Probe pro Intervall und ein Neuaufbau nur bei einer Änderung |
| Push-Change-Feed | Eine Kafka- oder WebSocket-Quelle; `change_signal: debezium` an einer Quelle mit `cdc`-Block; `change_signal: native` bei PostgreSQL, MongoDB oder Cosmos DB | Aktuell innerhalb der Verzögerung des Feeds und des Debounce | Kafka, WebSocket und Debezium: nach dem ersten Aufbau keine, der Feed trägt die Zeilen. PostgreSQL, MongoDB und Cosmos DB: ein Neuaufbau pro Änderungsschub |
| Externes Signal | `change_signal: signal` | Neu aufgebaut, sobald `POST /data/sources/{source_id}/signal` meldet, dass sich die Quelle geändert hat; nie nach einer Uhr | Ein Neuaufbau pro Signal |
| TTL pro Rolle | `role_ttl` an einer Tabelle | Eine aufgeführte Rolle akzeptiert ein Replikat bis zu ihrer eigenen TTL; das `cache_ttl` der Tabelle bleibt die Untergrenze | Weniger Neuaufbauten: Der Lesezugriff einer toleranten Rolle verlangt keinen |
| Lastschutz | `load_protected: true`, mit `cache_ttl`, einer Probe oder einem `off_peak_window` | Abfragen lesen die Quelle nie; allein der Runner aktualisiert das Replikat, mit einem Off-Peak-Fenster nur, solange es offen ist | Nur die geplanten Aktualisierungen |
| Aktualitäts-Gate | `freshness_gate: true` an einer Quelle | Jeder Lesezugriff wird zuerst gegen `change_signal` und `cache_ttl` der Quelle geprüft; ein Urteil „veraltet“ aktualisiert vor dem Lesen | Eine Aktualisierung im Lesepfad, wenn veraltet |
| Läuft nie ab | `cache_ttl: -1` an einer Quelle oder Tabelle | Einmal geladen; jeder Lesezugriff wird aus dem Replikat beantwortet, und das Lesen einer noch nicht geladenen Tabelle wird mit ihrem Namen abgelehnt | Nur das eine Laden: Eine Quelle, deren Tabellen alle geladen sind, wird nie wieder angebunden, geprüft oder gelesen und braucht keine Anmeldedaten |

Eine Probe, die nichts zum Vergleichen hat (kein Watermark, Sentinel oder keine Probe-Abfrage), fällt auf die TTL zurück.

### Nahezu Echtzeit aus einem Change-Feed {: #near-real-time-from-a-change-feed }

Wo eine Quelle keine Live-Abfragelast verträgt, ihre Änderungen aber bereits veröffentlicht, liefert ein Change-Feed Daten in nahezu Echtzeit, ohne die Quelle zur Abfragezeit anzufassen. Abfragen lesen das lokale Replikat, das schnell ist und die Quelle nicht belastet, während die Event-Schleife Änderungen anwendet, sobald sie eintreffen. Die Daten hinken nur um den Feed hinterher, typischerweise Sekunden.

Dass der Feed existiert, genügt nicht. Provisa folgt ihm nur für eine Tabelle, die daran angeschlossen ist:

- **Eine Kafka-Quelle.** Die Tabelle nennt ihr Topic, und jede Änderung wird per Primärschlüssel auf das Replikat angewendet, Löschungen eingeschlossen.

  ```yaml
  live:
    strategy: kafka
    kafka:
      topic: orders.cdc
  push_debounce_quiet: 0      # seconds of quiet before a batch lands; 0 lands each message
  push_debounce_max_delay: 5  # a batch never waits longer than this
  ```

- **Eine WebSocket-Quelle.** Ihr Stream wird auf dieselbe Weise angewendet.
- **Eine Tabelle mit `change_signal: debezium`** (an der Tabelle oder ihrer Quelle) an einer Quelle, deren `cdc`-Block den Kafka-Transport und das Topic-Präfix des Connectors nennt. Der erste Lesevorgang baut das Replikat auf; danach wird jedes Änderungsereignis auf dem Topic `{topic_prefix}.{schema}.{table}` per Primärschlüssel angewendet, Löschungen eingeschlossen, und der Offset wird erst festgeschrieben, wenn die Änderung angekommen ist.
- **Eine Tabelle einer solchen Quelle mit `change_signal: kafka`**. Ihre Änderungen werden aus dem Delta-Topic gelesen, das ihr `live.kafka.topic` nennt, über denselben Transport, und auf dieselbe Weise angewendet; eine Tabelle mit diesem Signal, die kein Topic nennt, wird beim Speichern der Tabelle oder ihrer Quelle und beim Laden der Konfiguration namentlich abgelehnt.
- **Eine MongoDB- oder Cosmos-DB-Tabelle mit `change_signal: native`** (an der Tabelle oder ihrer Quelle). Jeder Änderungsschub im Change-Stream fordert einen Neuaufbau des Replikats an.
- **Eine PostgreSQL-Tabelle mit `change_signal: native`** (an der Tabelle oder ihrer Quelle). Provisa liest die logische Replikation der Datenbank aus einem eigenen Slot, `provisa_<source id>`, über eine gleichnamige Publikation, die die nativen Tabellen der Quelle aufführt. Kafka ist nicht beteiligt. Jeder Änderungsschub an einer Tabelle fordert einen Neuaufbau ihres Replikats an. Die Datenbank braucht dasselbe wie für Debezium: `wal_level` auf `logical`, eine Rolle mit dem Replikationsrecht, die die Publikation anlegen darf, und einen Primärserver statt eines Lesereplikats; einer Quelle, der etwas davon fehlt, wird beim Speichern und beim Laden der Konfiguration namentlich abgelehnt. Slot und Publikation entstehen, wenn ihnen die erste Tabelle folgt, und werden entfernt, wenn die letzte damit aufhört oder die Quelle gelöscht wird. Beim Start prüft ein Server jede registrierte PostgreSQL-Quelle und entfernt einen Provisa-Slot, dem keine native Tabelle folgt; so hält ein Slot, der von einer Konfigurationsänderung ohne laufenden Server übrig blieb, das WAL der Quelle nicht weiter fest.

Eine per Push versorgte Tabelle braucht einen deklarierten Primärschlüssel; eine ohne wird übersprungen und protokolliert.

Eine Tabelle mit einem Push-Änderungssignal gilt auf dem Lesepfad als aktuell, weil ihr Feed ihre Aktualitätsprüfung ist. Deshalb wird ein Push-Signal abgelehnt, das nichts empfangen würde: `change_signal: native` an einem anderen Typ als Kafka, WebSocket, Ingest, PostgreSQL, MongoDB oder Cosmos DB wird beim Speichern der Quelle oder Tabelle und beim Laden der Konfiguration abgelehnt, ebenso `debezium` an einer Quelle ohne `cdc`-Block.
