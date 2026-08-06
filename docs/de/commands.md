# Commands

Ein Command ist eine registrierte, regierte Funktion, die externe Berechnung unter Provisas
Governance-, Audit- und Lineage-System bringt. Während die Föderations-Engine SQL nativ verarbeitet, ist ein Command
die Nahtstelle für Berechnungen, die sie nicht ausdrücken kann: ein Anreicherungs-Microservice, ein Python-Modell, ein Shell-
Skript, eine native Datenbank-Stored-Procedure. Einmal registrieren; jede Client-Oberfläche — GraphQL,
pgwire-SQL, REST, Arrow Flight, gRPC, Bolt/Cypher — kann ihn mit identischer Governance aufrufen
(REQ-885, REQ-1156). [tool-verified: function_dispatch.py Modul-Docstring + REQ-885 in requirements.md]

Der entscheidende Unterschied: Ein Command ist ein **geregeltes RPC**, kein ad-hoc-ETL. Seine Eingaben und Ausgaben sind
deklariert, typisiert, validiert, verfolgt (traced) und in die Lineage verdrahtet. Ein ungeregelter curl-Aufruf oder Subprozess
ist nichts davon.

## Implementierungsarten

Fünf `impl_kind`-Werte werden unterstützt [tool-verified: `_EXECUTORS`-Dict in function_dispatch.py:420-426]:

| `impl_kind` | Transport |
| --- | --- |
| `source_procedure` | Native Stored Procedure auf einer registrierten Quelle |
| `script` | Lokaler Subprozess, dem JSON über stdin zugeführt wird, liest JSON von stdout |
| `http` | HTTP/S-Endpunkt; JSON-Request-Body, JSON-Antwort |
| `grpc` | gRPC unary; proto-lose JSON-Brücke |
| `python` | In-Process-Python-Callable (`module:attr`) |

Adressierung (der Katalog-`name` und `function_name`) ist entkoppelt von `binding` (Transport und
Ort). Tauschen Sie das Binding aus, und die Governance, Lineage und Aufrufer-Verträge des Commands bleiben
unverändert. [tool-verified: Function-Modell in models.py:710-750]

## Argumentarten

Jedes Argument deklariert eine `arg_kind` [tool-verified: FunctionArgument.arg_kind in models.py:691-700]:

| `arg_kind` | Verhalten |
| --- | --- |
| `column_value` | Skalar; direkt im Request-Payload übergeben |
| `table_ref` | Lazy; Provisa übergibt die Relationsreferenz unverändert; der Dienst ruft die Daten ab |
| `result_set` | Eager; Provisa materialisiert die referenzierte Relation und sendet ihre Zeilen |

`http`- und `grpc`-Commands **müssen** mindestens ein `table_ref`- oder `result_set`-Argument deklarieren.
Ein externer Command, der nur skalare Argumente erhält, würde einmal pro Zeile aufgerufen, was Batching zunichtemacht.
Der Dispatcher weist diese Konfiguration zum Aufrufzeitpunkt zurück (422). [tool-verified:
`_reject_rowwise_external` in function_dispatch.py:322-344]

Ein Command, der eine Menge zurückgibt (deklariert über `output_columns` und `return_schema`), ist eine
tabellenwertige Funktion. Verwenden Sie ihn in einer `FROM`-Klausel oder einem `JOIN`. [abgeleitet aus models.py:744-748
und command_localize.py:52-63]

## Der Dataset-Vertrag (REQ-1159)

Jedes `table_ref`- oder `result_set`-Argument kann einen **Eingabe-Spaltenvertrag** deklarieren: eine geordnete,
IR-typisierte Liste von Spalten in `FunctionArgument.columns`. Der Command selbst deklariert einen
**Ausgabe-Spaltenvertrag** in `Function.output_columns`. [tool-verified: DatasetColumn-Modell in
models.py:675-683, Function.output_columns in models.py:748]

Beide Verträge werden bei jedem Aufruf laut fehlschlagend validiert:

- **Eingabe (nur result_set):** Nach der Materialisierung validiert Provisa die Zeilen gegen die
  deklarierten Spalten. Zusätzliche Felder, fehlende Felder und falsche Typen lösen alle HTTP 422 aus.
  [tool-verified: `_validate_against` aufgerufen in `_prepare_args` bei function_dispatch.py:243-248]
- **Ausgabe:** Vom Command zurückgegebene Zeilen werden gegen `output_columns` validiert, bevor sie
  den Aufrufer erreichen. [tool-verified: function_dispatch.py:488-490]
- **Enge Projektion:** Wenn ein Eingabevertrag deklariert ist, projiziert die Materialisierungsabfrage
  **nur diese Spalten** (`SELECT "id", "region" FROM ...`) statt `SELECT *`.
  [tool-verified: `_materialize_relation` bei function_dispatch.py:155-177, col_names übergeben
  an Projektion in Zeile 171]

### Das IR-Typvokabular

Vertragsspaltentypen verwenden das kanonische IR-Typsystem (REQ-846), nicht GraphQL-Skalare oder
quellennative Schreibweisen. Die gültigen Namen sind [tool-verified: `_IR_TO_SA`-Schlüssel in ir_types.py:45-63]:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Gängige Aliase werden automatisch aufgelöst (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
usw.). [tool-verified: `_ALIASES`-Dict in ir_types.py:67-90]

`return_schema` ist die **GraphQL-Projektion** von `output_columns`, nicht die Quelle der Wahrheit.
Deklarieren Sie `output_columns` für Validierung und Lineage; fügen Sie `return_schema` für die GraphQL-Typ-
Generierung hinzu. [tool-verified: models.py:744-748, Kommentar "return_schema is its GraphQL projection"]

## Einen Command verfassen

### Konfigurationsdatei

```yaml
functions:
  - name: enrich_orders
    description: Enrich orders inline — deterministic score + region label
    domain_id: sales-analytics
    kind: query
    impl_kind: python
    source_id: ""
    function_name: enrich_orders
    returns: ""
    binding:
      callable: demo.py_functions:enrich_orders
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}   # narrow input contract
          - {name: region, type: text}
    visible_to: [admin]
    output_columns:
      - {name: id, type: integer}
      - {name: score, type: double}
      - {name: region_label, type: text}
    return_schema:
      type: array
      items:
        type: object
        properties:
          id: {type: integer}
          score: {type: number}
          region_label: {type: string}
```

[tool-verified: sample_config.yaml enrich_orders-Block]

Die gRPC-Variante (`enrich_grpc_set`) folgt demselben Muster, gibt aber `impl_kind: grpc`
und ein `binding` mit den Schlüsseln `target` und `method` statt `callable` an:

```yaml
  - name: enrich_grpc_set
    impl_kind: grpc
    binding:
      target: ${env:DEMO_GRPC_TARGET:-localhost:50071}
      method: /provisa.demo.Enrich/EnrichRows
    arguments:
      - name: input
        type: String
        arg_kind: result_set
        columns:
          - {name: id, type: integer}
          - {name: region, type: text}
    output_columns:
      - {name: id, type: integer}
      - {name: embedding, type: text}
      - {name: geo, type: text}
```

[tool-verified: config/provisa.yaml enrich_grpc_set-Block]

### Admin-UI

Das Command-Formular unter **Einstellungen → Commands** enthält einen Pro-Dataset-Eingabespalten-Editor (eine Zeile
pro deklarierter Spalte, mit einem IR-Typ-Selektor) und einen Ausgabespalten-Editor. Speichern Sie das Formular, um
den Command ohne Konfigurations-Neuladen zu registrieren oder zu aktualisieren. [abgeleitet aus CommandFormFields.tsx]

## Inline-Komposition (REQ-1159)

Commands können **innerhalb** einer größeren SQL-Anweisung erscheinen — gejoint, als Subquery oder projiziert. Sie
sind nicht auf `SELECT * FROM fn(args)` beschränkt.

```sql
-- Enrich the orders relation and join the result back inline.
SELECT o.id, o.amount, e.score, e.region_label
FROM   orders o
JOIN   enrich_orders('main.public.orders') e ON o.id = e.id
WHERE  e.score > 0.8;
```

Bevor Governance, Validierung oder Routing läuft, erkennt die Pipeline registrierte Command-Aufrufe,
führt jeden über den gemeinsamen geregelten Executor aus (sodass der I/O-Vertrag und das Identitätsmodell exakt
wie bei einem direkten Aufruf gelten), und schreibt die Aufrufstelle in eine typisierte lokale Relation um.
[tool-verified: `_localize_inline_commands` in _pipeline.py:145-163 und localize_commands in
command_localize.py:178-222]

Die Substitution ist größenadaptiv: bis zu 1.000 Zeilen wird das Ergebnis als typisierte `VALUES`-Liste inline eingefügt;
darüber wird es als benannte lokale Relation in der Engine registriert.
[tool-verified: `_DEFAULT_VALUES_MAX_ROWS = 1000` in command_localize.py:49, Pfad in Zeilen 211-216]

Eine lokalisierte Anweisung routet normal. Einzelquellen-Abfragen bleiben auf der Quelle; nur echt
quellenübergreifende Abfragen gehen zur Föderations-Engine. [tool-verified: _pipeline.py:304 Kommentar
"REQ-1159: a localized statement carries an inline local relation..."]

## Commands und Lineage

Weil jeder Command seine Eingabe- und Ausgabespalten deklariert, schließt sich die Spaltenebene-Lineage **über
die undurchsichtige Command-Grenze hinweg**. Die Lineage-Engine wendet einen Taint-Closure an: jede deklarierte Ausgabe-
spalte leitet sich von jeder deklarierten Eingabespalte ab. [tool-verified: `_splice_commands` in graph.py:223-242]

**Die handlungsrelevante Konsequenz:** Die Breite Ihres Eingabevertrags bestimmt die Präzision dieses
Closures. Eine enge Eingabe — nur die Spalten, die der Command tatsächlich benötigt — erzeugt einen engen,
lesbaren Lineage-Kegel. Die Deklaration jeder Spalte in der Quellrelation fächert sich breit über jede
Ausgabe auf, was weiterhin korrekt ist (keine Lineage geht verloren), aber die Nachverfolgbarkeit verwischt.

**Faustregel:** Übergeben Sie die minimale Projektion, die der Command benötigt, und geben Sie nur abgeleitete Spalten
zurück (nicht unverändert durchgereichte Eingaben). Das hält den Taint-Kegel akkurat. [abgeleitet aus dem
Verhalten von _splice_commands in graph.py und der engen Projektion in _materialize_relation in function_dispatch.py:161]

Siehe [Lineage](lineage.md), um zu erfahren, wie Command-Knoten im DAG erscheinen und wie man sie liest.

## Egress-Allowlist

`http`- und `grpc`-Commands rufen externe Endpunkte auf. Jeder Zielhost muss in der `udf_egress_allowlist`
der Bereitstellung aufgeführt sein. Loopback (`localhost`, `127.0.0.1`, `::1`) ist immer
erlaubt. Eine fehlende Allowlist verweigert jeden externen Egress mit HTTP 403 — es gibt keinen stillen
Standard. [tool-verified: `_check_egress` in function_dispatch.py:292-311]

## Aufruf-Tracing (REQ-886)

Jeder Aufruf erzeugt einen Trace, unabhängig vom Ergebnis. Der Trace enthält den Command-Namen,
die Transportart, das Identitätsmodell (DEFINER oder INVOKER), Eingabe-Relationsreferenzen, die Rollen-ID und
die Ausgabe-Kardinalität. Der Dispatcher erzeugt den Trace — kein `impl_kind` kann ihn umgehen.
[tool-verified: `udf_invocation_trace`-Kontext in dispatch_function:475-492]

## CLI: provisa metadata export

`provisa metadata export` ist ein Job der Shell-Ebene, kein governter RPC. Der Befehl stößt die
bedarfsgesteuerte Metadatenveröffentlichung des laufenden Servers an (REQ-1072/REQ-1074), indem er
an `/admin/metadata-export/publish` postet — denselben Endpunkt, den die Schaltfläche **Jetzt
veröffentlichen** im Admin-Tab aufruft. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

Nutze ihn für zeitgesteuerte Exporte aus cron oder CI, wenn der konfigurierte Zeitplan
`reconcile_cron` nicht feingranular genug ist:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Exit 0 = vollständige Veröffentlichung. Exit 1 = teilweise Veröffentlichung oder Verbindungsfehler.

Die vollständige Flag-Referenz, Auth-Optionen, Hostbenennung bei Mandantenfähigkeit und ein
cron-Beispiel finden sich unter [Metadatenexport — Von der Kommandozeile](metadata-export.md#from-the-command-line).
