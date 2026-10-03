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

Sechs `impl_kind`-Werte werden unterstützt [tool-verified: `_EXECUTORS` dict in `provisa/executor/function_dispatch.py`]:

| `impl_kind` | Transport |
| --- | --- |
| `source_procedure` | Native Stored Procedure auf einer registrierten Quelle |
| `source_operation` | Eine Schreiboperation einer OpenAPI-, externen GraphQL- oder gRPC-Quelle, unverändert durchgereicht (siehe [Schreiboperation einer externen Quelle](#schreiboperation-einer-externen-quelle-req-1924)) |
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
sind nicht auf `SELECT * FROM fn(args)` beschränkt. Die Ausnahme ist die Schreiboperation einer externen Quelle, die für sich allein aufgerufen wird (siehe [Warum er nicht komponierbar ist](#warum-er-nicht-komponierbar-ist)).

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

## Schreiboperation einer externen Quelle (REQ-1924)

Eine OpenAPI-, externe GraphQL- oder gRPC-Quelle bietet Schreiboperationen an. Wird eine davon als Command registriert, ist sie von jeder Oberfläche aus aufrufbar, geregelt und auditiert. Das Hinzufügen der Quelle registriert keine davon; Sie registrieren die gewünschten einzeln, so wie Sie Tabellen registrieren. Die Registrierung ist die Kuration. [tool-verified: `provisa/executor/source_operation.py` module docstring; `provisa/api/admin/schema_common.py` `remote_source_counts`, `"mutations": 0`]

Was eine Quelle anbietet [tool-verified: `provisa/executor/source_operation.py` `offered_operations`]:

| Quellentyp | Angebotene Operationen | Operationsname |
| --- | --- | --- |
| `openapi` | Jede Nicht-GET-Operation in der Spezifikation | Die `operationId` |
| `graphql_remote` | Jedes Feld des externen Typs `Mutation` | Der Feldname |
| `grpc_remote` | Jede als Mutation klassifizierte Methode | `Service.Method` |

### Eines registrieren

1. Öffnen Sie **Modell → Befehle** und fügen Sie einen Command hinzu.
2. Wählen Sie die externe Quelle. Das Formular wechselt zu einer Operationsauswahl, die auflistet, was die Quelle anbietet.
3. Wählen Sie die Operation, eine Domäne und die Rollen, die sie aufrufen dürfen.
4. Schalten Sie optional **Erfordert Genehmigung** ein und füllen Sie das Feld **Schreibt Tabelle**.

[tool-verified: `provisa-ui/src/components/navGroups.ts` (`/commands` in the Model group); `provisa-ui/src/pages/commands/CommandFormFields.tsx` `isSourceOperation`, `command-requires-approval-switch`, `writesTable`; `provisa/api/admin/schema_query.py` `available_functions` ("Listing them registers none")]

Über die Admin-GraphQL-API listet `availableFunctions(sourceId, schemaName)` die Operationen auf. Der Schemaname ist je nach Quellentyp `openapi`, `graphql` oder `grpc_remote`. [tool-verified: `OPERATION_SCHEMA` in `source_operation.py`; `available_functions` returns `[]` when the schema name does not match the source type]

Alles Weitere ergibt sich aus der Operation, nicht aus dem, was das Formular sendet. `_as_source_operation` überschreibt diese Felder:

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

[tool-verified: `provisa/api/admin/actions_router.py` `_as_source_operation`]

Jedes Argument ist als `json` typisiert. Die Argumente der Operation sind ihre OpenAPI-Pfadparameter (plus `body`, wenn die Operation einen Request-Body entgegennimmt), ihre GraphQL-Mutationsargumente oder ihre gRPC-Anfragefelder. [tool-verified: `_openapi_operations`, `_graphql_operations`, `_grpc_operations` in `source_operation.py`] Eine Operation, die die Quelle nicht anbietet, wird mit 422, `functions.operation_not_offered`, abgelehnt. [tool-verified: `offered_operation`]

### Aufruf

Provisa formt, typisiert oder prüft die Eingabe nicht. Jedes Argument geht unverändert und mit der Zugangsberechtigung der Quelle an den externen Dienst, und dessen Antwort kommt unverändert zurück. Provisa regelt, wer aufrufen darf, in welcher Domäne, ob eine Genehmigung nötig ist, und protokolliert den Aufruf. [tool-verified: `source_operation.py` module docstring]

So erhält der externe Dienst die Argumente:

- **OpenAPI.** Pfadparameter füllen die Pfadvorlage. `body` ist der JSON-Request-Body. Jedes andere Argument kommt in den Query-String. [tool-verified: `_call_openapi`]
- **GraphQL.** Argumente werden als typisierte Variablen gesendet, jeweils mit dem Typ deklariert, den das externe Schema vorgibt. Die Mutation fragt die skalaren und Enum-Felder der Antwort zurück, ebenso die der darin enthaltenen Objekte bis zu zwei Ebenen tief. [tool-verified: `mutation_document`, `_selection`, `_ANSWER_DEPTH = 2`]
- **gRPC.** Die Argumente werden zur Anfragemeldung der `Service.Method`. [tool-verified: `_call_grpc`]

Die Antwort sind die Zeilen des Commands: ein Objekt ist eine Zeile, eine Liste von Objekten sind ihre Zeilen, alles andere ist eine Zeile `{"result": ...}`. [tool-verified: `_rows`]

Bei GraphQL ist der Command ein Mutation-Feld und seine Antwort der JSON-Skalar. [tool-verified: `provisa/compiler/actions_schema.py` (`gql_return = JSONScalar` for `source_operation`; `kind` defaults to `"mutation"`)]

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

[inferred: argument names are those of the remote operation; the example call was not run]

Auf den SQL-Oberflächen (pgwire und die übrigen, die SQL durchreichen) schreiben Sie jedes Argument als JSON-Literal in einer Zeichenkette. `'{"title": "x"}'` ist ein Objekt, `'"text"'` eine Zeichenkette, `'3'` eine Zahl. Ein Literal, das kein gültiges JSON ist, scheitert mit 422, `functions.json_argument_invalid`. [tool-verified: `_json_arguments_from_sql` in `function_dispatch.py`]

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

[inferred: the first-argument shape follows `_json_arguments_from_sql`; the statement was not run, and the command's argument list is the operation's]

Senden Sie bei REST per POST ein JSON-Objekt mit Argumenten an `/data/rest/{domain}/commands/{command}`. Ein `json`-Argument wird in der erzeugten Spezifikation als beliebiger Wert dokumentiert. [tool-verified: `provisa/api/rest/openapi_spec.py` `cmd_path = f"/{cmd_domain}/commands/{cmd_name}"`, `_arg_type_to_openapi` (`"json"` returns `{}`)]

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

[inferred: host, domain and command name are placeholders; not run]

### Ablehnungen

Die Ablehnung des externen Dienstes kommt so zurück, wie er sie formuliert hat. [tool-verified: `_refused` in `source_operation.py`]

| Externer Dienst | Provisa antwortet |
| --- | --- |
| Lehnt den Aufruf ab (HTTP 4xx, GraphQL-`errors`, ein abgelehnter gRPC-Aufruf) | 422, `functions.remote_refused`, mit `remote_status` und `answer` |
| Schlägt fehl (HTTP 5xx) | 502, `functions.remote_refused` |

Ob die Zugangsberechtigung der Quelle die Operation ausführen darf, entscheidet der externe Dienst beim Aufruf der Operation. Provisa kann bei der Registrierung keinen Schreibvorgang ausprobieren, ohne ihn auszuführen. [tool-verified: REQ-1924 CREDENTIAL AT CALL amendment in `docs/arch/requirements.yaml`; no credential check in `_as_source_operation`]

### Genehmigung

Schalten Sie **Erfordert Genehmigung** ein, und jeder Aufruf wird dem Genehmigungs-Hook der Bereitstellung vorgelegt, bevor er läuft. Er läuft nur, wenn der Hook genehmigt. [tool-verified: `provisa/api/data/action_exec.py` `_require_approval`]

- Kein Hook konfiguriert: 403, `functions.approval_unavailable`.
- Der Hook lehnt ab: 403, `functions.approval_denied`, mit der Begründung des Hooks.

Der Hook erhält den Aufrufer, die Rolle, den Namen des Commands und seine Argumente. Siehe [ABAC-Genehmigungs-Hook](security.md#abac-genehmigungs-hook). Das Flag wird als `Function.requires_approval` gespeichert, und die Prüfung gilt für jeden Command, der es setzt. [tool-verified: `action_exec.py` `if fn.get("requires_approval")`]

### Schreibt Tabelle

Tragen Sie im Feld **Schreibt Tabelle** die Tabelle ein, in die die Operation schreibt, als `schema.table`. Es muss eine registrierte Tabelle der eigenen Quelle des Commands sein, sonst wird das Speichern mit 422, `actions.written_table_not_registered`, abgelehnt. Die Einstellung ist optional. [tool-verified: `_check_written_table` in `actions_router.py`; `written_table` in `source_operation.py`]

Nach jedem Aufruf, den der externe Dienst annimmt, behandelt Provisa ihn als Schreibvorgang auf diese Tabelle. Es verwirft die zwischengespeicherten Antworten der Tabelle, markiert die materialisierten Sichten darauf als veraltet, sendet das Änderungsereignis, führt die Senken der Tabelle aus und lädt die Tabelle neu, wenn sie im Hot-Bestand gehalten wird. [tool-verified: `provisa/api/data/table_written.py` `after_table_written`]

Wird die Tabelle aus ihrem Replikat der ganzen Tabelle gelesen (die Einstellungen des Betreibers legen sie dorthin, oder die Engine kann ihre Quelle nicht direkt lesen), wird nach dem Aufruf ein Aufbau des Replikats angefordert, mit dem Grund `write`. Leser behalten das alte Replikat, bis das neue an seine Stelle tritt. Eine zeilenweise replizierte Tabelle oder eine mit einer Parameterspalte hat kein ganzes Replikat, das neu aufzubauen wäre. [tool-verified: `provisa/api/data/table_written.py` `_request_replica_build`; `provisa/federation/replica_state.py` `REASON_WRITE`]

### Warum er nicht komponierbar ist

Eine Schreiboperation ist eine Aktion, keine Transformation von Daten. Eine View oder materialisierte View, die eine enthielte, würde den Schreibvorgang bei jedem Lesen oder Aktualisieren ausführen. Der Aufruf steht daher allein: `SELECT * FROM create_issue(...)` für sich führt ihn aus, und derselbe Aufruf innerhalb einer größeren Anweisung wird abgelehnt, wo immer er steht -- in einem Join, einer Subquery oder einer Projektion. [tool-verified: `provisa/pgwire/_pipeline.py` `_refuse_composed_mutators`; `provisa/executor/source_operation.py` `writes_called_in`]

Eine View oder materialisierte View, deren Definition eine aufruft, wird beim Speichern abgelehnt. Eine Definition, die sich nicht parsen lässt, wird ebenfalls abgelehnt, solange irgendeine Schreiboperation registriert ist, da sich nicht zeigen lässt, dass sie keine aufruft. [tool-verified: `refuse_writes_in_definition` in `source_operation.py`, called from `provisa/api/admin/_table_ops.py` `_build_columns_for_input` (views) and `provisa/api/admin/schema_common.py` (materialized views)]

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view (REQ-1924)
```

Eine Quellenoperation ist außerdem kein Lineage-Knoten: Lineage wird aus dem SQL von Views und Abfragen gelesen, wo ein Command als Knoten erscheint, und keine gespeicherte Definition kann eine Quellenoperation aufrufen. [tool-verified: `provisa/lineage/graph.py` (`kind="command"` for a call in the SQL); `refuse_writes_in_definition`]

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

`provisa metadata export` ist ein Job der Shell-Ebene, kein geregeltes RPC. Der Befehl stößt die
bedarfsgesteuerte Metadatenveröffentlichung des laufenden Servers an (REQ-1072/REQ-1074), indem er
an `/admin/metadata-export/publish` postet — denselben Endpunkt, den die Schaltfläche **Jetzt
veröffentlichen** im Admin-Tab aufruft. [tool-verified: `_cmd_metadata_export` in provisa/cli.py:272-310]

Nutzen Sie ihn für zeitgesteuerte Exporte aus cron oder CI, wenn der konfigurierte Zeitplan
`reconcile_cron` nicht feingranular genug ist:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Exit 0 = vollständige Veröffentlichung. Exit 1 = teilweise Veröffentlichung oder Verbindungsfehler.

Die vollständige Flag-Referenz, Auth-Optionen, Hostbenennung bei Mandantenfähigkeit und ein
cron-Beispiel finden sich unter [Metadatenexport — Von der Kommandozeile](metadata-export.md#from-the-command-line).


Commands erscheinen in der Git-Projektion jeder Umgebung. Unter [Umgebungen](environments.md) steht, wie ein Command und seine Tag-Zuweisungen Merge und Pull überstehen.
