# Commands

Ein Befehl ist eine registrierte, gesteuerte Funktion, die externe Berechnung unter die Governance, das Audit und die Lineage von Provisa bringt. Die Federation-Engine verarbeitet SQL nativ. Ein Befehl deckt die Berechnung ab, die SQL nicht ausdrücken kann, etwa einen Anreicherungs-Microservice, ein Python-Modell, ein Shell-Skript oder eine native gespeicherte Prozedur der Datenbank. Sie registrieren ihn einmal, und jede Client-Oberfläche (GraphQL, pgwire-SQL, REST, Arrow Flight, gRPC, Bolt/Cypher) kann ihn unter identischer Governance aufrufen.

Befehle sind eine der Arten, Geschäftslogik in Provisa zu schreiben. Siehe [Geschäftslogik in Provisa](business-logic.md).

Ein Befehl ist ein **geregeltes RPC**. Seine Ein- und Ausgaben sind deklariert, typisiert, validiert, verfolgt und in die Lineage eingebunden. Ein Ad-hoc-ETL-Schritt, ein ungesteuerter curl-Aufruf oder ein Subprozess hat nichts davon.

## Implementierungsarten {: #implementation-kinds }

Sechs `impl_kind`-Werte werden unterstützt:

| `impl_kind` | Transport |
| --- | --- |
| `source_procedure` | Native Stored Procedure auf einer registrierten Quelle |
| `source_operation` | Eine Schreiboperation einer OpenAPI-, externen GraphQL- oder gRPC-Quelle, unverändert durchgereicht (siehe [Schreiboperation einer externen Quelle](#a-remote-sources-write-operation)) |
| `script` | Lokaler Subprozess, dem JSON über stdin zugeführt wird, liest JSON von stdout |
| `http` | HTTP/S-Endpunkt; JSON-Request-Body, JSON-Antwort |
| `grpc` | gRPC unary; proto-lose JSON-Brücke |
| `python` | In-Process-Python-Callable (`module:attr`) |

`script` und `python` sind Host-Code: Sie laufen auf dem eigenen Host des Servers, und ein Deployment erlaubt sie nur, wenn sein Betreiber Host-Code einschaltet (siehe [Host-Code](#host-code)).

Adressierung (der Katalog-`name` und `function_name`) ist entkoppelt von `binding` (Transport und
Ort). Tauschen Sie das Binding aus, und die Governance, Lineage und Aufrufer-Verträge des Commands bleiben
unverändert.

## Argumentarten {: #argument-kinds }

Jedes Argument deklariert eine `arg_kind`:

| `arg_kind` | Verhalten |
| --- | --- |
| `column_value` | Skalar; direkt im Request-Payload übergeben |
| `table_ref` | Lazy; Provisa übergibt die Relationsreferenz unverändert; der Dienst ruft die Daten ab |
| `result_set` | Eager; Provisa materialisiert die referenzierte Relation und sendet ihre Zeilen |

`http`- und `grpc`-Commands **müssen** mindestens ein `table_ref`- oder `result_set`-Argument deklarieren.
Ein externer Command, der nur skalare Argumente erhält, würde einmal pro Zeile aufgerufen, was Batching zunichtemacht.
Der Dispatcher weist diese Konfiguration zum Aufrufzeitpunkt zurück (422).

Ein Command, der eine Menge zurückgibt (deklariert über `output_columns` und `return_schema`), ist eine
tabellenwertige Funktion. Verwenden Sie ihn in einer `FROM`-Klausel oder einem `JOIN`. [abgeleitet aus models.py:744-748
und command_localize.py:52-63]

## Der Dataset-Vertrag {: #the-dataset-contract }

Jedes `table_ref`- oder `result_set`-Argument kann einen **Eingabe-Spaltenvertrag** deklarieren: eine geordnete,
IR-typisierte Liste von Spalten in `FunctionArgument.columns`. Der Command selbst deklariert einen
**Ausgabe-Spaltenvertrag** in `Function.output_columns`.

Beide Verträge werden bei jedem Aufruf laut fehlschlagend validiert:

- **Eingabe (nur result_set):** Nach der Materialisierung validiert Provisa die Zeilen gegen die
  deklarierten Spalten. Zusätzliche Felder, fehlende Felder und falsche Typen lösen alle HTTP 422 aus.

- **Ausgabe:** Vom Command zurückgegebene Zeilen werden gegen `output_columns` validiert, bevor sie
  den Aufrufer erreichen.
- **Enge Projektion:** Wenn ein Eingabevertrag deklariert ist, projiziert die Materialisierungsabfrage
  **nur diese Spalten** (`SELECT "id", "region" FROM ...`) statt `SELECT *`.

### Das IR-Typvokabular {: #the-ir-type-vocabulary }

Spaltentypen in Verträgen verwenden die kanonischen IR-Typnamen:

`smallint` `integer` `bigint` `text` `boolean` `float` `double` `numeric`
`date` `timestamp` `time` `uuid` `bytea` `json`

Gängige Aliase werden automatisch aufgelöst (`varchar` → `text`, `int4` → `integer`, `jsonb` → `json`,
usw.).

`return_schema` ist die **GraphQL-Projektion** von `output_columns`. Maßgeblich ist `output_columns`, also deklarieren Sie es für Validierung und Lineage und ergänzen `return_schema` für die Erzeugung der GraphQL-Typen.

## Einen Command verfassen {: #authoring-a-command }

### Konfigurationsdatei {: #config-file }

Dieses Beispiel ist ein `python`-Befehl, er wird also nur geladen, wo Host-Code erlaubt ist (siehe [Host-Code](#host-code)).

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

### Admin-UI

Das Command-Formular unter **Einstellungen → Commands** enthält einen Pro-Dataset-Eingabespalten-Editor (eine Zeile
pro deklarierter Spalte, mit einem IR-Typ-Selektor) und einen Ausgabespalten-Editor. Speichern Sie das Formular, um
den Command ohne Konfigurations-Neuladen zu registrieren oder zu aktualisieren. [abgeleitet aus CommandFormFields.tsx]

## Inline-Komposition {: #inline-composition }

Befehle können **innerhalb** einer größeren SQL-Anweisung stehen, wo Sie sie joinen, in Unterabfragen nutzen oder projizieren; `SELECT * FROM fn(args)` ist nur die einfachste Form. Die Ausnahme ist die Schreiboperation einer Remote-Quelle, denn sie wird für sich allein aufgerufen (siehe [Warum er nicht komponierbar ist](#why-it-cannot-be-composed)).

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

Die Substitution ist größenadaptiv: bis zu 1.000 Zeilen wird das Ergebnis als typisierte `VALUES`-Liste inline eingefügt;
darüber wird es als benannte lokale Relation in der Engine registriert.

Eine lokalisierte Anweisung routet normal. Einzelquellen-Abfragen bleiben auf der Quelle; nur echt
quellenübergreifende Abfragen gehen zur Föderations-Engine.

## Schreiboperation einer externen Quelle {: #a-remote-sources-write-operation }

Eine OpenAPI-, externe GraphQL- oder gRPC-Quelle bietet Schreiboperationen an. Wird eine davon als Command registriert, ist sie von jeder Oberfläche aus aufrufbar, geregelt und auditiert. Das Hinzufügen der Quelle registriert keine davon; Sie registrieren die gewünschten einzeln, so wie Sie Tabellen registrieren. Die Registrierung ist die Kuration.

Was eine Quelle anbietet:

| Quellentyp | Angebotene Operationen | Operationsname |
| --- | --- | --- |
| `openapi` | Jede Nicht-GET-Operation in der Spezifikation | Die `operationId` |
| `graphql_remote` | Jedes Feld des externen Typs `Mutation` | Der Feldname |
| `grpc_remote` | Jede als Mutation klassifizierte Methode | `Service.Method` |

### Eines registrieren {: #register-one }

1. Öffnen Sie **Modell → Befehle** und fügen Sie einen Command hinzu.
2. Wählen Sie die externe Quelle. Das Formular wechselt zu einer Operationsauswahl, die auflistet, was die Quelle anbietet.
3. Wählen Sie die Operation, eine Domäne und die Rollen, die sie aufrufen dürfen.
4. Schalten Sie optional **Erfordert Genehmigung** ein und füllen Sie das Feld **Schreibt Tabelle**.

Über die Admin-GraphQL-API listet `availableFunctions(sourceId, schemaName)` die Operationen auf. Der Schemaname ist je nach Quellentyp `openapi`, `graphql` oder `grpc_remote`.

Alles Weitere ergibt sich aus der Operation, was immer das Formular sendet. Provisa überschreibt diese Felder:

```python
body.implKind = "source_operation"
body.kind = "mutation"
body.schemaName = OPERATION_SCHEMA[source_type]
body.returns = ""
body.binding = {}
body.materialize = False
body.arguments = [{"name": a, "type": "json"} for a in operation.arguments]
```

Jedes Argument ist als `json` typisiert. Die Argumente der Operation sind ihre OpenAPI-Pfadparameter (plus `body`, wenn die Operation einen Request-Body entgegennimmt), ihre GraphQL-Mutationsargumente oder ihre gRPC-Anfragefelder. Eine Operation, die die Quelle nicht anbietet, wird mit 422, `functions.operation_not_offered`, abgelehnt.

### Aufruf {: #calling-it }

Provisa formt, typisiert oder prüft die Eingabe nicht. Jedes Argument geht unverändert und mit der Zugangsberechtigung der Quelle an den externen Dienst, und dessen Antwort kommt unverändert zurück. Provisa regelt, wer aufrufen darf, in welcher Domäne, ob eine Genehmigung nötig ist, und protokolliert den Aufruf.

So erhält der externe Dienst die Argumente:

- **OpenAPI.** Pfadparameter füllen die Pfadvorlage. `body` ist der JSON-Request-Body. Jedes andere Argument kommt in den Query-String.
- **GraphQL.** Argumente werden als typisierte Variablen gesendet, jeweils mit dem Typ deklariert, den das externe Schema vorgibt. Die Mutation fragt die skalaren und Enum-Felder der Antwort zurück, ebenso die der darin enthaltenen Objekte bis zu zwei Ebenen tief.
- **gRPC.** Die Argumente werden zur Anfragemeldung der `Service.Method`.

Die Antwort sind die Zeilen des Commands: ein Objekt ist eine Zeile, eine Liste von Objekten sind ihre Zeilen, alles andere ist eine Zeile `{"result": ...}`.

Bei GraphQL ist der Command ein Mutation-Feld und seine Antwort der JSON-Skalar.

```graphql
mutation {
  createIssue(input: {repositoryId: "R_kgDO...", title: "Crash on save"})
}
```

Auf den SQL-Oberflächen (pgwire und die übrigen, die SQL durchreichen) schreiben Sie jedes Argument als JSON-Literal in einer Zeichenkette. `'{"title": "x"}'` ist ein Objekt, `'"text"'` eine Zeichenkette, `'3'` eine Zahl. Ein Literal, das kein gültiges JSON ist, scheitert mit 422, `functions.json_argument_invalid`.

```sql
SELECT * FROM create_issue('{"repositoryId": "R_kgDO...", "title": "Crash on save"}');
```

Senden Sie bei REST per POST ein JSON-Objekt mit Argumenten an `/data/rest/{domain}/commands/{command}`. Ein `json`-Argument wird in der erzeugten Spezifikation als beliebiger Wert dokumentiert.

```bash
curl -X POST https://acme.provisa.org/data/rest/engineering/commands/create_issue \
  -H "Content-Type: application/json" \
  -d '{"input": {"repositoryId": "R_kgDO...", "title": "Crash on save"}}'
```

### Ablehnungen {: #refusals }

Die Ablehnung des externen Dienstes kommt so zurück, wie er sie formuliert hat.

| Externer Dienst | Provisa antwortet |
| --- | --- |
| Lehnt den Aufruf ab (HTTP 4xx, GraphQL-`errors`, ein abgelehnter gRPC-Aufruf) | 422, `functions.remote_refused`, mit `remote_status` und `answer` |
| Schlägt fehl (HTTP 5xx) | 502, `functions.remote_refused` |

Ob die Zugangsberechtigung der Quelle die Operation ausführen darf, entscheidet der externe Dienst beim Aufruf der Operation. Provisa kann bei der Registrierung keinen Schreibvorgang ausprobieren, ohne ihn auszuführen.

### Genehmigung {: #approval }

Schalten Sie **Erfordert Genehmigung** ein, und jeder Aufruf wird dem Genehmigungs-Hook der Bereitstellung vorgelegt, bevor er läuft. Er läuft nur, wenn der Hook genehmigt.

- Kein Hook konfiguriert: 403, `functions.approval_unavailable`.
- Der Hook lehnt ab: 403, `functions.approval_denied`, mit der Begründung des Hooks.

Der Hook erhält den Aufrufer, die Rolle, den Namen des Commands und seine Argumente. Siehe [ABAC-Genehmigungs-Hook](security.md#abac-approval-hook). Das Flag wird als `Function.requires_approval` gespeichert, und die Prüfung gilt für jeden Command, der es setzt.

### Schreibt Tabelle {: #writes-table }

Tragen Sie im Feld **Schreibt Tabelle** die Tabelle ein, in die die Operation schreibt, als `schema.table`. Es muss eine registrierte Tabelle der eigenen Quelle des Commands sein, sonst wird das Speichern mit 422, `actions.written_table_not_registered`, abgelehnt. Die Einstellung ist optional.

Nach jedem Aufruf, den der externe Dienst annimmt, behandelt Provisa ihn als Schreibvorgang auf diese Tabelle. Es verwirft die zwischengespeicherten Antworten der Tabelle, markiert die materialisierten Sichten darauf als veraltet, sendet das Änderungsereignis, führt die Commands aus, die bei ihrer Änderung laufen, und lädt die Tabelle neu, wenn sie im Hot-Bestand gehalten wird.

Wird die Tabelle aus ihrem Replikat der ganzen Tabelle gelesen (die Einstellungen des Betreibers legen sie dorthin, oder die Engine kann ihre Quelle nicht direkt lesen), wird nach dem Aufruf ein Aufbau des Replikats angefordert, mit dem Grund `write`. Leser behalten das alte Replikat, bis das neue an seine Stelle tritt. Eine zeilenweise replizierte Tabelle oder eine mit einer Parameterspalte hat kein ganzes Replikat, das neu aufzubauen wäre.

### Warum er nicht komponierbar ist {: #why-it-cannot-be-composed }

Eine Schreiboperation ist eine Aktion. Sie transformiert keine Daten, und eine View oder materialisierte View, die eine enthielte, führte den Schreibvorgang bei jedem Lesen oder Aktualisieren aus. Deshalb steht der Aufruf allein. `SELECT * FROM create_issue(...)` für sich allein führt ihn aus, und derselbe Aufruf innerhalb einer größeren Anweisung wird abgelehnt, wo immer er steht, ob in einem Join, einer Unterabfrage oder einer Projektion.

Eine View oder materialisierte View, deren Definition eine aufruft, wird beim Speichern abgelehnt. Eine Definition, die sich nicht parsen lässt, wird ebenfalls abgelehnt, solange irgendeine Schreiboperation registriert ist, da sich nicht zeigen lässt, dass sie keine aufruft.

```text
command 'create_issue' writes to its source and is called on its own: it cannot be composed in a query, a view or a materialized view
```

Eine Quelloperation ist auch kein Lineage-Knoten. Lineage wird aus dem SQL von Views und Abfragen gelesen, wo ein Befehl als Knoten erscheint, und keine gespeicherte Definition kann eine Quelloperation aufrufen.

## Commands und Lineage {: #commands-and-lineage }

Weil jeder Command seine Eingabe- und Ausgabespalten deklariert, schließt sich die Spaltenebene-Lineage **über
die undurchsichtige Command-Grenze hinweg**. Die Lineage-Engine wendet einen Taint-Closure an: jede deklarierte Ausgabe-
spalte leitet sich von jeder deklarierten Eingabespalte ab.

**Die handlungsrelevante Konsequenz:** Die Breite Ihres Eingabevertrags bestimmt die Präzision dieses Abschlusses. Eine schmale Eingabe, die nur die Spalten enthält, die der Befehl braucht, ergibt einen engen, lesbaren Lineage-Kegel. Wer jede Spalte der Quellrelation deklariert, fächert breit über jede Ausgabe auf. Das bleibt korrekt, denn keine Lineage geht verloren, verwischt aber die Nachvollziehbarkeit.

**Faustregel:** Übergeben Sie die kleinste Projektion, die der Befehl braucht, und geben Sie nur abgeleitete Spalten zurück. Unverändert durchgereichte Eingaben weiten den Taint-Kegel, also lassen Sie sie weg.

Siehe [Lineage](lineage.md), um zu erfahren, wie Command-Knoten im DAG erscheinen und wie man sie liest.

## Egress-Allowlist

Befehle vom Typ `http` und `grpc` rufen externe Endpunkte auf. Jeder Zielhost muss auf der `udf_egress_allowlist` des Deployments stehen. Loopback (`localhost`, `127.0.0.1`, `::1`) ist immer erlaubt. Ohne Allowlist wird jeder externe Ausgang mit HTTP 403 verweigert, und es gibt keinen stillen Standardwert.

## Host-Code {: #host-code }

Ein `python`-Befehl ruft ein `module:attr` im Prozess des Servers auf, und ein `script`-Befehl startet einen Unterprozess auf dem Host des Servers. Beide laufen mit allem, was der Server erreichen kann: seinem Speicher, seinen Zugangsdaten, seinen Dateien und seinem Netzwerk. Bösartiger, fehlerhafter oder gieriger Code kann all das lesen, den Server aushungern oder ihn zum Absturz bringen. Der Modelleintrag des Befehls enthält nur einen Verweis auf den Code (`module:attr` oder den Pfad des Skripts), daher reist der Code selbst nicht mit dem Modell, wenn das Modell in eine andere Umgebung befördert wird.

Die Deployment-Einstellung `commands.allow_host_code` regelt beide Arten. Sie ist standardmäßig aus. Solange sie aus ist:

- wird das Registrieren eines Befehls beider Arten abgelehnt, ob über die Admin-API oder aus einer Konfigurationsdatei;
- wird das Ändern eines Befehls auf eine der beiden Arten abgelehnt;
- wird der Aufruf eines bestehenden Befehls beider Arten abgelehnt, mit HTTP 403 und dem Code
  `functions.host_code_refused`.

Die Einstellung gehört dem Betreiber, nie einer Organisation. Sie steht auf der Seite Deployment-Einstellungen unter Sicherheit und kann auch in der Konfigurationsdatei oder der Umgebung gesetzt werden:

```yaml
server:
  commands:
    allow_host_code: true
```

```bash
PROVISA_COMMANDS_ALLOW_HOST_CODE=true
```

[Code-Quellen](code-sources.md) sind die geregelte Alternative: Ihr Code läuft in einer Sandbox, ist auf eine Version festgelegt und wird mit dem Modell befördert.

## Aufruf-Tracing {: #invocation-tracing }

Jeder Aufruf erzeugt einen Trace, unabhängig vom Ergebnis. Der Trace enthält Befehlsname, Transportart, Identitätsmodell (DEFINER oder INVOKER), Referenzen auf Eingaberelationen, Rollen-ID und Ausgabekardinalität. Der Dispatcher erzeugt ihn, also kann kein `impl_kind` ihn umgehen.

## CLI: provisa metadata export

`provisa metadata export` ist ein Job der Shell-Ebene und läuft außerhalb des geregelten RPC-Wegs. Er stößt die bedarfsgesteuerte Metadaten-Veröffentlichung des laufenden Servers an, indem er an `/admin/metadata-export/publish` sendet, den Endpunkt, den die Schaltfläche **Jetzt veröffentlichen** im Admin-Tab aufruft.

Nutzen Sie ihn für zeitgesteuerte Exporte aus cron oder CI, wenn der konfigurierte Zeitplan
`reconcile_cron` nicht feingranular genug ist:

```bash
provisa metadata export --api https://acme.provisa.org --token "$PROVISA_API_TOKEN"
```

Exit 0 = vollständige Veröffentlichung. Exit 1 = teilweise Veröffentlichung oder Verbindungsfehler.

Die vollständige Flag-Referenz, Auth-Optionen, Hostbenennung bei Mandantenfähigkeit und ein
cron-Beispiel finden sich unter [Metadatenexport — Von der Kommandozeile](metadata-export.md#from-the-command-line).

Commands erscheinen in der Git-Projektion jeder Umgebung. Unter [Umgebungen](environments.md) steht, wie ein Command und seine Tag-Zuweisungen Merge und Pull überstehen.
