# Column-Level Lineage

Provisa erfasst Column-Level Data Lineage statisch — berechnet aus SQL-Definitionen und
Command-Contracts, ohne dass eine Ausführung erforderlich ist. Zwei Ansichten stehen zur
Verfügung: ein DAG pro Statement und ein föderationsweiter Provenance-Graph über alle
registrierten Views und materialisierten Views (MVs).

## Der Lineage-Explorer {: #the-lineage-explorer }

Navigieren Sie in der UI zu **Lineage** (`/lineage`). Fügen Sie ein SQL-Statement ein und klicken
Sie auf **Build statement graph**, um dessen Column-Level-DAG zu sehen. Klicken Sie auf
**Federation graph**, um den Provenance-Graphen über alle MVs in der Registry zu laden.
[tool-verified: LineagePage.tsx:28-119]

## DAG auf Statement-Ebene (REQ-1160) {: #statement-level-dag-req-1160 }

Jede benannte Ausgabespalte in Ihrem SQL wird zu einem Knoten. Der Builder verfolgt sie durch
jede CTE, Subquery, jeden Join und jeden Inline-Command-Aufruf zurück bis zu ihren Quellspalten
und baut so einen gerichteten Graphen von Quelleingaben zu den finalen Ausgaben.

### Durchgerechnetes Beispiel {: #worked-example }

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

Dieses Statement erzeugt drei Ausgabespalten. Der Graph für `geo_u` sieht so aus:

```text
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region` und `orders.geo` sind **source**-Knoten (der schmale
  Eingabe-Contract von `enrich_grpc_set` deklariert `id` und `region`; die vollständige
  Taint-Closure verbindet alle deklarierten Eingaben mit allen Ausgaben).
  [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` und `e.geo` sind **command**-Knoten — die Grenze von `enrich_grpc_set`.
- `geo_u` ist ein **derived**-Knoten, erzeugt durch die SQL-Funktion `UPPER`.

Die Command-Grenze ist **nicht undurchsichtig**. Weil `enrich_grpc_set` seine Eingabespalten
(`id`, `region`) und Ausgabespalten (`id`, `embedding`, `geo`) deklariert, verbindet die
Lineage-Engine die Taint-Closure durchgehend von den deklarierten Spalten der Quellrelation bis
zu jeder Ausgabe. [tool-verified: `_splice_commands` und `_input_relation` in graph.py:245-271]

### Knotenarten und visuelle Hinweise {: #node-kinds-and-visual-cues }

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| Knotenart | Farbe | Bedeutung |
| --- | --- | --- |
| `source` | Grün | Eine Basistabellenspalte |
| `derived` | Blau | Erzeugt durch einen SQL-Ausdruck (Funktion, Operator, CTE) |
| `command` | Violett | Eine Ausgabespalte eines registrierten Commands |

Zusätzliche Ringe an einem Knoten:

- **Oranger Ring** — eine finale Ausgabespalte des Statements.
- **Doppelter Rahmen** — die Relation der Spalte ist eine materialisierte View (MV/CTAS-Snapshot).
- **Roter Ring** — Mitglied eines als Fehler klassifizierten Zyklus.
- **Gelber Ring** — Mitglied eines als Feedback-Loop klassifizierten Zyklus.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Benannte Transforms an Kanten {: #named-transforms-on-edges }

Jede Kante trägt den rohen SQL-Ausdruck, der die Zielspalte erzeugt, plus eine Liste benannter
Operationen: SQL-Funktionen (`sql_function`), arithmetische/logische Operatoren (`operator`),
registrierte Commands (`command`), reine Spaltenreferenzen (`identity`) und Literale (`constant`).
[tool-verified: TransformOp and name_transform in graph.py:36-145]

Eine Kante aus einem Command-Aufruf wird in der UI als gestrichelte violette Linie dargestellt.
[tool-verified: LineageDag.tsx:122-124]

## Föderationsweiter Graph (REQ-1161) {: #federation-wide-graph-req-1161 }

Der Föderationsgraph führt die Per-Statement-Lineage jeder registrierten MV zu einem einzigen
Provenance-Graphen zusammen. Die Knotenidentität ist `relation.column` — die Ausgabespalte einer
View und die Eingabereferenz einer anderen View auf dieselbe Spalte fallen zu einem Knoten
zusammen. Das Ergebnis ist ein einziger DAG von Basis-Quellspalten zu jedem abgeleiteten Dataset
in der Plattform. [tool-verified: `build_federation_graph` in merge.py:205-229 and
`qualify_outputs` in graph.py:275-299]

Verwenden Sie `focus`, `direction` und `depth`, um die Ansicht auf Föderationsebene einzugrenzen,
ohne den Graphen neu zu berechnen. [tool-verified: `slice_graph` in merge.py:160-189]

## Zyklen (REQ-1161) {: #cycles-req-1161 }

Zyklen werden beschrieben, nicht abgelehnt. Die Lineage-Engine erkennt jeden gerichteten Zyklus
und **klassifiziert** ihn. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| Klassifikation | Randfarbe | Bedeutung |
| --- | --- | --- |
| `feedback` | Gelb | Der Zyklus durchquert einen materialisierten Knoten — ein legaler, zeitversetzter Feedback-Loop. Der MV-Snapshot ist die Versionsgrenze, die ihn wohldefiniert macht. |
| `error` | Rot | Keine Materialisierungsgrenze im Loop — eine zirkuläre Definition ohne stabile Auswertungsreihenfolge. Vermutlich ein Designfehler. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

Ein `feedback`-Zyklus ist kein Fehler. Eine Enrichment-MV, die eine abgeleitete Spalte zurück in
ihre eigene Quellrelation einspeist, ist ein gültiges Muster, solange ein Knoten im Loop
materialisiert ist — der Snapshot isoliert die beiden Hälften zeitlich. Ein `error`-Zyklus
erfordert menschliches Urteilsvermögen: Er bedeutet in der Regel, dass sich zwei Views ohne
dazwischenliegenden Snapshot gegenseitig referenzieren.

## API

Beide Endpunkte sind **statisch** — sie lesen Definitionen und Contracts, keine Daten.

### POST /admin/lineage/graph

Liefert den Column-Level-DAG für ein einzelnes SQL-Statement.

```http
POST /admin/lineage/graph
Content-Type: application/json

{
  "sql": "SELECT o.id, e.embedding FROM orders o JOIN enrich_grpc_set('main.public.orders') e ON o.id = e.id",
  "dialect": "postgres"
}
```

[tool-verified: `lineage_graph` endpoint at lineage_router.py:45-54, LineageGraphRequest model at
lineage_router.py:29-31]

Form der Antwort [tool-verified: `LineageGraph.to_dict` in graph.py:82-105]:

```json
{
  "nodes": [
    {"id": "orders.id", "column": "id", "relation": "orders", "kind": "source", "materialized": false}
  ],
  "edges": [
    {
      "source": "orders.id",
      "target": "e.id",
      "transform": "enrich_grpc_set(...)",
      "ops": [{"name": "enrich_grpc_set", "kind": "command"}]
    }
  ],
  "outputs": ["id", "embedding"]
}
```

Liefert HTTP 422, wenn das SQL nicht geparst werden kann.
[tool-verified: lineage_router.py:51-54]

### GET /admin/lineage/federation

Liefert den zusammengeführten Provenance-Graphen über alle MVs in der Registry.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Query-Parameter [tool-verified: function signature at lineage_router.py:73-76]:

| Parameter | Werte | Standard | Effekt |
| --- | --- | --- | --- |
| `focus` | Eine Knoten-ID | — | Beschränkt die Antwort auf den Sub-Graphen um diesen Knoten |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Richtung, in der von `focus` aus traversiert wird |
| `depth` | Ganzzahl | unbegrenzt | Maximale Hop-Distanz von `focus` |

Die Antwort hat dieselbe Form wie der Statement-Graph, mit einem zusätzlichen Feld `cycles`
[tool-verified: `MergedGraph.to_dict` in merge.py:60-64]:

```json
{
  "nodes": [...],
  "edges": [...],
  "outputs": [...],
  "cycles": [
    {
      "nodes": ["orders.region", "enriched_orders.region"],
      "has_materialization_boundary": true,
      "classification": "feedback"
    }
  ]
}
```

## Woraus jede Aktualisierung gebaut wurde (REQ-862) {: #what-each-refresh-was-built-from-req-862 }

Jede Aktualisierung einer materialisierten Sicht hält fest, woraus sie gebaut wurde: die Version der Definition der Sicht, die Version ihrer Eingaben und wie diese Version ermittelt wurde, sowie die Quellspalten und den Ausdruck jeder Ausgabespalte. Die Betriebssicht `"ops"."mv_refresh_lineage"` liefert eine Zeile pro Aktualisierung:

| Spalte | Enthält |
|---|---|
| `mv_id` | Die aktualisierte materialisierte Sicht |
| `target_table` | Die Tabelle, die die Aktualisierung geschrieben hat |
| `definition_version` | Die Version der Definition der Sicht |
| `input_version`, `input_version_kind` | Die Version ihrer Eingaben: ein Iceberg-Snapshot, ein Watermark, ein Freshness-Token oder der Zeitpunkt der Aktualisierung |
| `trace_id` | Der Trace der Aktualisierung in `"ops"."traces"` |
| `refreshed_at` | Wann sie lief |
| `column_derivations` | Jede Ausgabespalte mit ihren Quellspalten und ihrem Ausdruck, als JSON |

```sql
SELECT refreshed_at, definition_version, input_version, input_version_kind
FROM "ops"."mv_refresh_lineage"
WHERE mv_id = 'view-view_exposure'
ORDER BY refreshed_at
```

Die Sicht liest die Aktualisierungsdatensätze in `"ops"."traces"` und antwortet auf jeder Engine, die sie aufbewahrt. Auf Trino sind das die Iceberg-Tabellen des Katalogs `otel`. Auf DuckDB schreibt der Server seine eigenen Trace-Datensätze als Parquet-Dateien in seinen Telemetrie-Ordner (`PROVISA_TELEMETRY_DIR`, standardmäßig `~/.provisa/telemetry`), eine Datei pro Export-Batch in einem Ordner pro Tag. Die geplante Telemetrie-Kompaktierung (`otel.compact_cron`) fasst die kleinen Dateien jedes Tages zu Dateien von bis zu 64 MB zusammen. Jede Organisation liest nur ihre eigenen Aktualisierungen, und die Datensätze werden unbegrenzt aufbewahrt. Spans werden nur erfasst, solange das OpenTelemetry-SDK aktiviert ist (`OTEL_SDK_DISABLED` nicht gesetzt oder false).

## Eine Sicht, wie sie stand und wie sie erfasst wurde (REQ-1163) {: #a-view-as-it-stood-and-as-it-was-recorded-req-1163 }

Eine als bitemporal deklarierte Sicht behält jede Version, die sie je hatte: Jede Aktualisierung hängt an, und keine Version wird überschrieben, auf jedem Speicher. Ein einfacher Lesezugriff liefert den aktuellen Stand. Zwei Request-Header lesen ihre Vergangenheit:

- `X-Provisa-As-Of` liest die Sicht zu einem Zeitpunkt oder über einen Zeitraum, auf ihrer Uhr: der eigenen Änderungszeit der Quelle, wo die Sicht `bitemporal_source_time` deklariert, sonst der Ladezeit von Provisa.
- `X-Provisa-As-Recorded` liest sie so, wie Provisa sie zu einem Zeitpunkt erfasst hatte. Später erfasste Versionen bleiben außen vor, sodass eine später erfasste Korrektur die Wiedergabe der Vergangenheit nicht ändert.

Stand 30. September, erfasst am 1. Oktober, ergibt den ursprünglichen Wert; erfasst am 5. Oktober ergibt die Korrektur. Der Audit-Eintrag jeder Anweisung hält beide Zeitpunkte fest (`as_of`, `as_recorded`), sodass sich ein Bericht so reproduzieren lässt, wie er zuerst erstellt wurde. [Sichten mit Historie](configuration.md#views-with-history-time-travel) beschreibt, wie eine bitemporale Sicht deklariert wird, und jede Oberfläche, die die beiden Werte annimmt.

## Was eine Spaltenumbenennung oder -löschung beeinträchtigen würde (REQ-1484) {: #what-a-column-rename-or-drop-would-break-req-1484 }

Eine Spalte trägt zwei Namen, und jeder wird von einer anderen Gruppe von Artefakten gespeichert.

Der **exponierte Name** ist das, was die SQL- und GraphQL-Oberflächen zeigen: `table_columns.alias`,
mit Rückfall auf den snake_case-Standard, wenn kein Alias gesetzt ist [tool-verified:
`computed_sql_alias` at `schema_helpers.py:317`]. Views, materialisierte Views,
Metrik-Ausdrücke, RLS-Prädikate, DQ-Contracts, Metric-View-Granularitäten und MV-Row-Keys sind
alle gegen diesen Namen geschrieben, sodass **das Umbenennen eines Alias sie genauso sicher
bricht wie das Löschen der Spalte**.

Der **physische Name** ist `table_columns.column_name`, die Identität, die den vollständigen
Spaltenersatz beim Tabellen-Upsert überlebt. Relationships, [Glossar](glossary.md)-Bindungen,
Tag-Zuweisungen, die Watermark-Spalte und Spalten-Presets speichern diesen, sodass sie nur
brechen, wenn die Spalte **entfernt** wird.

`columnDependents` meldet beides. Nachgelagerte Views und MVs stammen aus dem Slicing des
Föderationsgraphen am exponierten Namen der Spalte; die Artefakte, die dieser Graph nicht
abdeckt, stammen aus einem direkten Scan der Registry [tool-verified: `graph_dependents` in
`provisa/lineage/dependents.py`, registry scans in `provisa/api/admin/column_dependents.py`].

```graphql
query {
  columnDependents(tableId: "42", renamed: ["order_total"], removed: ["legacy_code"]) {
    columnName
    dependents { kind name detail breaksOn }
  }
}
```

`breaksOn` ist `rename` für eine Referenz auf den exponierten Namen und `remove` für eine auf
den physischen Namen, sodass ein Aufrufer erkennen kann, auf welche Hälfte der Änderung jedes
Artefakt reagiert.

Stellen Sie diese Abfrage **vor** dem Speichern. Eine umbenannte Spalte wird über den
exponierten Namen gefunden, den sie in der Registry noch trägt; sobald der Alias übernommen
wurde, ist der alte Name verschwunden und die Abfrage findet nichts.

Die Tables-Seite führt die Abfrage automatisch aus, wenn eine ausstehende Änderung einen Alias
ändert oder die Spaltenmenge verkleinert, und listet die Ergebnisse auf [tool-verified:
`diffEditedColumns` in `provisa-ui/src/pages/tables/columnDiff.ts`, dialog in `TablesPage.tsx`].
Die Warnung ist beratend: Sie nennt die betroffenen Artefakte, und der Administrator entscheidet.
Sie blockiert das Speichern nicht, weil nicht alle Verbraucher des Bestands erreichbar sind — ein
externes Dashboard oder eine Client-Anwendung, die die Spalte namentlich abfragt, liegt außerhalb
des Wissens der Registry. Aus demselben Grund matchen Scans über freien SQL-Text die Spalte als
Bezeichner-Token, statt den Geltungsbereich aufzulösen, was ein Artefakt benennen kann, das die
Spalte am Ende gar nicht verwendet. Überberichten ist bei einer Warnung die sichere Richtung.

## Lineage zur Steuerung von Command-Contracts nutzen {: #using-lineage-to-govern-command-contracts }

Weil die Taint-Closure jede deklarierte Eingabespalte mit jeder deklarierten Ausgabespalte
verbindet, hängt die Breite dieser Closure vollständig davon ab, was Sie deklarieren.

Betrachten Sie einen Command, der eine vollständige Orders-Tabelle (`id`, `region`, `amount`,
`customer_id`, `discount`, `notes`, ...) entgegennimmt und ein `embedding` zurückgibt. Listet der
Eingabe-Contract all diese Spalten auf, zeigt jede nachgelagerte Spalte, die das Embedding
verwendet, Lineage von allen davon. Das ist korrekt, aber nicht nützlich — es ist schwer zu
erkennen, was tatsächlich relevant war.

Deklarieren Sie nur `id` und `text` (die Spalten, die das Embedding-Modell tatsächlich liest),
und der Lineage-Kegel verengt sich auf diese beiden Quellspalten. Die Herleitung ist sowohl
korrekt als auch präzise.

Siehe [Commands](commands.md) für die Mechanik der Deklaration eines schmalen Eingabe-Contracts.
