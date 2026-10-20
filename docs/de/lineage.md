# Column-Level Lineage

Provisa verfolgt die Lineage auf Spaltenebene statisch. Es berechnet sie aus SQL-Definitionen und Befehlsverträgen, also muss nichts ausgeführt werden. Sie erhalten zwei Ansichten: einen DAG pro Anweisung und einen föderationsweiten Herkunftsgraphen über jede registrierte View und materialisierte View (MV).

## Der Lineage-Explorer {: #the-lineage-explorer }

Öffnen Sie in der UI **Lineage** (`/lineage`). Fügen Sie ein SQL-Statement ein und klicken
Sie auf **Build statement graph**, um dessen Column-Level-DAG zu sehen. Klicken Sie auf
**Federation graph**, um den Provenance-Graphen über alle MVs in der Registry zu laden.

## DAG auf Statement-Ebene {: #statement-level-dag }

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
- `e.embedding` und `e.geo` sind **command**-Knoten an der Grenze von `enrich_grpc_set`.
- `geo_u` ist ein **derived**-Knoten, erzeugt durch die SQL-Funktion `UPPER`.

Die Command-Grenze ist **nicht undurchsichtig**. Weil `enrich_grpc_set` seine Eingabespalten
(`id`, `region`) und Ausgabespalten (`id`, `embedding`, `geo`) deklariert, verbindet die
Lineage-Engine die Taint-Closure durchgehend von den deklarierten Spalten der Quellrelation bis
zu jeder Ausgabe.

### Knotenarten und visuelle Hinweise {: #node-kinds-and-visual-cues }

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

### Benannte Transforms an Kanten {: #named-transforms-on-edges }

Jede Kante trägt den rohen SQL-Ausdruck, der die Zielspalte erzeugt, plus eine Liste benannter
Operationen: SQL-Funktionen (`sql_function`), arithmetische/logische Operatoren (`operator`),
registrierte Commands (`command`), reine Spaltenreferenzen (`identity`) und Literale (`constant`).

Eine Kante aus einem Command-Aufruf wird in der UI als gestrichelte violette Linie dargestellt.

## Föderationsweiter Graph {: #federation-wide-graph }

Der Föderationsgraph führt die Lineage jeder registrierten MV pro Anweisung zu einem Herkunftsgraphen zusammen. Die Identität eines Knotens ist `relation.column`, also fallen die Ausgabespalte einer View und die Eingabereferenz einer anderen View auf diese Spalte zu einem Knoten zusammen. Das Ergebnis ist ein einziger DAG von den Basisspalten der Quellen bis zu jedem abgeleiteten Datensatz der Plattform.

Verwenden Sie `focus`, `direction` und `depth`, um die Ansicht auf Föderationsebene einzugrenzen,
ohne den Graphen neu zu berechnen.

## Zyklen {: #cycles }

Die Lineage-Engine erkennt jeden gerichteten Zyklus und **klassifiziert** ihn. Ein Zyklus wird beschrieben und nie abgelehnt.

| Klassifikation | Randfarbe | Bedeutung |
| --- | --- | --- |
| `feedback` | Gelb | Der Zyklus durchquert einen materialisierten Knoten — ein legaler, zeitversetzter Feedback-Loop. Der MV-Snapshot ist die Versionsgrenze, die ihn wohldefiniert macht. |
| `error` | Rot | Keine Materialisierungsgrenze im Loop — eine zirkuläre Definition ohne stabile Auswertungsreihenfolge. Vermutlich ein Designfehler. |

Ein `feedback`-Zyklus ist ein gültiges Muster. Eine Anreicherungs-MV, die eine abgeleitete Spalte in ihre eigene Quellrelation zurückführt, funktioniert, solange ein Knoten der Schleife materialisiert ist, denn der Snapshot trennt die beiden Hälften zeitlich. Ein `error`-Zyklus braucht das Urteil eines Operators. Meist verweisen dann zwei Views aufeinander ohne Snapshot dazwischen.

## API

Beide Endpunkte sind **statisch**. Sie lesen Definitionen und Verträge, nie Daten.

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

Form der Antwort:

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

### GET /admin/lineage/federation

Liefert den zusammengeführten Provenance-Graphen über alle MVs in der Registry.

```http
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

Query-Parameter:

| Parameter | Werte | Standard | Effekt |
| --- | --- | --- | --- |
| `focus` | Eine Knoten-ID | — | Beschränkt die Antwort auf den Sub-Graphen um diesen Knoten |
| `direction` | `upstream` \| `downstream` \| `both` | `both` | Richtung, in der von `focus` aus traversiert wird |
| `depth` | Ganzzahl | unbegrenzt | Maximale Hop-Distanz von `focus` |

Die Antwort hat dieselbe Form wie der Statement-Graph, mit einem zusätzlichen Feld `cycles`
:

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

## Woraus jede Aktualisierung gebaut wurde {: #what-each-refresh-was-built-from }

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

## Eine Sicht, wie sie stand und wie sie erfasst wurde {: #a-view-as-it-stood-and-as-it-was-recorded }

Eine Sicht, die die Spalten ihrer Quelle als `business_key`, `valid_time` und `recorded_time` abbildet, behält jede Version, die sie je hatte: Jede Aktualisierung hängt an, und keine Version wird überschrieben, auf jedem Speicher. Ein einfacher Lesezugriff liefert den aktuellen Stand. Zwei Request-Header lesen ihre Vergangenheit:

- `X-Provisa-As-Of` liest die Sicht zu einem Zeitpunkt oder über einen Zeitraum.
- `X-Provisa-As-Recorded` liest sie so, wie Provisa sie zu einem Zeitpunkt erfasst hatte. Später erfasste Versionen bleiben außen vor, sodass eine später erfasste Korrektur die Wiedergabe der Vergangenheit nicht ändert.

Stand 30. September, erfasst am 1. Oktober, ergibt den ursprünglichen Wert; erfasst am 5. Oktober ergibt die Korrektur. Der Audit-Eintrag jeder Anweisung hält beide Zeitpunkte fest (`as_of`, `as_recorded`), sodass sich ein Bericht so reproduzieren lässt, wie er zuerst erstellt wurde. [Sichten mit Historie](configuration.md#views-with-history-time-travel) beschreibt die drei Deklarationen mit Beispielen und jede Oberfläche, die die beiden Werte annimmt.

## Was eine Spaltenumbenennung oder -löschung beeinträchtigen würde {: #what-a-column-rename-or-drop-would-break }

Eine Spalte trägt zwei Namen, und jeder wird von einer anderen Gruppe von Artefakten gespeichert.

Der **exponierte Name** ist das, was die SQL- und GraphQL-Oberflächen zeigen: `table_columns.alias`,
mit Rückfall auf den snake_case-Standard, wenn kein Alias gesetzt ist. Views, materialisierte Views,
Metrik-Ausdrücke, RLS-Prädikate, DQ-Contracts, Metric-View-Granularitäten und MV-Row-Keys sind
alle gegen diesen Namen geschrieben, sodass **das Umbenennen eines Alias sie genauso sicher
bricht wie das Löschen der Spalte**.

Der **physische Name** ist `table_columns.column_name`, die Identität, die den vollständigen
Spaltenersatz beim Tabellen-Upsert überlebt. Relationships, [Glossar](glossary.md)-Bindungen,
Tag-Zuweisungen, die Watermark-Spalte und Spalten-Presets speichern diesen, sodass sie nur
brechen, wenn die Spalte **entfernt** wird.

`columnDependents` meldet beides. Nachgelagerte Views und MVs stammen aus dem Slicing des
Föderationsgraphen am exponierten Namen der Spalte; die Artefakte, die dieser Graph nicht
abdeckt, stammen aus einem direkten Scan der Registry.

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

Die Seite Tabellen führt die Abfrage automatisch aus, wenn eine ausstehende Änderung einen Alias ändert oder die Spaltenmenge verkleinert, und listet die Funde. Die Warnung ist ein Hinweis. Sie nennt die betroffenen Artefakte, und der Administrator entscheidet. Sie blockiert das Speichern nie, denn nicht alle Verbraucher des Bestands sind erreichbar: Ein externes Dashboard oder eine Client-Anwendung, die die Spalte über ihren Namen abfragt, liegt außerhalb des Wissens der Registry. Aus demselben Grund gleichen Scans über freien SQL-Text die Spalte als Bezeichner-Token ab, ohne den Gültigkeitsbereich aufzulösen, und können so ein Artefakt nennen, das die Spalte am Ende nicht nutzt. Bei einer Warnung ist zu viel melden die sichere Richtung.

## Lineage zur Steuerung von Command-Contracts nutzen {: #using-lineage-to-govern-command-contracts }

Weil die Taint-Closure jede deklarierte Eingabespalte mit jeder deklarierten Ausgabespalte
verbindet, hängt die Breite dieser Closure vollständig davon ab, was Sie deklarieren.

Nehmen Sie einen Befehl, der eine vollständige Bestelltabelle (`id`, `region`, `amount`, `customer_id`, `discount`, `notes`, ...) annimmt und ein `embedding` zurückgibt. Listet der Eingabevertrag alle diese Spalten, zeigt jede nachgelagerte Spalte, die das Embedding nutzt, Lineage von allen. Das ist korrekt, hilft aber nicht, denn Sie erkennen nicht, was tatsächlich zählte.

Deklarieren Sie nur `id` und `text` (die Spalten, die das Embedding-Modell tatsächlich liest),
und der Lineage-Kegel verengt sich auf diese beiden Quellspalten. Die Herleitung ist sowohl
korrekt als auch präzise.

Siehe [Commands](commands.md) für die Mechanik der Deklaration eines schmalen Eingabe-Contracts.
