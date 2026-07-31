# Lineage auf Spaltenebene

Provisa verfolgt die Data Lineage auf Spaltenebene statisch — berechnet aus SQL-Definitionen und
Command-Contracts, ohne dass eine Ausführung erforderlich ist. Es stehen zwei Ansichten zur
Verfügung: ein DAG pro Statement und ein föderationsweiter Provenance-Graph, der alle registrierten
Views und materialisierten Views (MVs) umfasst.

## Der Lineage-Explorer

Navigieren Sie in der Benutzeroberfläche zu **Lineage** (`/lineage`). Fügen Sie ein SQL-Statement
ein und klicken Sie auf **Build statement graph**, um dessen DAG auf Spaltenebene anzuzeigen.
Klicken Sie auf **Federation graph**, um den Provenance-Graph über jede MV im Registry zu laden.
[tool-verified: LineagePage.tsx:28-119]

## DAG auf Statement-Ebene (REQ-1160)

Jede benannte Ausgabespalte in Ihrem SQL wird zu einem Knoten. Der Builder verfolgt sie durch jede
CTE, Subquery, jeden Join und Inline-Command-Aufruf zurück bis zu ihren Quellspalten und erstellt
so einen gerichteten Graphen von den Quelleingaben bis zu den finalen Ausgaben.

### Durchgerechnetes Beispiel

```sql
SELECT o.id, e.embedding, upper(e.geo) AS geo_u
FROM   orders o
JOIN   enrich_grpc_set('main.public.orders') e ON o.id = e.id
```

Dieses Statement erzeugt drei Ausgabespalten. Der Graph für `geo_u` sieht so aus:

```
orders.geo  ──[enrich_grpc_set(...)]──►  e.geo  ──[UPPER]──►  geo_u
orders.id   ─╮                                              (taint closure)
orders.region ─╯
```

- `orders.id`, `orders.region` und `orders.geo` sind **source**-Knoten (der enge Input-Contract von
  `enrich_grpc_set` deklariert `id` und `region`; die vollständige Taint-Closure verbindet alle
  deklarierten Eingaben mit allen Ausgaben). [tool-verified: `_splice_commands` in graph.py:223-242]
- `e.embedding` und `e.geo` sind **command**-Knoten — die Grenze von `enrich_grpc_set`.
- `geo_u` ist ein **derived**-Knoten, der von der SQL-Funktion `UPPER` erzeugt wird.

Die Command-Grenze ist **nicht opak**. Da `enrich_grpc_set` seine Eingabespalten (`id`, `region`)
und Ausgabespalten (`id`, `embedding`, `geo`) deklariert, verbindet die Lineage-Engine die
Taint-Closure durchgängig von den deklarierten Spalten der Quellrelation bis zu jeder Ausgabe.
[tool-verified: `_splice_commands` und `_input_relation` in graph.py:245-271]

### Knotenarten und visuelle Kennzeichen

[tool-verified: LineageDag.tsx:25-29, KIND_COLOR constants; LineagePage.tsx:21-26 LEGEND]

| Knotenart | Farbe | Bedeutung |
|---|---|---|
| `source` | Grün | Eine Spalte einer Basistabelle |
| `derived` | Blau | Erzeugt durch einen SQL-Ausdruck (Funktion, Operator, CTE) |
| `command` | Violett | Eine Ausgabespalte eines registrierten Commands |

Zusätzliche Ringe an einem Knoten:

- **Orangefarbener Ring** — eine finale Ausgabespalte des Statements.
- **Doppelter Rahmen** — die Relation der Spalte ist eine materialisierte View (MV-/CTAS-Snapshot).
- **Roter Ring** — Mitglied eines Zyklus, der als Fehler klassifiziert ist.
- **Gelber Ring** — Mitglied eines Zyklus, der als Feedback-Loop klassifiziert ist.

[tool-verified: LineageDag.tsx:88-103 Cytoscape style selectors]

### Benannte Transformationen an Kanten

Jede Kante trägt den rohen SQL-Ausdruck, der die Zielspalte erzeugt, sowie eine Liste benannter
Operationen: SQL-Funktionen (`sql_function`), arithmetische/logische Operatoren (`operator`),
registrierte Commands (`command`), einfache Spaltenreferenzen (`identity`) und Literale
(`constant`). [tool-verified: TransformOp and name_transform in graph.py:36-145]

Eine Kante aus einem Command-Aufruf wird in der Benutzeroberfläche als gestrichelte violette Linie
dargestellt. [tool-verified: LineageDag.tsx:122-124]

## Föderationsweiter Graph (REQ-1161)

Der Föderationsgraph führt die statementweise Lineage jeder registrierten MV zu einem einzigen
Provenance-Graphen zusammen. Die Identität eines Knotens ist `relation.column` — die Ausgabespalte
einer View und die Eingabereferenz einer anderen View auf dieselbe Spalte fallen zu einem einzigen
Knoten zusammen. Das Ergebnis ist ein einziger DAG von den Basis-Quellspalten bis zu jedem
abgeleiteten Dataset auf der Plattform. [tool-verified: `build_federation_graph` in
merge.py:205-229 und `qualify_outputs` in graph.py:275-299]

Verwenden Sie `focus`, `direction` und `depth`, um die Ansicht auf Föderationsebene einzugrenzen,
ohne den Graphen neu zu berechnen. [tool-verified: `slice_graph` in merge.py:160-189]

## Zyklen (REQ-1161)

Zyklen werden beschrieben, nicht abgelehnt. Die Lineage-Engine erkennt jeden gerichteten Zyklus und
**klassifiziert** ihn. [tool-verified: `Cycle.classification` property in merge.py:43-46]

| Klassifikation | Randfarbe | Bedeutung |
|---|---|---|
| `feedback` | Gelb | Der Zyklus durchläuft einen materialisierten Knoten — ein legitimer, zeitlich verzögerter Feedback-Loop. Der MV-Snapshot bildet die Versionsgrenze, die ihn wohldefiniert macht. |
| `error` | Rot | Keine Materialisierungsgrenze im Loop — eine zirkuläre Definition ohne stabile Auswertungsreihenfolge. Vermutlich ein Designfehler. |

[tool-verified: LineagePage.tsx:83-98 cycle alert rendering; merge.py:38-48]

Ein `feedback`-Zyklus ist kein Fehler. Eine Enrichment-MV, die eine abgeleitete Spalte in ihre
eigene Quellrelation zurückspeist, ist ein gültiges Muster, solange ein Knoten im Loop materialisiert
ist — der Snapshot isoliert die beiden Hälften zeitlich voneinander. Ein `error`-Zyklus erfordert
das Urteilsvermögen eines Operators: Er bedeutet meist, dass sich zwei Views gegenseitig referenzieren,
ohne dass dazwischen ein Snapshot liegt.

## API

Beide Endpunkte sind **statisch** — sie lesen Definitionen und Contracts, keine Daten.

### POST /admin/lineage/graph

Liefert den DAG auf Spaltenebene für ein einzelnes SQL-Statement.

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

Liefert den zusammengeführten Provenance-Graph über alle MVs im Registry.

```
GET /admin/lineage/federation
GET /admin/lineage/federation?focus=orders.id&direction=downstream&depth=3
```

[tool-verified: `federation_graph` endpoint at lineage_router.py:73-98]

Abfrageparameter [tool-verified: function signature at lineage_router.py:73-76]:

| Parameter | Werte | Standard | Wirkung |
|---|---|---|---|
| `focus` | Eine Knoten-ID | — | Grenzt die Antwort auf den Subgraphen um diesen Knoten ein |
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

## Lineage zur Steuerung von Command-Contracts nutzen

Da die Taint-Closure jede deklarierte Eingabespalte mit jeder deklarierten Ausgabespalte verbindet,
hängt die Breite dieser Closure vollständig davon ab, was Sie deklarieren.

Betrachten Sie einen Command, der eine vollständige `orders`-Tabelle (`id`, `region`, `amount`,
`customer_id`, `discount`, `notes`, ...) entgegennimmt und ein `embedding` zurückgibt. Listet der
Input-Contract alle diese Spalten auf, zeigt jede nachgelagerte Spalte, die das Embedding verwendet,
Lineage von allen diesen Spalten. Das ist korrekt, aber nicht hilfreich — es ist schwer zu erkennen,
was tatsächlich relevant war.

Deklarieren Sie stattdessen nur `id` und `text` (die Spalten, die das Embedding-Modell tatsächlich
liest), verengt sich der Lineage-Kegel auf diese beiden Quellspalten. Die Herleitung ist dadurch
sowohl korrekt als auch präzise.

Siehe [Commands](commands.md) für die Mechanik der Deklaration eines engen Input-Contracts.
