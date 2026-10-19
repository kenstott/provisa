# Domänenmodell-Prinzipien {: #domain-model-principles }

---

## 1. Governance

### Kernprinzipien {: #core-principles }

1. **Jede Ressource muss einer Domäne gehören.** Tabellen, Sichten und Beziehungen sind alle Domänen-Assets. Es gibt keine ungovernten frei schwebenden Ressourcen. Die Domäne ist die Einheit der Rechenschaftspflicht.
2. **Jede Domäne muss einen Steward haben.** Eine Domäne kann sich in einem ausstehenden Zustand befinden, bis ein Steward zugewiesen wird, aber sie kann ohne einen solchen keine governten Daten bereitstellen.
3. **Der Admin besitzt die Quellen.** Quellen sind Infrastruktur, keine Domänen-Ressourcen. Der Admin registriert und verwaltet Verbindungen zu externen Datensystemen.
4. **Stewards können Tabellen für eine Domäne beanspruchen.** Ein Anspruch ist exklusiv: Eine Tabelle gehört genau einer Domäne. Das Beanspruchen ist der gesteuerte Akt, der die Infrastruktur mit dem Modell verbindet.
5. **Stewards können domäneninterne Sichten aus Domänen-Assets erstellen.** Sichten drücken Geschäftslogik aus, etwa Joins, Aggregationen und abgeleitete Kennzahlen, über Assets, die der Steward in derselben Domäne besitzt. Eine Sicht schafft neue semantische Bedeutung und braucht die Genehmigung des Stewards.
6. **Analysten können domänenübergreifende Abfragen aus genehmigten Beziehungen erstellen.** Abfragen sind domänenübergreifende Sichten in jeder unterstützten Abfragesprache. Sie schaffen keine neue Semantik; sie traversieren genehmigte Beziehungspfade. Eine weitere Genehmigung ist nicht nötig, weil die Governance schon vorher greift, auf den Ebenen Beziehung und Spaltensichtbarkeit. Der Katalog setzt durch: Der Compiler lehnt jede Traversierung ab, die nicht im Katalog genehmigter Beziehungen steht.
7. **Jeder kann Zugriff auf eine Domänenressource anfragen.** Zugriff wird pro Ressource gewährt, nie pro Abfrage: Wer Zugriff auf eine Ressource hat, kann sie abfragen. Die Governance wird zur Ausführungszeit über die Pipeline durchgesetzt.

### Ressourcen: Tabellen und Sichten als Gleichrangige {: #resources-tables-and-views-as-peers }

Eine Tabelle und eine Sicht unterscheiden sich nur in ihrer Herkunft: Eine Tabelle wird aus einer Quelle beansprucht, eine Sicht von einem Steward definiert. Sobald eine von beiden ein Domänen-Asset ist, behandelt das Governance-Modell sie gleich:

- Beide sind erstklassige Domänen-Assets, die im Katalog sichtbar sind
- Beide können Ziel einer Beziehung sein
- Beide können unter Prinzip 6 gewährt werden
- Beide unterliegen derselben Governance-Pipeline

Ein Steward kann Tabellen privat beanspruchen und nur kuratierte Sichten als öffentlich zugängliche Datenprodukte bereitstellen.

### Sicht-Komposition {: #view-composition }

Eine Sicht gehört immer zu genau einer Domäne. Es gibt nur eine Art von Sicht, und sie ist immer domänenintern. Eine Sicht dient einem von zwei Zwecken:

- **Domänenübergreifender Import** — die Quelle liegt außerhalb der Domäne. Domänenübergreifende Daten dürfen nur über eine Sicht in eine Domäne gelangen, die als schreibgeschützter Adapter fungiert, der die externen Daten als Domänen-Geschäftskonzept benennt.
- **Lokale Ableitung** — die Quelle liegt in derselben Domäne. Die Sicht leitet neue oder berechnete Daten aus vorhandenen Domänen-Assets ab. Neue oder abgeleitete Daten dürfen nur als Sicht existieren.

Eine Sicht kann referenzieren:

- Beanspruchte Tabellen innerhalb derselben Domäne
- Felder, die aus einer anderen Domäne unter einer Feldzugriffsberechtigung importiert wurden
- Eine andere Sicht innerhalb derselben Domäne, wobei die Abweichung zweckgebunden ist: Feldeinschränkung, Aggregation oder Anreicherung über einen zusätzlichen Join

Wie tief Sichten aufeinander aufbauen, begrenzt nichts Technisches. Die Qualitätskontrolle ist das Urteil des Stewards in der HITL-Prüfung.

Jede Sicht trägt einen deklarierten Geschäftszweck, der zum Erstellungszeitpunkt angegeben wird:

- Teil des governten Artefakts, sodass Stewards genehmigen im Wissen, wofür die Sicht bestimmt ist
- Wird bei Zugriffsanfragen unter Prinzip 7 referenziert, damit der Steward die Eignung beurteilen kann
- Begleitet die Sicht von ihrer Erstellung durch den gesamten Governance-Workflow

### Abfragen {: #queries }

Eine Abfrage traversiert genehmigte Beziehungspfade über Domänen-Assets. Anders als eine Sicht schafft sie keine neue semantische Bedeutung; sie folgt der genehmigten Struktur des Modells. Eine Abfrage kann in jeder unterstützten Abfragesprache geschrieben werden (SQL, GraphQL, Cypher).

**Strukturelle Durchsetzung:** Der Beziehungskatalog setzt durch. Der Compiler prüft jede Traversierung gegen die genehmigten Katalogeinträge und lehnt eine Abfrage ab, die einen nicht genehmigten Pfad nennt. Die Governance steckt in der Struktur, eine eigene Prüfung zur Laufzeit gibt es nicht.

**Keine Genehmigung nötig:** Die Governance greift vorher, auf den Ebenen Beziehung und Spaltensichtbarkeit. Darf ein Benutzer die Spalten sehen und ist der Traversierungspfad genehmigt, ist die Abfrage zulässig, ohne weitere Schranke.

**Unterschied zu Sichten:**

- Sichten: intradomän, führen neue semantische Bedeutung ein, von Stewards kuratiert
- Abfragen: durchlaufen genehmigte Beziehungen, keine neue Semantik, kein Genehmigungs-Gate

**Domänenausdruck nach Abfragesprache:**

Jede unterstützte Sprache stellt die Domäne als strukturellen Namensraum dar, der dieser Sprache nativ ist:

| Sprache | Domänenausdruck | Beispiel |
| --- | --- | --- |
| GraphQL | Typ- und Feldnamenpräfix | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Schemaname | `SELECT * FROM sales.orders` |
| Cypher | Zusätzliches Knoten-Label (Domäne nur erforderlich, wenn der Typname mehrdeutig ist) | `MATCH (o:Sales:Order)` |

Der Compiler ermittelt die Domänenzugehörigkeit aus diesen strukturellen Positionen; eine Annotation oder ein Hinweis ist nicht nötig.

### Beziehungen {: #relationships }

Eine Beziehung ist ein genehmigter Traversierungspfad zwischen zwei Assets. Domänengrenzen ändern nichts daran, was eine Beziehung ist; sie bestimmen nur, wer sie genehmigt.

**Genehmigung:**

- Über einen Beziehungsantrag entscheiden die Domänen, die er berührt: die Domäne seiner Quelltabelle und die Domäne seiner Zieltabelle
- Eine Genehmigung zählt nur von einem Benutzer, dessen Recht, Beziehungen anzulegen, eine dieser Domänen erreicht; ablehnen kann jeder solche Benutzer
- Der Antrag wird ausgeführt, sobald zwei verschiedene Benutzer, keiner davon der Antragsteller, zugestimmt haben und jede beteiligte Domäne einen Genehmiger hat: Eine Beziehung über zwei Domänen braucht ein Ja von jeder Seite, eine Beziehung innerhalb einer Domäne zwei Genehmiger dieser Domäne
- Die Genehmigung einer Beziehung baut den Abhängigkeitsgraphen jedes Stewards auf und ermöglicht proaktive Benachrichtigungen über Schemaentwicklung

Beziehungen entstehen, wenn jemand sie braucht, nie auf Vorrat. Das erste Team mit dem fachlichen Bedarf erledigt die Arbeit, spätere Teams erben sie.

**Folge für die Optimierung:** Eine Beziehungsdeklaration ist ein Governance-Artefakt und zugleich eine strukturelle Beschreibung einer Join-Form. Die zwei Tabellen, zwei Spalten und der Join-Typ, die eine Beziehung definieren, sind genau das, was der Abfrageoptimierer braucht, um diesen Join vorzumaterialisieren. Quellenübergreifende Beziehungen bekommen automatisch vormaterialisierte Join-Tabellen, Beziehungen innerhalb einer Quelle können das mit `materialize: true` wählen. Stewards, die gültige Beziehungen durchdenken und genehmigen, bekommen schnellere Abfragen als direkte Folge: Governance-Arbeit und Optimierungsarbeit sind derselbe Akt.

### Feldzugriffsberechtigungen {: #field-access-grants }

Eine Feldzugriffsfreigabe ist eine Erlaubnis von einer Domäne an eine andere: Domäne A darf bestimmte Felder aus Domäne B in ihren Sichten verwenden.

**Lebenszyklus der Berechtigung:**

- Wird durch die Sichterstellung ausgelöst, wenn fremde Felder als benötigt identifiziert werden
- Wird einmalig vom Steward der Zieldomäne genehmigt
- Gehört zur anfragenden Domäne, nicht zur Sicht, die sie ausgelöst hat
- Jede nachfolgende Sicht in der anfragenden Domäne darf die gewährten Felder ohne weitere domänenübergreifende Beteiligung verwenden
- Zusätzliche nicht gewährte Felder erfordern eine neue Anfrage

**Benachrichtigung nach der Nutzung:** Wird eine Sicht mit freigegebenen Feldern erstellt, erfährt der Steward der Quelle davon, ohne dass er genehmigen muss. Die Benachrichtigung nennt den Namen der Sicht, ihren erklärten Geschäftszweck, die verwendeten Felder und den Steward, der sie genehmigt hat. Das gibt dem Steward der Quelle:

- **Sichtbarkeit** — Kenntnis darüber, wie seine Daten genutzt werden
- **Aufsicht** — Grundlage, um Bedenken zu äußern, falls die Nutzung unangemessen erscheint
- **Rückgriff** — Möglichkeit, die Berechtigung zu widerrufen, wodurch abhängige Sichten ungültig werden

Der Kompromiss: Die Quelldomäne genehmigt den Feldzugriff, ohne jede zukünftige Nutzung zu kennen. Eine Genehmigung pro Sicht ist theoretisch korrekt und praktisch nicht umsetzbar.

### Workflow zur Abfrageerstellung {: #query-creation-workflow }

Drei Phasen, in dieser Reihenfolge.

**Phase 1 — Shaping (SQL-Discovery, von der Beziehungen-Seite aus):**

- Der Analyst öffnet das Shaping-Werkzeug von der Beziehungen-Seite aus, um potenzielle Join-Pfade in rohem SQL zu erkunden
- SQL wird gegen zugängliche Daten ausgeführt, vorbehaltlich bestehender RLS und Spaltenmaskierung
- JOINs im SQL werden geparst und als Kandidaten-Beziehungsvorschläge angezeigt
- Maschinell vorgeschlagene Kandidaten (FK-Inferenz, semantische Inferenz) werden zusammen mit der SQL-Exploration des Analysten in derselben Ansicht angezeigt
- Der Analyst wählt Kandidaten aus, um sie zu einer formalen Beziehungsanfrage zu befördern

**Phase 2 — Beziehungsgenehmigung** (folgenreich — strukturell und dauerhaft):

- Entschieden von den Domänen, die die Beziehung berührt, wie unter [Beziehungen](#relationships) beschrieben
- Ist das ein legitimer Traversierungspfad? Ist der Join semantisch gültig?
- Zwei verschiedene Benutzer, keiner davon der Antragsteller, müssen genehmigen, und jede beteiligte Domäne braucht einen Genehmiger; danach wird die Beziehung ein dauerhafter Katalogeintrag

**Phase 3 — Abfrageerstellung:**

- Der Analyst erstellt die Abfrage in jeder unterstützten Sprache (SQL, GraphQL, Cypher) und durchläuft dabei genehmigte Beziehungspfade
- Nur genehmigte Katalogbeziehungen sind durchlaufbar, und der Compiler setzt dies strukturell durch
- Keine Genehmigung erforderlich: Spaltensichtbarkeit und Beziehungsgenehmigung sind die einzigen Gates

### HITL als primäre Kontrolle {: #hitl-as-the-primary-control }

Technische Regeln übernehmen, was objektiv ist: das Nachverfolgen der Feldherkunft, das Durchsetzen von Domänengrenzen und die Prüfung durch den Compiler. Urteile, die vom Kontext abhängen, bleiben beim Steward. Grenzen wie die Kompositionstiefe von Sichten, ein erklärter Zweck je Abfrage und Entscheidungen über Beziehungsgenehmigungen sind HITL-Themen, die der Compiler nicht durchsetzt.

**Neutralität der Quelldomäne:** Der Steward der Quelldomäne genehmigt die Beziehung einmalig und die Feldberechtigung einmalig. Danach operieren nachgelagerte Domänen innerhalb dieser gewährten Grenzen:

- **Hohe Sorgfalt** bei der Entscheidung über das Überschreiten der Grenze
- **Leichtgewichtige Wahrnehmung** danach über Benachrichtigungen und Abfrageverlauf

---

## 2. Auffindbarkeit {: #2-discoverability }

### Discovery-Ebenen {: #discovery-tiers }

Discovery ist in fünf Ebenen mit zunehmender Governance strukturiert. Jede Ebene ist Voraussetzung für die nächste.

| Ebene | Beschreibung | Governance-Zustand |
| --- | --- | --- |
| 1 — Registriertes Quellschema | Jede Tabelle, Spalte und jeder Typ aus einer registrierten Quelle. Sichtbarkeit auf Admin-Ebene. | Keine — rohes Inventar |
| 2 — Unbeanspruchte Tabellen | Tabellen, die aus registrierten Quellen introspiziert wurden und keinen Domäneneigentümer haben. Sichtbar für Stewards mit Quellzugriff. | Verfügbar, aber ungovernt |
| 3 — Domänen-Assets | Beanspruchte Tabellen und von Stewards definierte Sichten. Vollständig governt, im Besitz, katalogsichtbar. | Vollständig governt |
| 4 — Beziehungen | Genehmigte Traversierungspfade zwischen Assets der Ebene 3. Voraussetzung für die Erstellung domänenübergreifender Sichten. | Von beiden Stewards genehmigt |
| 5 — Feldberechtigungen | Domäne-zu-Domäne-Feldzugriffsberechtigungen. Der spezifischste und bewussteste governte Zugriff. | Vom Quell-Steward genehmigt |

Eine nicht beanspruchte Tabelle zeigt eine Lücke an. Gibt es die benötigten Daten nur auf Stufe 2, muss ein Steward sie beanspruchen, bevor die Governance weitergehen kann. Hat keine Stufe einen Kandidaten, wenden Sie sich an einen Admin.

### FK-Constraints

FK-Constraints gehören zu einer einzelnen Quelle und können daher keine Quellen überspannen. Quellenübergreifende Join-Pfade stammen ganz aus genehmigten Katalogbeziehungen (Stufe 4), die stärkere Belege sind, weil beide Stewards sie geprüft haben.

Innerhalb einer Quelle:

- FK-Constraints werden bei der Quellregistrierung automatisch als Kandidatenbeziehungen angezeigt
- Sie repräsentieren explizite Modellierungsabsicht: in den meisten analytischen SQL-Systemen nicht erzwungen, aber bewusst deklariert
- Eine Steward-Validierung ist weiterhin erforderlich, bevor ein Kandidat zu einer genehmigten Beziehung wird

### Vertrauenshierarchie für Beziehungen {: #relationship-confidence-hierarchy }

| Beleg | Vertrauen |
| --- | --- |
| Genehmigte Katalogbeziehung — quellübergreifend, von beiden Stewards validiert | Am höchsten |
| Quelleninterner FK-Constraint — explizite Modellierungsabsicht, nicht erzwungen, aber bewusst | Hoch |
| Quelleninterne semantische Inferenz — Ähnlichkeit von Spaltenname/-typ innerhalb eines konsistenten Schemas | Mittel |
| Quellübergreifende semantische Inferenz — Namenskonventionen weichen zwischen Systemen ab; hohes Risiko falscher Positive | Niedrig |

Vorschläge, die durch mehrere Belegtypen bestätigt werden, akkumulieren Vertrauen.

### Daten-Probing und Korrelation {: #data-probing-and-correlation }

Für semantisch abgeleitete Kandidaten bietet Daten-Probing einen Validierungsschritt:

- **Wertüberlappung** — Anteil der Quellspaltenwerte, die in der Zielspalte vorkommen
- **Kardinalität** — ob die Verteilung dem erwarteten Beziehungstyp entspricht
- **Null-Rate** — Anteil der Quellspalte, der null ist, was auf Optionalität hinweist

Hohe Korrelation erhöht das Vertrauen, niedrige unterdrückt den Kandidaten oder stuft ihn herab. Probing bestätigt, es beweist nicht. Ganzzahlbereiche können sich zufällig überschneiden, und teilweise referentielle Integrität ist in analytischen Systemen häufig, es bleibt also viel Raum für Fehler. Das semantische Urteil des Stewards ist die einzige verlässliche letzte Prüfung.

### LLM-unterstützte Discovery {: #llm-assisted-discovery }

Das LLM operiert gleichzeitig über alle fünf Ebenen und schlägt Beziehungen, Kandidaten-Beanspruchungen und Traversierungspfade vor, geordnet nach Vertrauen.

**Was das LLM aufzeigt:**

- Kandidatenbeziehungen, geordnet nach Vertrauen
- Unbeanspruchte Tabellen, die einen Datenbedarf erfüllen könnten, mit einer Aufforderung zur Einleitung der Beanspruchung
- Gibt es gar keinen Kandidaten, ist das das Signal zur Eskalation an den Admin

**Sicht-Design aus Geschäftsbeschreibung:**

Der Analyst liefert eine natürlichsprachliche Beschreibung und optionale Einschränkungen. Das LLM erzeugt eine vorgeschlagene Sichtstruktur.

*Eingabe:*

- Geschäftsbeschreibung: Entitäten, Kennzahlen, Beziehungen, Absicht
- Optionale Einschränkungen: Filter, Zeitfenster, Aggregationen, ausgeschlossene Felder, Sensibilitätsbeschränkungen

*Beispiel:*
> "Tägliche Handelsvolumen nach Gegenpartei für die letzten 30 Tage, nur aktive Gegenparteien, mit Anzeige des rechtlichen Namens und der Bonität der Gegenpartei. Keine PII."

*LLM-Prozess:*

1. Parsen — Entitäten, Kennzahlen, Dimensionen, Filter, Ausschlüsse identifizieren
2. Suchen — alle Katalogebenen nach passenden Assets durchsuchen
3. Vorschlagen — Domänen-Assets, Beziehungen, Felder, Aggregationsstruktur
4. Bewerten — Vertrauen pro Komponente basierend auf Ebenen-Belegen
5. Voraussetzungen — geordnete Liste von Beanspruchungen, Beziehungen und Feldberechtigungen, die erforderlich sind
6. Lücken — Entitäten oder Felder ohne Kandidaten auf irgendeiner Ebene, markiert zur Eskalation an den Admin

*Ausgabe:*

- Entwurfsabfrage zur Prüfung und Verfeinerung durch den Analysten
- Vertrauenswerte pro Komponente
- Geordnete Voraussetzungsliste
- Lückenliste

Die Geschäftsbeschreibung wird zum deklarierten Geschäftszweck der Sicht, sobald die Sicht formal erstellt wird.

**SQL-first Beziehungs-Discovery (Modeling-Werkzeug):**

Es öffnet sich als Dialog auf der Seite Beziehungen. Sein Zweck ist der Aufbau des semantischen Modells: strukturelle Join-Pfade finden, bevor sie als gesteuerte Beziehungen formalisiert werden.

1. Der Analyst schreibt freies SQL gegen zugängliche Tabellen (RLS und Maskierung gelten weiterhin)
2. Der SQL-AST wird geparst, und jede JOIN-Bedingung wird zu einem Kandidaten-Beziehungsvorschlag
3. Die Kandidatenliste wird zusammen mit maschinell vorgeschlagenen Kandidaten (FK-Inferenz, semantische Inferenz) für eine einheitliche Prüfung angezeigt
4. Der Analyst befördert ausgewählte Kandidaten zu formalen Beziehungsanfragen
5. Genehmigte Beziehungen werden dem Katalog hinzugefügt und in Abfragen durchlaufbar

Für die strukturelle Erkundung darf das Modellierungswerkzeug jede registrierte Tabelle zeigen, auch solche, deren Daten der Analyst nicht sehen kann. Die Sichtbarkeit des Schemas ist eine eigene Frage; den Zugriff auf die Daten selbst regelt die Genehmigung des Stewards.

---

## 3. Nutzung {: #3-usage }

### Abfrage-Audit-Trail {: #query-audit-trail }

Jede Abfrage, die ein Domänen-Asset berührt, wird in einem nur anfügbaren `query_audit_log` erfasst. Jeder Eintrag erfasst:

- `tenant_id`, `user_id`, `role_id` — den Identitätskontext
- Einen SHA-256-Hash der Abfrage; der wörtliche Abfragetext wird nie gespeichert
- `table_ids` — die von der Abfrage berührten Domänen-Assets
- `source`, `status_code`, `duration_ms`
- `logged_at` — den Zeitstempel

Das Log ist nur anfügbar (DELETE und UPDATE werden auf Datenbankebene blockiert) und indiziert nach `(tenant_id, logged_at)` und `(user_id, logged_at)`.

Der Abfrageverlaufsbericht des Stewards ist eine aggregierte Sicht über dieses Protokoll, filterbar nach Asset, Rolle und Zeitfenster. Damit wird der Katalog zu einem lebendigen Governance-Instrument: Stewards sehen, wie ihre Assets genutzt werden, während es geschieht, und erfahren es nicht erst hinterher.

**Zwei Sichtbarkeitsmechanismen:**

- **Push** — Benachrichtigungen nach Nutzung für strukturelle Akte (eine neue Sicht wurde unter Verwendung Ihrer Felder erstellt)
- **Pull** — Abfrageverlauf für Laufzeit-Nutzungsmuster
