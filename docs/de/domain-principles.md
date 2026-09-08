# Domänenmodell-Prinzipien

---

## 1. Governance

### Kernprinzipien

1. **Jede Ressource muss einer Domäne gehören.** Tabellen, Sichten und Beziehungen sind alle Domänen-Assets. Es gibt keine ungovernten frei schwebenden Ressourcen. Die Domäne ist die Einheit der Rechenschaftspflicht.
2. **Jede Domäne muss einen Steward haben.** Eine Domäne kann sich in einem ausstehenden Zustand befinden, bis ein Steward zugewiesen wird, aber sie kann ohne einen solchen keine governten Daten bereitstellen.
3. **Der Admin besitzt die Quellen.** Quellen sind Infrastruktur, keine Domänen-Ressourcen. Der Admin registriert und verwaltet Verbindungen zu externen Datensystemen.
4. **Stewards können Tabellen für eine Domäne beanspruchen.** Das Beanspruchen ist exklusiv — eine Tabelle gehört genau einer Domäne. Dies ist der governte Akt, der Infrastruktur und semantische Schicht verbindet.
5. **Stewards können intradomänen Sichten aus Domänen-Assets erstellen.** Sichten drücken Geschäftslogik aus — Joins, Aggregationen, abgeleitete Kennzahlen — über Assets, die der Steward innerhalb derselben Domäne besitzt. Sichten erzeugen neue semantische Bedeutung und erfordern die Genehmigung des Stewards.
6. **Analysten können domänenübergreifende Abfragen aus genehmigten Beziehungen erstellen.** Abfragen sind interdomäne Sichten, ausgedrückt in jeder unterstützten Abfragesprache. Sie erzeugen keine neue Semantik — sie durchlaufen genehmigte Beziehungspfade. Es ist keine zusätzliche Genehmigung erforderlich: Governance wird vorgelagert auf der Ebene der Beziehung und der Spaltensichtbarkeit gehandhabt. Der Katalog ist der Durchsetzungsmechanismus: Der Compiler weist Traversierungen zurück, die nicht im genehmigten Beziehungskatalog stehen.
7. **Jeder kann Zugriff auf eine Domänen-Ressource anfordern.** Der Zugriff wird auf Ressourcenebene gewährt, nicht auf Abfrageebene. Wenn Sie Zugriff auf eine Ressource haben, können Sie sie abfragen. Governance wird zur Ausführungszeit über die Pipeline durchgesetzt.

### Ressourcen: Tabellen und Sichten als Gleichrangige

Der Unterschied zwischen einer Tabelle und einer Sicht ist nur die Herkunft — eine Tabelle wird aus einer Quelle beansprucht, eine Sicht wird von einem Steward definiert. Sobald eines von beiden als Domänen-Asset existiert, behandelt das Governance-Modell sie identisch:

- Beide sind erstklassige Domänen-Assets, die im Katalog sichtbar sind
- Beide können Ziel einer Beziehung sein
- Beide können unter Prinzip 6 gewährt werden
- Beide unterliegen derselben Governance-Pipeline

Ein Steward kann Tabellen privat beanspruchen und nur kuratierte Sichten als öffentlich zugängliche Datenprodukte bereitstellen.

### Sicht-Komposition

Eine Sicht gehört immer zu einer einzigen Domäne — es gibt nur einen Sichttyp, immer intradomän. Eine Sicht existiert für einen von zwei Zwecken:

- **Domänenübergreifender Import** — die Quelle liegt außerhalb der Domäne. Domänenübergreifende Daten dürfen nur über eine Sicht in eine Domäne gelangen, die als schreibgeschützter Adapter fungiert, der die externen Daten als Domänen-Geschäftskonzept benennt.
- **Lokale Ableitung** — die Quelle liegt in derselben Domäne. Die Sicht leitet neue oder berechnete Daten aus vorhandenen Domänen-Assets ab. Neue oder abgeleitete Daten dürfen nur als Sicht existieren.

Eine Sicht kann referenzieren:

- Beanspruchte Tabellen innerhalb derselben Domäne
- Felder, die aus einer anderen Domäne unter einer Feldzugriffsberechtigung importiert wurden
- Eine andere Sicht innerhalb derselben Domäne, wobei die Abweichung zweckgebunden ist: Feldeinschränkung, Aggregation oder Anreicherung über einen zusätzlichen Join

Die Kompositionstiefe wird technisch nicht erzwungen — die Beurteilung des Stewards während der HITL-Prüfung ist der Qualitätskontrollmechanismus.

Jede Sicht trägt einen deklarierten Geschäftszweck, der zum Erstellungszeitpunkt angegeben wird:

- Teil des governten Artefakts — Stewards genehmigen im Wissen, wofür die Sicht bestimmt ist
- Wird bei Zugriffsanfragen unter Prinzip 7 referenziert, damit der Steward die Eignung beurteilen kann
- Begleitet die Sicht von ihrer Erstellung durch den gesamten Governance-Workflow

### Abfragen

Eine Abfrage durchläuft genehmigte Beziehungspfade über Domänen-Assets. Anders als Sichten erzeugen Abfragen keine neue semantische Bedeutung — sie durchlaufen die genehmigte Struktur des Modells. Abfragen können in jeder unterstützten Abfragesprache ausgedrückt werden (SQL, GraphQL, Cypher).

**Strukturelle Durchsetzung:** Der Beziehungskatalog ist der Durchsetzungsmechanismus. Der Compiler validiert jede Traversierung gegen genehmigte Katalogeinträge und weist Abfragen zurück, die nicht genehmigte Pfade referenzieren. Governance ist strukturell, keine Laufzeitprüfung.

**Keine Genehmigung erforderlich:** Governance geschieht vorgelagert — auf der Ebene der Beziehung und der Spaltensichtbarkeit. Wenn ein Benutzer Zugriff auf die Spalten hat und der Traversierungspfad genehmigt ist, ist die Abfrage eine gültige Nutzung. Kein zusätzliches Gate.

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

Der Compiler löst die Domänenzugehörigkeit aus diesen strukturellen Positionen auf — keine Annotation oder Hinweis ist erforderlich.

### Beziehungen

Eine Beziehung ist ein genehmigter Traversierungspfad zwischen zwei Assets. Domänengrenzen sind irrelevant dafür, was eine Beziehung ist — sie bestimmen nur, wer sie genehmigt.

**Genehmigung:**

- Die Genehmigung ist von jedem einzelnen Steward erforderlich, der ein an der Beziehung beteiligtes Asset besitzt
- Wenn ein Steward beide Assets besitzt, ist eine Genehmigung erforderlich. Sind zwei Stewards beteiligt, sind zwei Genehmigungen erforderlich
- Es gibt keine Klassifizierung in intradomän/domänenübergreifend — die Eigentümerschaft bestimmt den Genehmigungsaufwand auf natürliche Weise
- Die Genehmigung einer Beziehung baut den Abhängigkeitsgraphen jedes Stewards auf und ermöglicht proaktive Benachrichtigungen über Schemaentwicklung

Beziehungen werden bedarfsgesteuert erstellt, nicht spekulativ. Das erste Team mit dem Geschäftsbedarf leistet die Arbeit; nachfolgende Teams erben die Infrastruktur.

**Optimierungsfolge:** Eine Beziehungsdeklaration ist nicht nur ein Governance-Artefakt — sie ist auch eine strukturelle Beschreibung einer Join-Form. Die beiden Tabellen, die beiden Spalten und der Join-Typ, die eine Beziehung definieren, sind genau das, was der Abfrageoptimierer benötigt, um diesen Join vorab zu materialisieren. Quellenübergreifende Beziehungen erzeugen automatisch vormaterialisierte Join-Tabellen; Beziehungen innerhalb derselben Quelle können sich über `materialize: true` dafür entscheiden. Stewards, die gültige Beziehungen durchdenken und genehmigen, erhalten Abfragebeschleunigung als direktes Nebenprodukt — Governance-Arbeit und Optimierungsarbeit sind derselbe Akt.

### Feldzugriffsberechtigungen

Eine Feldzugriffsberechtigung ist eine Domäne-zu-Domäne-Berechtigung — Domäne A darf bestimmte Felder von Domäne B in ihren Sichten verwenden.

**Lebenszyklus der Berechtigung:**

- Wird durch die Sichterstellung ausgelöst, wenn fremde Felder als benötigt identifiziert werden
- Wird einmalig vom Steward der Zieldomäne genehmigt
- Gehört zur anfragenden Domäne, nicht zur Sicht, die sie ausgelöst hat
- Jede nachfolgende Sicht in der anfragenden Domäne darf die gewährten Felder ohne weitere domänenübergreifende Beteiligung verwenden
- Zusätzliche nicht gewährte Felder erfordern eine neue Anfrage

**Benachrichtigung nach Nutzung:** Wenn eine Sicht unter Verwendung gewährter Felder erstellt wird, wird der Quell-Steward benachrichtigt — nicht um Genehmigung gebeten. Die Benachrichtigung enthält den Namen der Sicht, den deklarierten Geschäftszweck, die konkret verwendeten Felder und welcher Steward sie genehmigt hat. Dies gibt dem Quell-Steward:

- **Sichtbarkeit** — Kenntnis darüber, wie seine Daten genutzt werden
- **Aufsicht** — Grundlage, um Bedenken zu äußern, falls die Nutzung unangemessen erscheint
- **Rückgriff** — Möglichkeit, die Berechtigung zu widerrufen, wodurch abhängige Sichten ungültig werden

Der Kompromiss: Die Quelldomäne genehmigt den Feldzugriff, ohne jede zukünftige Nutzung zu kennen. Eine Genehmigung pro Sicht ist theoretisch korrekt und praktisch nicht umsetzbar.

### Workflow zur Abfrageerstellung

Drei Phasen, in dieser Reihenfolge.

**Phase 1 — Shaping (SQL-Discovery, von der Beziehungen-Seite aus):**

- Der Analyst öffnet das Shaping-Werkzeug von der Beziehungen-Seite aus, um potenzielle Join-Pfade in rohem SQL zu erkunden
- SQL wird gegen zugängliche Daten ausgeführt, vorbehaltlich bestehender RLS und Spaltenmaskierung
- JOINs im SQL werden geparst und als Kandidaten-Beziehungsvorschläge angezeigt
- Maschinell vorgeschlagene Kandidaten (FK-Inferenz, semantische Inferenz) werden zusammen mit der SQL-Exploration des Analysten in derselben Ansicht angezeigt
- Der Analyst wählt Kandidaten aus, um sie zu einer formalen Beziehungsanfrage zu befördern

**Phase 2 — Beziehungsgenehmigung** (folgenreich — strukturell und dauerhaft):

- Wird an jeden einzelnen Steward gerichtet, der ein an der Beziehung beteiligtes Asset besitzt
- Ist dies ein legitimer Traversierungspfad? Ist der Join semantisch gültig?
- Alle beteiligten Stewards müssen genehmigen; die Beziehung wird zu einem dauerhaften Katalogeintrag

**Phase 3 — Abfrageerstellung:**

- Der Analyst erstellt die Abfrage in jeder unterstützten Sprache (SQL, GraphQL, Cypher) und durchläuft dabei genehmigte Beziehungspfade
- Nur genehmigte Katalogbeziehungen sind durchlaufbar — der Compiler setzt dies strukturell durch
- Keine Genehmigung erforderlich — Spaltensichtbarkeit und Beziehungsgenehmigung sind die einzigen Gates

### HITL als primäre Kontrolle

Technische Regeln behandeln das Objektive — Feldherkunftsverfolgung, Durchsetzung von Domänengrenzen, Compiler-Validierung. Die kontextuelle Beurteilung bleibt beim Steward. Einschränkungen wie die Kompositionstiefe von Sichten, Anforderungen an den Zweck pro Abfrage und Entscheidungen zur Beziehungsgenehmigung sind HITL-Angelegenheiten, keine vom Compiler durchgesetzten Regeln.

**Neutralität der Quelldomäne:** Der Steward der Quelldomäne genehmigt die Beziehung einmalig und die Feldberechtigung einmalig. Danach operieren nachgelagerte Domänen innerhalb dieser gewährten Grenzen:

- **Hohe Sorgfalt** bei der Entscheidung über das Überschreiten der Grenze
- **Leichtgewichtige Wahrnehmung** danach über Benachrichtigungen und Abfrageverlauf

---

## 2. Auffindbarkeit

### Discovery-Ebenen

Discovery ist in fünf Ebenen mit zunehmender Governance strukturiert. Jede Ebene ist Voraussetzung für die nächste.

| Ebene | Beschreibung | Governance-Zustand |
| --- | --- | --- |
| 1 — Registriertes Quellschema | Jede Tabelle, Spalte und jeder Typ aus einer registrierten Quelle. Sichtbarkeit auf Admin-Ebene. | Keine — rohes Inventar |
| 2 — Unbeanspruchte Tabellen | Tabellen, die aus registrierten Quellen introspiziert wurden und keinen Domäneneigentümer haben. Sichtbar für Stewards mit Quellzugriff. | Verfügbar, aber ungovernt |
| 3 — Domänen-Assets | Beanspruchte Tabellen und von Stewards definierte Sichten. Vollständig governt, im Besitz, katalogsichtbar. | Vollständig governt |
| 4 — Beziehungen | Genehmigte Traversierungspfade zwischen Assets der Ebene 3. Voraussetzung für die Erstellung domänenübergreifender Sichten. | Von beiden Stewards genehmigt |
| 5 — Feldberechtigungen | Domäne-zu-Domäne-Feldzugriffsberechtigungen. Der spezifischste und bewussteste governte Zugriff. | Vom Quell-Steward genehmigt |

Eine unbeanspruchte Tabelle ist ein Lückensignal — wenn benötigte Daten nur auf Ebene 2 existieren, muss ein Steward sie beanspruchen, bevor Governance fortschreiten kann. Das Fehlen jeglichen Kandidaten über alle Ebenen hinweg erfordert eine Eskalation an den Admin.

### FK-Constraints

FK-Constraints sind ein Konstrukt auf Quellebene — sie können sich nicht über Datenquellen hinweg erstrecken. Quellübergreifende Join-Pfade werden ausschließlich aus genehmigten Katalogbeziehungen (Ebene 4) abgeleitet, die stärker sind, da sie von beiden Stewards validiert wurden.

Innerhalb einer Quelle:

- FK-Constraints werden bei der Quellregistrierung automatisch als Kandidatenbeziehungen angezeigt
- Sie repräsentieren explizite Modellierungsabsicht — in den meisten analytischen SQL-Systemen nicht erzwungen, aber bewusst deklariert
- Eine Steward-Validierung ist weiterhin erforderlich, bevor ein Kandidat zu einer genehmigten Beziehung wird

### Vertrauenshierarchie für Beziehungen

| Beleg | Vertrauen |
| --- | --- |
| Genehmigte Katalogbeziehung — quellübergreifend, von beiden Stewards validiert | Am höchsten |
| Quelleninterner FK-Constraint — explizite Modellierungsabsicht, nicht erzwungen, aber bewusst | Hoch |
| Quelleninterne semantische Inferenz — Ähnlichkeit von Spaltenname/-typ innerhalb eines konsistenten Schemas | Mittel |
| Quellübergreifende semantische Inferenz — Namenskonventionen weichen zwischen Systemen ab; hohes Risiko falscher Positive | Niedrig |

Vorschläge, die durch mehrere Belegtypen bestätigt werden, akkumulieren Vertrauen.

### Daten-Probing und Korrelation

Für semantisch abgeleitete Kandidaten bietet Daten-Probing einen Validierungsschritt:

- **Wertüberlappung** — Anteil der Quellspaltenwerte, die in der Zielspalte vorkommen
- **Kardinalität** — ob die Verteilung dem erwarteten Beziehungstyp entspricht
- **Null-Rate** — Anteil der Quellspalte, der null ist, was auf Optionalität hinweist

Hohe Korrelation erhöht das Vertrauen; niedrige Korrelation unterdrückt oder degradiert den Kandidaten. Probing ist bestätigendes Indiz, kein Beweis — Ganzzahlbereiche können zufällig überlappen, und partielle referenzielle Integrität ist in analytischen Systemen üblich. Ein erheblicher Fehlerspielraum bleibt bestehen. Die semantische Beurteilung des Stewards ist die einzige verlässliche Endkontrolle.

### LLM-unterstützte Discovery

Das LLM operiert gleichzeitig über alle fünf Ebenen und schlägt Beziehungen, Kandidaten-Beanspruchungen und Traversierungspfade vor, geordnet nach Vertrauen.

**Was das LLM aufzeigt:**

- Kandidatenbeziehungen, geordnet nach Vertrauen
- Unbeanspruchte Tabellen, die einen Datenbedarf erfüllen könnten, mit einer Aufforderung zur Einleitung der Beanspruchung
- Fehlen jeglichen Kandidaten — Signal zur Eskalation an den Admin

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

Zugänglich als Modal von der Beziehungen-Seite aus. Die Absicht ist der Aufbau des semantischen Modells — die Identifizierung struktureller Join-Pfade, bevor sie als governte Beziehungen formalisiert werden.

1. Der Analyst schreibt freies SQL gegen zugängliche Tabellen (RLS und Maskierung gelten weiterhin)
2. Der SQL-AST wird geparst — jede JOIN-Bedingung wird zu einem Kandidaten-Beziehungsvorschlag
3. Die Kandidatenliste wird zusammen mit maschinell vorgeschlagenen Kandidaten (FK-Inferenz, semantische Inferenz) für eine einheitliche Prüfung angezeigt
4. Der Analyst befördert ausgewählte Kandidaten zu formalen Beziehungsanfragen
5. Genehmigte Beziehungen werden dem Katalog hinzugefügt und in Abfragen durchlaufbar

Das Modeling-Werkzeug kann alle registrierten Tabellen zur strukturellen Exploration anzeigen, selbst dort, wo der Analyst die zugrunde liegenden Daten nicht sehen kann — die Steward-Genehmigung regelt den tatsächlichen Datenzugriff, nicht die Schemasichtbarkeit.

---

## 3. Nutzung

### Abfrage-Audit-Trail

Jede Abfrage, die ein Domänen-Asset berührt, wird in einem nur anfügbaren `query_audit_log` erfasst. Jeder Eintrag erfasst:

- `tenant_id`, `user_id`, `role_id` — den Identitätskontext
- Einen SHA-256-Hash der Abfrage — der wörtliche Abfragetext wird nie gespeichert
- `table_ids` — die von der Abfrage berührten Domänen-Assets
- `source`, `status_code`, `duration_ms`
- `logged_at` — den Zeitstempel

Das Log ist nur anfügbar (DELETE und UPDATE werden auf Datenbankebene blockiert) und indiziert nach `(tenant_id, logged_at)` und `(user_id, logged_at)`.

Der Abfrageverlaufsbericht des Stewards ist eine aggregierte Sicht über dieses Log, filterbar nach Asset, Rolle und Zeitfenster. Der Katalog ist ein lebendiges Governance-Instrument — Stewards behalten in Echtzeit den Überblick darüber, wie ihre Assets genutzt werden, nicht erst im Nachhinein.

**Zwei Sichtbarkeitsmechanismen:**

- **Push** — Benachrichtigungen nach Nutzung für strukturelle Akte (eine neue Sicht wurde unter Verwendung Ihrer Felder erstellt)
- **Pull** — Abfrageverlauf für Laufzeit-Nutzungsmuster


