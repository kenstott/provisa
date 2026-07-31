# Prinzipien des Domänenmodells

---

## 1. Governance

### Kernprinzipien

1. **Jede Ressource muss einer Domäne gehören.** Tabellen, Sichten und Beziehungen sind allesamt Domänen-Assets. Es gibt keine ungovernte, frei schwebende Ressource. Die Domäne ist die Einheit der Verantwortlichkeit.
2. **Jede Domäne muss einen Data Steward haben.** Eine Domäne kann sich in einem ausstehenden Zustand befinden, bis ein Data Steward zugewiesen ist, aber sie kann ohne ihn keine governten Daten bereitstellen.
3. **Der Administrator besitzt die Quellen.** Quellen sind Infrastruktur, keine Domänen-Ressourcen. Der Administrator registriert und verwaltet Verbindungen zu externen Datensystemen.
4. **Data Stewards können Tabellen für eine Domäne beanspruchen.** Die Beanspruchung ist exklusiv — eine Tabelle gehört genau einer Domäne. Dies ist der governte Akt, der Infrastruktur und semantische Schicht verbindet.
5. **Data Stewards können domäneninterne Sichten aus Domänen-Assets erstellen.** Sichten drücken Geschäftslogik aus — Joins, Aggregationen, abgeleitete Metriken — über Assets, die der Data Steward innerhalb derselben Domäne besitzt. Sichten erzeugen neue semantische Bedeutung und erfordern die Genehmigung des Data Stewards.
6. **Analysten können domänenübergreifende Abfragen aus genehmigten Beziehungen erstellen.** Abfragen sind domänenübergreifende Sichten, ausgedrückt in jeder unterstützten Abfragesprache. Sie erzeugen keine neue Semantik — sie durchlaufen genehmigte Beziehungspfade. Es ist keine zusätzliche Genehmigung erforderlich: Governance wird vorgelagert auf den Ebenen Beziehung und Spaltensichtbarkeit gehandhabt. Der Katalog ist der Durchsetzungsmechanismus: Der Compiler lehnt Durchläufe ab, die nicht im genehmigten Beziehungskatalog enthalten sind.
7. **Jeder kann Zugriff auf eine Domänen-Ressource anfordern.** Zugriff wird auf Ressourcenebene gewährt, nicht auf Abfrageebene. Wenn Sie Zugriff auf eine Ressource haben, können Sie sie abfragen. Governance wird zur Laufzeit über die Pipeline durchgesetzt.

### Ressourcen: Tabellen und Sichten als Gleichrangige

Der Unterschied zwischen einer Tabelle und einer Sicht liegt nur im Ursprung — eine Tabelle wird von einer Quelle beansprucht, eine Sicht wird von einem Data Steward definiert. Sobald eines von beiden als Domänen-Asset existiert, behandelt das Governance-Modell sie identisch:

- Beide sind erstklassige Domänen-Assets, die im Katalog sichtbar sind
- Beide können Ziel einer Beziehung sein
- Beide können gemäß Prinzip 6 gewährt werden
- Beide unterliegen derselben Governance-Pipeline

Ein Data Steward kann Tabellen privat beanspruchen und nur kuratierte Sichten als öffentlich zugängliche Datenprodukte bereitstellen.

### Zusammensetzung von Sichten

Eine Sicht gehört immer zu genau einer Domäne — es gibt nur einen Sichttyp, immer domänenintern. Eine Sicht existiert für einen von zwei Zwecken:

- **Domänenübergreifender Import** — die Quelle liegt außerhalb der Domäne. Domänenübergreifende Daten dürfen nur über eine Sicht in eine Domäne gelangen, die als schreibgeschützter Adapter fungiert und die externen Daten als Geschäftskonzept der Domäne benennt.
- **Lokale Ableitung** — die Quelle liegt in derselben Domäne. Die Sicht leitet neue oder berechnete Daten aus bestehenden Domänen-Assets ab. Neue oder abgeleitete Daten dürfen nur als Sicht existieren.

Eine Sicht kann referenzieren:

- Beanspruchte Tabellen innerhalb derselben Domäne
- Felder, die im Rahmen einer Feldzugriffsfreigabe aus einer anderen Domäne importiert wurden
- Eine weitere Sicht innerhalb derselben Domäne, sofern die Abweichung zweckgebunden ist: Feldeinschränkung, Aggregation oder Anreicherung über einen zusätzlichen Join

Die Kompositionstiefe wird technisch nicht erzwungen — das Urteilsvermögen des Data Stewards während der HITL-Überprüfung ist der Mechanismus der Qualitätskontrolle.

Jede Sicht trägt einen deklarierten Geschäftszweck, der bei der Erstellung festgelegt wird:

- Teil des governten Artefakts — Data Stewards genehmigen in Kenntnis dessen, wofür die Sicht bestimmt ist
- Wird bei Zugriffsanfragen gemäß Prinzip 7 referenziert, damit der Data Steward die Eignung beurteilen kann
- Begleitet die Sicht von ihrer Erstellung durch den gesamten Governance-Workflow

### Abfragen

Eine Abfrage durchläuft genehmigte Beziehungspfade über Domänen-Assets. Anders als Sichten erzeugen Abfragen keine neue semantische Bedeutung — sie durchlaufen die genehmigte Struktur des Modells. Abfragen können in jeder unterstützten Abfragesprache ausgedrückt werden (SQL, GraphQL, Cypher).

**Strukturelle Durchsetzung:** Der Beziehungskatalog ist der Durchsetzungsmechanismus. Der Compiler validiert jeden Durchlauf gegen genehmigte Katalogeinträge und lehnt Abfragen ab, die auf nicht genehmigte Pfade verweisen. Governance ist strukturell, keine Laufzeitprüfung.

**Keine Genehmigung erforderlich:** Governance findet vorgelagert statt — auf den Ebenen Beziehung und Spaltensichtbarkeit. Wenn ein Benutzer Zugriff auf die Spalten hat und der Durchlaufpfad genehmigt ist, ist die Abfrage eine gültige Nutzung. Kein zusätzliches Gate.

**Unterschied zu Sichten:**

- Sichten: domänenintern, führen neue semantische Bedeutung ein, von Data Stewards kuratiert
- Abfragen: durchlaufen genehmigte Beziehungen, keine neue Semantik, kein Genehmigungs-Gate

**Domänenausdruck je Abfragesprache:**

Jede unterstützte Sprache stellt die Domäne als strukturellen Namensraum dar, der dieser Sprache nativ entspricht:

| Sprache | Domänenausdruck | Beispiel |
| --- | --- | --- |
| GraphQL | Präfix des Typ- und Feldnamens | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Schemaname | `SELECT * FROM sales.orders` |
| Cypher | Zusätzliches Knoten-Label (Domäne nur erforderlich, wenn der Typname mehrdeutig ist) | `MATCH (o:Sales:Order)` |

Der Compiler löst die Domänenzugehörigkeit aus diesen strukturellen Positionen auf — es ist keine Annotation oder kein Hinweis erforderlich.

### Beziehungen

Eine Beziehung ist ein genehmigter Durchlaufpfad zwischen zwei Assets. Domänengrenzen sind für das, was eine Beziehung ist, irrelevant — sie bestimmen nur, wer sie genehmigt.

**Genehmigung:**

- Die Genehmigung ist von jedem einzelnen Data Steward erforderlich, dem ein an der Beziehung beteiligtes Asset gehört
- Wenn ein Data Steward beide Assets besitzt, ist eine Genehmigung erforderlich. Sind zwei Data Stewards beteiligt, sind zwei Genehmigungen erforderlich
- Es gibt keine Klassifizierung in domänenintern/domänenübergreifend — der Besitz bestimmt auf natürliche Weise den Genehmigungsaufwand
- Die Genehmigung einer Beziehung baut den Abhängigkeitsgraphen jedes Data Stewards auf und ermöglicht proaktive Benachrichtigungen zur Schema-Evolution

Beziehungen werden bedarfsgesteuert erstellt, nicht spekulativ. Das erste Team mit dem Geschäftsbedarf erledigt die Arbeit; nachfolgende Teams erben die Infrastruktur.

**Optimierungskonsequenz:** Eine Beziehungsdeklaration ist nicht nur ein Governance-Artefakt — sie ist auch eine strukturelle Beschreibung der Form eines Joins. Die zwei Tabellen, zwei Spalten und der Join-Typ, die eine Beziehung definieren, sind genau das, was der Abfrageoptimierer benötigt, um diesen Join vorzumaterialisieren. Quellenübergreifende Beziehungen erzeugen automatisch vormaterialisierte Join-Tabellen; Beziehungen innerhalb derselben Quelle können dies über `materialize: true` aktivieren. Data Stewards, die gültige Beziehungen durchdenken und genehmigen, erhalten Abfragebeschleunigung als direktes Nebenprodukt — Governance-Arbeit und Optimierungsarbeit sind derselbe Akt.

### Feldzugriffsfreigaben

Eine Feldzugriffsfreigabe ist eine Berechtigung von Domäne zu Domäne — Domäne A darf bestimmte Felder aus Domäne B in ihren Sichten verwenden.

**Lebenszyklus der Freigabe:**

- Wird durch die Sichterstellung ausgelöst, wenn fremde Felder als benötigt identifiziert werden
- Wird einmalig vom Data Steward der Zieldomäne genehmigt
- Gehört zur anfragenden Domäne, nicht zur Sicht, die sie ausgelöst hat
- Jede nachfolgende Sicht in der anfragenden Domäne kann die freigegebenen Felder ohne weitere domänenübergreifende Beteiligung verwenden
- Zusätzliche, nicht freigegebene Felder erfordern eine neue Anfrage

**Benachrichtigung nach der Nutzung:** Wenn eine Sicht unter Verwendung freigegebener Felder erstellt wird, wird der Quell-Data-Steward benachrichtigt — nicht um Genehmigung gebeten. Die Benachrichtigung enthält den Namen der Sicht, den deklarierten Geschäftszweck, die spezifisch verwendeten Felder und welcher Data Steward sie genehmigt hat. Dies gibt dem Quell-Data-Steward:

- **Sichtbarkeit** — Bewusstsein darüber, wie seine Daten genutzt werden
- **Aufsicht** — Grundlage, um Bedenken zu äußern, falls die Nutzung unangemessen erscheint
- **Einspruchsmöglichkeit** — die Fähigkeit, die Freigabe zu widerrufen und dadurch abhängige Sichten ungültig zu machen

Der Kompromiss: Die Quelldomäne genehmigt den Feldzugriff, ohne jede zukünftige Nutzung zu kennen. Eine Genehmigung pro Sicht ist theoretisch korrekt und in der Praxis nicht umsetzbar.

### Workflow zur Abfrageerstellung

Drei Stufen, in dieser Reihenfolge.

**Stufe 1 — Shaping (SQL-Erkundung, von der Seite Beziehungen aus):**

- Der Analyst öffnet das Shaping-Tool von der Seite Beziehungen aus, um potenzielle Join-Pfade in rohem SQL zu erkunden
- Das SQL wird gegen zugängliche Daten ausgeführt, vorbehaltlich der bestehenden RLS und Spaltenmaskierung
- JOINs im SQL werden geparst und als Kandidatenvorschläge für Beziehungen dargestellt
- Maschinell vorgeschlagene Kandidaten (FK-Inferenz, semantische Inferenz) werden neben der SQL-Erkundung des Analysten in derselben Ansicht angezeigt
- Der Analyst wählt Kandidaten aus, die zu einer formalen Beziehungsanfrage befördert werden sollen

**Stufe 2 — Genehmigung der Beziehung** (folgenreich — strukturell und dauerhaft):

- Wird jedem einzelnen Data Steward vorgelegt, dem ein an der Beziehung beteiligtes Asset gehört
- Handelt es sich um einen legitimen Durchlaufpfad? Ist der Join semantisch gültig?
- Alle beteiligten Data Stewards müssen genehmigen; die Beziehung wird zu einem dauerhaften Katalogeintrag

**Stufe 3 — Abfrageerstellung:**

- Der Analyst erstellt die Abfrage in jeder unterstützten Sprache (SQL, GraphQL, Cypher) und durchläuft dabei genehmigte Beziehungspfade
- Nur genehmigte Katalogbeziehungen sind durchlaufbar — der Compiler setzt dies strukturell durch
- Keine Genehmigung erforderlich — Spaltensichtbarkeit und Beziehungsgenehmigung sind die einzigen Gates

### HITL als primäre Kontrolle

Technische Regeln handhaben, was objektiv ist — Nachverfolgung der Feldherkunft, Durchsetzung von Domänengrenzen, Compiler-Validierung. Das kontextuelle Urteilsvermögen verbleibt beim Data Steward. Einschränkungen wie die Kompositionstiefe von Sichten, Anforderungen an den Zweck pro Abfrage und Entscheidungen zur Beziehungsgenehmigung sind HITL-Angelegenheiten, keine vom Compiler durchgesetzten Regeln.

**Neutralität der Quelldomäne:** Der Data Steward der Quelldomäne genehmigt die Beziehung einmal und die Feldfreigabe einmal. Danach operieren nachgelagerte Domänen innerhalb dieser gewährten Grenzen:

- **Hohe Sorgfalt** bei der Entscheidung zur Grenzüberschreitung
- **Leichtgewichtiges Bewusstsein** danach, über Benachrichtigungen und Abfrageverlauf

---

## 2. Auffindbarkeit

### Erkennungsebenen

Die Erkennung ist über fünf Ebenen mit zunehmender Governance strukturiert. Jede Ebene ist eine Voraussetzung für die nächste.

| Ebene | Beschreibung | Governance-Status |
| --- | --- | --- |
| 1 — Registriertes Quellschema | Jede Tabelle, Spalte und jeder Typ aus einer registrierten Quelle. Sichtbarkeit auf Administratorebene. | Keine — rohes Inventar |
| 2 — Nicht beanspruchte Tabellen | Aus registrierten Quellen introspektierte Tabellen ohne Domäneninhaber. Sichtbar für Data Stewards mit Quellzugriff. | Verfügbar, aber ungovernt |
| 3 — Domänen-Assets | Beanspruchte Tabellen und vom Data Steward definierte Sichten. Vollständig governt, im Besitz, im Katalog sichtbar. | Vollständig governt |
| 4 — Beziehungen | Genehmigte Durchlaufpfade zwischen Assets der Ebene 3. Voraussetzung für die domänenübergreifende Sichterstellung. | Von beiden Data Stewards genehmigt |
| 5 — Feldfreigaben | Berechtigungen für den Feldzugriff von Domäne zu Domäne. Der spezifischste und bewussteste governte Zugriff. | Vom Quell-Data-Steward genehmigt |

Eine nicht beanspruchte Tabelle ist ein Lückensignal — wenn benötigte Daten nur auf Ebene 2 existieren, muss ein Data Steward sie beanspruchen, bevor Governance fortschreiten kann. Das Fehlen jeglichen Kandidaten über alle Ebenen hinweg erfordert eine Eskalation an den Administrator.

### FK-Constraints

FK-Constraints sind eine Konstruktion auf Quellebene — sie können sich nicht über mehrere Datenquellen erstrecken. Quellenübergreifende Join-Pfade werden vollständig aus genehmigten Katalogbeziehungen (Ebene 4) abgeleitet, die stärker sind, da sie von beiden Data Stewards validiert wurden.

Innerhalb einer Quelle:

- FK-Constraints werden bei der Quellenregistrierung automatisch als Beziehungskandidaten dargestellt
- Sie repräsentieren eine explizite Modellierungsabsicht — in den meisten analytischen SQL-Systemen nicht durchgesetzt, aber bewusst deklariert
- Eine Validierung durch den Data Steward ist weiterhin erforderlich, bevor ein Kandidat zu einer genehmigten Beziehung wird

### Vertrauenshierarchie für Beziehungen

| Evidenz | Vertrauen |
| --- | --- |
| Genehmigte Katalogbeziehung — quellenübergreifend, von beiden Data Stewards validiert | Höchstes |
| Quelleninterner FK-Constraint — explizite Modellierungsabsicht, nicht durchgesetzt, aber bewusst | Hoch |
| Quelleninterne semantische Inferenz — Ähnlichkeit von Spaltenname/-typ innerhalb eines konsistenten Schemas | Mittel |
| Quellenübergreifende semantische Inferenz — Namenskonventionen weichen zwischen Systemen ab; hohes Risiko falsch-positiver Ergebnisse | Niedrig |

Durch mehrere Evidenztypen bestätigte Vorschläge sammeln Vertrauen an.

### Datensondierung und Korrelation

Für semantisch abgeleitete Kandidaten bietet die Datensondierung einen Validierungsschritt:

- **Wertüberschneidung** — Anteil der Werte der Quellspalte, die in der Zielspalte erscheinen
- **Kardinalität** — ob die Verteilung dem erwarteten Beziehungstyp entspricht
- **Nullrate** — Anteil der Quellspalte, der null ist, was auf Optionalität hinweist

Hohe Korrelation erhöht das Vertrauen; niedrige Korrelation unterdrückt oder degradiert den Kandidaten. Sondierung ist unterstützende Evidenz, kein Beweis — Ganzzahlbereiche können sich zufällig überschneiden, und teilweise referenzielle Integrität ist in analytischen Systemen üblich. Ein erheblicher Fehlerspielraum bleibt bestehen. Das semantische Urteilsvermögen des Data Stewards ist die einzige zuverlässige abschließende Prüfung.

### LLM-gestützte Erkennung

Das LLM operiert gleichzeitig auf allen fünf Ebenen und schlägt Beziehungen, Kandidaten für Beanspruchungen und Durchlaufpfade vor, geordnet nach Vertrauen.

**Was das LLM präsentiert:**

- Nach Vertrauen geordnete Beziehungskandidaten
- Nicht beanspruchte Tabellen, die einen Datenbedarf erfüllen könnten, mit einer Aufforderung, die Beanspruchung einzuleiten
- Fehlen jeglichen Kandidaten — Signal zur Eskalation an den Administrator

**Sichtdesign aus einer Geschäftsbeschreibung:**

Der Analyst liefert eine Beschreibung in natürlicher Sprache und optionale Einschränkungen. Das LLM erzeugt eine vorgeschlagene Sichtstruktur.

*Eingabe:*

- Geschäftsbeschreibung: Entitäten, Metriken, Beziehungen, Absicht
- Optionale Einschränkungen: Filter, Zeitfenster, Aggregationen, ausgeschlossene Felder, Sensibilitätseinschränkungen

*Beispiel:*
> „Tägliche Handelsvolumina nach Kontrahent für die letzten 30 Tage, nur aktive Kontrahenten, mit Anzeige des rechtlichen Namens des Kontrahenten und der Bonitätseinstufung. Keine personenbezogenen Daten."

*LLM-Prozess:*

1. Parsen — Entitäten, Metriken, Dimensionen, Filter, Ausschlüsse identifizieren
2. Suchen — alle Katalogebenen nach passenden Assets
3. Vorschlagen — Domänen-Assets, Beziehungen, Felder, Aggregationsstruktur
4. Bewerten — Vertrauen pro Komponente basierend auf Ebenen-Evidenz
5. Voraussetzungen — geordnete Liste erforderlicher Beanspruchungen, Beziehungen und Feldfreigaben
6. Lücken — Entitäten oder Felder ohne Kandidaten auf jeglicher Ebene, zur Eskalation an den Administrator markiert

*Ausgabe:*

- Abfrageentwurf zur Überprüfung und Verfeinerung durch den Analysten
- Vertrauenswerte pro Komponente
- Geordnete Liste der Voraussetzungen
- Lückenliste

Die Geschäftsbeschreibung wird zum deklarierten Geschäftszweck der Sicht, sobald diese formal erstellt wird.

**SQL-first Beziehungserkennung (Modeling-Tool):**

Zugänglich als modaler Dialog von der Seite Beziehungen aus. Die Absicht besteht darin, das semantische Modell aufzubauen — strukturelle Join-Pfade zu identifizieren, bevor sie als governte Beziehungen formalisiert werden.

1. Der Analyst schreibt freies SQL gegen zugängliche Tabellen (RLS und Maskierung weiterhin angewendet)
2. Der SQL-AST wird geparst — jede JOIN-Bedingung wird zu einem Kandidatenvorschlag für eine Beziehung
3. Die Kandidatenliste wird zusammen mit maschinell vorgeschlagenen Kandidaten (FK-Inferenz, semantische Inferenz) für eine einheitliche Überprüfung angezeigt
4. Der Analyst befördert ausgewählte Kandidaten zu formalen Beziehungsanfragen
5. Genehmigte Beziehungen werden dem Katalog hinzugefügt und in Abfragen durchlaufbar

Das Modeling-Tool kann alle registrierten Tabellen für die strukturelle Erkundung anzeigen, selbst wenn der Analyst die zugrunde liegenden Daten nicht sehen kann — die Genehmigung des Data Stewards regelt den tatsächlichen Datenzugriff, nicht die Schemasichtbarkeit.

---

## 3. Nutzung

### Prüfpfad für Abfragen

Jede Abfrage, die ein Domänen-Asset berührt, wird in einem nur anfügbaren `query_audit_log` erfasst. Jeder Eintrag enthält:

- `tenant_id`, `user_id`, `role_id` — den Identitätskontext
- Einen SHA-256-Hash der Abfrage — der wortgetreue Abfragetext wird nie gespeichert
- `table_ids` — die von der Abfrage berührten Domänen-Assets
- `source`, `status_code`, `duration_ms`
- `logged_at` — den Zeitstempel

Das Protokoll ist nur anfügbar (DELETE und UPDATE sind auf Datenbankebene blockiert) und indexiert nach `(tenant_id, logged_at)` und `(user_id, logged_at)`.

Der Abfrageverlaufsbericht des Data Stewards ist eine aggregierte Sicht auf dieses Protokoll, filterbar nach Asset, Rolle und Zeitfenster. Der Katalog ist ein lebendiges Governance-Instrument — Data Stewards behalten das Bewusstsein darüber, wie ihre Assets genutzt werden, in Echtzeit und nicht im Nachhinein.

**Zwei Sichtbarkeitsmechanismen:**

- **Push** — Benachrichtigungen nach der Nutzung für strukturelle Akte (eine neue Sicht wurde unter Verwendung Ihrer Felder erstellt)
- **Pull** — Abfrageverlauf für Laufzeit-Nutzungsmuster
