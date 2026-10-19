# Business-Glossar {: #business-glossary }

Das Geschäftsglossar ist ein lebendiges Vokabular über Ihrem Datenmodell. Jede physische Spalte im Modell wird einem Begriff zugeordnet, und Spalten, die dasselbe Konzept tragen, teilen sich einen Begriff, wie unterschiedlich sie es auch schreiben. Jeder Begriff kann eine Definition, eine Reihe typisierter Beziehungen zu anderen Begriffen und eine Liste von Fachexperten haben, denen seine Bedeutung gehört.

Das Glossar ist eine Ontologie: ein Graph aus Konzepten, ihren Definitionen und den Beziehungen zwischen ihnen. Provisa bildet sie auf die Datenbankstrukturen ab, in denen die Werte liegen; dadurch kann es Fragen zu Ihren Daten beantworten. Ein Konzept ist ein Entwurf, bis es die Daten erreicht, direkt oder über andere Konzepte. Das Glossar ist von den Metriken getrennt: Ein Begriff sagt, was ein Konzept ist, eine Metrik sagt, wie eine Zahl berechnet wird.

Dieses gemeinsame Vokabular verbindet die Sprache des Geschäfts mit den physischen Daten. Ein KI-Agent, der weiß, dass „customer“ jede Spalte mit einer Kundenkennung bezeichnet, muss nicht raten, welche von `cust_id`, `customerId` und `CUSTOMER_KEY` die richtige ist. Alle werden demselben Begriff zugeordnet, und der Begriff trägt die Definition.

## Wie Begriffe abgeleitet werden {: #how-terms-are-derived }

Provisa leitet aus jedem Spaltennamen automatisch einen Begriff ab, mithilfe einer deterministischen
Normalisierungsregel: Groß-/Kleinschreibung angleichen, nach Trennzeichen und camelCase
tokenisieren, Abkürzungen ausschreiben und nachgestellte Proxy-Token entfernen.

Die **Abkürzungserweiterung** bildet übliche Unternehmenskürzel auf ihre volle Form ab: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` und so weiter. `id` und `key` werden beide zu `identifier`. Die Tabelle ist fest und vorsichtig: Mehrdeutige Kürzel wie `st`, `min` und `no` bleiben, wie sie sind, damit nichts falsch geraten wird.

**Das Entfernen von Proxy-Token** streicht ein nachgestelltes `identifier`-, `code`-, `index`- oder
`reference`-Token. Eine Spalte namens `cust_id` benennt nicht die Kennung selbst; sie benennt eine
Kundin über einen Surrogatwert. Das Entfernen des Proxys lässt `cust_id` und `customerId` beide auf
dem Begriff `customer` landen. Nur nachgestellte Token fallen weg, und nie das letzte verbliebene
Token: Eine bloße `id`-Spalte wird zu `identifier` und bleibt dort.

Um die **Deduplizierung** geht es. Die Normalisierungsregel ist deterministisch, also ergeben `cust_id`, `customerId` und `CUSTOMER_KEY` alle `customer`. Jede Spalte bekommt eine Referenz auf diesen einen Begriff, und die Kuratierung hat einen einzigen Ort für die Definition statt drei.

### Generische Phrasen {: #generic-phrases }

Manche normalisierten Ausdrücke sind zu allgemein, um für sich ein Konzept zu sein. Eine bloße Spalte `name`, `date` oder `identifier` bezeichnet ein Attribut des Konzepts ihrer Tabelle, das es ohne diese Tabelle nicht gibt. Mitarbeiter haben Namen, Produkte haben Namen, aber das ist nicht dasselbe.

Fällt eine Phrase in die generische Menge und ist ein Tabellenkontext verfügbar, qualifiziert sich der
Begriff zu `<table concept> <phrase>`: `employees.first_name` normalisiert zu `employee first name`,
und `orders.id` normalisiert zu `order`, weil das Entfernen des Proxys die qualifizierte Phrase dann auf
das Konzept zusammenzieht, das sie identifiziert. Dieser letzte Fall ist wichtig: Der Primärschlüssel von
`orders` und jeder Fremdschlüssel `order_id` auf anderen Tabellen landen alle auf `order`, ohne dass
zusätzliche Kuratierung nötig wäre.

Die generische Menge umfasst Attributsubstantive (`name`, `date`, `status`, `type`, `amount`,
`quantity`), Audit-Trail-Phrasen (`created_at`, `modified_by`, `submitted_timestamp`) und eine Handvoll
weiterer, die auf nahezu jeder Tabelle auftauchen.

### Der fachliche Name, nicht der physische {: #the-business-name-not-the-physical-name }

Ein abgeleiteter Begriff folgt dem **fachlichen Namen** der Spalte: ihrem Alias, wenn der Modellierer einen gesetzt hat, sonst ihrem physischen Namen. Ist `usr_nm` als `user name` aliasiert, lautet der abgeleitete Begriff `user name`, wo eine Erweiterung von `usr_nm` vielleicht `user number` ergeben hätte.

Einer Spalte einen Alias zu geben, ist die stärkere Korrektur. Ein Alias gelangt auf jede Oberfläche, die die Spalte liest (SQL, GraphQL, KI-Agenten, der Katalog), sodass das Modell sich überall richtig beschreibt. Den Begriff umzubenennen, korrigiert einen Katalogeintrag und lässt die Spalte für den nächsten Leser `usr_nm` heißen. Das Banner für vorgeschlagene Begriffe in der UI sagt genau das: zuerst der Spalte einen Alias geben, den Begriff nur umbenennen, wenn der Spaltenname stimmt und das Vokabular nicht.

Ein neuer Alias für eine Spalte leitet ihren vorgeschlagenen Begriff neu ab, sodass das Glossar dem Modell folgt und nie zweimal dieselbe Korrektur verlangt. Hat ein Kurator einem Begriff eine Definition, eine Beziehung oder einen Experten hinzugefügt, lässt eine Alias-Änderung die Referenz, wo sie ist, denn diese Arbeit gehört dem Kurator und bleibt.

### Tabellennamen als Zugriffspfad {: #access-path-table-names }

Manche Tabellennamen beschreiben einen Zugriffspfad statt eines Konzepts: `user_by_name` ist ein Benutzer, den man über eine Namenssuche erreicht, und keine eigene Art von Entität. Leitet Provisa das Tabellenkonzept ab, um allgemeine Ausdrücke zu qualifizieren, schneidet es den Namen am Bindewort ab, sodass `user_by_name` zu `user` und `orders_by_customer` zu `order` wird.

Ohne den Schnitt würde der Surrogatschlüssel von `user_by_name` zu `user name` normalisiert und mit dem echten Attribut `users.name` kollidieren; ein Begriff hielte dann ein Ding und eines seiner eigenen Felder. Der Schnitt gilt nur für Tabellenkonzepte. In einem Spaltennamen gehört `by` zum zusammengesetzten Substantiv, sodass `pet_by_name` und `pet_name` zum selben Begriff `pet name` normalisiert werden.

## Was einen Begriff kuratiert macht {: #what-makes-a-term-curated }

Ein Begriff, der aus der Normalisierung einer Spalte entsteht, beginnt leer, als Vorschlag, der noch kein Vokabular ist. Kuratiert wird er, sobald eines der folgenden zutrifft:

- Eine Definition wurde gespeichert.
- Eine Beziehungskante wurde hinzugefügt.
- Eine Fachexpertin wurde zugewiesen.
- Eine kuratierende Person hat ihn manuell stillgelegt.

Die Kuratierung zählt für den Lebenszyklus des Begriffs. Verlässt die letzte physische Spalte eines kuratierten Begriffs das Modell, wird der Begriff als veraltet markiert und behalten: Er geht außer Betrieb, behält den Inhalt seiner Bearbeiter und kehrt automatisch zurück, wenn dieselbe Spalte wieder auftaucht. Ein nicht kuratierter Begriff ohne verbleibende Spalten wird einfach entfernt.

## Erneutes Synchronisieren aus Tabellen {: #re-syncing-from-tables }

Jedes Mal, wenn eine Tabelle gespeichert oder neu geladen wird, gleicht `sync_table_refs` die Spalten
dieser Tabelle gegen die vorhandenen Refs ab. Neue Spalten legen Begriffe an oder verknüpfen sich mit
ihnen; verschwundene Spalten lassen ihre Refs fallen; und die Regel „entfernen oder abkündigen“ klärt
jeden Begriff, der seine letzte Ref verliert.

Neu abgeleitet werden nur nicht kuratierte Begriffe. Haben Sie einer Spalte einen Alias gegeben und weicht der vorgeschlagene Begriff nun ab, wandert die Referenz zum neuen Begriff. Ist der Begriff kuratiert, bleibt die Verknüpfung, weil eine Alias-Änderung die Begriffswahl des Kurators nicht überstimmt.

Ein abstrakter Begriff, dessen einziger Pfad zu physischen Daten über einen abgehenden Begriff lief, wird
abgekündigt statt entfernt, wodurch die konzeptionelle Struktur erhalten bleibt, bis sie neu verdrahtet
ist.

## Beziehungen {: #relationships }

Begriffe stehen über typisierte Kanten mit anderen Begriffen in Beziehung. Die unterstützten
Beziehungstypen sind:

| Typ | Bedeutung |
| --- | --- |
| `KIND_OF` | Der Ausgangsbegriff ist eine Art des Zielbegriffs. |
| `PART_OF` | Der Ausgangsbegriff ist ein Bestandteil des Zielbegriffs. |
| `SYNONYM_OF` | Die beiden Begriffe sind in dieser Domäne austauschbar. |
| `RELATED_TO` | Eine lose Zuordnung — keine stärkere Aussage passt. |
| `VALID_VALUE_OF` | Die Quelle ist ein zulässiger Wert der Ziel-Aufzählung oder -Domäne. |
| `DERIVED_FROM` | Die Quelle wird aus dem Ziel berechnet oder bezogen. |
| `REPLACES` | Die Quelle löst das abgekündigte Ziel ab. |
| `PREFERRED_TERM_FOR` | Die Quelle ist der bevorzugte Begriff gegenüber dem unerwünschten Ziel. |
| `TRANSLATION_OF` | Die Quelle ist eine Locale- oder Sprachübersetzung des Ziels. |
| `ANTONYM_OF` | Die Quelle ist das semantische Gegenteil des Ziels. |

Beziehungen sind gerichtet. Die Oberfläche zeigt sowohl ausgehende Kanten (dieser Begriff → ein anderer)
als auch eingehende Kanten (ein anderer Begriff → dieser) und beschriftet jede Richtung mit ihrer eigenen
umgangssprachlichen Formulierung.

Die Kanten liegen in `glossary_term_edges`, einer Zuordnungstabelle, die als Junction-Beziehung deklariert ist
: Ihre Spalte `rel_type` ist der Diskriminator, sodass jeder der obigen Typen ein
eigener Cypher-Beziehungstyp zwischen zwei `GlossaryTerm`-Knoten ist und keine Eigenschaft an einem
reifizierten Knoten. Die Tabelle wird mit dem übrigen Metadatenschema bereitgestellt und erscheint in
Graph-Clients nicht als Knoten — sie ist die Kante. Nichts daran ist glossarspezifisch: Sie wird genauso
deklariert, wie Sie eine Junction über Ihre eigenen Tabellen deklarieren würden, und sie wird vom selben
Code gelesen.

## Abstrakte Begriffe {: #abstract-terms }

Ein abstrakter Begriff hat keine eigenen Refs auf physische Spalten. Verwenden Sie einen für ein
fachliches Konzept, das mehrere konkrete Begriffe überspannt — ein Dach, das Sie anschließend mit den
konkreten Begriffen verdrahten, die tatsächlich Spalten halten. `revenue` etwa könnte abstrakt sein, mit
`PART_OF`-Kanten von `order amount`, `adjustment amount` und `refund amount`, die darauf zeigen.

Ein abstrakter Begriff, der über den Beziehungsgraphen keine physische Spalte erreicht, ist ein hängender Vorschlag. Er erscheint weder in der Begriffssuche für Agenten noch im Metadatenexport, weil ein Begriff, der keine Daten benennt, nichts beantworten kann.

## Die Zulassungsregel für konsumierende Oberflächen {: #the-admission-rule-for-consuming-surfaces }

Ein Begriff, den eine konsumierende Oberfläche anbieten darf, muss drei Bedingungen erfüllen:

1. **Im Dienst** — nicht stillgelegt (jemand hat ihn außer Dienst genommen) und nicht abgekündigt (er hat
   seine letzte Spalte verloren und wurde nur gehalten, weil ein Löschen etwas lose zurückgelassen hätte).
2. **Definiert** — er trägt eine Definition. Ein aus einem Spaltennamen abgeleiteter Begriff ist ein
   Token, keine Bedeutung. Ohne Definition ist er ein Vorschlag, der auf Kuratierung wartet, nie ein
   Vokabular, auf das ein Agent eine Frage gründen kann.
3. **Verankert** — über Begriffe im Dienst mit mindestens einem Begriff verbunden, der eine Ref auf eine
   physische Spalte hält. Das Glossar ist ein Einstiegspunkt in die Daten, also muss jede Kette an einer
   Spalte enden.

Die Verbindung breitet sich durch den Graphen aus: Ein abstrakter Begriff erreicht Daten über jeden Nachbarn in Betrieb, der es tut. Begriffe außer Betrieb leiten nicht weiter, ein stillgelegter Begriff hält also seine Abhängigen nicht am Leben.

## Metadaten-Export {: #metadata-export }

Das Glossar wird im Rahmen des Metadatenexports in externe Datenkataloge veröffentlicht. Es gilt dieselbe Zulassungsregel, in einem Punkt verengt: Ob ein Begriff verwurzelt ist, wird nur an Spalten gemessen, die tatsächlich veröffentlicht werden. Ein Begriff, dessen Spalten alle vom Export zurückgehalten werden, weil ihre Tabellen nicht als Datenprodukte markiert sind oder technische Filter sie ausschließen, ist für den Export nicht verwurzelt, auch wenn er in der Steuerungsebene Referenzen hält.

Beziehungskanten veröffentlichen nur, wenn beide Endpunktbegriffe veröffentlichen.

Spalten-Assets exportieren unabhängig davon. Ein ausgeschlossener Begriff verbirgt die zugrunde liegenden
Daten nicht.

### Einen Begriff vom Export ausschließen {: #excluding-a-term-from-export }

Manche Spalten tragen Installationstechnik statt Fachdaten: ETL-Batch-Kennungen, Zeilenversionen,
Ingest-Zeitstempel. Ein aus einer solchen Spalte abgeleiteter Begriff kann eine völlig zutreffende
Definition haben, die schlicht kein Fachvokabular ist. Das Steuerelement **Vom
Metadaten-Export ausschließen** hält den Begriff und alle Beziehungskanten, die auf ihm enden, von den
Katalogen zurück, in die Provisa veröffentlicht, während die Spalten selbst weiterhin als Assets
exportieren.

Entscheidend ist, ob das Geschäft das Wort verwendet; wie gut die Definition ist, entscheidet es nicht. Eine ETL-Batch-Kennung hat eine klare Bedeutung, die für Ingenieure ins Glossar gehört, aber nicht in einen Geschäftskatalog neben `customer` und `revenue`.

## Arbeiten mit dem Glossar {: #working-with-the-glossary }

Öffnen Sie in der Oberfläche **Admin → Glossar**. Das linke Panel listet jeden Begriff; klicken Sie einen
an, um seine Detailansicht zu öffnen. Von dort aus:

- **Umbenennen** Sie den Begriff, um seine Formulierung zu ändern, ohne seine Spalten zu verschieben.
- **Eine Definition hinzufügen**, indem Sie eine tippen oder auf die KI-Entwurfsschaltfläche klicken, um
  aus dem Namen des Begriffs, seinen physischen Spalten und seinen Beziehungen einen Ausgangspunkt zu
  erzeugen. Der Entwurf wird erst gespeichert, wenn Sie ihn bestätigen.
- **Eine Ref verschieben**, um zwei Begriffe zusammenzuführen: Wählen Sie den Zielbegriff aus dem
  Dropdown neben einer beliebigen physischen Ref. Verliert der Ausgangsbegriff seine letzte Ref, wird er
  automatisch nach der Regel „entfernen oder abkündigen“ geklärt.
- **Eine Beziehung hinzufügen** zwischen diesem und einem anderen Begriff, wobei Sie den Typ aus der
  geschlossenen Menge wählen. Ändern Sie den Typ einer bestehenden Kante an Ort und Stelle, statt sie zu
  löschen und neu anzulegen.
- **Experten zuweisen** über die Benutzer-ID, mit einer Art von `expert` oder `author`.
- **Stilllegen** Sie einen Begriff, um ihn außer Dienst zu nehmen. Er behält seine Spalten und bleibt hier
  bearbeitbar, aber die Begriffssuche von Agenten und der Metadaten-Export überspringen ihn beide.
  Stellen Sie ihn später wieder her, wenn das Konzept zurückkommt.
- **Definitionen im Stapel erzeugen**, um jede leere Definition in einem Durchgang zu füllen. Nur leere
  Definitionen werden geschrieben; von Menschen verfasster Text wird nie überschrieben.
- **Beziehungen im Stapel erzeugen**, um typisierte Kanten über die gesamte Begriffsliste vorzuschlagen.
  Fehlerhafte Vorschläge — unbekannte Begriffsnamen, Kanten auf sich selbst, unbekannte Typen — werden
  automatisch verworfen.

Das Banner **Vorgeschlagen** auf einem Begriff ohne Definition sagt Ihnen, ob der Begriff undefiniert ist
(die Spalte aliasieren oder eine Definition hinzufügen) oder unverankert (ihn mit einem Begriff in
Beziehung setzen, der Spalten hat). Wenn Sie es sehen, ist der Begriff für Agenten und Kataloge noch nicht
erreichbar.

## Siehe auch {: #see-also }

- [Metadaten-Export](metadata-export.md) — wie Begriffe und Beziehungen in externe Datenkataloge
  veröffentlichen, einschließlich der Frage, welche Begriffe die Zulassungsregel des Exports zulässt.
- [Lineage auf Spaltenebene](lineage.md) — der Lineage-Explorer und wie `columnDependents`
  Glossar-Bindungen als Abhängige einer physischen Spalte meldet.
