# Business-Glossar

Das Business-Glossar ist ein lebendes Vokabular über Ihrem Datenmodell. Jede physische Spalte in der
semantischen Schicht löst auf einen Begriff auf — auf einen gemeinsamen Begriff immer dann, wenn
mehrere Spalten dasselbe Konzept tragen, wie unterschiedlich sie es auch schreiben. Jeder Begriff kann
eine Definition halten, dazu einen Satz typisierter Beziehungen zu anderen Begriffen und eine Liste von
Fachexperten, denen die Bedeutung gehört.

Dieses gemeinsame Vokabular ist die Brücke zwischen Fachsprache und physischen Daten. Ein KI-Agent,
der weiß, dass „customer“ jede Spalte benennt, die eine Kundenkennung trägt, muss nicht raten,
welche von `cust_id`, `customerId` und `CUSTOMER_KEY` die richtige ist — sie alle lösen auf denselben
Begriff auf, und der Begriff trägt die Definition.

## Wie Begriffe abgeleitet werden

Provisa leitet aus jedem Spaltennamen automatisch einen Begriff ab, mithilfe einer deterministischen
Normalisierungsregel (REQ-1387): Groß-/Kleinschreibung angleichen, nach Trennzeichen und camelCase
tokenisieren, Abkürzungen ausschreiben und nachgestellte Proxy-Token entfernen.

**Das Ausschreiben von Abkürzungen** bildet gängige Unternehmenskürzel auf ihre Langform ab: `cust` →
`customer`, `txn` → `transaction`, `qty` → `quantity` und so weiter. Sowohl `id` als auch `key` werden
zu `identifier`. Die Tabelle ist fest und zurückhaltend — mehrdeutige Kürzel wie `st`, `min` und
`no` bleiben, wie sie geschrieben sind, statt falsch zu raten.

**Das Entfernen von Proxy-Token** streicht ein nachgestelltes `identifier`-, `code`-, `index`- oder
`reference`-Token. Eine Spalte namens `cust_id` benennt nicht die Kennung selbst; sie benennt eine
Kundin über einen Surrogatwert. Das Entfernen des Proxys lässt `cust_id` und `customerId` beide auf
dem Begriff `customer` landen. Nur nachgestellte Token fallen weg, und nie das letzte verbliebene
Token: Eine bloße `id`-Spalte wird zu `identifier` und bleibt dort.

**Deduplizierung** ist der Zweck. Die Normalisierungsregel ist deterministisch, also ergeben `cust_id`,
`customerId` und `CUSTOMER_KEY` alle `customer`. Jede Spalte erhält eine Ref auf den einen daraus
entstehenden Begriff statt auf drei getrennte Begriffe. Die Kuratierung hat dann eine Stelle für die
Definition, nicht drei.

### Generische Phrasen

Manche normalisierten Phrasen sind zu generisch, um für sich ein Konzept zu sein. Eine bloße Spalte
`name`, `date` oder `identifier` benennt ein Attribut des Konzepts ihrer Tabelle, kein von dieser
Tabelle unabhängiges Konzept. Mitarbeitende haben Namen; Produkte haben Namen; das ist nicht dasselbe.

Fällt eine Phrase in die generische Menge und ist ein Tabellenkontext verfügbar, qualifiziert sich der
Begriff zu `<Tabellenkonzept> <Phrase>`: `employees.first_name` normalisiert zu `employee first name`,
und `orders.id` normalisiert zu `order`, weil das Entfernen des Proxys die qualifizierte Phrase dann auf
das Konzept zusammenzieht, das sie identifiziert. Dieser letzte Fall ist wichtig: Der Primärschlüssel von
`orders` und jeder Fremdschlüssel `order_id` auf anderen Tabellen landen alle auf `order`, ohne dass
zusätzliche Kuratierung nötig wäre.

Die generische Menge umfasst Attributsubstantive (`name`, `date`, `status`, `type`, `amount`,
`quantity`), Audit-Trail-Phrasen (`created_at`, `modified_by`, `submitted_timestamp`) und eine Handvoll
weiterer, die auf nahezu jeder Tabelle auftauchen.

### Der fachliche Name, nicht der physische

Ein abgeleiteter Begriff folgt dem **fachlichen Namen** der Spalte — ihrem Alias, wenn die modellierende
Person einen gesetzt hat, ihrem physischen Namen, wenn nicht (REQ-1581). Ist `usr_nm` auf `user name`
aliasiert, lautet der abgeleitete Begriff `user name`, nicht `user number` oder irgendeine Ausschreibung
von `usr_nm`.

Eine Spalte zu aliasieren ist die stärkere Korrektur. Ein Alias wandert zu jeder Oberfläche, die die
Spalte liest — SQL, GraphQL, KI-Agenten, den Katalog —, sodass sich das Modell überall korrekt
beschreibt. Ein Umbenennen des Begriffs richtet einen Katalogeintrag und lässt die Spalte für die
nächste lesende Person weiter `usr_nm` heißen. Das Banner für vorgeschlagene Begriffe in der Oberfläche
sagt genau das: erst die Spalte aliasieren; den Begriff nur dann umbenennen, wenn der Spaltenname
richtig ist und das Vokabular nicht.

Ein erneutes Aliasieren einer Spalte leitet ihren vorgeschlagenen Begriff neu ab, sodass das Glossar dem
Modell folgt, statt zweimal um dieselbe Korrektur zu bitten. Sobald eine kuratierende Person einem
Begriff eine Definition, eine Beziehung oder einen Experten hinzugefügt hat, verschiebt eine
Alias-Änderung die Ref nicht — diese Arbeit gehört der Kuratierung, und sie bleibt.

### Tabellennamen als Zugriffspfad

Manche Tabellennamen beschreiben einen Zugriffspfad statt eines Konzepts: `user_by_name` ist ein Benutzer,
der über eine Namenssuche erreicht wird, keine eigene Art von Entität. Wenn Provisa das Tabellenkonzept
für die Qualifizierung generischer Phrasen ableitet, schneidet es den Namen am Bindewort (REQ-1582).
`user_by_name` wird zu `user`; `orders_by_customer` wird zu `order`.

Ohne den Schnitt würde der Surrogatschlüssel auf `user_by_name` zu `user name` normalisieren und mit dem
echten Attribut `users.name` kollidieren — ein Begriff, der eine Sache und zugleich eines ihrer eigenen
Felder hält. Der Schnitt gilt nur für Tabellenkonzepte. In einem Spaltennamen ist `by` Teil des
zusammengesetzten Substantivs: `pet_by_name` und `pet_name` normalisieren auf denselben Begriff,
`pet name`.

## Was einen Begriff kuratiert macht

Ein Begriff, der aus der Spaltennormalisierung entstanden ist, beginnt leer — als Vorschlag, noch nicht
als Vokabular. Er wird kuratiert, sobald eines der Folgenden zutrifft:

- Eine Definition wurde gespeichert.
- Eine Beziehungskante wurde hinzugefügt.
- Eine Fachexpertin wurde zugewiesen.
- Eine kuratierende Person hat ihn manuell stillgelegt.

Die Kuratierung ist für den Lebenszyklus des Begriffs entscheidend. Wird die letzte physische Spalte
eines kuratierten Begriffs aus dem Modell entfernt, wird der Begriff abgekündigt statt gelöscht: Er geht
außer Dienst, behält seine redaktionellen Inhalte und wird automatisch wiederbelebt, wenn dieselbe Spalte
erneut auftaucht. Ein nicht kuratierter Begriff ohne weitere Spalten wird schlicht entfernt.

## Erneutes Synchronisieren aus Tabellen

Jedes Mal, wenn eine Tabelle gespeichert oder neu geladen wird, gleicht `sync_table_refs` die Spalten
dieser Tabelle gegen die vorhandenen Refs ab. Neue Spalten legen Begriffe an oder verknüpfen sich mit
ihnen; verschwundene Spalten lassen ihre Refs fallen; und die Regel „entfernen oder abkündigen“ klärt
jeden Begriff, der seine letzte Ref verliert.

Neu abgeleitet wird nur für nicht kuratierte Begriffe. Haben Sie eine Spalte aliasiert und weicht der
vorgeschlagene Begriff nun ab, wandert die Ref zum neuen Begriff. Ist der Begriff kuratiert, bleibt die
Verknüpfung bestehen — die Alias-Änderung hat die Begriffswahl der Kuratierung nicht überstimmt.

Ein abstrakter Begriff, dessen einziger Pfad zu physischen Daten über einen abgehenden Begriff lief, wird
abgekündigt statt entfernt, wodurch die konzeptionelle Struktur erhalten bleibt, bis sie neu verdrahtet
ist.

## Beziehungen

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

## Abstrakte Begriffe

Ein abstrakter Begriff hat keine eigenen Refs auf physische Spalten. Verwenden Sie einen für ein
fachliches Konzept, das mehrere konkrete Begriffe überspannt — ein Dach, das Sie anschließend mit den
konkreten Begriffen verdrahten, die tatsächlich Spalten halten. `revenue` etwa könnte abstrakt sein, mit
`PART_OF`-Kanten von `order amount`, `adjustment amount` und `refund amount`, die darauf zeigen.

Ein abstrakter Begriff, der über den Beziehungsgraphen keine physische Spalte erreichen kann, ist ein
loser Vorschlag. Er erscheint weder in der Begriffssuche von Agenten noch im Metadaten-Export — ein
Begriff, der keine Daten benennt, kann nichts beantworten.

## Die Zulassungsregel für konsumierende Oberflächen

Ein Begriff, den eine konsumierende Oberfläche anbieten darf, muss drei Bedingungen erfüllen (REQ-1387):

1. **Im Dienst** — nicht stillgelegt (jemand hat ihn außer Dienst genommen) und nicht abgekündigt (er hat
   seine letzte Spalte verloren und wurde nur gehalten, weil ein Löschen etwas lose zurückgelassen hätte).
2. **Definiert** — er trägt eine Definition. Ein aus einem Spaltennamen abgeleiteter Begriff ist ein
   Token, keine Bedeutung. Ohne Definition ist er ein Vorschlag, der auf Kuratierung wartet, nie ein
   Vokabular, auf das ein Agent eine Frage gründen kann.
3. **Verankert** — über Begriffe im Dienst mit mindestens einem Begriff verbunden, der eine Ref auf eine
   physische Spalte hält. Das Glossar ist ein Einstiegspunkt in die Daten, also muss jede Kette an einer
   Spalte enden.

Konnektivität pflanzt sich durch den Graphen fort: Ein abstrakter Begriff erreicht Daten über jeden
Nachbarn im Dienst, der das tut. Begriffe außer Dienst leiten nicht — ein stillgelegter Begriff hält seine
Abhängigen nicht am Leben.

## Metadaten-Export

Das Glossar veröffentlicht im Rahmen des Metadaten-Exports in externe Datenkataloge. Es gilt dieselbe
Zulassungsregel, mit einer Einschränkung: Die Verankerung eines Begriffs wird nur gegen Spalten beurteilt,
die tatsächlich veröffentlichen. Ein Begriff, dessen Spalten allesamt vom Export zurückgehalten werden —
weil ihre Tabellen nicht als Datenprodukte markiert sind oder weil technische Filter sie ausschließen —,
gilt für Exportzwecke als nicht verankert, selbst wenn er in der Control Plane Refs hält.

Beziehungskanten veröffentlichen nur, wenn beide Endpunktbegriffe veröffentlichen.

Spalten-Assets exportieren unabhängig davon. Ein ausgeschlossener Begriff verbirgt die zugrunde liegenden
Daten nicht.

### Einen Begriff vom Export ausschließen

Manche Spalten tragen Installationstechnik statt Fachdaten: ETL-Batch-Kennungen, Zeilenversionen,
Ingest-Zeitstempel. Ein aus einer solchen Spalte abgeleiteter Begriff kann eine völlig zutreffende
Definition haben, die schlicht kein Fachvokabular ist (REQ-1583). Das Steuerelement **Vom
Metadaten-Export ausschließen** hält den Begriff und alle Beziehungskanten, die auf ihm enden, von den
Katalogen zurück, in die Provisa veröffentlicht, während die Spalten selbst weiterhin als Assets
exportieren.

Der Maßstab ist, ob das Fachgeschäft dieses Wort spricht, nicht ob die Definition gut ist. Eine
ETL-Batch-Kennung hat eine klare Bedeutung, die für Engineers ins Glossar gehört; in einen Fachkatalog
neben `customer` und `revenue` gehört sie nicht.

## Arbeiten mit dem Glossar

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

## Siehe auch

- [Metadaten-Export](metadata-export.md) — wie Begriffe und Beziehungen in externe Datenkataloge
  veröffentlichen, einschließlich der Frage, welche Begriffe die Zulassungsregel des Exports zulässt.
- [Lineage auf Spaltenebene](lineage.md) — der Lineage-Explorer und wie `columnDependents`
  Glossar-Bindungen als Abhängige einer physischen Spalte meldet.
