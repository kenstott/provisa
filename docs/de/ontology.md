# Ontologie {: #ontology }

Die Ontologie ist ein lebendiges Vokabular über Ihrem Datenmodell. Jede physische Spalte wird einem Begriff zugeordnet, und Spalten mit derselben Bedeutung teilen sich einen Begriff, wie unterschiedlich sie ihn auch schreiben. Jeder Begriff kann eine Definition, typisierte Beziehungen zu anderen Begriffen, eine Liste von Fachexperten und Tags wie `pii` tragen, die jede Spalte mit dieser Bedeutung übernimmt.

Sie ist ein Graph aus Konzepten, ihren Definitionen und den Beziehungen zwischen ihnen. Provisa bildet sie auf die Tabellen und Spalten ab, in denen die Werte liegen; dadurch kann es Fragen zu Ihren Daten beantworten. Ein Begriff ist ein Entwurf, bis er eine Definition hat und Daten erreicht. Die Ontologie ist von den Metriken getrennt: Ein Begriff sagt, was ein Konzept ist, eine Metrik sagt, wie eine Zahl berechnet wird.

Dieses gemeinsame Vokabular verbindet die Sprache des Geschäfts mit den physischen Daten. Ein KI-Agent, der weiß, dass „customer“ jede Spalte mit einer Kundenkennung bezeichnet, muss nicht raten, welche von `cust_id`, `customerId` und `CUSTOMER_KEY` die richtige ist. Alle werden demselben Begriff zugeordnet, und der Begriff trägt die Definition.

## Klassen, Eigenschaften und Konzepte {: #classes-properties-and-concepts }

Jeder Begriff hat genau eine Rolle, abgeleitet aus dem, worin er verankert ist. Sie legen sie nie selbst fest.

| Rolle | Verankert in | Beispiel |
| --- | --- | --- |
| **Klasse** | Einer oder mehreren Tabellen oder Views. Ihre Instanzen sind deren Zeilen. | `customer`, verankert in `customers` und `crm_contacts` |
| **Eigenschaft** | Spalten, die keine Schlüssel sind. | `customer email`, mit `customers.email` |
| **Konzept** | Nichts direkt. Es erreicht Daten über seine Beziehungen. | `revenue`, mit `PART_OF`-Kanten von `order amount` und `refund amount` |

Ein Begriff ist das eine oder das andere. Provisa lehnt es ab, eine Tabelle in einem Begriff zu verankern, der Nicht-Schlüssel-Spalten hält, und lehnt es ab, eine Nicht-Schlüssel-Spalte auf einen Begriff zu verschieben, der in Tabellen verankert ist. Jede Ablehnung nennt den Begriff und das, was er bereits hält.

### Jede Tabelle mit Schlüssel schlägt eine Klasse vor {: #every-keyed-table-proposes-a-class }

Wird eine Tabelle mit Primärschlüssel gespeichert, verankert Provisa sie in einer Klasse im Entwurf, benannt nach dem Konzept der Tabelle: `orders` schlägt `order` vor, `order_lines` schlägt `order line` vor. Tabellen desselben Konzepts teilen sich eine Klasse. Eine Tabelle ohne Primärschlüssel kann keine Klasse sein; ihre Verankerung wird abgelehnt, mit Nennung der Tabelle.

Eine Verankerung, die Sie selbst vornehmen, in der UI, in der Konfiguration oder über Polly, ist kuratiert. Die Synchronisierung verschiebt sie nie, und eine Tabelle in einem kuratierten Begriff zu verankern ersetzt ihren Entwurfsvorschlag. Ist ein Konzept bereits eine Eigenschaft mit Nicht-Schlüssel-Spalten, erhalten seine Tabellen keinen Vorschlag; die Ontologie führt sie stattdessen unter **Zu prüfen**.

### Schlüssel gehören zur Klasse {: #keys-belong-to-the-class }

Ein Primärschlüssel hat keinen eigenen Begriff. Jede seiner Spalten, die Teile eines zusammengesetzten Schlüssels eingeschlossen, legt ihre Referenz auf die Klasse ihrer Tabelle.

Ein Fremdschlüssel funktioniert genauso. Jede Spalte der Viele-Seite einer deklarierten Beziehung legt ihre Referenz auf die Klasse der Zieltabelle, wie die Spalte auch heißt. `manager_id` auf `employees`, die zurück auf `employees` zeigt, landet auf `employee`; die Rolle „manager“ steckt im Namen der Beziehung. Eine Spalte ohne deklarierte Beziehung ist eine Eigenschaft, auch wenn ihr Name einer Klasse entspricht.

Provisa platziert Schlüsselreferenzen allein durch Ableitung, immer wenn eine Tabelle synchronisiert wird, sich eine Beziehung ändert oder sich die Klasse einer Tabelle ändert. Sie können eine Schlüsselspalte nicht von Hand verschieben; die Ablehnung nennt die Spalte, ihre Tabelle und ihre Klasse. Verankern Sie eine Tabelle in einem anderen Begriff, folgen ihre Schlüssel sofort.

### Dieselbe Entität in mehreren Tabellen {: #the-same-entity-in-several-tables }

Instanzen aus verschiedenen Tabellen sind verschieden, bis Sie etwas anderes sagen. Beschreiben `customers` und `crm_contacts` beide denselben Kunden, markieren Sie die Beziehung, die sie verbindet, als **dieselbe Entität**. Sie muss zwei Tabellen verbinden, die dieselbe Klasse verankert, eins zu eins, über einen Schlüssel jeder Seite: den Primärschlüssel oder einen deklarierten Unique-Constraint.

Solange eine Beziehung „dieselbe Entität“ auf einer Tabelle besteht, kann deren Klasse weder entzogen noch ersetzt werden. Heben Sie die Markierung zuerst auf.

Eine Klasse, die in mehreren Tabellen verankert ist, die nicht alle durch Beziehungen „dieselbe Entität“ verbunden sind, wird unter **Zu prüfen** als *nicht verknüpfte Tabellen* markiert.

## Wie Begriffe abgeleitet werden {: #how-terms-are-derived }

Provisa legt aus jedem Spaltennamen automatisch einen Begriff im Entwurf an, mithilfe einer deterministischen Normalisierungsregel: Groß-/Kleinschreibung angleichen, nach Trennzeichen und camelCase tokenisieren, Abkürzungen ausschreiben und nachgestellte Proxy-Token entfernen.

Die **Abkürzungserweiterung** bildet übliche Unternehmenskürzel auf ihre volle Form ab: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` und so weiter. `id` und `key` werden beide zu `identifier`. Die Tabelle ist fest und vorsichtig. Mehrdeutige Kürzel wie `st`, `min` und `no` bleiben, wie sie sind, damit nichts falsch geraten wird.

**Das Entfernen von Proxy-Token** streicht ein nachgestelltes `identifier`-, `code`-, `index`- oder `reference`-Token. Eine Spalte namens `cust_id` benennt nicht die Kennung selbst; sie benennt einen Kunden über einen Surrogatwert. Das Entfernen des Proxys lässt `cust_id` und `customerId` beide auf dem Begriff `customer` landen. Nur nachgestellte Token fallen weg, und nie das letzte verbliebene, also wird eine bloße `id`-Spalte zu `identifier` und bleibt dort.

Um die **Deduplizierung** geht es. Die Regel ist deterministisch, also ergeben `cust_id`, `customerId` und `CUSTOMER_KEY` alle `customer`. Jede Spalte bekommt eine Referenz auf diesen einen Begriff, und die Kuratierung hat einen einzigen Ort für die Definition statt drei.

Schlüsselspalten überspringen diese Regel. Ihre Referenzen gehen an eine Klasse, wie oben beschrieben.

### Generische Phrasen {: #generic-phrases }

Manche normalisierten Phrasen sind zu allgemein, um für sich ein Konzept zu sein. Eine bloße Spalte `name`, `date` oder `identifier` benennt ein Attribut des Konzepts ihrer Tabelle, das es losgelöst von dieser Tabelle nicht gibt. Mitarbeiter haben Namen, und Produkte haben Namen, aber es ist nicht dasselbe.

Fällt eine Phrase in die generische Menge und ist ein Tabellenkontext vorhanden, wird der Begriff zu `<Tabellenkonzept> <Phrase>` qualifiziert: `employees.first_name` normalisiert zu `employee first name`.

Die generische Menge umfasst Attributsubstantive (`name`, `date`, `status`, `type`, `amount`, `quantity`), Audit-Phrasen (`created_at`, `modified_by`, `submitted_timestamp`) und eine Handvoll weiterer, die auf fast jeder Tabelle vorkommen.

### Begriffe folgen dem fachlichen Namen {: #terms-follow-the-business-name }

Ein abgeleiteter Begriff folgt dem **fachlichen Namen** der Spalte: ihrem Alias, wenn der Modellierer einen gesetzt hat, und sonst ihrem physischen Namen. Ist `usr_nm` als `user name` aliasiert, lautet der abgeleitete Begriff `user name`, wo das Ausschreiben von `usr_nm` vielleicht `user number` ergeben hätte.

Eine Spalte zu aliasieren ist die stärkere Korrektur. Ein Alias wandert zu jeder Oberfläche, die die Spalte liest (SQL, GraphQL, KI-Agenten, der Katalog), sodass sich das Modell überall korrekt beschreibt. Den Begriff umzubenennen korrigiert einen Katalogeintrag und lässt die Spalte für den nächsten Leser weiterhin `usr_nm` heißen. Das Entwurfsbanner in der UI sagt es: Aliasieren Sie zuerst die Spalte, und benennen Sie den Begriff nur um, wenn der Spaltenname stimmt und das Vokabular nicht.

Eine Spalte neu zu aliasieren leitet ihren Entwurfsbegriff neu ab, sodass die Ontologie dem Modell folgt und nie zweimal um dieselbe Korrektur bittet. Sobald ein Kurator einem Begriff eine Definition, eine Beziehung, einen Experten oder ein Tag hinzugefügt hat, lässt eine Alias-Änderung die Spalte, wo sie ist, denn diese Arbeit gehört dem Kurator und bleibt.

### Tabellennamen als Zugriffspfad {: #access-path-table-names }

Manche Tabellennamen beschreiben einen Zugriffspfad statt eines Konzepts: `user_by_name` ist ein Benutzer, erreicht über eine Namenssuche, und keine eigene Art von Entität. Leitet Provisa das Konzept einer Tabelle ab, schneidet es den Namen am Bindewort ab, sodass `user_by_name` zu `user` und `orders_by_customer` zu `order` wird.

Ohne den Schnitt würde `user_by_name` eine Klasse `user name` vorschlagen, die mit dem echten Attribut `users.name` kollidiert. Der Schnitt gilt nur für Tabellenkonzepte. In einem Spaltennamen gehört `by` zum zusammengesetzten Substantiv, sodass `pet_by_name` und `pet_name` zum selben Begriff normalisieren, `pet name`.

## Status {: #status }

Ein Begriff hat zu jedem Zeitpunkt einen Status.

| Status | Bedeutung | Wie er dorthin kommt |
| --- | --- | --- |
| **Entwurf** | Für Agenten, Suche und Kataloge nicht verfügbar, bis er eine Definition hat und Daten erreicht. | Jeder abgeleitete Begriff beginnt hier. |
| **Aktiv** | Zugelassen: Agenten, Suche und Katalogen angeboten. | Definiert, verankert und im Dienst. |
| **Veraltet** | Auf dem Weg nach draußen. | Ein Kurator markiert einen aktiven Begriff als veraltet, oder ein kuratierter Begriff verliert seine letzte Spalte. |
| **Außer Dienst** | Außer Betrieb, als Nachweis aufbewahrt. | Ein Kurator stellt ihn außer Dienst und nennt einen Nachfolger. |

### Die Zulassungsregel {: #the-admission-rule }

Ein Begriff ist aktiv, wenn er drei Bedingungen erfüllt.

1. **Im Dienst.** Er ist nicht außer Dienst gestellt und nicht dadurch veraltet, dass er seine letzte Spalte verloren hat. Ein Begriff, den ein Kurator als veraltet markiert hat, bleibt bis zu seiner geplanten Außerdienststellung im Dienst.
2. **Definiert.** Er trägt eine Definition. Ein aus einem Spaltennamen abgeleiteter Begriff ist nur ein Token. Ohne Definition ist er ein Entwurf, der auf einen Kurator wartet, nie Vokabular, auf das ein Agent eine Frage gründen kann.
3. **Verankert.** Er ist über Begriffe im Dienst mit einem Begriff verbunden, der eine Spalte oder eine Tabelle hält. Die Ontologie ist ein Einstiegspunkt in die Daten, also muss jede Kette bei den Daten enden.

Die Verbindung breitet sich durch den Graphen aus. Ein Konzept erreicht Daten über jeden Nachbarn im Dienst, der es tut. Begriffe außer Dienst leiten nicht weiter, ein außer Dienst gestellter Begriff hält also seine Abhängigen nicht am Leben.

### Einen Begriff als veraltet markieren {: #deprecating-a-term }

Markieren Sie einen aktiven Begriff als veraltet, wenn er auf dem Weg nach draußen, aber noch in Gebrauch ist. Sie geben einen Grund an, der Pflicht ist, und optional ein Datum für die geplante Außerdienststellung, heute oder später. Der Begriff bleibt im Dienst und wirkt bis zu diesem Datum weiter an der Zulassungsregel mit. Dann stellt ihn der Scheduler außer Dienst.

Ein fälliger Begriff, der noch Spalten oder Tabellen hält, kann nicht per Datum außer Dienst gestellt werden, denn nur ein Nachfolger kann sie übernehmen. Er wartet, und **Zu prüfen** führt ihn als *Außerdienststellung blockiert*, bis Sie ihn mit einem Nachfolger außer Dienst stellen.

Ein kuratierter Begriff, der seine letzte Spalte verliert, wird automatisch als veraltet markiert und aufbewahrt. Er geht außer Betrieb, behält die Inhalte seiner Bearbeiter und kehrt zurück, wenn dieselbe Spalte wieder auftaucht. Ein nicht kuratierter Begriff ohne verbleibende Spalten wird entfernt.

### Einen Begriff außer Dienst stellen {: #retiring-a-term }

Einen Begriff außer Dienst zu stellen verschiebt alles, was an ihm hängt, auf einen **Nachfolger** und nimmt ihn dann außer Betrieb.

- Die Spalten einer Eigenschaft gehen an den Nachfolger. Er muss eine Eigenschaft oder ein Konzept sein.
- Die Tabellen einer Klasse gehen an den Nachfolger, und ihre Schlüssel folgen ihnen. Er muss eine Klasse oder ein Konzept sein.
- Ein Konzept hält nichts, sein Nachfolger ist also optional.

Die Außerdienststellung braucht einen Grund und schreibt eine `REPLACES`-Kante vom Nachfolger zum außer Dienst gestellten Begriff. Dieser bleibt als passiver Nachweis erhalten: seine Definition, Tags, Beziehungen, Experten und der Grund. Er leitet nichts weiter.

Da seine Spalten nun dem Nachfolger gehören, zeigt das Panel eines außer Dienst gestellten Begriffs, worauf er **zuletzt angewendet** wurde: seine Spalten und Tabellen, wie das Modell sie im Commit vor der Außerdienststellung hielt. Das Panel verlinkt auch auf den Begriff, der ihn ersetzt.

### Einen Begriff wiederherstellen {: #restoring-a-term }

**Wiederherstellen** macht rückgängig, was ein Kurator festgelegt hat. Ein veralteter Begriff ist nicht mehr veraltet. Ein außer Dienst gestellter Begriff kehrt als leeres Konzept und Entwurf zurück, da seine Spalten an den Nachfolger gingen, und seine `REPLACES`-Kante wird entfernt.

### Was einen Begriff kuratiert macht {: #what-makes-a-term-curated }

Ein aus der Spaltennormalisierung entstandener Begriff beginnt leer. Kuratiert wird er, sobald er eines davon hat:

- eine gespeicherte Definition
- eine Beziehungskante
- einen zugewiesenen Fachexperten
- ein Tag, etwa `pii`
- eine Außerdienststellung

Kuratierte Begriffe bleiben erhalten, wenn ihre Spalten verschwinden. Die Synchronisierung leitet die Spalten eines kuratierten Begriffs nie anderswohin ab.

## Erneutes Synchronisieren aus Tabellen {: #re-syncing-from-tables }

Jedes Mal, wenn eine Tabelle gespeichert oder neu geladen wird, gleicht Provisa ihre Spalten mit der Ontologie ab. Neue Spalten legen Begriffe an oder verknüpfen sie, verschwundene Spalten verlieren ihre Referenzen, und jeder Begriff ohne verbleibende Referenzen wird nach der Regel oben entfernt oder als veraltet markiert. Schlüsselspalten gehen an ihre Klasse, und eine Tabelle mit Schlüssel ohne Klasse schlägt eine vor.

Nur nicht kuratierte Begriffe werden neu abgeleitet. Aliasieren Sie eine Spalte und ändert sich ihr Entwurfsbegriff, wandert die Spalte zum neuen Begriff. Ist der Begriff kuratiert, bleibt die Spalte, denn eine Alias-Änderung setzt sich nicht über die Wahl eines Kurators hinweg.

Würde das Entfernen eines Begriffs ein Konzept ohne Weg zu den Daten zurücklassen, markiert Provisa diesen Begriff stattdessen als veraltet, sodass die aufgebaute Struktur erhalten bleibt, bis Sie sie neu verdrahten.

## Beziehungen {: #relationships }

Begriffe stehen über typisierte Kanten mit anderen Begriffen in Beziehung.

| Typ | Bedeutung |
| --- | --- |
| `KIND_OF` | Der Quellbegriff ist eine Art des Zielbegriffs. |
| `PART_OF` | Der Quellbegriff ist ein Bestandteil des Zielbegriffs. |
| `SYNONYM_OF` | Die beiden Begriffe sind in dieser Domäne austauschbar. |
| `RELATED_TO` | Eine lose Verbindung, wenn keine stärkere Aussage passt. |
| `VALID_VALUE_OF` | Die Quelle ist ein zulässiger Wert der Zielaufzählung oder -domäne. |
| `DERIVED_FROM` | Die Quelle wird aus dem Ziel berechnet oder bezogen. |
| `REPLACES` | Die Quelle löst das Ziel ab. Das Außerdienststellen eines Begriffs schreibt eine solche Kante von seinem Nachfolger. |
| `PREFERRED_TERM_FOR` | Die Quelle ist der bevorzugte Begriff gegenüber dem nicht empfohlenen Ziel. |
| `TRANSLATION_OF` | Die Quelle ist eine Übersetzung des Ziels in eine andere Sprache oder ein anderes Gebietsschema. |
| `ANTONYM_OF` | Die Quelle ist das semantische Gegenteil des Ziels. |

Beziehungen sind gerichtet. Die UI zeigt ausgehende Kanten (dieser Begriff → ein anderer) und eingehende Kanten (ein anderer Begriff → dieser Begriff) und beschriftet jede Richtung mit einer eigenen verständlichen Formulierung.

Die Kanten liegen in einer assoziativen Tabelle, die als Junction-Beziehung deklariert ist. Ihre Spalte für den Beziehungstyp ist der Diskriminator, sodass jeder Typ oben ein eigener Cypher-Beziehungstyp zwischen zwei Begriffsknoten ist. Nichts daran ist ontologiespezifisch: Sie ist so deklariert, wie Sie eine Junction über Ihren eigenen Tabellen deklarieren würden, und derselbe Code liest sie.

## Konzepte {: #concepts }

Ein Konzept hat keine eigenen Spalten oder Tabellen. Verwenden Sie eines für eine fachliche Idee, die mehrere konkrete Begriffe überspannt, und verdrahten Sie es dann mit den Begriffen, die Daten halten. `revenue` etwa könnte ein Konzept sein, mit `PART_OF`-Kanten von `order amount`, `adjustment amount` und `refund amount`.

Ein Konzept, das über den Beziehungsgraphen keine Spalte und keine Tabelle erreicht, benennt keine Daten. Es erscheint weder in der Begriffssuche für Agenten noch im Metadatenexport, und **Zu prüfen** führt es als *keine Daten*. Domänen gelten nur für ein Konzept. Jeder andere Begriff übernimmt seine Domänen von den Tabellen, in denen er verankert ist, oder von den Spalten, die er hält.

## PII nach Bedeutung {: #pii-on-meanings }

PII ist eine Eigenschaft dessen, was eine Spalte bedeutet. Provisa lässt Sie sie deshalb einmal am Begriff markieren und deckt jede Spalte mit dieser Bedeutung ab, auch später registrierte Spalten.

Das Tag `pii` gehört an eine Spalte oder an einen Begriff mit der Rolle Eigenschaft, nie an eine Quelle, eine Tabelle, eine Klasse oder ein Konzept. Ein Begriff muss definiert und im Dienst sein (aktiv oder veraltet), um es zu tragen. Einen Begriff im Entwurf zu markieren wird mit „Definieren Sie *Begriff*, um ihn zu markieren“ abgelehnt, und Sie können die Definition eines markierten Begriffs erst leeren, wenn Sie die Markierung aufheben.

Jede Spalte eines markierten Begriffs trägt `pii` und damit `sensitive`, überall, wo Tags gelesen werden: Governance, Maskierung, Sensitivität, Export und die Tag-Ansichten. Nichts wird auf die Spalten kopiert. Eine auf den Begriff verschobene Spalte trägt das Tag sofort, und eine davon wegverschobene trägt es nicht mehr. Jede Spalte sagt, woher ihr Tag kommt: an der Spalte gesetzt, mit Begründung, oder über einen Begriff, mit dessen Namen.

Markieren Sie eine Spalte, deren Begriff eine Eigenschaft ist, fragt die UI, ob die anderen Spalten mit derselben Bedeutung ebenfalls markiert werden sollen.

- **Ja** setzt das Tag am Begriff.
- **Nur diese** markiert nur die Spalte und fragt nach dem Grund.

Eine Spalte, die PII über ihren Begriff ist, kann nicht für sich allein entmarkiert werden. Heben Sie die Markierung am Begriff auf, oder verschieben Sie die Spalte zu dem Begriff, der zu ihrem Inhalt passt.

### Verschiebungen, die Tags verlieren {: #moves-that-lose-tags }

Eine Spalte auf einen anderen Begriff zu verschieben kann ihr ein Tag nehmen: `customers.email` von `customer email` (markiert mit `pii`) auf einen unmarkierten Begriff zu verschieben, ließe die E-Mail-Spalte ungeschützt. Jede Verschiebung, die eine Person vornimmt, wird vorher geprüft. Das betrifft das Übernehmen oder Verschieben einer Spalte, das Außerdienststellen eines Begriffs und das Speichern oder Löschen einer Beziehung, die Schlüsselspalten umhängt.

Würde eine Spalte `pii` ganz verlieren, wird die Verschiebung abgelehnt, bis Sie entscheiden. Die UI zeigt eine Zusammenfassung dessen, was jede verschobene Spalte verliert und gewinnt, Verluste zuerst.

- **Verschieben** führt die Verschiebung aus, jedes verlorene Tag bestätigt.
- **Verschieben und markieren** überträgt die verlorenen Tags auf den Begriff, zu dem die Spalten wandern, in derselben Änderung.
- **Abbrechen** verschiebt nichts.

Eine Spalte, die das Tag über ihre eigene Markierung behält, braucht keine Bestätigung. Verschiebungen, die das Modell selbst vornimmt, etwa das Neuableiten der Spalten eines Entwurfs, ändern nie ein geerbtes Tag.

## Zu prüfen {: #to-review }

Die Ontologie meldet als Befunde, was Ihre Aufmerksamkeit braucht. Ein Begriff mit einem Befund ist **Zu prüfen**, und die Begriffsliste lässt sich danach filtern. Ein gewöhnlicher Entwurf ist kein Befund; Entwürfe sind der normale Zustand der Ontologie.

| Befund | Was er bedeutet | Die Lösung |
| --- | --- | --- |
| Nicht verknüpfte Tabellen | Eine Klasse verankert mehrere Tabellen, die Beziehungen „dieselbe Entität“ nicht alle verbinden. | Markieren Sie Beziehungen „dieselbe Entität“ zwischen ihnen. |
| Klasse und Attribut | Eine Klasse hält Spalten, die nicht ihre Schlüssel sind, oder eine Tabelle mit Schlüssel hat keine Klasse, weil ihr Konzept eine Eigenschaft ist. | Verschieben Sie die Spalten zu einer Eigenschaft, oder verankern Sie die Tabelle neu. |
| Tag für falsche Objektart | Ein Begriff, der keine Eigenschaft ist, trägt ein Tag, das nur eine Eigenschaft an Spalten weitergibt. | Heben Sie das Tag auf. |
| Außerdienststellung blockiert | Ein veralteter Begriff hat seine geplante Außerdienststellung überschritten, hält aber noch Spalten oder Tabellen. | Stellen Sie ihn mit einem Nachfolger außer Dienst. |
| Keine Daten | Ein Konzept im Dienst erreicht keine Daten. | Verbinden Sie es mit einem Begriff, der Daten hält. |

Befunde melden; sie ändern nichts, bis Sie handeln.

## Metadaten-Export {: #metadata-export }

Die Ontologie wird im Rahmen des Metadatenexports in externe Datenkataloge veröffentlicht. Nur aktive Begriffe werden veröffentlicht, beurteilt an den Spalten und Tabellen, die tatsächlich veröffentlicht werden. Ein Begriff, dessen Spalten alle vom Export zurückgehalten werden, weil ihre Tabellen keine Datenprodukte sind oder technische Filter sie ausschließen, gilt für den Export nicht als verankert, auch wenn er in der Steuerungsebene Spalten hält.

Ein veralteter Begriff wird mit seinem Status, seinem Grund und seinem Datum der geplanten Außerdienststellung veröffentlicht, damit jeder Katalog ihn auf seine eigene Veraltungsmarkierung abbilden kann.

Beziehungskanten werden nur veröffentlicht, wenn beide Endbegriffe veröffentlicht werden. Spalten-Assets werden unabhängig exportiert, ein ausgeschlossener Begriff verbirgt also nicht die zugrunde liegenden Daten.

### Einen Begriff vom Export ausschließen {: #excluding-a-term-from-export }

Manche Spalten tragen Infrastruktur, etwa ETL-Batch-Kennungen, Zeilenversionen und Ingest-Zeitstempel. Ein aus einer solchen Spalte abgeleiteter Begriff kann eine zutreffende Definition haben und trotzdem kein Geschäftsvokabular sein. Die Option **Vom Metadatenexport ausschließen** hält den Begriff und alle auf ihn endenden Beziehungskanten von den Katalogen zurück, in die Provisa veröffentlicht. Die Spalten selbst werden weiterhin als Assets exportiert.

Entscheidend ist, ob das Geschäft das Wort verwendet. Eine ETL-Batch-Kennung hat eine klare Bedeutung, die für Engineers in die Ontologie gehört, aber nicht in einen Geschäftskatalog neben `customer` und `revenue`.

## Arbeiten mit der Ontologie {: #working-with-the-ontology }

Öffnen Sie **Ontologie** in der Navigationsleiste. Das linke Panel listet jeden Begriff mit seiner Rolle und, sofern er nicht aktiv ist, seinem Status. Die Kästchen **Veraltet** und **Entwurf** blenden diese Begriffe ein oder aus, und **Zu prüfen** beschränkt die Liste auf Begriffe mit Befunden. Klicken Sie auf einen Begriff, um sein Panel zu öffnen.

Das Panel zeigt Rolle und Status des Begriffs und, bei einer Eigenschaft, ihre PII-Markierung mit der Zahl der abgedeckten Spalten. Ein veralteter oder außer Dienst gestellter Begriff zeigt, wer seinen Status geändert hat und warum. Ein Begriff mit Befunden öffnet sich mit einem Abschnitt **Zu prüfen**: Klicken Sie auf einen Befund, um zu sehen, was gefunden wurde und welche Aktion ihn behebt.

**Bearbeiten** öffnet den ganzen Begriff als ein Formular. Speichern wendet alle Änderungen gemeinsam an.

- **Benennen Sie den Begriff um**, um seine Formulierung zu ändern, ohne seine Spalten zu verschieben.
- **Fügen Sie eine Definition hinzu**, indem Sie eine eingeben, oder klicken Sie auf die KI-Entwurfsschaltfläche, um einen Ausgangspunkt aus dem Namen des Begriffs, seinen Spalten und seinen Beziehungen zu erzeugen. Der Entwurf wird erst gespeichert, wenn Sie das Formular speichern.
- **Übernehmen Sie Spalten** von anderen Begriffen auf diesen, oder wählen Sie neben einer seiner eigenen Spalten **Zu Begriff verschieben**. Jede Spalte gehört zu genau einem Begriff. Schlüsselspalten folgen ihrer Klasse und lassen sich nicht verschieben.
- **Verankern Sie Tabellen** in einer Klasse, oder entfernen Sie sie.
- **Markieren Sie PII** an einer Eigenschaft.
- **Fügen Sie eine Beziehung hinzu** und wählen Sie ihren Typ. Ändern Sie den Typ einer bestehenden Kante an Ort und Stelle, statt sie zu löschen und neu anzulegen.
- **Weisen Sie Experten zu** per Benutzer-ID, als `expert` oder `author`.

Neben Bearbeiten bietet das Panel die Statusaktionen an, die der Begriff zulässt: **Als veraltet markieren** für einen aktiven Begriff, **Außer Dienst stellen** für jeden Begriff im Dienst und **Wiederherstellen** für einen veralteten oder außer Dienst gestellten.

Im ✨-Menü füllt **Definitionen** jede leere Definition in einem Durchgang und überschreibt nie den Text einer Person. **Beziehungen** schlägt typisierte Kanten über die ganze Begriffsliste vor; Vorschläge mit unbekannten Begriffen, demselben Begriff zweimal oder einem unbekannten Typ werden verworfen.

Das Banner **Entwurf** an einem Begriff sagt Ihnen, ob er undefiniert ist (Spalte aliasieren oder eine Definition hinzufügen) oder nicht verankert (mit einem Begriff verbinden, der Daten hat). Solange Sie es sehen, können Agenten und Kataloge den Begriff nicht erreichen.

## Siehe auch {: #see-also }

- [Metadaten-Export](metadata-export.md) beschreibt, wie Begriffe und Beziehungen in externe Datenkataloge veröffentlicht werden.
- [Lineage auf Spaltenebene](lineage.md) beschreibt den Lineage-Explorer und wie eine physische Spalte die an sie gebundenen Begriffe meldet.
