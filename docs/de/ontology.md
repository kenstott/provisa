# Ontologie {: #ontology }

Die Ontologie ist ein lebendiges Vokabular über Ihrem Datenmodell. Jede physische Spalte löst sich zu einem Begriff auf, und Spalten mit derselben Bedeutung teilen sich einen Begriff, wie unterschiedlich sie auch geschrieben sind. Jeder Begriff kann eine Definition, typisierte Beziehungen zu anderen Begriffen, eine Liste von Fachexperten und Tags wie `pii` tragen, die jede Spalte mit dieser Bedeutung übernimmt.

Sie ist ein Graph aus Konzepten, ihren Definitionen und den Beziehungen zwischen ihnen. Provisa bildet ihn auf die Tabellen und Spalten ab, die die Werte enthalten, und kann deshalb Fragen zu Ihren Daten beantworten. Ein Begriff ist ein Entwurf, bis ein Steward ihn freigibt, und ob er Daten erreicht, wird aus dem Graphen ermittelt. Die Ontologie ist von den Metriken getrennt: Ein Begriff sagt, was ein Konzept ist, eine Metrik sagt, wie eine Zahl berechnet wird.

Dieses gemeinsame Vokabular verbindet die Sprache des Fachbereichs mit den physischen Daten. Ein KI-Agent, der weiß, dass „customer“ jede Spalte mit einer Kundenkennung bezeichnet, muss nicht raten, welche von `cust_id`, `customerId` und `CUSTOMER_KEY` die richtige ist. Alle lösen sich zum selben Begriff auf, und der Begriff trägt die Definition.

## Klassen, Eigenschaften und Konzepte {: #classes-properties-and-concepts }

Jeder Begriff hat genau eine Rolle, abgeleitet aus dem, was er verankert. Sie legen sie nie selbst fest.

| Rolle | Verankert in | Beispiel |
| --- | --- | --- |
| **Klasse** | Einer oder mehreren Tabellen oder Views. Ihre Instanzen sind deren Zeilen. | `customer`, verankert in `customers` und `crm_contacts` |
| **Eigenschaft** | Spalten, die keine Schlüssel sind. | `customer email`, mit `customers.email` |
| **Konzept** | Nichts direkt. Es erreicht Daten über seine Beziehungen. | `revenue`, mit `PART_OF`-Kanten von `order amount` und `refund amount` |

Ein Begriff ist das eine oder das andere. Provisa lehnt es ab, eine Tabelle in einem Begriff zu verankern, der Nicht-Schlüsselspalten hält, und lehnt es ab, eine Nicht-Schlüsselspalte auf einen in Tabellen verankerten Begriff zu verschieben. Jede Ablehnung nennt den Begriff und das, was er bereits hält.

### Jede Tabelle mit Schlüssel schlägt eine Klasse vor {: #every-keyed-table-proposes-a-class }

Wird eine Tabelle mit Primärschlüssel gespeichert, verankert Provisa sie in einem Klassenbegriff im Entwurf, benannt nach dem Konzept der Tabelle: `orders` schlägt `order` vor, `order_lines` schlägt `order line` vor. Tabellen desselben Konzepts teilen sich eine Klasse. Eine Tabelle ohne Primärschlüssel kann keine Klasse sein, und ihre Verankerung wird unter Nennung der Tabelle abgelehnt.

Eine Verankerung, die Sie selbst vornehmen, in der UI, in der Konfiguration oder über Polly, ist kuratiert. Die Synchronisierung verschiebt sie nie, und das Verankern einer Tabelle in einem kuratierten Begriff ersetzt ihren Entwurfsvorschlag. Ist ein Konzept bereits eine Eigenschaft mit Nicht-Schlüsselspalten, erhalten seine Tabellen keinen Vorschlag; die Ontologie listet sie stattdessen unter **Zu prüfen**.

### Schlüssel gehören zur Klasse {: #keys-belong-to-the-class }

Ein Primärschlüssel hat keinen eigenen Begriff. Jede seiner Spalten, auch die Teile eines zusammengesetzten Schlüssels, legt ihre Referenz auf die Klasse ihrer Tabelle.

Ein Fremdschlüssel funktioniert ebenso. Jede Spalte der Mehrseite einer deklarierten Beziehung legt ihre Referenz auf die Klasse der Zieltabelle, wie auch immer die Spalte heißt. `manager_id` auf `employees`, das zurück auf `employees` zeigt, landet bei `employee`; die Rolle „manager“ steckt im Namen der Beziehung. Eine Spalte ohne deklarierte Beziehung ist eine Eigenschaft, auch wenn ihr Name mit einer Klasse übereinstimmt.

Provisa platziert Schlüsselreferenzen allein durch Ableitung, sobald eine Tabelle synchronisiert wird, sich eine Beziehung ändert oder sich die Klasse einer Tabelle ändert. Eine Schlüsselspalte können Sie nicht von Hand verschieben, und die Ablehnung nennt die Spalte, ihre Tabelle und ihre Klasse. Verankern Sie eine Tabelle in einem anderen Begriff, folgen ihre Schlüssel sofort.

### Dieselbe Entität in mehreren Tabellen {: #the-same-entity-in-several-tables }

Instanzen aus verschiedenen Tabellen sind verschieden, bis Sie etwas anderes sagen. Beschreiben `customers` und `crm_contacts` beide denselben Kunden, markieren Sie die Beziehung zwischen ihnen als **dieselbe Entität**. Sie muss zwei Tabellen verbinden, die dieselbe Klasse verankert, eins zu eins, über einen Schlüssel jeder Tabelle: den Primärschlüssel oder eine deklarierte Unique-Bedingung.

Solange auf einer Tabelle eine Dieselbe-Entität-Beziehung besteht, kann ihre Klasse weder entzogen noch ersetzt werden. Heben Sie zuerst die Markierung der Beziehung auf.

Eine Klasse, die in mehreren Tabellen verankert ist, die nicht alle durch Dieselbe-Entität-Beziehungen verbunden sind, wird unter **Zu prüfen** als *unverbundene Tabellen* gemeldet.

## Wie Begriffe abgeleitet werden {: #how-terms-are-derived }

Provisa erzeugt aus jedem Spaltennamen automatisch einen Begriff im Entwurf, nach einer deterministischen Normalisierungsregel: Vereinheitlichung der Groß- und Kleinschreibung, Zerlegung an Trennzeichen und camelCase, Auflösung von Abkürzungen und Entfernen nachgestellter Stellvertreter-Token.

**Auflösung von Abkürzungen** bildet gängige Unternehmenskürzel auf ihre Vollform ab: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` und so weiter. Sowohl `id` als auch `key` werden zu `identifier`. Die Tabelle ist fest und vorsichtig. Mehrdeutige Kürzel wie `st`, `min` und `no` bleiben, wie sie sind, damit nichts falsch geraten wird.

**Entfernen von Stellvertreter-Token** streicht ein nachgestelltes Token `identifier`, `code`, `index` oder `reference`. Eine Spalte namens `cust_id` bezeichnet nicht die Kennung selbst; sie bezeichnet einen Kunden über einen Ersatzwert. Durch das Entfernen landen `cust_id` und `customerId` beide auf dem Begriff `customer`. Nur nachgestellte Token werden entfernt, und nie das letzte verbleibende, sodass eine bloße Spalte `id` zu `identifier` aufgelöst wird und dort bleibt.

**Deduplizierung** ist der Zweck. Die Regel ist deterministisch, also ergeben `cust_id`, `customerId` und `CUSTOMER_KEY` alle `customer`. Jede Spalte erhält eine Referenz auf diesen einen Begriff, und die Kuration hat eine einzige Stelle für die Definition statt drei.

Schlüsselspalten überspringen diese Regel. Ihre Referenzen gehen an eine Klasse, wie oben beschrieben.

### Generische Phrasen {: #generic-phrases }

Manche normalisierten Phrasen sind zu allgemein, um für sich ein Konzept zu sein. Eine bloße Spalte `name`, `date` oder `identifier` bezeichnet ein Attribut des Konzepts ihrer Tabelle, das es außerhalb dieser Tabelle nicht gibt. Mitarbeiter haben Namen, Produkte haben Namen, aber das ist nicht dasselbe.

Fällt eine Phrase in die generische Menge und ist ein Tabellenkontext verfügbar, wird der Begriff zu `<table concept> <phrase>` qualifiziert: `employees.first_name` normalisiert zu `employee first name`.

Die generische Menge umfasst Attributsubstantive (`name`, `date`, `status`, `type`, `amount`, `quantity`), Phrasen des Prüfpfads (`created_at`, `modified_by`, `submitted_timestamp`) und einige weitere, die in fast jeder Tabelle vorkommen.

### Begriffe folgen dem fachlichen Namen {: #terms-follow-the-business-name }

Ein abgeleiteter Begriff folgt dem **fachlichen Namen** der Spalte: ihrem Alias, wenn der Modellierer einen gesetzt hat, sonst ihrem physischen Namen. Ist `usr_nm` als `user name` aliasiert, lautet der abgeleitete Begriff `user name`, wo die Auflösung von `usr_nm` vielleicht `user number` ergeben hätte.

Eine Spalte zu aliasieren ist die stärkere Korrektur. Ein Alias gelangt auf jede Oberfläche, die die Spalte liest (SQL, GraphQL, KI-Agenten, der Katalog), sodass sich das Modell überall richtig beschreibt. Den Begriff umzubenennen korrigiert einen Katalogeintrag und lässt die Spalte für den nächsten Leser `usr_nm` heißen. Das Entwurfsbanner in der UI sagt das: Aliasieren Sie zuerst die Spalte, und benennen Sie den Begriff nur um, wenn der Spaltenname stimmt und das Vokabular nicht.

Wird eine Spalte neu aliasiert, wird ihr Entwurfsbegriff neu abgeleitet, sodass die Ontologie dem Modell folgt und nie zweimal um dieselbe Korrektur bittet. Hat ein Kurator einem Begriff eine Definition, eine Beziehung, einen Experten oder ein Tag hinzugefügt, lässt eine Alias-Änderung die Spalte, wo sie ist, denn diese Arbeit gehört dem Kurator und bleibt.

### Tabellennamen als Zugriffspfad {: #access-path-table-names }

Manche Tabellennamen beschreiben einen Zugriffspfad statt eines Konzepts: `user_by_name` ist ein Benutzer, der über eine Namenssuche erreicht wird, keine eigene Art von Entität. Leitet Provisa das Konzept einer Tabelle ab, schneidet es den Namen am Bindewort ab, sodass `user_by_name` zu `user` und `orders_by_customer` zu `order` wird.

Ohne diesen Schnitt würde `user_by_name` eine Klasse `user name` vorschlagen, die mit dem echten Attribut `users.name` kollidiert. Der Schnitt gilt nur für Tabellenkonzepte. In einem Spaltennamen ist `by` Teil des zusammengesetzten Substantivs, sodass `pet_by_name` und `pet_name` zum selben Begriff normalisieren, `pet name`.

## Warum Freigabe und Verankerung zählen {: #why-approval-and-grounding-matter }

Agenten und andere Automatisierung finden die Daten, auf denen sie arbeiten, über die Ontologie, und sie zitieren sie. Ein Agent beginnt bei einem Begriff, den ein Mensch freigegeben hat. Er folgt einem Weg hinunter zu Daten, deren Vertrauenswürdigkeit er sehen kann, verankert oder noch auf eine Freigabe wartend. Er gelangt zu Tabellen und Spalten, die er unter den Berechtigungen, Zeilenregeln und Masken seiner Rolle liest. Jeder Schritt dieses Weges hat einen Namen, den ein Agent in ein Zitat setzen kann.

Zwei Eigenschaften eines Begriffs entscheiden also, was ein Agent mit ihm tun kann. Menschen setzen seinen **Status**. Provisa berechnet seine **Verankerung**. Die Seite zeigt sie als zwei getrennte Chips, und Sie filtern nach jedem.

## Status {: #status }

Ein Mensch setzt den Status eines Begriffs. Er hat jeweils genau einen.

| Status | Bedeutung | Wie er dorthin kommt |
| --- | --- | --- |
| **Entwurf** | Noch nicht freigegeben. | Jeder abgeleitete Begriff beginnt hier, ebenso jeder Begriff, den Sie anlegen. |
| **Freigegeben** | Die Entscheidung eines Stewards: Dies ist Vokabular, mit dem gearbeitet wird. | Ein Steward gibt ihn frei. Die Freigabe erfordert eine Definition. |
| **Veraltet** | Auf dem Weg hinaus. | Ein Kurator markiert einen freigegebenen Begriff als veraltet, oder ein kuratierter Begriff verliert seine letzte Spalte. |
| **Außer Dienst** | Außer Betrieb, als Nachweis aufbewahrt. | Ein Kurator stellt ihn außer Dienst und nennt einen Nachfolger. |

Eine Definition allein gibt nichts frei. Geben Sie einen Begriff frei, sobald seine Definition sagt, was der Fachbereich darunter versteht.

## Verankerung {: #grounding }

Die Verankerung sagt, ob ein Begriff Daten erreicht. Niemand setzt sie: Provisa ermittelt sie aus den Beziehungen des Begriffs, jedes Mal wenn sich der Graph ändert.

Die Verankerung eines Begriffs blickt seine eigenen Pfade zu den Daten hinunter. Ein Pfad führt vom Begriff über seine Beziehungen zum ersten Begriff, der eine Spalte oder eine Tabelle hält: dem Begriff am Datenende, der die Bindung hält. Nur die Begriffe strikt zwischen den beiden Enden zählen. Der Begriff selbst zählt nie gegen seine eigene Verankerung, und ebenso wenig etwas über ihm.

| Verankerung | Markierung | Bedeutung |
| --- | --- | --- |
| **Verankert** | grünes ⏚-Symbol | Ein Pfad nach unten hat keinen Entwurfsbegriff zwischen dem Begriff und dem Datenende. |
| **Verankert · Pfad wartet auf Freigabe** | gedämpftes ⏚-Symbol | Jeder Pfad nach unten hat einen Entwurfsbegriff dazwischen. Fahren Sie mit der Maus über das Symbol: Es lautet „Erreicht Daten, aber nur über noch nicht freigegebene Begriffe“. |
| **Nicht verankert** | keine | Kein Pfad erreicht Daten. |

Das Symbol ist das elektrische Erdungszeichen. Es steht in der Begriffsliste neben der Spaltenanzahl, auf der Seite des Begriffs, vor dem Namen des Begriffs in den Graphen und in der Graphlegende. Der Hovertext eines verankerten Begriffs sagt, wie weit er reicht, zum Beispiel „Verankert: erreicht Daten in 3 Spalten“; ein Screenreader liest dieselben Worte.

Einige Fälle folgen direkt aus der Regel:

- Ein Konzept im Entwurf, das Sie über einer freigegebenen Kette anlegen, ist verankert. Sein Status-Chip sagt weiterhin Entwurf, und die Begriffe darunter ändern sich nicht.
- Ein aus einer Spalte abgeleiteter Begriff ist ein Entwurf am Datenende. Er macht die Kette über ihm nie wartend.
- Ein Entwurfsbegriff in der Mitte des einzigen Pfades macht jeden Begriff über ihm zu gedämpftem ⏚. Die Freigabe dieses einen Entwurfs verankert sie.
- Ein zweiter, sauberer Pfad genügt. Ein Entwurf daneben ändert nichts.
- Veraltete und außer Dienst gestellte Begriffe unterbrechen die Kette. Ein Begriff, dessen einziger Pfad durch einen solchen lief, ist nicht mehr verankert, und seine Chips und Filter sagen das.

## Was ein Agent sieht {: #what-an-agent-sees }

Die MCP-Begriffssuche, das Vokabular von SPARQL und jede andere Oberfläche für Agenten zeigen einem Agenten denselben Ausschnitt der Ontologie:

- einen freigegebenen Begriff auf einem Pfad zu Daten;
- einen definierten Entwurfsbegriff auf einem Pfad zu Daten, der mindestens einen freigegebenen Begriff enthält, als Entwurf markiert;
- einen undefinierten Begriff auf einem solchen Pfad nur als anonymen Schritt, ohne Namen oder Definition. Jeder Schritt bleibt eigenständig, sodass die Form des Weges unverändert bleibt.

Nichts anderes erreicht einen Agenten. Eine Kette ohne freigegebenen Begriff bleibt verborgen, ebenso Begriffe, die keine Daten erreichen, und veraltete oder außer Dienst gestellte Begriffe. Menschen sehen jeden Begriff auf der Ontologie-Seite, mit seinem Status und seiner Verankerung.

Jedes Suchergebnis trägt Status und Verankerung des Begriffs, seine Spalten und Tabellen, seine Experten und seine Pfade hinunter zu den Daten. Jeder Schritt auf einem Pfad ist ein benannter Begriff oder ein anonymer Schritt, und das Datenende listet, woran es gebunden ist. Diesem Weg folgt ein Agent, und ihn zitiert er.

### Einen Begriff als veraltet markieren {: #deprecating-a-term }

Markieren Sie einen freigegebenen Begriff als veraltet, wenn er auf dem Weg hinaus ist. Sie geben einen Grund an, der Pflicht ist, und optional ein geplantes Datum für die Außerdienststellung, heute oder später. An diesem Datum stellt der Scheduler ihn außer Dienst. Ein veralteter Begriff unterbricht die Kette durch ihn ab dem Moment der Markierung, daher lohnt es sich, zuerst die Vorschau der Auswirkungen zu lesen.

Ein Begriff, der zur Außerdienststellung fällig ist und noch Spalten oder Tabellen hält, kann nicht per Datum außer Dienst gestellt werden, denn nur ein Nachfolger kann sie übernehmen. Er wartet, und **Zu prüfen** listet ihn als *Außerdienststellung blockiert*, bis Sie ihn mit einem Nachfolger außer Dienst stellen.

Ein kuratierter Begriff, der seine letzte Spalte verliert, wird automatisch als veraltet markiert und behalten. Er geht außer Betrieb, behält den Inhalt, den seine Bearbeiter geliefert haben, und kehrt zurück, wenn dieselbe Spalte wieder auftaucht. Ein nicht kuratierter Begriff ohne verbleibende Spalten wird entfernt.

### Einen Begriff außer Dienst stellen {: #retiring-a-term }

Einen Begriff außer Dienst zu stellen verschiebt alles, was an ihm hängt, auf einen **Nachfolger** und nimmt ihn dann außer Betrieb.

- Die Spalten einer Eigenschaft gehen an den Nachfolger. Er muss eine Eigenschaft oder ein Konzept sein.
- Die Tabellen einer Klasse gehen an den Nachfolger, und ihre Schlüssel folgen ihnen. Er muss eine Klasse oder ein Konzept sein.
- Ein Konzept hält nichts, daher ist sein Nachfolger optional.

Die Außerdienststellung braucht einen Grund, und sie schreibt eine `REPLACES`-Kante vom Nachfolger zum außer Dienst gestellten Begriff. Der außer Dienst gestellte Begriff bleibt als inerter Nachweis: seine Definition, Tags, Beziehungen, Experten und der Grund. Er leitet nichts weiter. Der Nachfolger behält seinen eigenen Status; eine Außerdienststellung gibt ihn nie frei.

Da seine Spalten nun dem Nachfolger gehören, zeigt das Panel eines außer Dienst gestellten Begriffs, worauf er **zuletzt angewendet wurde**: seine Spalten und Tabellen, wie das Modell sie im Commit vor der Außerdienststellung hielt. Sein Panel verlinkt auch auf den Begriff, der ihn ersetzt.

### Einen Begriff wiederherstellen {: #restoring-a-term }

**Wiederherstellen** macht rückgängig, was ein Kurator festgelegt hat. Ein veralteter Begriff wird wieder freigegeben. Ein außer Dienst gestellter Begriff kehrt als leeres Konzept im Entwurf zurück, da seine Spalten an den Nachfolger gingen, und seine `REPLACES`-Kante wird entfernt.

### Was einen Begriff kuratiert macht {: #what-makes-a-term-curated }

Ein aus der Spaltennormalisierung entstandener Begriff beginnt leer. Er wird kuratiert, sobald er eines davon hat:

- eine gespeicherte Definition
- eine Beziehungskante
- einen zugewiesenen Fachexperten
- ein Tag, etwa `pii`
- eine Außerdienststellung

Kuratierte Begriffe bleiben erhalten, wenn ihre Spalten gehen. Die Synchronisierung leitet die Spalten eines kuratierten Begriffs nie anderswohin neu ab.

## Erneutes Synchronisieren aus Tabellen {: #re-syncing-from-tables }

Jedes Mal, wenn eine Tabelle gespeichert oder neu geladen wird, gleicht Provisa die Spalten dieser Tabelle mit der Ontologie ab. Neue Spalten erzeugen oder verknüpfen Begriffe, entfernte Spalten geben ihre Referenzen ab, und jeder Begriff ohne verbleibende Referenzen wird nach der obigen Regel entfernt oder als veraltet markiert. Schlüsselspalten gehen an ihre Klasse, und eine Tabelle mit Schlüssel ohne Klasse schlägt eine vor.

Nur nicht kuratierte Begriffe werden neu abgeleitet. Aliasieren Sie eine Spalte und ändert sich ihr Entwurfsbegriff, wandert die Spalte zum neuen Begriff. Ist der Begriff kuratiert, bleibt die Spalte, denn eine Alias-Änderung setzt sich nicht über die Entscheidung eines Kurators hinweg.

Würde das Entfernen eines Begriffs ein Konzept ohne Pfad zu Daten zurücklassen, markiert Provisa diesen Begriff stattdessen als veraltet, sodass die von Ihnen aufgebaute Struktur erhalten bleibt, bis Sie sie neu verdrahten.

## Beziehungen {: #relationships }

Begriffe beziehen sich über typisierte Kanten auf andere Begriffe.

| Typ | Bedeutung |
| --- | --- |
| `KIND_OF` | Der Quellbegriff ist eine Art des Zielbegriffs. |
| `PART_OF` | Der Quellbegriff ist ein Bestandteil des Zielbegriffs. |
| `SYNONYM_OF` | Die beiden Begriffe sind in dieser Domäne austauschbar. |
| `RELATED_TO` | Zwei verschiedene Themen, die zusammengehören, wenn kein anderer Typ passt. Die Kante hält einen Platz und verankert nicht. |
| `VALID_VALUE_OF` | Die Quelle ist ein zulässiger Wert der Zielaufzählung oder -domäne. |
| `DERIVED_FROM` | Die Quelle wird aus dem Ziel berechnet oder bezogen. |
| `REPLACES` | Die Quelle löst das Ziel ab. Die Außerdienststellung eines Begriffs schreibt eine solche Kante von seinem Nachfolger aus. |
| `PREFERRED_TERM_FOR` | Die Quelle ist der bevorzugte Begriff gegenüber dem nicht empfohlenen Ziel. |
| `TRANSLATION_OF` | Die Quelle ist eine Übersetzung des Ziels in eine Sprache oder ein Gebietsschema. |
| `ANTONYM_OF` | Die Quelle ist das semantische Gegenteil des Ziels. |

Beziehungen sind gerichtet. Die UI zeigt sowohl ausgehende Kanten (dieser Begriff → ein anderer) als auch eingehende Kanten (ein anderer Begriff → dieser Begriff) und beschriftet jede Richtung mit ihrer eigenen Formulierung in Alltagssprache.

Die Kanten liegen in einer assoziativen Tabelle, die als Junction-Beziehung deklariert ist. Ihre Spalte für den Beziehungstyp ist der Diskriminator, sodass jeder Typ oben ein eigener Cypher-Beziehungstyp zwischen zwei Begriffsknoten ist. Nichts daran ist ontologiespezifisch: Sie wird so deklariert, wie Sie eine Junction über Ihren eigenen Tabellen deklarieren würden, und derselbe Code liest sie.

## Ein Glossar aufbauen, das verankert {: #building-a-glossary-that-grounds }

Ein Begriff hilft einem Agenten nur, wenn er verankert ist, wenn also ein Pfad aus Beziehungen von ihm zu einer Spalte oder Tabelle führt. Agenten sehen nur die Begriffe auf solchen Pfaden, wie [Was ein Agent sieht](#what-an-agent-sees) beschreibt. Ein verankerter Begriff sagt Ihnen, wo seine Daten liegen. Ein nicht verankerter benennt eine Idee und überlässt es Ihnen, die Daten selbst zu finden.

Die Beziehungstypen, die Sie wählen, entscheiden, ob dieser Pfad existiert. Die Verankerung läuft von einem breiteren Begriff hinunter zu einem engeren. `revenue` erreicht Daten, weil `order amount` Teil davon ist und `order amount` eine Spalte hält. Umgekehrt läuft sie nie, also gewinnt `order amount` nichts dadurch, Teil von `revenue` zu sein.

### Jeden Typ prüfen, bevor Sie ihn wählen {: #test-each-type-before-you-pick-it }

Lesen Sie eine Kante mit X als dem Begriff, an dem sie beginnt, und Y als dem Begriff, auf den sie zeigt. Wenden Sie die Tests dann in dieser Reihenfolge an und nehmen Sie den ersten, der besteht.

| Typ | Test | Beispiel | Was er verankert |
| --- | --- | --- | --- |
| **Art von** | Jedes X ist ein Y. | Jeder Großkunde ist ein Kunde. | Y erreicht die Daten von X. |
| **Teil von** | Ein Y hat ein X. | Eine Bestellung hat einen Bestellbetrag. | Y erreicht die Daten von X. |
| **Gültiger Wert von** | X ist einer der zulässigen Werte von Y. | Platin ist eine Kundenstufe. | Y erreicht die Daten von X. |
| **Synonym von** | X und Y bedeuten dasselbe. | Klient und Kunde. | Jeder erreicht die Daten des anderen. |
| **Verwandt mit** | X und Y sind verschiedene Themen, die zusammengehören, und keines ist eine Art, ein Teil oder ein Wert des anderen. | Kuchen und Kerzen. | Nichts. |

Abgeleitet von, Ersetzt, Bevorzugter Begriff für, Übersetzung von und Gegenbegriff zu halten je eine Tatsache über zwei Begriffe fest. Keiner davon verankert.

Eine Eigenschaft, die ein Konzept beschreibt, ist Teil davon. Schichtbeginn beschreibt eine Schicht, also ist er **Teil von** Schichtabdeckung. Bloß verwandt damit ist er nie.

Wenn Sie eine Beziehung hinzufügen oder ihren Typ ändern, listet die Typauswahl zuerst Art von, Teil von und Gültiger Wert von, jeweils mit ihrem Test darunter. Verwandt mit kommt zuletzt, mit seinem Test und dem Hinweis, dass es nicht verankert. Der Link **Eine verankernde Beziehung wählen** unter der Auswahl öffnet diesen Abschnitt.

### Verwandt mit hält einen Platz {: #related-to-holds-a-place }

Verwandt mit ist der letzte Ausweg. Da es nicht verankert, erreicht ein Begriff, dessen einzige Verknüpfungen Verwandt mit sind, keine Daten, wie viele es auch sind. Behandeln Sie jede als Platzhalter für ein Paar, das Sie noch nicht aufgelöst haben.

Um die Platzhalter abzuarbeiten, setzen Sie über der Begriffsliste das Häkchen bei **Verwandt mit**, neben **Veraltet** und **Zu prüfen**. Die Liste beschränkt sich auf Begriffe mit mindestens einer Verwandt-mit-Verknüpfung, an einem der beiden Enden. Das Buchsymbol neben dem Kästchen öffnet diesen Abschnitt.

Ein Begriff, der keine Daten erreicht und nur Verwandt-mit-Verknüpfungen hat, zeigt auf seiner Seite einen Hinweis. Der Hinweis sagt, dass dem Paar ein Begriff dazwischen fehlen könnte, und nennt die Formen, die es auflösen würden. Er ist nur eine Orientierung. Nichts am Begriff wird blockiert.

### Ein Paar mit einem neuen Begriff auflösen {: #resolve-a-pair-with-one-new-term }

Den meisten Verwandt-mit-Paaren fehlt ein Begriff dazwischen. Ein neuer Begriff macht aus dem Platzhalter zwei Kanten, die verankern, und fünf Formen leisten das. Hier sind sie für Kuchen und Kerzen, die engste zuerst:

| Form | Neuer Begriff | Kanten |
| --- | --- | --- |
| **Komponententyp** | Kuchendekoration | Kerzen Art von Kuchendekoration; Kuchendekoration Teil von Kuchen |
| **Spezialisierung** | Geburtstagskuchen | Kerzen Teil von Geburtstagskuchen; Geburtstagskuchen Art von Kuchen |
| **Gemeinsames Ganzes** | Party | Kuchen Teil von Party; Kerzen Teil von Party |
| **Gemeinsame Gattung** | Partybedarf | Kuchen Art von Partybedarf; Kerzen Art von Partybedarf |
| **Gemeinsame Aufzählung** | Partyartikeltyp | Kuchen Gültiger Wert von Partyartikeltyp; Kerzen Gültiger Wert von Partyartikeltyp |

Folgen Sie der Verankerung in jeder Zeile nach unten. In den ersten beiden Formen erreicht Kuchen über den neuen Begriff die Daten von Kerzen. In den letzten drei ist der neue Begriff das breitere Ende beider Kanten, er erreicht also die Daten beider. Weder Kuchen noch Kerzen erreicht die des anderen.

Manchmal existiert der Begriff dazwischen schon, und Sie müssen nur die Typen der Verknüpfungen ändern. Die mitgelieferte Tierheim-Demo hat ein Konzept, `shift coverage`, definiert als Zeitfenster und Ort, an dem ein Mitarbeiter zur Arbeit eingeteilt ist. Seine Teile `shift start`, `shift end` und `schedule location` halten je eine Spalte. Mit Verwandt mit an sie geknüpft, erreichte shift coverage keine Daten. Seit jeder der drei als Teil von shift coverage verknüpft ist, ist es über alle drei Spalten verankert. Das ist ein gemeinsames Ganzes, und das Ganze war die ganze Zeit da.

### Die engste Form bevorzugen {: #prefer-the-tightest-shape }

Je breiter der neue Begriff, desto weniger sagt seine Verankerung einem Leser. Party verbindet Kuchen und Kerzen genauso sicher wie Kuchendekoration. Doch ein so breiter Knoten steht auch über der Gästeliste, dem Veranstaltungsort und der Musik, sodass seine Verankerung nicht mehr sagt, welche Daten eine Frage zu Kerzen beantworten. Kuchendekoration gibt einem Agenten einen kurzen Pfad zu den richtigen Spalten. Party gibt ihm einen langen Pfad zu vielen.

Beginnen Sie mit dem Komponententyp und gehen Sie in der Liste nur weiter nach unten, wenn eine engere Form nicht passt. Verwenden Sie eine gemeinsame Gattung oder eine gemeinsame Aufzählung, wenn das Paar nichts Engeres teilt.

### Wenn Sie Verwandt mit wählen {: #when-you-choose-related-to }

Wählen Sie **Verwandt mit** für eine Verknüpfung, die Sie hinzufügen, öffnet sich ein Dialog, zum Beispiel „Kerzen und Kuchen mit „Verwandt mit“ verknüpfen?“. Ändern Sie den Typ einer bestehenden Verknüpfung zu Verwandt mit, öffnet er sich ebenfalls. Die Unterbrechung ist gewollt, denn dies ist der günstigste Moment, die Verknüpfung zu korrigieren.

Der Dialog erklärt zuerst, warum Verwandt mit die am wenigsten nützliche Kante ist. Sie verankert nicht, sodass der Begriff über sie weiterhin keine Daten erreichen kann. Darunter zeichnet er die fünf Formen für Ihre beiden Begriffe, die engste zuerst. Jede Form zeigt einen Namen für den neuen Begriff, vorgeschlagen vom KI-Modell Ihrer Organisation, zum Beispiel Kerzen → Kuchendekoration → Kuchen. Schlägt das Modell keine Namen vor, sagt der Dialog, warum, und Sie tippen den Namen selbst ein.

Dann wählen Sie eine von drei Aktionen.

- **Beginnen** neben einer Form öffnet das Formular für einen neuen Begriff. Der vorgeschlagene Name ist eingetragen, und **Damit hinzugefügte Kanten** listet beide Kanten. Benennen Sie den Begriff um, fügen Sie eine Definition und seine Domänen hinzu und klicken Sie dann auf **Erstellen**. Erstellen legt den Begriff an. Die Kante, die den Begriff berührt, den Sie bearbeiten, kommt in Ihr Formular und wird mit ihm gespeichert. Die andere Kante wird sofort geschrieben.
- **Trotzdem „Verwandt mit“ hinzufügen** fügt die Verknüpfung mit einem Klick hinzu. Sie reiht sich in die Platzhalter ein, die der Filter **Verwandt mit** listet.
- **Abbrechen** ändert nichts.

### Wie der Beziehungsassistent wählt {: #how-the-relationship-assistant-chooses }

**Beziehungen** im ✨-Menü bittet das KI-Modell Ihrer Organisation um Beziehungen über alle Begriffe in Ihren Domänen. Die Anweisungen des Modells enthalten dieselben Tests. Es bevorzugt Art von, Teil von und Gültiger Wert von, wo immer einer passt, und verknüpft eine Eigenschaft mit dem Konzept, das sie beschreibt, über Teil von. Verwandt mit schlägt es nur vor, wenn zwei Begriffe klar zusammengehören und kein anderer Typ passt.

Der Assistent verknüpft Begriffe, die bereits existieren, und fügt nie einen Begriff hinzu. Für jedes Verwandt mit, das er hinzufügt, nennt er Kandidaten für jede passende Form. Ist er fertig, listet **Zu prüfende „Verwandt mit“-Verknüpfungen** jedes dieser Paare mit seinen Formen, die engste zuerst, und den Kandidaten für jede. Klicken Sie auf einen der beiden Begriffe, um ihn zu öffnen. Ob ein Paar einen neuen Begriff braucht und wie er heißen soll, entscheiden Sie.

### Das Glossar messen {: #measure-the-glossary }

Der Bericht **Ontology quality** misst das Glossar aus dem Modell, nach Domäne und nach Steward. Öffnen Sie ihn im [Berichtsbetrachter](admin.md#reports-viewer). Er berichtet:

- den Anteil der Begriffe in Betrieb, die verankert sind, wobei vollständig verankerte und auf Freigabe wartende getrennt gezählt werden;
- den Anteil der freigegebenen und verankerten Begriffe, also der Begriffe, die ein Agent verwenden darf;
- ungelöste Platzhalter, also Verwandt-mit-Verknüpfungen und die Begriffe, deren einzige Verknüpfungen Verwandt mit sind;
- die Präzision der Verankerung, als Schritte von jedem Begriff zu den Daten und als Anzahl der Pfade, die durch einen breiten Knotenbegriff laufen;
- Begriffe, die nur über Entwürfe verankert sind, jeweils mit dem Begriff, dessen Freigabe sie vollständig verankern würde;
- undefinierte Begriffe auf Pfaden zu Daten;
- die Drift, also die Begriffe, die seit dem vorigen Zeitraum ihre Verankerung verloren oder gewonnen haben.

Jede Zeile verlinkt auf ihren Begriff, und jede Platzhalterzeile auf die Verwandt-mit-Verknüpfungen dieses Begriffs zur Prüfung. Wie jeder Bericht liest er über die gesteuerte Pipeline unter Ihrer Rolle, und Sie können ihn exportieren.

## Konzepte {: #concepts }

Ein Konzept hat keine eigenen Spalten oder Tabellen. Verwenden Sie eines für eine fachliche Idee, die mehrere konkrete Begriffe umspannt, und verknüpfen Sie es dann mit den Begriffen, die Daten halten. `revenue` könnte zum Beispiel ein Konzept sein, mit `PART_OF`-Kanten von `order amount`, `adjustment amount` und `refund amount`.

Ein Konzept, das über den Beziehungsgraphen keine Spalte und keine Tabelle erreichen kann, bezeichnet keine Daten. Es erscheint nicht in der Begriffssuche für Agenten oder im Metadaten-Export, und **Zu prüfen** listet es als *keine Daten*. Domänen gelten nur für ein Konzept. Jeder andere Begriff übernimmt seine Domänen von den Tabellen, die er verankert, oder den Spalten, die er hält.

## PII nach Bedeutung {: #pii-on-meanings }

PII ist eine Eigenschaft dessen, was eine Spalte bedeutet, daher lässt Provisa Sie es einmal am Begriff markieren und deckt damit jede Spalte mit dieser Bedeutung ab, auch später registrierte.

Das Tag `pii` kommt an eine Spalte oder an einen Eigenschaftsbegriff, nie an eine Quelle, eine Tabelle, eine Klasse oder ein Konzept. Ein Begriff muss definiert und nicht außer Dienst gestellt sein, um es zu tragen. Das Markieren eines Entwurfsbegriffs wird mit „Definieren Sie *Begriff*, um ihn zu markieren“ abgelehnt, und Sie können die Definition eines markierten Begriffs nicht leeren, bevor Sie die Markierung aufheben.

Jede Spalte eines markierten Begriffs trägt `pii`, und `sensitive` dazu, überall, wo Tags gelesen werden: Governance, Maskierung, Sensitivität, Export und die Tag-Views. Nichts wird auf die Spalten kopiert. Eine auf den Begriff verschobene Spalte trägt das Tag sofort, und eine davon wegverschobene nicht mehr. Jede Spalte sagt, woher ihr Tag stammt: an der Spalte gesetzt, mit Begründung, oder über einen Begriff, unter dessen Nennung.

Markieren Sie eine Spalte, deren Begriff eine Eigenschaft ist, fragt die UI, ob die anderen Spalten mit derselben Bedeutung ebenfalls markiert werden sollen.

- **Ja** setzt das Tag auf den Begriff.
- **Nur diese** markiert nur die Spalte und fragt nach dem Grund.

Eine Spalte, die über ihren Begriff PII ist, kann nicht für sich allein entmarkiert werden. Heben Sie die Markierung am Begriff auf, oder verschieben Sie die Spalte auf den Begriff, der zu ihrem Inhalt passt.

### Verschiebungen, die Tags verlieren {: #moves-that-lose-tags }

Eine Spalte auf einen anderen Begriff zu verschieben kann ihr ein Tag nehmen: `customers.email` von `customer email` (markiert mit `pii`) auf einen unmarkierten Begriff zu verschieben, ließe die E-Mail-Spalte ungeschützt. Jede Verschiebung durch einen Menschen wird vorher geprüft. Das umfasst das Beanspruchen oder Verschieben einer Spalte, das Außerdienststellen eines Begriffs sowie das Speichern oder Löschen einer Beziehung, die Schlüsselspalten neu zuordnet.

Würde eine Spalte `pii` ganz verlieren, wird die Verschiebung abgelehnt, bis Sie entscheiden. Die UI zeigt eine Zusammenfassung dessen, was jede verschobene Spalte verliert und gewinnt, Verluste zuerst.

- **Verschieben** führt die Verschiebung aus, wobei jedes verlorene Tag bestätigt wird.
- **Verschieben und markieren** überträgt die verlorenen Tags in derselben Änderung auf den Begriff, auf den die Spalten wandern.
- **Abbrechen** verschiebt nichts.

Eine Spalte, die das Tag über ihre eigene Markierung behält, braucht keine Bestätigung. Verschiebungen, die das Modell selbst vornimmt, etwa das Neuableiten der Spalten eines Entwurfs, ändern nie ein geerbtes Tag.

## Zu prüfen {: #to-review }

Die Ontologie meldet, was Ihre Aufmerksamkeit braucht, als Befunde. Ein Begriff mit einem Befund ist **Zu prüfen**, und die Begriffsliste filtert danach. Ein einfacher Entwurf ist kein Befund; Entwürfe sind der gewöhnliche Zustand der Ontologie. Ein Begriff, der Daten nur über Entwürfe erreicht, ist einer, denn eine einzige Freigabe würde ihn vor Agenten bringen.

| Befund | Was er bedeutet | Die Lösung |
| --- | --- | --- |
| Unverbundene Tabellen | Eine Klasse verankert mehrere Tabellen, die nicht alle durch Dieselbe-Entität-Beziehungen verbunden sind. | Markieren Sie Dieselbe-Entität-Beziehungen zwischen ihnen. |
| Klasse und Attribut | Eine Klasse hält Spalten, die nicht ihre Schlüssel sind, oder eine Tabelle mit Schlüssel hat keine Klasse, weil ihr Konzept eine Eigenschaft ist. | Verschieben Sie die Spalten auf eine Eigenschaft, oder verankern Sie die Tabelle neu. |
| Tag an falscher Art | Ein Begriff, der keine Eigenschaft ist, trägt ein Tag, das nur eine Eigenschaft an Spalten weitergibt. | Heben Sie das Tag auf. |
| Außerdienststellung blockiert | Ein veralteter Begriff hat sein geplantes Datum für die Außerdienststellung überschritten, hält aber noch Spalten oder Tabellen. | Stellen Sie ihn mit einem Nachfolger außer Dienst. |
| Keine Daten | Ein Konzept im Entwurf erreicht keine Daten. | Verknüpfen Sie es mit einem Begriff, der Daten hält. |
| Freigegeben, nicht verankert | Ein freigegebener Begriff erreicht keine Daten, daher sieht ihn kein Agent. | Verknüpfen Sie ihn mit einem Begriff, der Daten hält. |
| Verankert · Pfad wartet auf Freigabe | Jeder Pfad hinunter zu Daten läuft durch einen Entwurf. Der Befund nennt den Entwurf, dessen Freigabe den Begriff verankert. | Öffnen Sie diesen Begriff und geben Sie ihn frei. |

Befunde melden; sie ändern nichts, bis Sie handeln.

## Metadaten-Export {: #metadata-export }

Die Ontologie wird als Teil des Metadaten-Exports in externe Datenkataloge veröffentlicht. Freigegebene Begriffe werden veröffentlicht, jeder mit seiner Verankerung: `grounded`, `grounded_pending` oder `not_grounded`. Ein Entwurf verlässt Provisa nie. Die Verankerung für den Export wird anhand der Spalten und Tabellen beurteilt, die tatsächlich veröffentlicht werden. Ein Begriff, dessen Spalten alle vom Export zurückgehalten werden, weil ihre Tabellen keine Datenprodukte sind oder technische Filter sie ausschließen, wird mit ihnen zurückgehalten.

Ein freigegebener Begriff, den ein Kurator inzwischen als veraltet markiert hat, wird mit seinem Status, seinem Grund und seinem geplanten Datum für die Außerdienststellung veröffentlicht, sodass jeder Katalog ihn auf seine eigene Veraltet-Markierung abbilden kann.

Beziehungskanten werden nur veröffentlicht, wenn beide Endbegriffe veröffentlicht werden. Spalten-Assets werden unabhängig exportiert, daher verbirgt das Ausschließen eines Begriffs nicht die zugrunde liegenden Daten.

### Einen Begriff vom Export ausschließen {: #excluding-a-term-from-export }

Manche Spalten tragen Technik, etwa ETL-Batch-Kennungen, Zeilenversionen und Ingestions-Zeitstempel. Ein aus einer solchen Spalte abgeleiteter Begriff kann eine zutreffende Definition haben und trotzdem kein Fachvokabular sein. Die Einstellung **Vom Metadaten-Export ausschließen** hält den Begriff und alle auf ihm endenden Beziehungskanten aus den Katalogen zurück, in die Provisa veröffentlicht. Die Spalten selbst werden weiterhin als Assets exportiert.

Entscheidend ist, ob der Fachbereich das Wort verwendet. Eine ETL-Batch-Kennung hat eine klare Bedeutung, die für Engineers in die Ontologie gehört, aber nicht in einen Fachkatalog neben `customer` und `revenue`.

## Arbeiten mit der Ontologie {: #working-with-the-ontology }

Öffnen Sie **Ontologie** in der Navigationsleiste. Das linke Panel listet jeden Begriff mit seiner Rolle, seinem Status, sofern er nicht freigegeben ist, und seinem Verankerungs-Chip. Die Filter **Status** und **Verankerung** schränken die Liste nach beidem ein, das Kästchen **Veraltet** blendet veraltete Begriffe ein oder aus, **Zu prüfen** beschränkt sie auf Begriffe mit Befunden, und **Verwandt mit** beschränkt sie auf Begriffe mit einer Verwandt-mit-Verknüpfung. Klicken Sie auf einen Begriff, um sein Panel zu öffnen.

Das Panel zeigt Rolle, Status und Verankerung des Begriffs nebeneinander, und für eine Eigenschaft ihre PII-Markierung mit der Anzahl der abgedeckten Spalten. Ein Begriff mit gedämpftem ⏚ nennt die Entwürfe, deren Freigabe ihn verankern würde. Ein veralteter oder außer Dienst gestellter Begriff zeigt, wer seinen Status geändert hat und warum. Ein Begriff mit Befunden öffnet sich mit einem Abschnitt **Zu prüfen**: Klicken Sie auf einen Befund, um zu sehen, was gefunden wurde, und die Aktion, die ihn behebt.

### Stewardship-Übersicht {: #stewardship-summary }

Ist kein Begriff geöffnet, zeigt das Panel die Stewardship-Übersicht. Sie listet die Begriffe, die Agenten sehen können, die Begriffe mit gedämpftem ⏚, die jeweils auf eine Freigabe warten, mit dem freizugebenden Begriff, und die freigegebenen Begriffe, die keine Daten erreichen. **Definierte, verankerte Entwürfe freigeben** listet jeden Entwurf, der eine Definition hat und verankert ist; entfernen Sie das Häkchen bei jedem, dessen Definition nicht stimmt, und geben Sie die übrigen gemeinsam frei. Die Zwischenablage-Schaltfläche in der Kopfzeile holt die Übersicht zurück.

### Die Auswirkungen zuerst sehen {: #seeing-the-impact-first }

Freigeben, Außerdienststellen und Binden eines Begriffs ändern jeweils, was Daten erreicht. Vor jeder dieser Aktionen zeigt die Seite, was sie ändern würde: wie viele Begriffe verankert, gedämpft ⏚ oder nicht verankert werden und zu wie vielen Tabellen Agenten einen Pfad gewinnen oder verlieren. Zum Beispiel „Die Freigabe von region verankert 3 Begriffe und gibt Agenten einen Pfad zu 2 weiteren Tabellen“. **Freigeben** öffnet die Vorschau mit ihrer Bestätigungsschaltfläche, **Außer Dienst stellen** zeigt sie unter dem Nachfolger, den Sie wählen, und das Verankern einer Tabelle im Bearbeitungsformular zeigt sie unter der Tabelle, bevor Sie speichern.

**Bearbeiten** öffnet den ganzen Begriff als ein Formular. Speichern wendet alle Änderungen zusammen an.

- **Benennen** Sie den Begriff um, um seinen Wortlaut zu ändern, ohne seine Spalten zu verschieben.
- **Fügen Sie eine Definition hinzu**, indem Sie eine eintippen, oder klicken Sie auf die KI-Entwurfsschaltfläche, um einen Ausgangspunkt aus dem Namen des Begriffs, seinen Spalten und seinen Beziehungen zu erzeugen. Der Entwurf wird erst gespeichert, wenn Sie das Formular speichern.
- **Beanspruchen Sie Spalten**, um Spalten von anderen Begriffen auf diesen zu verschieben, oder wählen Sie **Zu Begriff verschieben** neben einer seiner eigenen Spalten. Jede Spalte gehört zu einem Begriff. Schlüsselspalten folgen ihrer Klasse und können nicht verschoben werden.
- **Verankern Sie Tabellen** in einer Klasse, oder entfernen Sie sie.
- **Markieren Sie PII** an einer Eigenschaft.
- **Fügen Sie eine Beziehung hinzu** und wählen Sie ihren Typ. Ändern Sie den Typ einer bestehenden Kante an Ort und Stelle, statt sie zu löschen und neu hinzuzufügen.
- **Weisen Sie Experten zu**, per Benutzer-ID, als `expert` oder `author`.

Neben Bearbeiten bietet das Panel die Statusaktionen, die der Begriff erlaubt: **Freigeben** für einen definierten Entwurf, **Als veraltet markieren** für einen freigegebenen Begriff, **Außer Dienst stellen** für jeden Begriff in Betrieb und **Wiederherstellen** für einen veralteten oder außer Dienst gestellten.

Im ✨-Menü füllt **Definitionen** jede leere Definition in einem Durchgang und überschreibt nie den Text eines Menschen. **Beziehungen** fügt typisierte Kanten über die Begriffsliste hinzu; Vorschläge, die unbekannte Begriffe, zweimal denselben Begriff oder einen unbekannten Typ nennen, werden verworfen. Jedes Verwandt mit, das es hinzufügt, wird zur Prüfung gelistet, wie [Wie der Beziehungsassistent wählt](#how-the-relationship-assistant-chooses) beschreibt.

Das Banner **Entwurf** an einem Begriff sagt Ihnen, ob er noch eine Definition braucht (aliasieren Sie die Spalte oder fügen Sie eine hinzu) oder nur Ihre Freigabe. Ein eigenes Banner sagt, wenn ein Begriff keine Daten erreicht: Verknüpfen Sie ihn mit einem Begriff, der Daten hat.

## Die Ontologie abfragen {: #querying-the-ontology }

Die Ontologie ist ein Schema, das Sie abfragen können, namens `ontology`, neben dem Schema `meta`, das die Struktur des Modells beschreibt. Seine Tabellen beginnen mit einem Unterstrich, sodass die eigene Tabelle einer Klasse nie einen Namen mit einer von ihnen teilt.

| Tabelle | Eine Zeile pro |
|---|---|
| `_terms` | Begriff, mit seinem Status, seiner Verankerung, seiner Definition und ob er eine Klasse, eine Eigenschaft oder beides ist |
| `_term_refs` | Bindung einer Eigenschaft an eine Spalte, mit der Tabelle und Domäne der Spalte |
| `_term_edges` | Beziehung zwischen zwei Begriffen |
| `_domains` | Domäne, zu der ein Begriff gehört |
| `_experts` | Einem Begriff zugewiesener Experte oder Autor |
| `_axioms` | Aussage der Ontologie in RDFS, OWL und SKOS |
| `_closure` | Begriff, gepaart mit jedem Begriff, von dem er eine Art ist, in beliebiger Tiefe, und mit sich selbst |

Eine Bindung verknüpft einen Begriff mit einer Spalte im Schema `meta`, sodass Sie `ontology._term_refs` mit `meta.table_columns` und `meta.registered_tables` verbinden können. Die Tabellen der Ontologie sind schreibgeschützt. Sie ändern sich, während Sie Begriffe kuratieren.

Sie lesen einen Begriff nur, wenn Sie eine seiner Domänen erreichen. Ein Begriff ohne Domäne ist für alle sichtbar, ebenso ein unternehmensweiter Begriff (`*`). Eine Beziehung oder ein Axiom ist sichtbar, wenn beide Begriffe es sind.

### Die Ontologie in SPARQL {: #the-ontology-in-sparql }

SPARQL beantwortet die Ontologie in den Standardvokabularen, sodass ein RDF-Werkzeug sie liest wie jede OWL-Ontologie:

- eine Klasse ist eine `owl:Class`, und eine Art-von-Beziehung zwischen Klassen ist `rdfs:subClassOf`;
- eine Eigenschaft ist eine `owl:DatatypeProperty`, oder eine `owl:ObjectProperty`, wenn jede Spalte, die sie bindet, ein Schlüssel zu einer Tabelle mit Klasse ist;
- die `rdfs:domain` einer Eigenschaft ist die Klasse jeder Tabelle, von der sie eine Spalte bindet, und ihre `rdfs:range` ist die Klasse, zu der ihre Schlüssel führen, oder der XML-Schema-Typ ihrer Spalten;
- eine Art-von-Beziehung zwischen Eigenschaften ist `rdfs:subPropertyOf`;
- jeder Begriff hat seinen Namen als `skos:prefLabel` und seine Definition als `skos:definition`.

Ein Begriff wird über seine Vokabular-IRI benannt, `<your Provisa address>/vocab/term/<name>`, die IRI, die die Zeilen der Tabellen typisiert, die er verankert. Ein außer Dienst gestellter Begriff sagt nichts aus.

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?class ?parent ?label WHERE {
  ?class a owl:Class ; rdfs:subClassOf ?parent .
  ?parent skos:prefLabel ?label .
}
```

Eine Abfrage kann die Ontologie mit den Zeilen verbinden, die Sie lesen. Die Instanzen einer Klasse umfassen die jeder Klasse, die eine Art von ihr ist, in beliebiger Tiefe:

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX term: <https://your-provisa/vocab/term/>

SELECT ?party ?class WHERE {
  ?party a ?class .
  ?class rdfs:subClassOf* term:Party .
}
```

Eine Eigenschaft funktioniert ebenso: `?x ?p ?v . ?p rdfs:subPropertyOf* term:amount` liest die Werte jeder Eigenschaft, die eine Art von `amount` ist. Sie erhalten die Instanzen der Tabellen, die Sie lesen, sodass eine Klasse, deren Tabellen Sie nicht lesen können, keine hinzufügt.

Dieselben Aussagen sind Zeilen von `ontology._axioms` in SQL, und die Art-von-Paare, über jede Tiefe geschlossen, sind Zeilen von `ontology._closure`. GraphQL, Cypher und die anderen Abfragesprachen bieten diese beiden Tabellen nicht an.

## Siehe auch {: #see-also }

- [Metadaten-Export](metadata-export.md) behandelt, wie Begriffe und Beziehungen in externe Datenkataloge veröffentlicht werden.
- [Lineage auf Spaltenebene](lineage.md) behandelt den Lineage-Explorer und wie eine physische Spalte die an sie gebundenen Begriffe meldet.

## Das Ontologie-Schema {: #the-ontology-schema }

Das Systemschema `ontology` macht das Vokabular aus SQL, GraphQL, Cypher, REST und MCP abfragbar, über dieselben Oberflächen, die Sie für Ihre eigenen Daten verwenden. Es steht neben `meta`, das die Struktur des Modells hält; `ontology` hält seine Bedeutung.

Zwei Teile bilden das Schema: Definitions-Views und Klassentabellen.

### Definitions-Views {: #definition-views }

Fünf Views legen den Inhalt des Glossars offen:

| View | Inhalt |
| --- | --- |
| `_terms` | Jeder Begriff: sein Name, seine Rolle (Klasse, Eigenschaft, Konzept), Definition, Status und Verankerung. |
| `_term_refs` | Jede Bindung von Begriff an Spalte: der Begriffsname, die Quelle, die Tabelle und die Spalte. |
| `_term_edges` | Jede Beziehungskante: Quellbegriff, Kantentyp und Zielbegriff. |
| `_domains` | Domänenzugehörigkeit: der Begriffsname und jede Domäne, zu der er gehört. |
| `_experts` | Expertenzuweisungen: der Begriffsname, die Benutzer-ID und die Rolle (`expert` oder `author`). |

Eine sechste View, `_axioms`, hält jedes RDFS/OWL/SKOS-Tripel, das das Schema ausdrücken kann, eine Zeile pro Tripel, mit den Spalten `subject`, `predicate`, `object`, `object_iri` und `object_literal`. `object` benennt das Objekt, wenn es ein Begriff ist; `object_iri` hält es, wenn es eine IRI ist, und `object_literal`, wenn es ein Literal ist. Eine der drei hält den Wert in jeder Zeile. Werkzeuge, die Tripel gegenüber tabellarischen Zeilen bevorzugen, lesen von hier.

Eine **Begriffsbindung**, also ein an eine Spalte gebundener Begriff, festgehalten in `_term_refs`, unterscheidet sich von einer Befehlsbindung. Eine Begriffsbindung sagt, was eine Spalte bedeutet; eine Befehlsbindung verdrahtet eine Funktion in die Abfrage-Pipeline.

### Klassentabellen {: #class-tables }

Jeder Klassenbegriff, der nicht außer Dienst gestellt ist, wird zu einer View im Schema `ontology`, benannt nach diesem Begriff, mit Unterstrichen statt Leerzeichen. Eine in einer Tabelle verankerte Klasse ergibt eine View; eine in mehreren Tabellen verankerte Klasse ergibt die Vereinigung dieser Tabellen.

In einer Vereinigungs-View erscheinen die Zeilen jeder Quelltabelle. Die Spalte `_source_table` gibt an, aus welcher Tabelle jede Zeile stammt: Sie hält den qualifizierten Namen der Tabelle, `domain.table`. Eine Eigenschaftsspalte, die eine Quelltabelle nicht bindet, ist für deren Zeilen `NULL`. Eine Eigenschaft, die an mehrere Spalten einer Tabelle gebunden ist, wird zu einer Array-Spalte.

Jede Spalte in einer Klassen-View entspricht einer gebundenen Eigenschaft. Die View wird über dieselbe Pipeline gesteuert wie jeder andere Lesezugriff.

**Beispiel.** Eine Klasse `customer`, verankert in `sales.customers` und `crm.contacts`, mit einer Eigenschaft `name`, gebunden an `customers.name` und `contacts.full_name`, und einer Eigenschaft `segment`, nur an `customers.segment` gebunden:

```sql
-- rows from both tables; segment is NULL for crm.contacts rows
SELECT _source_table, name, segment
FROM ontology.customer
WHERE segment = 'enterprise';
```

### Leserechte {: #read-rights }

Dieselben Governance-Regeln, die für Ihre eigenen Tabellen gelten, gelten hier.

Ein Begriff erscheint in den Definitions-Views nur, wenn Sie mindestens eine seiner Domänen erreichen. Ein Begriff ohne Domäne ist für alle sichtbar. Eine Bindungszeile erscheint nur, wenn ihre Spalte für Sie ausgeliefert wird. Eine Kante erscheint nur, wenn beide Begriffe für Sie sichtbar sind. Eine Eigenschaftsspalte in einer Klassen-View erscheint nur, wenn ihre zugrunde liegenden Spalten ausgeliefert werden.

Kein besonderer Zugriff ist nötig. Wenn Sie eine Tabelle abfragen können, können Sie die darin verankerten Klassenbegriffe abfragen.

### SPARQL {: #sparql }

Der SPARQL-Endpunkt liefert das Schema `ontology` als Standard-RDFS/OWL/SKOS-Ontologie aus. Jeder Klassenbegriff wird zu einer `owl:Class`, jede Art-von-Kante zu `rdfs:subClassOf`, jeder Eigenschaftsbegriff zu `owl:DatatypeProperty` oder `owl:ObjectProperty` mit `rdfs:domain` und `rdfs:range`, Untereigenschaftskanten werden zu `rdfs:subPropertyOf`, Namen zu `skos:prefLabel` und Definitionen zu `skos:definition`. Dieselben Leserechte gelten.

Die Basis-IRI für alle Begriffe wird aus der Einstellung `server.public_url` abgeleitet, unter dem Pfad `/vocab/term/`. Ein Klassenbegriff namens `customer` hat die IRI `{public_url}/vocab/term/customer`.

Drei Abfragen, wie ein Ontologe sie schreiben würde:

**Alle Untertypen von Party finden, in beliebiger Tiefe:**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?c WHERE {
  ?c rdfs:subClassOf* :Party .
}
```

**Kunden mit einem Segmentwert finden, über die Begriffs-IRI:**

```sparql
PREFIX : <{public_url}/vocab/term/>

SELECT ?x ?s WHERE {
  ?x a :Customer ;
     :segment ?s .
}
```

**Alle an Customer deklarierten Eigenschaften mit ihren Wertebereichen auflisten:**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?p ?range WHERE {
  ?p rdfs:domain :Customer ;
     rdfs:range ?range .
}
```

Ersetzen Sie `{public_url}` durch die öffentliche Adresse Ihrer Bereitstellung, wie in `server.public_url` konfiguriert.
