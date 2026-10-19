# Sicherheitsmodell {: #security-model }

Jede Abfrage durchläuft dieselbe Governance, gleich in welcher Sprache sie geschrieben ist (GraphQL, SQL, Cypher) und auf welchem Weg sie ankommt (REST, gRPC, Arrow Flight, JDBC, WebSocket). Es gibt keinen Weg daran vorbei.

Die Prüfungen laufen der Reihe nach. Eine Anfrage muss jede bestehen, bevor die nächste angesehen wird.

## Schichtenmodell {: #layered-model }

### Schicht 0 — Introspektionsfilterung {: #layer-0-introspection-filtering }

Eine Rolle sieht nur die Tabellen, die ihr `domain_access` erreicht, und die Spalten, die ihre `visible_to`-Regeln zulassen. Alles andere existiert für diese Rolle nicht: Es lässt sich weder abfragen noch autovervollständigen noch erraten. Das GraphQL-Schema, der SQL-Katalog und der Schema-Browser des Abfrage-Editors zeigen alle dieselbe eingeschränkte Sicht.

Siehe [Schema-Sichtbarkeit](#schema-visibility).

### Schicht 1 — Öffentlicher Zugriff {: #layer-1-public-access }

Eine Spalte, die mit dem Scope `public` veröffentlicht ist, wird jeder Rolle der Organisation ausgeliefert, auch einer, deren `domain_access` die Domäne der Tabelle nicht erreicht. Diese Rolle sieht die Tabelle mit ihren öffentlichen Spalten und sonst nichts. Nichts ist öffentlich, bis die Person, die die Tabelle registriert hat, es so festlegt. Siehe [Öffentliche Spalten](#public-columns).

### Schicht 2 — Domänenzugriff {: #layer-2-domain-access }

Jede Rolle hat eine `domain_access`-Liste von Domänen-IDs, und eine Abfrage, die eine Tabelle in einer anderen Domäne berührt, wird abgelehnt, bevor sie läuft. Das ist die grobe Eigentumsgrenze. Eine HR-Rolle erreicht keine Finanztabellen, wie auch immer das SQL geschrieben ist.

Siehe [Rechtemodell](#rights-model).

### Schicht 3 — Zeilenbasierte Sicherheit {: #layer-3-row-level-security }

Sobald der Domänenzugriff geklärt ist, wird der Zeilenfilter der Rolle für jede Tabelle als `WHERE`-Prädikat zu jedem `SELECT` hinzugefügt und gegen die Rohdaten ausgewertet. Ein regionaler Manager, der `SELECT *` auf die gemeinsame Bestelltabelle ausführt, bekommt trotzdem nur die Zeilen seiner Region zurück.

Siehe [Zeilenbasierte Sicherheit (RLS)](#row-level-security-rls).

### Schicht 4 — Spaltensichtbarkeit und Maskierung {: #layer-4-column-visibility-and-masking }

Eine Spalte, deren `visible_to`-Liste Ihre Rolle nicht enthält, fällt aus der Ausgabe. Bei einer maskierten Spalte werden die Werte ersetzt, bevor sie den Server verlassen, durch einen regulären Ausdruck, eine Konstante oder eine Kürzung. Das gilt in jeder Abfragesprache und jedem Ausgabeformat.

Siehe [Spaltenberechtigungsmodell](#column-permission-model) und [Spaltenmaskierung](#column-level-masking).

### Schicht 5 — Prädikatschutz {: #layer-5-predicate-guard }

Auf eine maskierte Spalte können Sie nicht filtern: `WHERE` und `HAVING` lehnen sie schon beim Parsen der Abfrage ab. Sonst könnte ein Aufrufer den echten Wert per Binärsuche zurückgewinnen, Filter für Filter, obwohl jedes Ergebnis, das er sah, maskiert war.

### Beziehungs-Governance (V002) {: #relationship-governance-v002 }

Ein SQL-Join muss einer registrierten und genehmigten Beziehung folgen, jeder andere Join wird abgelehnt. Jede Beziehung trägt einen Grund und eine Beschreibung, die Menschen und Agenten sagen, warum es den Pfad gibt. Verstehen Sie das als Governance-Richtlinie, nicht als Sicherheitsgrenze. Die Schichten 2 bis 5 gelten, wie auch immer die Joins aussehen, sodass ein Umgehen der Prüfung nichts preisgibt, was die Rolle nicht auch mit zwei getrennten Abfragen lesen könnte. Umgehungsversuche werden protokolliert.

Es gibt zwei Wege an V002 vorbei. Der erste ist eine Fähigkeit: Eine Rolle mit `ignore_relationships` kann Relationen verknüpfen, die der Katalog nicht abdeckt. Von den mitgelieferten Rollen hat nur `modeler` sie, weil ihre Aufgabe ist, das Modell erst herauszuarbeiten; `analyst` hat sie nicht.

Der zweite braucht zwei Dinge gleichzeitig:

1. **Rollen-Flag**: `relationship_guard: false` in der Rollendefinition (Standard ist `true`).
2. **Opt-out pro Abfrage**: der Kommentar `--relationship-guard=false` im SQL.

Jedes für sich bewirkt nichts.

**Der Hochsicherheitsmodus fixiert die Prüfung.** Unter `security.mode: high` funktioniert keiner der beiden Wege. `ignore_relationships` und `relationship_guard: false` werden beide ignoriert, und jeder Join muss im genehmigten Katalog stehen. Die Redundanz ist gewollt: Eine Produktionsrolle, der die Fähigkeit versehentlich erteilt wurde, kann trotzdem nicht aus dem Modell ausbrechen.

**GraphQL** überspringt V002. Seine Beziehungen kommen aus dem Modell und sind damit bereits genehmigt.

**SQL und Cypher** wenden V002 standardmäßig an, mit dem oben beschriebenen Opt-out aus zwei Bedingungen.

**pgwire** wendet dieselbe Prüfung an wie SQL. Der Kommentar `--relationship-guard=false` wird entfernt, bevor die Abfrage die Datenbank erreicht.

---

Die Schichten stapeln sich. Eine Rolle mit Domänenzugriff, Zeilenfiltern und maskierten Spalten hat alle gleichzeitig in Kraft. Eine neue Quelle, Spalte oder Beziehung bedeutet nicht, jede Regel anzufassen, denn jede Schicht wird für sich konfiguriert und gilt für jede Abfrage, die erreicht, was sie regelt.

---

## Rechtemodell {: #rights-model }

Unabhängig vergebene Capabilities mit optionaler Rollenhierarchie über `parent_role_id`. `admin` gewährt alles.

| Capability | Beschreibung |
| ----------- | ------------- |
| `source_registration` | Datenquellen registrieren |
| `table_registration` | Tabellen und Spalten registrieren |
| `create_relationship` | FK-Beziehungen definieren |
| `access_config` | RLS und Maskierung konfigurieren |
| `query_development` | Abfragen ausführen |
| `write` | Registrierte Mutationen aufrufen (grobes Gate; siehe Mutationsautorisierung) |
| `full_results` | Stichprobenlimits umgehen |
| `ignore_relationships` | Governance der Beziehungen (V002) umgehen. Unter den Systemrollen nur von `modeler` gehalten und im Hochsicherheitsmodus vollständig ignoriert |
| `admin` | Superuser — gewährt alles |

### Rollenvererbung {: #role-inheritance }

Rollen können über `parent_role_id` von einer übergeordneten Rolle erben, gesetzt in der Konfiguration oder auf der Seite Sicherheit als „Erbt von". Die Kette wird beim Start eingefaltet: Ein Kind hält die Vereinigung der Capabilities und des Domänenzugriffs seiner Vorfahren, jede Spalten-, Metrik-, Funktions- und Webhook-Freigabe, die einen Vorfahren nennt, und die RLS-Regeln der Vorfahren pro Tabelle, wobei das Kind Vorrang hat. Die nächstgelegene Rolle in der Kette mit einer Regel für eine Tabelle liefert das Prädikat dieser Tabelle, und eine für das Kind gespeicherte Regel ersetzt die des Elternteils für diese Tabelle.

```yaml
roles:
  - id: basic_user
    description: Queries the public domain.
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    description: Analyses sales; reads full results.
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```

## Rollen: gewährt, abgeleitet und meta {: #roles-granted-derived-and-meta }

Drei Arten von Rollen wirken im Zugriffsmodell von Provisa zusammen. Die Seite Rollen listet die ersten beiden gemeinsam auf; eine Meta-Rolle wird nie gelistet.

### Gewährte Rollen {: #granted-roles }

Eine gewährte Rolle wird einem Benutzer direkt zugewiesen. Sie hält Rechte: Capabilities, Domänenzugriff, Maskierungseinträge, Zeilenfilter. Die Vererbung (`parent_role_id`) faltet beim Start die Rechte eines Vorfahren in das Kind. Rechte kommen nur hinzu; es gibt keine Subtraktion. Um einen Leser einzuschränken, gewähren Sie einem Kompositum, das sowohl die Rolle als auch eine Bedingung nennt, oder setzen Sie einen Maskeneintrag vor den freizügigen.

### Ablaufende und ungenutzte Zuweisungen {: #expiring-and-unused-grants }

Eine Zuweisung, die Provisa hält, kann enden. Eine Zuweisung mit Ablaufzeitpunkt wird ab diesem Zeitpunkt bei keiner Anfrage mehr gehalten. Eine Rolle kann `inactivity_days` angeben: Eine Zuweisung dieser Rolle, von der länger als so viele Tage keine Anfrage abhing, gezählt ab dem späteren Zeitpunkt aus letzter Nutzung und Erstellung, wird entfernt. Rollen aus den Claims oder Rollenzuordnungen eines Identitätsanbieters befristet der Anbieter; legen Sie ihren Ablauf dort fest. Abgeleitete Rollen und Meta-Rollen laufen nie ab.

Eine Anfrage hing von einer Rolle ab, wenn diese Rolle sie allein bedient hat oder, wenn die Anfrage als mehrere Rollen handelt, wenn der Rolle eine Tabelle gewährt ist, die die Anweisung gelesen hat. Das Handeln als „Alle“ zählt nicht jede gehaltene Rolle als genutzt. Die letzte Nutzung einer Zuweisung wird höchstens einmal pro Tag erfasst.

Alle 15 Minuten löscht der Durchlauf jeder Organisation die abgelaufenen und ungenutzten Zuweisungen und verzeichnet jede im Verwaltungsprotokoll als `role.expired` oder `role.removed_inactive`. Wo ausgehende E-Mail eingerichtet ist, auch in einer Bereitstellung mit nur einer Organisation, erhalten der Inhaber und die Administratoren der Organisation `auth.role_grant_notice_days` Tage (Standard 7) vor dem Ende einer Zuweisung eine E-Mail, und die Administratoren erhalten eine, wenn eine entfernt wird; ist keine E-Mail eingerichtet, ist der Protokolleintrag der Nachweis. Diese Hinweise werden unabhängig von der E-Mail-Einstellung eines Benutzers versendet.

### Abgeleitete Rollen {: #derived-roles }

Eine abgeleitete Rolle stammt aus den Attributen eines Benutzers. Die Org deklariert, welche Attribute zu Rollen werden (`derived_roles`), jedes mit einer Beschreibung. Für jedes deklarierte Attribut `a`, dessen Wert `v` die Attribute des Benutzers liefern, hält der Benutzer die Elternrolle `a` und ihre Wertrolle `a__v`: `region = eu` ergibt `region` und `region__eu`. Kein Wert bedeutet keine der beiden Rollen.

Die Auflösung der Attribute zählt. Die eigene Aufzeichnung der Org über ein Mitglied hat Vorrang vor dem, was der Identity Provider liefert: Nennen beide dasselbe Attribut, wird der Wert der Org verwendet, und der des Providers wird als überschrieben markiert.

Abgeleitete Rollen tragen keine Rechte und werden nie von Hand zugewiesen. Eine Freigabeliste, eine Maske oder eine Zeilenregel nennt sie genau so, wie sie eine gewährte Rolle nennt; eine Anfrage nennt sie in `X-Provisa-Role` und handelt als sie.

Auf der Seite Rollen ist jedes deklarierte Attribut eine Elternrolle mit der Markierung *abgeleitet*; ihre Wertrollen werden darunter aufgelistet, sobald ihre Werte auftreten. **+ Rolle → Aus Attribut abgeleitet** deklariert eine. Ihr Panel zeigt das Attribut, seine Beschreibung, die bisher gesehenen Werte und jede Maske, Zeilenregel und Freigabe, die es oder eine seiner Wertrollen nennt. Es hat keine Freigabe-Steuerelemente. Die Beschreibung einer Wertrolle wird erzeugt: „Gehalten, wenn region des Benutzers eu ist“.

Deklarieren Sie sie in der Konfiguration als Liste:

```yaml
derived_roles:
- attribute: region
  description: The subsidiary a member works for; region__eu marks EU staff.
```

Denken Sie an ein JWT: Seine Claims sind Fakten über die Person. Abgeleitete Rollen machen ausgewählte Fakten zu Rollen, sodass eine Freigabe sie ansprechen kann, ohne für jede Kombination eine Rolle anzulegen.

### Meta-Rollen {: #meta-roles }

Wenn eine Anfrage mehr als eine Rolle nennt, baut Provisa eine flüchtige Meta-Rolle mit der Kennung `meta:<ids sorted, joined by +>`. Sie ist ein einziges Kind jeder Rolle der genannten Menge. Die ganze Pipeline läuft dann mit ihr als gewöhnlicher einzelner Rolle.

Die Rechte sind die Vereinigung der Rechte aller Mitglieder. Zeilenfilter sind das OR des aufgelösten Filters jedes Mitglieds; hat irgendein Mitglied, das eine Tabelle liest, keinen Filter, hat auch die Meta-Rolle keinen. Kein Filter bedeutet alle Zeilen. Eine Meta-Rolle wird nie gespeichert, gelistet oder exportiert. Sie wird aus ihren Mitgliedern neu gebaut, wenn sich das Modell ändert.

Die Spalten jeder Zeile werden nach den Rollen maskiert und gezeigt, deren Zeilenfilter diese Zeile zugelassen hat: Von diesen Rollen gilt die am wenigsten einschränkende Lesart der Spalte, und eine Rolle, die die Zeile nicht zugelassen hat, trägt nichts dazu bei, wie sie gelesen wird. Masken greifen dort, wo die Tabelle gelesen wird, vor jedem Join und jeder Aggregation, sodass eine Aggregation die maskierten Werte liest. Schreibvorgänge werden ebenso geprüft: Eine Spalte wird in einer Zeile nur geschrieben, wo eine Rolle, die diese Spalte gewährt, die Zeile zulässt.

Der Audit-Eintrag nennt jede handelnde Rolle, nicht die ID der Meta-Rolle. Da eine Meta-Rolle nie gespeichert wird, hat sie keine eigene Beschreibung: Wo immer eine angezeigt wird, ist ihre Beschreibung die ihrer Mitglieder, jede ID mit ihrer eigenen.

### Beschreibungen {: #descriptions }

Jede Rolle und jede Zeilenregel sagt, wofür sie da ist. Eine Zugriffsprüfung liest, wer was sehen darf und warum. Eine Rolle oder Regel, die niemand erklären kann, kann auch niemand prüfen; deshalb ist die Beschreibung Pflicht: Eine Rolle, eine abgeleitete Elternrolle oder eine Zeilenregel mit leerer Beschreibung wird vom Laden der Konfiguration, von der Admin-API und von der UI abgelehnt, mit Begründung. Die Listen Rollen und Zeilenregeln zeigen jede Beschreibung, und ihre Detail-Panels zeigen und bearbeiten sie.

Eine Rolle hat eine Beschreibung, nicht eine pro Domäne. Beschreibt eine allgemeine Rolle ihre Aufgaben in einer bestimmten Domäne nicht, braucht diese Domäne eine eigene Rolle, meist ein Kind der allgemeinen Rolle (`parent_role_id`), das deren Rechte erbt und beschreibt, was es dort tut.

### Wie eine Anfrage Rollen nennt {: #how-a-request-names-roles }

Ein Client sendet eine Rolle oder eine kommagetrennte Menge in `X-Provisa-Role`. Jede genannte Rolle muss gehalten werden. Eine Rolle, die der Identität nicht zugewiesen ist, wird namentlich abgelehnt.

Rechte und Daten folgen nur den Rollen, als die die Anfrage handelt: Eine Rolle zu halten heißt nicht, als sie zu handeln. Eine Rolle ist diese Rolle; zwei oder mehr handeln als ihre Meta-Rolle. Eine handelnde Rolle, die die Rechte jeder gehaltenen Rolle mitbrächte, bedeutete nichts; so erhält eine Anfrage das geringste Privileg, und der Audit-Eintrag nennt genau, was gehandelt hat.

Genannte abgeleitete Rollen gehen als Qualifikatoren ein. Ein Qualifikator hat keine eigenen Rechte, keinen Domänenzugriff, keine Masken und keine Zeilenfilter, also ist er kein Mitglied der Rechte der Meta-Rolle: Als Mitglied ohne Masken würde er jede Spalte demaskieren, und als eines, das keine Tabelle liest, würde er jedes OR von Zeilenfiltern beenden. Er geht nur in die Freigaben ein: Eine Freigabe oder ein Maskeneintrag, der `region__eu` nennt, erreicht eine Anfrage, die diesen Qualifikator neben einer gewährten Rolle nennt.

Ein MCP-Agent handelt nie über die Rollen hinaus, die ihm gegeben wurden. Ein entfernter Agent nennt nur Rollen, die der Prinzipal seines Tokens hält. Ein lokaler stdio-Agent nennt nur Rollen innerhalb der in `mcp.role` (`PROVISA_MCP_ROLE`) festgelegten Menge und handelt als diese Menge, wenn er keine nennt; jede andere Rolle wird namentlich abgelehnt.

Ein Agent fragt mit dem MCP-Werkzeug `describe_role`, als welche Rolle er handelt. Die Antwort ist diese Rolle und wofür sie da ist (eine Meta-Rolle mit jedem ihrer Mitglieder) sowie die Rollen, zu denen der Agent wechseln darf, jede mit ihrer Beschreibung. Sie enthält nie eine Zeilenregel, Maske, Freigabe oder Bedingung. Provisa sagt einem Agenten nie, warum ein Ergebnis eingeschränkt wurde, und ein Ergebnis trägt kein „gefiltert“-Kennzeichen: Die Daten einer Rolle sind ihre ganze Sicht, und ein Grund für das Fehlende würde dazu einladen, danach zu sondieren.

### Was `+` bedeutet {: #what-means }

`+` erscheint an zwei Stellen, mit jeweils anderer Bedeutung.

**In einem Freigabeeintrag.** `analyst+region__eu` in einer `visible_to`- oder `to`-Liste bedeutet, dass der Leser alle davon hält: `analyst` UND `region__eu` gleichzeitig. Ein Eintrag, der kein Kompositum ist, ist eine einzelne Rolle. Über die Einträge einer Liste hinweg gilt die Regel „irgendeiner": Ein Leser hält die Freigabe, wenn er irgendeinen Eintrag hält. UND innerhalb, ODER übergreifend.

**In einer Meta-Rollen-ID.** `meta:analyst+engineer` ist die Kennung des Servers für die Meta-Rolle der Menge {analyst, engineer}. Ihre Rechte sind die Vereinigung beider. Ein Client nennt die Meta-Rollen-ID nie direkt; er nennt die Rollen, die er hält, und der Server baut die Meta-Rolle.

### ABAC-Spaltenregeln als einfaches RBAC {: #abac-column-rules-as-plain-rbac }

Eine attributbasierte Richtlinie fragt: „Darf dieser Benutzer mit diesen Attributen diese Spalte sehen?" RBAC fragt: „Welche Rollen dürfen diese Spalte sehen?" Zusammengesetzte Freigaben und abgeleitete Rollen machen daraus dieselbe Frage.

„Diese E-Mail nur für einen Analysten in der EU demaskieren" ist ein Maskeneintrag `{to: [analyst+region__eu], mask: null}`. Jeder Leser, der sowohl die Rolle `analyst` als auch die abgeleitete Rolle `region__eu` hält, liest die Spalte unmaskiert. Keine Richtliniensprache. Keine Rolle pro Kombination.

Der Kompromiss ist Berechnung statt Aufzählung. Ein kleiner Rollenkatalog hält den Pflegeaufwand gering, aber „Wer darf diese EU-E-Mail sehen?" wird zu einer berechneten Antwort: Zusammengesetzte Einträge werden gegen die Zuweisungen und Claims jedes Benutzers ausgewertet. Provisa kann das in beide Richtungen beantworten, weil das Modell eine Definition ist. Die Zugriffsprüfung des Spalteneditors (`POST /admin/masks/check`) führt diese Auswertung für eine gewählte Menge von Rollen aus.

### Vorteile {: #benefits }

- Eine ABAC-Spaltenregel wird zu einem RBAC-Freigabeeintrag.
- Ein kleiner Rollenkatalog statt einer Rolle pro Kombination.
- Eine Definition, die die Regeln jeder Region koordiniert.
- Geringstes Privileg und ein genauer Audit-Eintrag pro Anfrage.

### Bewährte Praktiken {: #best-practices }

- Halten Sie den Rollenkatalog so klein, wie die Organisation es zulässt.
- Erben Sie innerhalb einer Achse (Seniorität, Freigabestufe). Kombinieren Sie über unabhängige Achsen (Rolle + Region, Rolle + Abteilung).
- Setzen Sie die spezifischsten Maskeneinträge an den Anfang. Ein früherer Eintrag gewinnt, auch wenn der Leser einen späteren hält.
- Prüfen Sie den wirksamen Zugriff mit `POST /admin/masks/check`, bevor Sie eine Kombination in Produktion bringen.

## Spaltenberechtigungsmodell {: #column-permission-model }

Jede Spalte hat ein Berechtigungsmodell, das Lese-, Schreib- und Maskierungszugriff pro Rolle steuert.

### Drei Sichtbarkeitsstufen {: #three-tier-visibility }

| Stufe | Bedingung | Ergebnis |
| ------ | ----------- | -------- |
| **Verborgen** | Rolle nicht in `visible_to` | Spalte fehlt in Schema und Katalog |
| **Maskiert** | Rolle in `visible_to`; der erste Maskeneintrag, den sie hält, hat eine `mask` | Spalte sichtbar, Wert in der Ausgabe ersetzt |
| **Unmaskiert** | Rolle in `visible_to`; der erste Maskeneintrag, den sie hält, hat keine `mask`, oder sie hält keinen | Voller Lesezugriff |

### Öffentliche Spalten {: #public-columns }

Jede Spalte hat einen `scope`, gesetzt in der Modelldatei, über die Admin-API oder auf der Seite Tabellen. Die drei Geltungsbereiche sind danach geordnet, wem sie dienen: `restricted`, dann `domain`, dann `public`. Ein Tabellenbearbeiter (`table_registration` in der Domäne der Tabelle) darf den Geltungsbereich einer Spalte verengen. Ihn zu erweitern — `restricted` zu `domain` oder `public`, `domain` zu `public` — stellt die Spalte mehr Rollen bereit und verlangt das Recht, das ihre Freigabe verlangt: `column_grant` oder `access_config` in der Domäne der Tabelle. Eine Spalte, die registriert wird, wird mit `domain` verglichen, daher ist eine Registrierung als `public` eine Erweiterung. Bei einer Spalte mit einem sensiblen Tag verlangt jede Änderung des Geltungsbereichs `sensitive_data`. Eine abgelehnte Änderung nennt die Spalte und das Recht.

| Geltungsbereich | Bereitgestellt für |
| ------- | ----------- |
| `domain` (Standard) | Rollen, die die Domäne der Tabelle erreichen, wie `visible_to` es sagt. Ein leeres `visible_to` ist jede solche Rolle. |
| `public` | Dieselben Rollen und außerdem Rollen, die die Domäne der Tabelle nicht erreichen. |
| `restricted` | Nur die Rollen, die `visible_to` nennt. Ein leeres `visible_to` ist niemand. |

Eine Spalte zu veröffentlichen entfernt die Domänenbedingung für diese Spalte und nichts sonst:

- Einer Rolle außerhalb der Domäne wird die Tabelle nur mit ihren öffentlichen Spalten bereitgestellt. Die anderen Spalten der Tabelle und Tabellen, die nichts veröffentlichen, bleiben aus ihrem Schema, ihrem Katalog und ihrem SQL heraus.
- Das eigene `visible_to` der Spalte entscheidet weiterhin, wer: Eine öffentliche Spalte, die einer Rolle freigegeben ist, ist nur für diese Rolle öffentlich.
- Masken, Zeilenfilter, Zeilenlimits und der Beziehungsschutz gelten wie innerhalb der Domäne. Ohne Zeilenfilter für die Rolle werden die veröffentlichten Spalten ungefiltert gelesen; ein Zeilenfilter auf der Tabelle oder ihrer Domäne gilt weiterhin.
- Eine Beziehung wird bereitgestellt, wenn die Spalten ihrer beiden Enden es werden.
- Eine Parameterspalte einer API-Tabelle (ein Pfad- oder Abfrageargument) ist keine Daten und veröffentlicht nichts.
- Die Domäne `ops` braucht für jede Spalte eine ausdrückliche Freigabe; nichts darin wird domänenübergreifend bereitgestellt, gleich welcher Geltungsbereich.

Die Regel ist auf jeder Oberfläche dieselbe, weil jede das kompilierte Schema der Rolle liest: GraphQL, SQL (HTTP, pgwire, Flight, JDBC), Cypher und Bolt, gRPC, REST und JSON:API, die Flight- und MCP-Kataloge und die Admin-Listen. Ein Geltungsbereich, der keiner der drei Werte ist, wird beim Speichern einer Spalte abgelehnt.

### Schreibberechtigungen {: #write-grants }

Das Lesen einer Spalte steuert `visible_to` (leer bedeutet, alle Rollen können lesen). Das Schreiben wird je Tabelle über `write_grants` gesteuert, nicht je Spalte. Eine Tabelle trägt standardmäßig keine Berechtigungen, daher kann keine Rolle in sie schreiben.

Eine Schreibberechtigung legt fest, was eine Rolle mit einer Operation (`insert`, `update` oder `delete`) in eine Tabelle schreiben darf:

| Feld | Zweck |
| ------- | --------- |
| `columns` | Die Spalten, die ein `INSERT` auflisten oder ein `UPDATE` setzen darf |
| `rows` | Die Zeilen, die ein `UPDATE` oder `DELETE` berühren darf (ein SQL-Prädikat) |
| `check` | Was jede von einem `INSERT` geschriebene oder von einem `UPDATE` hinterlassene Zeile erfüllen muss (ein SQL-Prädikat) |
| `presets` | Spalten, die die Berechtigung selbst setzt, aus einer Sitzungsvariable, einem Literal oder dem Zeitpunkt des Schreibvorgangs. Die Anweisung darf sie nicht liefern. |

Lassen Sie `rows` oder `check` weg, übernimmt die Berechtigung den Lese-Zeilenfilter der Rolle auf der Tabelle, sodass eine Rolle die Zeilen schreibt, die sie liest. `"TRUE"` steht für jede Zeile.

Eine einzige Zulassung entscheidet über jeden Schreibvorgang auf jeder Oberfläche, in dieser Reihenfolge: Die Rolle besitzt das Recht `write`; sie besitzt eine Berechtigung für die Operation auf der Tabelle; jede Spalte, die die Anweisung schreibt, steht in der Berechtigung; die Presets werden hinzugefügt; `rows` wird per AND in das `WHERE` aufgenommen; und `check` wird auf die neuen Zeilen ausgewertet, bevor etwas an die Quelle gesendet wird. Eine Ablehnung nennt Tabelle, Rolle und Operation.

Weitergehende Anweisungen brauchen weitergehende Berechtigungen. `TRUNCATE` braucht eine `delete`-Berechtigung über jede Zeile. `MERGE` braucht eine `insert`- und eine `update`-Berechtigung über jede Zeile. `INSERT ... ON CONFLICT DO UPDATE` braucht eine `update`-Berechtigung über jede Zeile ohne Presets. `COPY` braucht eine `insert`-Berechtigung ohne Presets.

GraphQL bietet jeder Rolle genau für die Operationen ein Mutationsfeld an, für die sie Berechtigungen besitzt, mit den Spalten der Berechtigung. Eine untergeordnete Rolle erbt die Berechtigungen ihrer übergeordneten Rolle, ihre eigenen zuerst. Eine Meta-Rolle hält die Faltung der Berechtigungen ihrer Mitglieder: die Vereinigung der Spalten, das ODER von `rows` und `check` und nur die Presets, die alle Mitglieder teilen. In der Admin-Oberfläche listet der Abschnitt **Schreibrechte** einer Tabelle ihre Berechtigungen auf und bearbeitet sie.

### Beispiel {: #example }

```yaml
tables:
  - source_id: hr-pg
    domain_id: people
    schema: public
    table: employees
    columns:
      - name: id
        visible_to: []                # all can read
      - name: email
        visible_to: [admin, analyst, viewer]
        masks:
          - to: [admin]               # admin sees raw value (no mask)
          - to: ["*"]                 # everyone else sees it masked
            mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}
      - name: salary
        visible_to: [admin, hr]
        masks:
          - to: [admin, hr]           # admin and hr see real value
          - to: ["*"]                 # all other visible roles see 0
            mask: {type: constant, value: 0}
      - name: created_at
        visible_to: []                # all can read
    write_grants:
      - role: admin
        operation: update
        columns: [email]
        rows: "TRUE"
      - role: hr
        operation: update
        columns: [salary]
      - role: hr
        operation: insert
        columns: [email, salary]
        presets:
          - {column: created_at, source: now}
```

In diesem Beispiel:

- `email`: admin sieht `alice@example.com` und kann es in jeder Zeile aktualisieren; analyst und viewer sehen `a***@example.com` und können nicht schreiben
- `salary`: admin und hr sehen den echten Wert; hr kann ihn in den Zeilen aktualisieren, die hr lesen kann, weil die Berechtigung kein `rows` angibt; jede andere sichtbare Rolle sieht `0`
- `created_at`: alle können es lesen, keine Rolle liefert es, und ein Insert von hr setzt es auf den Zeitpunkt des Schreibvorgangs

## Mutationsautorisierung {: #mutation-authorization }

Eine registrierte Mutation (Remote-GraphQL, OpenAPI, gRPC, Hasura) besteht zwei Prüfungen. Die Rolle muss die globale Fähigkeit `write` haben, und der Befehl muss ihr über `visible_to` zugewiesen sein; eine leere Liste weist ihn jeder Rolle zu. Ein Befehl hat keine `writable_by`-Liste, und die Konfiguration lehnt eine solche mit Namen ab.

Ob etwas ein Schreibvorgang ist, entscheidet sein Vertrag, nicht die Angabe des Aufrufers. Ein `SELECT`, das eine Funktion vom Typ Mutation aufruft, gilt als Schreibvorgang und durchläuft dieselben zwei Prüfungen, sodass Sie keine Mutation als Lesezugriff getarnt ausführen können. Eine Mutation als lesesicher einzustufen, erfordert die Fähigkeit `access_config` und wird als Governance-Entscheidung festgehalten. Keine Anfrage kann sich davon ausnehmen.

## Schema-Sichtbarkeit {: #schema-visibility }

Jede Rolle bekommt ihr eigenes GraphQL-Schema. Tabellen erscheinen nur aus den Domänen in ihrem `domain_access` (`"*"` steht für alle), und eine Spalte, die für die Rolle nicht in `visible_to` steht, fehlt auch im SDL. Was die Rolle nicht lesen darf, sieht sie nicht.

## Sicherheit auf Zeilenebene (RLS) {: #row-level-security-rls }

Ein Zeilenfilter ist ein `WHERE`-Prädikat für eine Tabelle und eine Rolle, eingefügt nach der Kompilierung und vor der Ausführung.

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

Der Filter wird per AND in die `WHERE`-Klausel der Abfrage eingefügt, bei Lesezugriffen ebenso wie bei `UPDATE` und `DELETE`.

Ein Filter liest die Anfrage über `current_setting('provisa.<name>')`. Die Anfrage bindet `user_id` aus der angemeldeten Identität, `role` aus der handelnden Rolle und jeden skalaren Claim des Tokens, kleingeschrieben und mit Unterstrichen statt Bindestrichen. `provisa.user_id` ist immer die angemeldete Identität, und kein Claim kann sie ersetzen.

Hasuras Sitzungsvariablen behalten ihre Namen. `X-Hasura-User-Id` ist `hasura_user_id`, gelesen aus dem Claim `x-hasura-user-id` des Tokens (auf oberster Ebene oder im Hasura-Claims-Objekt, etwa `https://hasura.io/jwt/claims`) oder aus dem für das Mitglied hinterlegten Attribut `hasura_user_id`. Ein importierter Hasura-Filter auf `X-Hasura-User-Id` liest also `provisa.hasura_user_id`.

Unter den Werten der Anfrage liegen die konfigurierten `session_vars` der Rolle, die die Anfrage überschreibt. Eine erbende Rolle übernimmt die Konstanten ihrer Vorfahren unter ihren eigenen, wobei für jeden Namen die nächstgelegene Rolle gewinnt. `user_id`, `role`, `region` und `org` kommen aus der Anfrage oder vom Knoten und lassen sich nicht als Konstanten konfigurieren. Im ungesicherten Modus bindet ein Header `x-provisa-session-<name>` den Wert `<name>`. Eine nirgends gebundene Variable ist NULL, und ein Filter darauf trifft keine Zeile.

Eine Rollenkonstante funktioniert umgekehrt wie eine abgeleitete Rolle. Eine abgeleitete Rolle macht aus einem Wert, den ein Mitglied hat, eine Rolle. Eine Rollenkonstante gibt jeder Anfrage, die als die Rolle gestellt wird, einen Wert, den eine Zeilenregel genauso liest wie einen Claim. Die Konstante kann eine Referenz `${env:NAME}` oder `${secret:NAME}` sein, die derselbe Resolver wie jede andere Referenz auflöst, sobald eine Abfrage sie verwendet. Das Modell behält die Referenz, nie den Wert, und eine Referenz, die sich zu nichts auflöst, lässt die Abfrage scheitern und nennt die Konstante. Aufgelöst ist der Wert Teil der Abfrage: Die eigenen Aufzeichnungen von Provisa darüber deckt die zentrale Schwärzung ab, die Logs einer föderierten Quelle liegen jedoch außerhalb der Reichweite von Provisa.

## Maskierung auf Spaltenebene {: #column-level-masking }

Die Maskierung wird als geordnete Liste an jeder Spalte definiert. Der erste Eintrag, den der Leser hält, entscheidet, was er sieht.

| Maskentyp | Unterstützte Spaltentypen | SQL-Ausdruck |
| ----------- | ----------------------- | --------------- |
| `regex` | Zeichenkette (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Beliebig | Literalwert (`NULL`, `0`, eine eigene Zeichenkette) |
| `truncate` | Datum/Zeitstempel | `DATE_TRUNC(precision, col)` |

Jeder Eintrag hat eine `to`-Liste, die die Leser nennt, auf die er zielt, und eine optionale `mask`. Ein Eintrag ohne `mask` bedeutet unmaskiert für diese Leser. `*` in `to` trifft jeden Leser.

Der Maskenausdruck wird in die SQL-SELECT-Projektion verschoben. Die Datenbank liefert den maskierten Wert; unmaskierte Daten gehen für maskierte Rollen nie über die Leitung. Maskierte Spalten werden außerdem in `WHERE`- und `HAVING`-Klauseln gesperrt (Prädikatsschutz der Schicht 5), um das Erschließen von Werten über Filter zu verhindern.

**Eine Maske gilt für jeden Namen der Spalte, nicht für eine verknüpfte Spalte.** Eine Spalte wird unter ihrem Alias und unter ihrem physischen Namen gleich maskiert. Eine Maske verbirgt keinen Schlüssel, den eine genehmigte Beziehung mit einer Spalte verknüpft, die die Rolle sehen kann: Der Join vergleicht die echten Werte, sodass die verknüpfte Spalte zeigt, was der maskierte Schlüssel enthält. Maskieren Sie die verknüpfte Spalte ebenfalls, oder genehmigen Sie die Beziehung für diese Rolle nicht.

**Kein gehaltener Eintrag bedeutet unmaskiert.** Ein Leser, der keinen Eintrag hält, liest die Spalte unmaskiert. Beenden Sie eine Liste mit einem `*`-Eintrag, wenn auf jeden Leser irgendeine Maske angewendet werden soll. Der Spalteneditor warnt, wenn einer Liste ein `*`-Eintrag fehlt (`unlisted_unmasked`).

**Verweigern durch Reihenfolge.** Ein früherer Eintrag gewinnt, auch wenn der Leser zusätzlich einen späteren, freizügigeren hält. Damit Auftragnehmer ein Gehalt immer maskiert sehen, gleich welche anderen Rollen sie haben, setzen Sie den Eintrag für Auftragnehmer vor den unmaskierten Eintrag. Eine gewährte Rolle verliert nie, was ihr Elternteil über die Vererbung gewährt, aber ein früher in der Liste stehender Maskeneintrag gilt zuerst. So funktioniert Einschränkung hier.

**Meta-Rollen.** Die Kette einer Meta-Rolle umfasst die Rollen jedes Mitglieds, deren Vorfahren und alle genannten abgeleiteten Rollen. Ein Eintrag, der irgendeinem Mitglied gewährt ist, erreicht die Meta-Rolle. Ein Eintrag davor, den ein anderes Mitglied hält, gilt weiterhin zuerst.

**Zugriff prüfen.** `POST /admin/masks/check` nimmt eine Maskenliste und eine optionale Menge von Rollen entgegen und liefert, welcher Eintrag für diese Rollen gilt, sowie alle Einträge, die kein Leser erreichen kann. Geschützt durch `masking_config`.

```yaml
# email column: analyst+region__eu sees raw; everyone else sees masked
masks:
  - to: [analyst+region__eu]          # composite: analyst AND in EU region
  - to: ["*"]                         # all others
    mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}

# hire_date column: hr sees raw; all other visible roles see year only
masks:
  - to: [hr]
  - to: ["*"]
    mask: {type: truncate, precision: year}
```

## Regulierung von Aktionsantworten {: #governing-action-responses }

Was eine verfolgte Funktion oder ein Webhook zurückgibt, wird wie die Zeilen einer Tabelle reguliert. Provisa bindet die zurückgegebenen Zeilen als Relation, deren Spalten die deklarierte Ausgabe der Aktion sind: `output_columns` bei einer Funktion, `inline_return_type` bei einem Webhook oder die Objekteigenschaften des Array-`return_schema` einer Funktion. Diese Relation durchläuft dann dieselbe Governance-Stufe wie jeder Tabellenzugriff, über eine VALUES-CTE mit den Zeilen, sodass Zeilensicherheit, Maskierung und Sichtbarkeit für Aktionen nicht neu implementiert werden.

Drei Regeln gelten, in derselben Reihenfolge wie bei einer Tabelle:

- **Zeilenfilter.** Die RLS-Regel der Rolle, die unter dem Namen der Aktion gespeichert ist (`upsertRlsRule` mit `actionName`), oder die Domänenregel der Aktion, wenn sie keine eigene hat. Das Prädikat wird beim Speichern gegen den Vertrag geprüft, wie bei einer Tabellenregel.
- **Spaltensichtbarkeit.** Eine Vertragsspalte, die `visible_to` deklariert, entfällt für jede Rolle, die die Liste nicht nennt. Eine Spalte ohne diese Angabe gehört zur öffentlichen Form der Aktion und bleibt.
- **Masken.** Eine Vertragsspalte trägt `masks`, dieselbe geordnete Liste wie eine Tabellenspalte.

Die Rollenvererbung löst die Kette für alle drei auf. Gibt eine Antwort eine Spalte außerhalb ihres deklarierten Vertrags zurück, scheitert der Aufruf mit 502, statt sie unreguliert durchzureichen. Eine Aktion, die keine Spalten deklariert, gibt einen Skalar zurück, der nichts zu binden hat, und kommt unverändert zurück. Der Testaufruf im Admin-Bereich meldet, welche Filter, Ausschlüsse und Masken griffen, und die regulierte Anweisung landet im Abfrage-Audit-Log.

## Stichproben {: #sampling }

Jede Rolle bekommt eine Stichprobe der Ergebnisse, standardmäßig 100 Zeilen, sofern sie nicht die Fähigkeit `full_results` hat. Die Größe stellen Sie mit der Umgebungsvariable `PROVISA_SAMPLE_SIZE` ein.

## Audit-Protokollierung {: #audit-logging }

Jede Abfrage, die ein Domänen-Asset berührt, wird in `query_audit_log` geschrieben. Das Log gehört der Organisation, nicht der Plattform: Das eigene Register der Plattform mit Organisationen, Benutzern und Einladungen enthält keine Audit-Einträge. Das Log jeder Organisation ist eine Tabelle in deren Schema, in dem Speicher, den die Organisation für ihre Aufzeichnungen in jeder gewählten Region nennt. Eine Organisation ohne Regionen hält es in der Control-Plane-Datenbank des Deployments, neben ihrem Modell.

Eine Zeile hält `tenant_id` (die Organisation), `user_id`, `role_id`, einen SHA-256-Hash des Abfragetexts, den Abfragetext selbst (verschlüsselt), `table_ids` und `table_names`, `source`, `status_code`, `duration_ms`, die Route und die Zeilenzahl, was auf die Anweisung angewendet wurde, und `logged_at` fest. Der Text wird mit dem Schlüssel der Organisation verschlüsselt, wenn sie einen gesetzt hat, sonst mit dem des Deployments. Ein Deployment ohne Verschlüsselungsanbieter speichert ihn unverschlüsselt. In keinem Fall erscheint er in den abfragbaren Ops-Tabellen, und er wird nur für berechtigte Admin-Lesezugriffe entschlüsselt.

Das Log ist in der Datenbank selbst nur anhängbar: PostgreSQL-Regeln machen `DELETE` und `UPDATE` auf `query_audit_log` wirkungslos. Zwei Indizes, `(tenant_id, logged_at)` und `(user_id, logged_at)`, halten organisationsweite und benutzerbezogene Compliance-Abfragen über einen Zeitraum schnell.

Zeilen bleiben dauerhaft erhalten. Provisa hat keine Aufbewahrungseinstellung und bereinigt das Log nie, und da `DELETE` nichts bewirkt, heißt alte Zeilen entfernen, dass ein Betreiber die Daten der Tabelle außerhalb von Provisa löscht oder archiviert.

## Schwärzung {: #redaction }

Was Provisa über seine eigene Arbeit aufzeichnet, durchläuft eine Liste von Schwärzungsregeln, bevor es irgendwohin geschrieben wird. Text, auf den eine Regel passt, wird durch `[REDACTED:<rule>]` ersetzt, das die passende Regel nennt.

Die Liste hat vier Teile:

- **Integrierte Muster.** Kartennummern, die die Luhn-Prüfung bestehen, US-Sozialversicherungsnummern, Bearer-Tokens, Provisa-Zugriffstokens, AWS-, GCP-, Azure-, GitHub- und Slack-Schlüssel sowie private Schlüssel. Sie sind ein Grundstock, keine Garantie.
- **Die Regeln des Deployments**, die in jeder Organisation gelten. Sie zu ändern erfordert das Recht `platform_settings`.
- **Die eigenen Regeln der Organisation.** Sie zu ändern erfordert das Recht `org_settings`.
- **Die automatische Regel** über die Secrets der Organisation: jeder Wert, den Provisas eigener Speicher für die Organisation hält, jedes `${secret:NAME}`, das Modell und Einstellungen der Organisation nennen (auf jedem Secrets-Backend), und jeder `${secret:…}`- oder `${user:…}`-Wert ab seiner ersten Auflösung. Keine Regel muss sie nennen.

Eine Regel ist entweder ein benannter regulärer Ausdruck oder eine benannte `${secret:NAME}`- / `${env:NAME}`-Referenz, die auf den Wert passt, zu dem die Referenz aufgelöst wird. Eine Referenz, die sich nicht auflösen lässt, wird mit Namen auf `ERROR` protokolliert und als nicht aufgelöst angezeigt; die übrigen Regeln gelten weiter. Werte mit weniger als vier Zeichen werden nicht erkannt.

Provisa ist nicht dafür zuständig, jedes sensible Format zu kennen. Die integrierten Muster decken verbreitete Formen ab; eine Organisation ergänzt Muster für ihre eigenen Formate, etwa Personalnummern, Kontokennungen oder interne Hostnamen. Die Schwärzung erfolgt nach bestem Bemühen: Ein Wert in einer Form, die keine Regel beschreibt, geht durch.

Die Regeln stehen auf der Seite **Org-Secrets** unter **Schwärzungsregeln**. Die Seite listet die integrierten Muster, die Regeln des Deployments (mit `platform_settings` bearbeitbar), die Regeln der Organisation, die Secrets der automatischen Regel nur mit Namen und jede Referenz, die sich nicht auflösen lässt. Die Regeln werden beim Start des Servers und beim Aufbau der Laufzeitumgebung einer Organisation geladen und neu geladen, wenn sich Modell oder Einstellungen ändern und wenn ein Secret der Organisation gespeichert oder entfernt wird.

Die Liste gilt an jedem Ausgang, an den Provisa schreibt:

- jeder Span, als eine geschwärzte Kopie, die jedem Exporter übergeben wird, und die Ansicht der letzten Spans;
- jeder Log-Eintrag in der OpenTelemetry-Log-Pipeline von Provisa und jede Zeile, die ein Logging-Handler schreibt, Standardausgabe und Container-Logs eingeschlossen;
- der Abfragetext des Audit-Logs, bevor er gehasht und verschlüsselt wird, sodass weder der Text noch sein Hash einen erkannten Wert preisgibt; und das Detail des administrativen Protokolls;
- die Fehlermeldungen, die ein Client über HTTP, pgwire, gRPC, Arrow Flight und MCP erhält.

Telemetrie und Client-Fehler sind beim Schreiben keiner einzelnen Organisation zugeordnet; sie werden daher mit den Regeln aller Organisationen zusammen geschwärzt. Zwei Fehler gehen ungeschwärzt durch: ein HTTP-Fehlerrumpf größer als 1 MiB und der Fehler eines Arrow-Flight-Streams, der fehlschlägt, nachdem er zu senden begonnen hat.

Eine föderierte Quelle führt ihre eigenen Logs, die Provisa weder schreibt noch erreicht. Aufzeichnungen einer Quelle über die Anweisungen, die Provisa an sie sendet, sind von dieser Quelle zu schützen.

## Statistik über MCP {: #statistics-over-mcp }

Die MCP-Statistik-Tools (`describe`, `correlate`, `regress`, `compare`, `forecast`) führen das SQL, das ein Agent definiert, in der Rolle des Agenten durch die gesteuerte Pipeline, sodass Zeilenregeln, Maskierung und Schwärzung für das gelten, was sie analysieren. Statistik-Tools geben nur Ergebnisse zurück, sodass die Zeilen hinter einer Analyse nie in den Kontext des Modells gelangen: Datenminimierung standardmäßig.

Die Zugriffskontrolle begrenzt, was ein Agent erfahren kann. Um Identitäten außer Reichweite eines Agenten zu halten, erteilen Sie seiner Rolle nur die Spalten, die niemanden identifizieren; eine Rolle, die eine Spalte lesen kann, kann sie aggregieren, und Aggregate über kleine Gruppen können dennoch Einzelpersonen identifizieren.

## Abdeckung sensibler Spalten {: #sensitive-column-coverage }

Die View `sensitive_coverage` in der Meta-Domäne listet jede sensible Spalte der Organisation mit dem Fake-Wert und der Syntheseregel, die sie deklariert, und einem Status: `covered`, `no_fake` oder `inadequate`, wenn eine der Angemessenheitsprüfungen von Provisa fehlgeschlagen ist. Die Prüfungen suchen nach einem Fake-Wert, der echte Werte unverändert lässt, einem Fake-Wert, dessen Art nicht zu dem passt, was die Spalte enthält, und einem Fake-Wert oder einer Regel, die den echten Wert einer anderen sensiblen Spalte liest. Ein Data Steward sieht die Spalten ohne Fake-Wert in den Details jeder Umgebung, und eine Test-(fake)-Umgebung verweigert die Auslieferung von Daten, bis jede sensible Spalte einen hat. Die Prüfungen und die View sind unter [Testumgebungen aufbauen](test-data.md#coverage-and-adequacy-checks) beschrieben.

## Ratenbegrenzung {: #rate-limiting }

Limits pro Rolle legen Sie in `provisa.yaml` fest: Anfragen pro Sekunde, gleichzeitige SSE-Abonnements und gleichzeitige Arrow-Flight-Streams. Sie werden an der API geprüft, bevor irgendetwas kompiliert oder ausgeführt wird, und eine Anfrage über dem Limit bekommt HTTP 429 mit einem `Retry-After`-Header.

Abfragen in natürlicher Sprache (`POST /query/nl`) haben ein eigenes Limit, `nl.rate_limit`, in Anfragen pro Minute und Rolle. Eine Anfrage darüber wird abgewiesen, bevor ein LLM-Aufruf erfolgt.

Die Zähler liegen als gleitende Fenster in Redis (`cache.redis_url`), ohne Zustand pro Instanz, sodass ein Limit gilt, gleich wie viele Provisa-Instanzen Sie betreiben.

## Limit für Abfragekomplexität {: #query-complexity-limit }

Ein Ratenlimit begrenzt, wie viele Anfragen eine Rolle sendet. Das Komplexitätslimit begrenzt, was eine Anweisung verlangen darf.

Jede Anweisung hat einen Komplexitätswert: die Relationen, die sie liest, ihre Joins, die Spalten, die sie auswählt (ein `*` zählt als die Spalten, für die es steht), und die in ihr verschachtelten Abfragen. Eine aus einer Remote-API-Quelle (OpenAPI, Remote-GraphQL, Remote-gRPC) gelesene Relation zählt 10, weil jeder Lesevorgang ein Budget verbraucht, das das entfernte System für alle Nutzer dieser Quelle festlegt.

Das Limit ist `max_query_complexity` im `rate_limit` einer Rolle. Die orgweite Einstellung `limits.max_query_complexity` ist eine Obergrenze für jede Rolle: Eine Rolle darf ein niedrigeres Limit setzen, ein höheres hat keine Wirkung. Keines von beiden ist standardmäßig gesetzt.

```yaml
roles:
  - id: analyst
    rate_limit:
      max_query_complexity: 200
```

Der Wert wird gemessen, nachdem eine Anfrage geparst und bevor sie reguliert oder ausgeführt wird, sodass ein Limit auf jeder Schnittstelle gilt, über die eine Anweisung kommen kann: GraphQL, SQL über HTTP, pgwire, Arrow Flight, gRPC, Cypher, JSON:API und MCP. Eine Anweisung über dem Limit wird mit ihrem Wert, dem Limit und dem, was sie verlangt hat, abgelehnt. HTTP-Schnittstellen antworten mit 413. Die Ablehnung wird als Richtlinienverweigerung erfasst.

## Authentifizierung {: #authentication }

Austauschbare Auth-Provider:

| Provider | Credential | Einsatzfall |
| ---------- | ----------- | ---------- |
| `none` | Header X-Provisa-Role | Nur Entwicklung |
| `basic` | Lokale Konten (bcrypt) + Sitzungs-JWT | Eigenständige Deployments |
| `firebase` | Firebase-ID-Token | Produktion |
| `keycloak` | Keycloak-JWT | Unternehmen |
| `oauth` / `oidc` | OIDC-JWT | PingFed, Okta, Azure AD, Auth0 |
| `ldap` | Verzeichnis-Benutzername und -Passwort (LDAP oder Active Directory) | Unternehmensverzeichnisse |
| `saml` | Signierte SAML-2.0-Assertion, dann ein Sitzungs-JWT | Unternehmensweites Single Sign-on |
| `simple` | bcrypt + JWT | Tests |

`ldap` meldet einen Benutzer per Search-then-Bind an: Provisa bindet als Service-Konto, findet den einen Eintrag, den der Benutzername benennt, und bindet dann als dieser Eintrag mit dem vorgelegten Passwort, sodass das Verzeichnis es prüft. Die Gruppen des Benutzers werden zu seinen Rollen. Das Konto und seine Gruppen werden bei jeder Rückkehr eines Sitzungs-Tokens neu gelesen, sodass ein aus dem Verzeichnis oder aus einer Gruppe entfernter Benutzer diesen Zugriff verliert, bevor das Token abläuft. `ldaps://` und StartTLS werden beide unterstützt.

`saml` ist SP-initiiertes Web Browser SSO. Die XML-Signatur der Response wird gegen die Metadaten des Identity Providers geprüft, und Claims werden nur aus dem signierten Element gelesen. Issuer, Audience, die ACS-URL, Ablauf und `InResponseTo` werden alle geprüft. Unaufgeforderte (IdP-initiierte) Responses, verschlüsselte Assertions, Dokumente mit DOCTYPE, mehrere Assertions und SHA-1-Signaturen werden abgelehnt. Eine SAML-Sitzung wird nicht erneut beim Identity Provider geprüft: Ein dort entfernter Benutzer behält den Zugriff, bis das Sitzungs-Token abläuft.

Rollenzuordnung: Identitäts-Claims → Provisa-Rolle über konfigurierbare Regeln. Das Feld `assignments_source` steuert, woher Rollenzuweisungen kommen: `claims` liest sie aus den Claims des JWT (Standard), `provisa` liest sie aus dem internen Zuweisungsspeicher von Provisa. Ein mandantenfähiges Deployment (`multitenancy: true`) vergibt Rollen selbst, pro Org, und muss daher `assignments_source: provisa` nutzen: Mit `claims` startet es nicht, und eine Einstellungsänderung, die diese Kombination ergäbe, wird abgelehnt. Ein Deployment mit einem Mandanten darf beides nutzen.

Ein in `provisa.yaml` konfigurierter Superuser (Benutzername plus ein Passwort aus einem Umgebungs-Secret) erhält immer die Admin-Rolle und alle Capabilities, unabhängig vom konfigurierten Provider, ein Bootstrap-Pfad für die Ersteinrichtung.

### Oberflächen und Anmeldedaten {: #surfaces-and-credentials }

Jede Oberfläche meldet Aufrufer über denselben Provider an, sodass ein Credential, das auf einer funktioniert, auf allen funktioniert, wo immer das Protokoll Platz dafür hat.

| Oberfläche | Passwort | Provider-Token | Personal Access Token | Client-Zertifikat (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | über einen terminierenden Proxy |
| pgwire | Passwortfeld (Klartext oder SCRAM) | Passwortfeld, OIDC-Deployments | Passwortfeld | ja |
| Bolt | Schema `basic` | Schema `bearer` | Schema `bearer` | ja |
| Arrow Flight | — | `token` im Handshake oder in der Ticket-Payload; Aufruf-Header `authorization: Bearer` bei `list_flights`, `get_flight_info` und `get_schema` | dasselbe | ja |
| gRPC | — | Metadaten `authorization` | Metadaten `authorization` | ja |
| Airport (DuckDB-Erweiterung `airport`, Opt-in) | — | `authorization: Bearer` (das `auth_token` des Secrets) | dasselbe | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | über einen terminierenden Proxy |

Die DuckDB-Erweiterung `airport` kann nur das Bearer-`auth_token` ihres Secrets senden. Ein DuckDB-Client trägt dort ein Provider-Token oder ein Personal Access Token ein und handelt als die Rolle, zu der seine Identität aufgelöst wird. Jeder Client, der Aufruf-Header setzen kann, darf in `x-provisa-role` eine seiner Rollen oder eine kommagetrennte Menge davon anfordern.

Jede Oberfläche sagt Ihnen, welche Art von Ablehnung Sie bekommen haben. Ein fehlendes, abgelehntes oder unbrauchbares Credential ist eine Authentifizierungsablehnung: HTTP 401, gRPC `UNAUTHENTICATED`, Arrow Flight und Airport `Unauthenticated`. Ein gültiges Credential, das als eine Rolle handeln will, die seine Identität nicht hat, ist eine Autorisierungsablehnung und nennt die Rolle: HTTP 403, gRPC `PERMISSION_DENIED`, Arrow Flight und Airport `Unauthorized`. Eine Ausnahme kommt aus der Arrow-Client-Bibliothek, nicht vom Server. Wird `get_schema` von Flight abgelehnt, sieht ein Python-Client einen allgemeinen Arrow-Fehler, der den Grund des Servers trägt, aber nicht die Art; `list_flights`, `get_flight_info` und `do_get` liefern die Art mit.

Ein `—` in der Tabelle bedeutet, dass das Protokoll kein Benutzernamenfeld hat, mit dem sich ein Passwort koppeln ließe; die Token-Formen decken es ab. pgwire ist der umgekehrte Fall. Sein Startpaket hat ein einziges Secret-Feld und kein Schema, also entscheidet das Secret selbst über die Methode: Ein Personal Access Token wird an seinem Präfix erkannt, das Secret wird als Bearer-Token gelesen, wenn der konfigurierte Provider Tokens ausgibt, und alles andere ist ein Passwort. Diese Wahl fällt einmal, und ein Credential, das der gewählte Validator ablehnt, wird nicht gegen einen anderen geprüft.

### Personal Access Tokens {: #personal-access-tokens }

Ein Personal Access Token ist ein langlebiges Bearer-Secret, das Sie für einen Client erzeugen, der kein interaktives Login schafft: ein Skript, ein BI-Tool, einen Treiber. Es trägt seine eigene Organisation und Rolle, und jede Oberfläche prüft es mit demselben Validator, sodass keine Oberfläche wissen muss, was es ist.

Auf der Leitung ist es `provisa_pat_`, gefolgt von 43 URL-sicheren Base64-Zeichen. Das Präfix leitet ein vorgelegtes Secret an den Token-Speicher statt an den Identitätsanbieter, und es macht ein geleaktes Token in Logs und Repositories leicht auffindbar.

Gespeichert wird nur der SHA-256 des Secrets. Sie sehen das Secret einmal, beim Erstellen, und danach lässt es sich nicht wiederherstellen; die Liste Ihrer Tokens zeigt ein Anzeigepräfix und Zeitpunkte, nie ein funktionierendes Credential. Ihre eigenen Tokens erstellen, auflisten und widerrufen Sie mit `POST /auth/tokens`, `GET /auth/tokens` und `DELETE /auth/tokens/{token_hash}` oder im Token-Bereich Ihres Profils in der Admin-Oberfläche.

Ein gültiges Token wird zum Konto seines Besitzers aufgelöst, mit Benutzer-ID, E-Mail und Anzeigename, sodass ein Audit-Eintrag oder Nutzungsbericht unter einem Token die Person nennt und nicht das Credential. Welches ihrer Tokens gehandelt hat, wird getrennt festgehalten, in `raw_claims["token_name"]`. Ein Token kann ein Ablaufdatum tragen, nach dem es abgelehnt wird, und das Entfernen der Mitgliedschaft eines Benutzers widerruft seine Tokens mit.

### SCRAM-SHA-256 auf pgwire {: #scram-sha-256-on-pgwire }

Unter dem Provider `basic` lässt `auth.scram: true` pgwire SASL (Authentifizierungscode 10) mit dem Mechanismus `SCRAM-SHA-256` anbieten, sodass ein Passwort nachgewiesen statt gesendet wird. Channel Binding (`SCRAM-SHA-256-PLUS`) wird nicht angeboten.

SCRAM braucht einen Verifier nach RFC 5802, der sich nicht aus einem bcrypt-Hash ableiten lässt. Ein Verifier wird immer dann geschrieben, wenn ein Passwort im Klartext durchläuft (Registrierung, Anmeldung, Passwortänderung, Admin-Zurücksetzung), sodass ein Deployment, das SCRAM einschaltet, Verifier sammelt, wenn sich seine Benutzer das nächste Mal authentifizieren, und die erste SCRAM-Verbindung jedes Benutzers auf seine nächste Passworteingabe folgt. Einem Benutzer ohne Verifier wird mit einem Schein-Austausch geantwortet, der von einem echten nicht zu unterscheiden ist, sodass die Leitung nicht verrät, wer schon umgestellt ist.

### Mutual TLS {: #mutual-tls }

Die Prüfung des Client-Zertifikats verlegt die erste Prüfung in den TLS-Handshake: Ein Aufrufer ohne ein von der CA des Deployments signiertes Zertifikat erreicht die Credential-Schicht nie. Sie ist auf pgwire, Bolt, gRPC und Arrow Flight verfügbar: den vier Transporten, die ihr eigenes TLS terminieren.

| Variable | Bedeutung |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | PEM-Bundle der CA(s), die Client-Zertifikate signieren dürfen |
| `PROVISA_MTLS_MODE` | `required` (Standard, sobald eine CA gesetzt ist) oder `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Wenn true, muss der Common Name des Zertifikats dem Benutzernamen entsprechen, als der sich die Verbindung dann authentifiziert |

Protokollspezifische Überschreibungen folgen derselben Benennung wie die TLS-Einstellungen. Nichts wird abgeleitet: Ein ohne CA gesetzter Modus verweigert den Start, und ein unbekannter Modus verweigert den Start, statt als sicherster Nachbar gelesen zu werden. Ein Deployment, das glaubt, Client-Zertifikate zu verlangen, es aber nicht tut, ist schlechter dran als eines, das nicht startet.

### TLS-Versionen {: #tls-versions }

Jeder Listener, den Provisa terminiert (die HTTP-API und die UI, pgwire, Bolt, MCP, gRPC und Arrow Flight), akzeptiert TLS 1.2 und TLS 1.3 und lehnt TLS 1.0 und 1.1 ab. Provisa legt weder eine Protokollversion noch eine Cipher-Liste selbst fest. Die HTTP-, pgwire- und Bolt-Listener verwenden die `ssl`-Server-Standards von Python (Minimum TLS 1.2). gRPC und Arrow Flight verwenden die Standards von gRPC (Minimum TLS 1.2). Die Versionen sind nicht konfigurierbar.

### Anmelde-Drosselung {: #login-throttling }

Passwortraten ist protokollunabhängig: Dasselbe Konto kann über HTTP, pgwire und Bolt bearbeitet werden. Der Zähler liegt daher auf der Credential-Validierungsschicht, nicht auf einer einzelnen Oberfläche, sodass eine irgendwo verdiente Sperre überall durchgesetzt wird.

Sie ist standardmäßig an (fünf Fehlschläge in fünf Minuten sperren das Subjekt für fünfzehn Minuten) und wird unter `auth.login_throttle` eingestellt. Ein gesperrtes Subjekt wird abgelehnt, bevor das Credential überhaupt geprüft wird, und eine erfolgreiche Authentifizierung löscht die Historie dieses Subjekts.

Der Schlüssel ist der Principal, den das Protokoll trägt. Eine reine Bearer-Oberfläche trägt keinen Principal, daher ist der Schlüssel ein Digest des Credentials selbst; das verhindert, dass ein schlechtes Token unbegrenzt wiederholt wird. Der Speicher ist pro Prozess, daher erlaubt ein Deployment mit mehreren API-Workern bis zu `max_attempts` pro Worker. Die Drosselung bremst das Raten, sie ist kein verteiltes Kontingent.

### Rollen-Sondierung {: #role-probing }

Das Nennen einer Rolle, die der Aufrufer nicht hält, wird auf jeder Oberfläche als erkanntes Sicherheitsereignis hoher Schwere behandelt: der HTTP-API, MCP, pgwire, Bolt, Arrow Flight, dem Airport-Flight-Dienst und gRPC. Ein legitimer Client nennt die Rollen, die ihm gegeben wurden; einer, der eine andere nennt, ist entweder fehlerhaft oder sondiert.

Die Anfrage wird abgelehnt. Der Aufrufer erfährt nur „not permitted“, nichts darüber, ob die Rolle existiert, und die Verbindung oder Sitzung endet, wo es eine gibt: die pgwire-Verbindung, die Bolt-Sitzung, der gRPC-Aufruf, die MCP-Sitzung. Die Ausnahme ist die HTTP-API mit allen Anmeldedaten außer einem persönlichen Zugriffstoken: Der Browser legt dort seine Sitzung oder das Token seines Identitätsanbieters vor, daher nennt die Ablehnung weiterhin die Rolle.

Was sonst geschieht, hängt von Anmeldedaten und Transport ab:

| Anmeldedaten | Transport | Gesperrt |
|---|---|---|
| Das Token eines Demo-Sandbox-Besuchers | die HTTP-API | nichts |
| Persönliches Zugriffstoken | jeder | das Token |
| Von einem externen Identitätsanbieter signiertes Token | MCP, pgwire, Bolt, Flight, gRPC | der Token-Zugriff des Prinzipals auf die Organisation |
| Von einem externen Identitätsanbieter signiertes Token | die HTTP-API | nichts |
| Eine Sitzung oder ein Passwort, das dieses Deployment selbst prüft | jeder | nichts |
| Eine lokale MCP-Sitzung (kein Bearer) | MCP | nichts; das Ereignis geht in das Protokoll des Betreibers |

Die HTTP-API ist die Ausnahme, weil der Browser dasselbe Bearer-Token vorlegt wie ein API-Client, sodass der Server sie nicht unterscheiden kann, und ein Tab, der noch eine Rolle nennt, die der Benutzer inzwischen verloren hat (nachdem ein Administrator seine Rollen geändert hat oder nach einem Organisationswechsel), alltäglich ist. MCP und die anderen Transporte sind nie eine Browser-Sitzung. Dort wird das Ereignis wie jedes andere abgelehnt, aufgezeichnet und gemeldet; nur die Sperre entfällt. Das Token eines Demo-Sandbox-Besuchers ist ein persönliches Zugriffstoken, aber die einzige Anmeldung des Browser-Tabs des Besuchers; auf der HTTP-API wird es daher wie eine Sitzung behandelt: abgelehnt und protokolliert, nie gesperrt.

Ein gesperrtes Token wird wie ein widerrufenes abgelehnt. Ein Prinzipal, dessen Token-Zugriff gesperrt ist, wird in dieser Organisation auf MCP, pgwire, Bolt, Flight und gRPC abgelehnt und kann sich weiterhin in der UI anmelden. Nur ein Administrator hebt eine Sperre auf; nichts hebt sie nach Zeitablauf auf. Team → Gesperrter Zugriff listet jede Sperre mit dem Ereignis dahinter und hebt sie auf (`GET /admin/orgs/{org}/suspensions`, `POST /admin/orgs/{org}/suspensions/lift`).

Jedes Ereignis wird in das Administrationsprotokoll der Organisation geschrieben (Aktion `security.role_probe`), mit dem Prinzipal, der Kennung der Anmeldedaten, der Client-Adresse, dem Transport, dem Endpunkt oder Werkzeug, der genannten Rolle, den gehaltenen Rollen, der Zeit und den Kennungen der jüngsten geprüften Anweisungen des Prinzipals. Ihr Text bleibt im Abfrage-Auditprotokoll und wird mit dem Recht `view_governance` gelesen.

Die Administratoren der Organisation erhalten eine E-Mail, wo das Deployment E-Mails versendet. Das Ereignis wird außerdem als JSON an den Sicherheits-Webhook des Betreibers gesendet, wenn `security.webhook_url` gesetzt ist. Was die Organisation verlässt, nennt den Prinzipal, die Kennung der Anmeldedaten, den Transport, den Endpunkt, die genannte Rolle, die Zeit und die Schwere, mit einem Link zur Administrationsansicht; es enthält keine Anweisung und keine gehaltene Rolle. Eine Meldung, die nicht zugestellt werden kann, wird protokolliert und macht die Ablehnung oder die Sperre nie rückgängig.

### Die Org benennen {: #naming-the-org }

Unter Mandantenfähigkeit nennt jede Anfrage die Org, für die sie ist. Genau einer Org anzugehören benennt sie nicht: Eine Anfrage, die keine nennt, wird abgelehnt, und die Ablehnung sagt, was zu senden ist. Jede Oberfläche nennt eine Org so:

| Oberfläche | Wie ein Client die Org nennt |
| ------- | -------------------------- |
| HTTP (UI, REST, GraphQL) | Der eigene Hostname der Org, `acme.provisa.dev`, oder der Header `X-Org-Provisa` auf dem Control-Plane-Host |
| pgwire | Der Datenbankname (`psql -d acme`, das Datenbankfeld eines BI-Tools) oder der eigene Hostname der Org, über TLS gewählt |
| Bolt | Der Datenbankname, `<org>.provisa_<role>` (oder `<org>.provisa_ops_<role>`), oder der eigene Hostname der Org, über TLS gewählt |
| Arrow Flight | `"org"` im Ticket |
| gRPC | Der Metadaten-Header `x-provisa-org` |
| MCP (HTTP-Transport) | Der eigene Hostname der Org oder der Header `X-Org-Provisa` |
| Python-Client | `org=` bei `ProvisaClient`, `connect()` und `adbc_connect()`; `?org=` in einer SQLAlchemy-URL |

Ein pgwire-Client, der einen Hostnamen über TLS wählt, sendet ihn im TLS-ClientHello, und Provisa liest die Org daraus. Nennen Hostname und Datenbankname beide eine Org und unterscheiden sich, wird die Verbindung abgelehnt; keiner wird bevorzugt. Der Datenbankname `provisa`, der Standard, nennt keine Org, und ein mit einer Org verbundenes BI-Tool zeigt
diese Org als seine Datenbank. Auf Bolt listet `SHOW DATABASES` ein `<org>.provisa_<role>` pro Org, der der
Benutzer angehört; ein bloßes `provisa_<role>` wird unter Mandantenfähigkeit abgelehnt, und eine Sitzung bleibt in der
Org, die sie zuerst genannt hat.

Ein Name ist eine Anfrage, keine Freigabe. Er erreicht überall denselben Resolver, der eine Org ablehnt, in der der authentifizierte Principal weder Mitglied ist noch das orgübergreifende Recht hält.

Ein Personal Access Token wird für eine Org ausgestellt und nennt sie selbst. Es öffnet nur diese Org: Eine Anfrage, die eine andere Org nennt, wird abgelehnt, auch wenn der Eigentümer des Tokens beiden angehört.

Ein Deployment mit einem Mandanten hat keine Org zu benennen. Keine Oberfläche liest dort einen Hostnamen, einen Datenbanknamen oder einen Header dafür.

## Secrets über Organisationen hinweg {: #secrets-across-organizations }

Die Secrets einer Organisation werden nur von dieser Organisation gelesen. Im eigenen Speicher von Provisa wird jedes Secret unter seiner Organisation gehalten. In einem zentralen Dienst (Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) auf einem Deployment mit vielen Organisationen erfolgt jeder Lesevorgang innerhalb des Präfixes der lesenden Organisation, und eine Referenz, die es verlassen würde, wird abgelehnt. Die Präfixe und die Schritte, bestehende Secrets darunter zu verschieben, stehen unter [Secrets](secrets.md#one-organizations-secrets-in-a-central-service).

## Telemetrie über Organisationen hinweg {: #telemetry-across-organizations }

Auf einem Deployment, das sich viele Organisationen teilen, sind die Betriebssignale (`"ops"."traces"` und auf der Trino-Engine die Metriken und Logs daneben) ein Store, in den die Arbeit jeder Organisation einfließt. Jeder Span hält die Organisation fest, für die er gestartet wurde, gespeichert als `tenant_id`, und jeder Lesezugriff auf die Signale liefert nur die Zeilen der lesenden Organisation, unabhängig von der Rolle. Ein Span, der außerhalb der Arbeit einer Organisation gestartet wurde (etwa der Start des Servers), gehört keiner und wird von keiner gelesen. Der Plattformadministrator hat in keiner Organisation Datenzugriff und liest keine davon über Provisa.

Die Betriebssignale werden auf beiden Engines unbegrenzt aufbewahrt, wie das Audit-Log: Die Kompaktierung schreibt die Dateien, in denen sie liegen, neu und entfernt keine ihrer Zeilen. Auf Trino sind sie die Iceberg-Tabellen des Katalogs `otel`. Auf einer nativen Engine (DuckDB) legt der Server seine eigenen Trace-Datensätze als Parquet-Dateien in seinem Telemetrie-Ordner ab, und die Engine liest sie unter demselben Namen, `"ops"."traces"`, mit der Refresh-Lineage-Sicht `"ops"."mv_refresh_lineage"` darüber; beide werden nach derselben Regel pro Organisation gelesen.

Die Signale liegen in ihrem Store im Klartext. Eine Workload, die mehr als eine Grenze pro Organisation innerhalb eines Deployments braucht, läuft auf einem dedizierten Deployment; eine hochsensible oder regulierte wird selbst gehostet.

## Hintergrundarbeit über Organisationen hinweg {: #background-work-across-organizations }

Ein Prozess, der viele Organisationen bedient, führt die Hintergrundarbeit jeder Organisation als diese Organisation aus. Die Änderungsverarbeitung jeder Organisation (die Ereignisschleife, die Quelländerungen landet und die Views darüber aktualisiert), ihre Replica-Builds und die Tabellen-Trigger, die sie auslösen, gehören zu ihrer eigenen Runtime: Eine Änderung wird vom Job ihrer eigenen Organisation verarbeitet, liest ihre Quelle mit Adresse und Zugangsdaten dieser Organisation und schreibt in deren Speicher. Jede Umgebung einer Organisation hält ihren eigenen PostgreSQL-Slot für logische Replikation samt Publication, benannt nach Organisation, Umgebung und Quelle, sodass zwei Organisationen, die eine Quelle auf derselben Datenbank gleich benennen, jeweils ihre eigenen Änderungen lesen.

## Datenresidenz {: #data-residency }

In einem Deployment mit Regionen kann eine Tabelle eine Region als ihre Heimat nennen; ihre personenbezogenen Daten werden dort gespeichert und verarbeitet.

- Nur die Heimatregion baut und aktualisiert die Replica der Tabelle. Jede andere Region liest diese Replica an Ort und Stelle, wobei die Zeilen- und Spaltenregeln des Lesers in der Heimatregion angewendet werden, sodass sie nur verlässt, was der Leser sehen darf.
- Eine Heimatregion, deren Replica nicht gebaut ist oder deren Speicher nicht erreichbar sind, verweigert das Lesen (`query.home_region_unavailable`). Die Quelle wird nie stattdessen live aus einer anderen Region gelesen.
- Ein Entwurf wird nur dort in der Vorschau gezeigt, wo er lebt. Eine Entwurfstabelle, die eine Region nennt, wird für die Vorschau ihres Stewards nur auf den Knoten dieser Region gelesen und anderswo verweigert; die Kopie, die die Vorschau landet, wird dort für die Freigabe aufbewahrt. Eine Kopie, die irgendeine Region von einer Tabelle hält (Entwurf oder veröffentlicht), deren Heimat nun eine andere Region ist, wird stillgelegt und dann gelöscht.
- Die Region einer Tabelle zu setzen, zu ändern oder zu entfernen erfordert das Recht `data_residency` für die beteiligten Regionen, zusätzlich zum Besitz der Tabelle über ihre Domäne. Ein Umzug kann über den Entwurf laufen, sodass die Tabelle dazwischen nirgends bereitgestellt wird.

Zeilen- und Spaltenregeln auf der Region des antwortenden Knotens (`current_setting('provisa.region')`) und auf dem Attribut `region` der Mitglieder entscheiden, was zwischen Regionen übertragen wird. [Regionen](regions.md) beschreibt Regionen, Homing und diese Regeln vollständig.

## Deployment-Härtung {: #deployment-hardening }

### Store-Anmeldedaten und wo die Stores lauschen {: #store-credentials-and-where-the-stores-listen }

Die gebündelten Stores haben keine Standard-Anmeldedaten. Die PostgreSQL-Control-Plane, Redis, MinIO, das Admin-Konto von Grafana und das gemeinsame Secret des Trino-Clusters erhalten jeweils einen für das Deployment generierten Wert:

- Die Launcher (`provisa start`, die Desktop- und Server-Installer) generieren sie einmal und legen sie in `~/.provisa/store-credentials.env` ab (Modus 600). Ein bereits in der Umgebung vorhandener Wert wird unverändert verwendet, und ein sekundärer Node, der die Stores des primären nutzt, verweigert den Start ohne die des primären.
- Die Terraform-Module generieren sie mit `random_password` und geben jedem Node dieselben Werte. Sie liegen im Terraform-State, schützen Sie den State daher wie die Passwörter.
- Das Helm-Chart setzt keine. Es verlangt ein von Ihnen erstelltes Secret, benannt durch `credentials.existingSecret`, mit `pg-password`, `redis-password`, `minio-access-key`, `minio-secret-key`, `grafana-admin-password` und `trino-shared-secret`.

Redis verlangt sein Passwort, und das Passwort der PostgreSQL-Rolle wird bei jedem Start erneut gesetzt, sodass ein früher angelegtes Volume dem aktuellen Passwort des Deployments folgt. Die Stores, die Föderations-Engine und die Telemetrie-Dienste veröffentlichen ihre Ports auf einer Adresse, `PROVISA_STORE_BIND_IP`: `127.0.0.1` auf einem eigenständigen Node oder die private Adresse des primären, wenn sich Sekundäre mit ihm verbinden. Das Admin-Konto von Grafana erhält das generierte Passwort; der Compose-Stack lässt anonyme Besucher Dashboards ansehen, das Helm-Chart lässt keinen anonymen Besucher zu.

### Transportverschlüsselung {: #transport-encryption }

Jeder Listener, den Provisa beendet, bedient TLS (siehe [TLS-Versionen](#tls-versions)). Ein Server-Node verwendet das Zertifikat in `PROVISA_TLS_CERT`/`PROVISA_TLS_KEY` oder generiert ein selbstsigniertes. Das Helm-Chart bedient standardmäßig TLS (`tls.enabled: true`) aus dem `kubernetes.io/tls`-Secret, das `tls.secretName` benennt.

### Das eingebettete PostgreSQL {: #the-embedded-postgresql }

Die Desktop- und nativen Tiers betreiben PostgreSQL eingebettet. Unter macOS und Linux lauscht es auf einem Unix-Socket in seinem Datenverzeichnis (Modus 700), den nur das Konto erreicht, dem das Verzeichnis gehört. Über TCP (Windows, wo es auf 127.0.0.1 lauscht) muss jede Verbindung ein Passwort angeben, das für das Datenverzeichnis generiert wurde und darin liegt (`provisa.password`, Modus 600). Es wird nie als root gestartet.

### Die Föderations-Engine (Trino) {: #the-federation-engine-trino }

- **Zwischen Nodes.** Ein Koordinator und seine Worker authentifizieren sich gegenseitig mit dem generierten gemeinsamen Secret des Deployments (`internal-communication.shared-secret`). Ihren Datenverkehr zu verschlüsseln ist eine Option des Betreibers, standardmäßig aus: Aus bleibt er im privaten Netz oder Cluster des Deployments. Schalten Sie sie ein, wo eine Compliance- oder Zero-Trust-Vorgabe Verschlüsselung innerhalb des Deployments verlangt. Trino dokumentiert, dass sie Shuffle-lastige Abfragen (verteilte Joins, Aggregationen, Fensterfunktionen) um 10 % bis über 100 % verlangsamt. Auf einem Compose-Deployment ist es die Einstellung `engine.trino_internal_tls` (Deployment-Einstellungen → Engine), daneben das Engine-Netz und die festgelegte Adresse des Koordinators, und sie greift beim nächsten Start. Unter dem Helm-Chart ist es der Wert `trino.internalTls`; das Chart erzeugt die CA und das Zertifikat, die die Nodes verwenden, behält sie über Upgrades hinweg und adressiert jeden Node über seinen Pod-DNS-Namen. Die Engines der gehosteten Plattform sind Einzel-Node-Koordinatoren ohne Datenverkehr zwischen Nodes und lehnen sie ab.
- **Ausgelagerte Daten.** Die fehlertolerante Ausführung verschlüsselt die Exchange-Daten, die sie auslagert, mit einem pro Abfrage generierten Schlüssel, der verworfen wird, wenn die Abfrage endet (`fault-tolerant-execution.exchange-encryption-enabled=true`, ausdrücklich gesetzt). Das Legacy-Spilling auf Festplatte wird nicht verwendet.
- **Organisationen auf einer Engine.** Die Kataloge jeder Organisation gehören ihr, sind nach der Organisation benannt, und die Anweisungen jeder Organisation laufen in ihrer eigenen Ressourcengruppe.
- **Kataloge und die Zugangsdaten darin liegen nur im Speicher der Engine.** Provisa erzeugt jeden Trino-Katalog zur Laufzeit, und jeder enthält die Verbindungseigenschaften seiner Quelle, Passwörter eingeschlossen. Jede Engine, die Provisa betreibt, hält sie im Speicher (`catalog.store=memory`): Kein Katalog wird auf die Festplatte der Engine geschrieben, und keiner wird aus einer Datei eingebunden. Ein Coordinator, der neu startet, kommt ohne Kataloge zurück; jeder Provisa-Prozess bemerkt das bei seiner nächsten Abfrage, der Engine-Watcher innerhalb einer Minute, und erzeugt die Kataloge, die er bedient, neu. Eine Kafka-Quelle mit SASL-Zugangsdaten braucht eine Datei: Der Kafka-Connector von Trino liest seine Client-Einstellungen nur aus einer Datei, daher schreibt Provisa sie, Passwort eingeschlossen, nach `kafka-client/<catalog>.properties`. Die Datei ist nur für den Benutzer von Trino lesbar (Modus 0600 in einem Verzeichnis mit 0700). Im Container-Stack ist das Verzeichnis ein speicherbasiertes Volume, das sich API und Engine teilen. Auf den Engines der gehosteten Plattform ist es das eigene Kubernetes-Secret der Engine, im Speicher des Nodes gehalten und nur für die Gruppe von Trino lesbar (Modus 0440). In beiden Fällen erreicht die Datei nie eine Festplatte. Auf den gehosteten Engines muss eine Kafka-Quelle eine Schema-Registry nennen: Eine, deren Topic-Spalten nur das Modell deklariert, wird beim Speichern oder Anwenden mit Namen abgelehnt, da ihre Tabellenbeschreibungen ein Verzeichnis bräuchten, das der laufenden Engine nicht gegeben werden kann. Beim Start des Stacks werden die Katalog- und Kafka-Client-Dateien entfernt, die frühere Releases auf der Festplatte hinterlassen haben.

### Quellen auf einem gemeinsamen Deployment {: #sources-on-a-shared-deployment }

Auf einem Deployment, das viele Organisationen hält, dürfen die Quellen einer Organisation nur öffentliche Adressen erreichen: nie das private Netz der Plattform, in dem ihre Control Plane, ihr Cache und ihr Objektspeicher liegen. Die Prüfung greift, wenn eine Quelle gespeichert wird, durch die Oberfläche, die Admin-API oder ein Anwenden der Konfiguration, und erneut bei jeder Verbindung und jeder HTTP-Weiterleitung. Die Quellen einer Demo-Sandbox sind zusätzlich an die Adresse gebunden, bei der sie geprüft wurden. Die eigene Organisation des Deployments, deren Quellen der Betreiber konfiguriert, unterliegt dem nicht.

## Verschlüsselung ruhender Daten und die eigenen Schlüssel der Organisation {: #encryption-at-rest-and-the-organizations-own-keys }

Die Verschlüsselung unter dem eigenen Schlüssel einer Organisation ist verfügbar und bleibt aus, bis die Organisation sie einschaltet. Nichts wird unter einem Schlüssel der Organisation verschlüsselt, solange kein Org-Admin einen anlegt.

**Der Verschlüsselungsschlüssel der Organisation** umhüllt die Secrets der Organisation, ihre verschlüsselten Spalten und ihren Audit-Abfragetext. Ein Org-Admin legt ihn unter **Admin → Org Secrets → Org Key** an oder mit `PUT /admin/org-encryption`: Ohne Body generiert Provisa den Schlüssel, der nie angezeigt wird, und ein `key_b64`-Body liefert 32 Bytes Ihres eigenen. `GET /admin/org-encryption` meldet nur seinen Fingerabdruck. Ohne Organisationsschlüssel werden die Daten der Organisation mit dem Schlüssel des Deployments umhüllt (`encryption.provider` in `provisa.yaml`). Ein Deployment, das keinen Verschlüsselungs-Provider konfiguriert, speichert verschlüsselte Spalten und Audit-Abfragetext unverschlüsselt; Secrets werden nie im Klartext gespeichert (siehe [Secrets](secrets.md#provisas-built-in-store)).

**Der Ordner der Organisation** enthält ihre hochgeladenen und gelandeten Dateien, die Spill-Dateien ihrer Engine und ihre temporären Dateien. Ein Org-Admin schaltet seine Verschlüsselung unter **Admin → Org Secrets → File folder encryption** ein oder mit `PUT /admin/org-files` (`{"encrypted": true, "key_reference": "${secret:NAME}"}`). `POST /admin/org-files/generate-key` antwortet einmalig mit einem neuen 256-Bit-Schlüssel, der im Vault abzulegen ist. Der Schlüssel bleibt im eigenen Vault der Organisation (AWS Secrets Manager, GCP Secret Manager, Azure Key Vault oder HashiCorp Vault): Provisa speichert nur die Referenz, und gocryptfs hält die Dateien des Ordners auf der Festplatte unter dem Schlüssel verschlüsselt. Das erfordert FUSE, also einen Linux-Host oder -Container mit `/dev/fuse`; unter macOS wird die Einstellung abgelehnt.

Auf einem selbst gehosteten Deployment, das typischerweise eine Organisation auf eigenen Engines betreibt, verschlüsselt das Einschalten beider die Secrets, Dateien, Spill-Dateien und temporären Dateien dieser Organisation unter ihren Schlüsseln:

- Die DuckDB-Engine, die eine Organisation betreibt, lagert in den Ordner der Organisation aus.
- Zeilen, die ein Zeilenfilter abruft, und hochgeladener Code werden im Ordner der Organisation bereitgestellt.
- Die fehlertolerante Ausführung von Trino verschlüsselt ausgelagerte Exchange-Daten mit einem pro Abfrage generierten Schlüssel, der verworfen wird, wenn die Abfrage endet (`fault-tolerant-execution.exchange-encryption-enabled=true`). Provisa schaltet das Legacy-Spilling von Trino auf Festplatte nicht ein.
- Trino hält die Kataloge der Organisation mit ihren Zugangsdaten im Speicher und schreibt keinen auf die Festplatte (`catalog.store=memory`); die SASL-Client-Einstellungen einer Kafka-Quelle liegen in einer Datei, die nur der Benutzer von Trino liest, im Container-Stack im Speicher (siehe [Die Föderations-Engine](#the-federation-engine-trino)).
- Eine PostgreSQL-Föderations-Engine schreibt ihre temporären Dateien dorthin, wo dieser PostgreSQL-Server sie ablegt, unter der eigenen Speicherverschlüsselung des Servers, nicht im Ordner der Organisation.

### Gemeinsam und dediziert gehostete Plattformen {: #shared-and-dedicated-hosted-platforms }

Die gemeinsam gehostete Plattform isoliert Organisationen logisch. Ein Deployment bedient alle, und die Daten jeder Organisation bleiben ihre eigenen durch Governance, ihre eigenen Schemas, Kataloge und Ressourcengruppen auf der Engine, ihre eigenen Schlüssel, Schlüssel pro Abfrage für ausgelagerte Engine-Daten und die Ablehnung ihrer Quellen an den privaten Adressen der Plattform. Sie wird für Experimente und kleine Unternehmen empfohlen. Eine dediziert gehostete Plattform ist physisch getrennt: eigene Maschinen, eigenes Netz und eigene Stores, die einen Kunden bedienen. Für hochsensible oder regulierte Workloads hosten Sie Provisa selbst innerhalb Ihrer eigenen Grenze, wo Ihre eigenen Betreiber jeden Schlüssel und jeden Host halten. Die gehosteten Plattformen haben keinen SOC-2-Bericht. Wenn Sie SOC 2 benötigen, empfehlen wir das Selbsthosting.

### Python im Browser {: #python-in-the-browser }

Die Tabs Python und Notebook von Explore führen Python im Browser des Besuchers aus. Jeder Lesezugriff geht als angemeldeter Benutzer an den Server, mit seiner handelnden Rolle, Organisation und Umgebung, und wird genau wie dieselbe Anweisung von jedem anderen Client gesteuert.

## Hochsicherheitsmodus {: #high-security-mode }

`security.mode: high` in `provisa.yaml` sichert eine Garantie zu: Das Provisa-Backend verarbeitet nie Klartextdaten. Jede relevante Spalte ist an der Quelle verschlüsselt, und nur ein Client mit dem Entschlüsselungsschlüssel kann sie lesen. Diese Garantie hat Folgen, die ein Deployment einplanen muss.

**Was der Modus tut:**

- **Datenendpunkte verlangen den Nachweis clientseitiger Entschlüsselung.** Alles unter `/data/` liefert 403, sofern der Aufrufer nicht den Header `X-Provisa-KMS-Key` vorlegt, das Kennzeichen eines JDBC- oder Python-Clients, der lokal entschlüsselt. Ein Browser oder ein REST-Konsument im Klartext trägt keinen solchen Schlüssel und wird abgelehnt. Das Gate ist ein Default-Deny über den ganzen Baum: Eine morgen hinzugefügte Route ist am Tag ihrer Auslieferung geschützt, und eine Ausnahme muss begründet werden.
- **Schema-Metadaten-Endpunkte bleiben offen.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` und `/data/compile` liefern keine Zeilendaten, und ein Client muss das Schema lesen (einschließlich der Felder, die `@encrypted` sind), bevor er sich überhaupt verbinden kann.
- **gRPC und Arrow Flight bedienen weiter, unter demselben Nachweis.** Sie sind die Transporte, die verschlüsselnde Clients tatsächlich nutzen; sie zu schließen ließe ein Hochsicherheits-Deployment ohne Wire-Protokoll. Ein Datenaufruf auf einem von beiden muss denselben KMS-Schlüssel als Aufruf-Metadaten tragen.
- **pgwire, Bolt und MCP starten nicht.** Keiner der drei hat einen Handshake pro Verbindung, der einen Entschlüsselungskontext tragen könnte: Eine pgwire-Zeilenmenge und ein Cypher-Ergebnis sind auf der Leitung Klartext, und ein MCP-Werkzeugaufruf übergibt seine Ergebnisse einem Modell als Text. Ein konfigurierter Port für einen von ihnen wird beim Start abgelehnt statt bedient.
- **Der Beziehungsschutz lässt sich nicht umgehen.** `ignore_relationships` und `relationship_guard: false` werden beide ignoriert; siehe [Governance der Beziehungen](#relationship-governance-v002).

**Prüfen, ob ein Deployment im Modus läuft:** Das Startlog nennt ihn, eine `/data/sql`-Anfrage ohne KMS-Schlüssel antwortet mit 403 und einer Meldung, die den Hochsicherheitsmodus nennt, und die Ports für pgwire, Bolt und MCP lauschen nicht.

## ABAC-Genehmigungs-Hook {: #abac-approval-hook }

Ein optionaler externer Richtlinien-Hook, der vor der Abfrageausführung auslöst. Ist er konfiguriert, ruft Provisa Ihre Policy-Engine mit Benutzeridentität, Rollen, Tabellen, Spalten und Operation auf. Die Antwort bestimmt, ob die Abfrage fortfährt. Ein mit **Erfordert Genehmigung** registrierter Command wird vor jedem Aufruf demselben Hook vorgelegt; siehe [Command-Aufrufe](#command-calls).

### Geltungsbereich {: #scoping }

Der Hook löst nur aus, wenn eine Abfrage eine einbezogene Tabelle oder Quelle berührt, also läuft jede andere Abfrage ohne zusätzlichen Aufwand.

| Konfiguration | Wirkung |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Jede Abfrage löst den Hook aus |
| `sources[].approval_hook: true` | Alle Tabellen dieser Quelle lösen den Hook aus |
| `tables[].approval_hook: true` | Diese Tabelle löst den Hook aus |

### Protokolle {: #protocols }

Drei Transporte werden unterstützt:

| Typ | Einsatzfall | Konfigurationsfeld |
| ------ | ---------- | ------------- |
| `webhook` | Jeder HTTP-fähige Richtliniendienst (OPA, eigene) | `url` |
| `unix_socket` | OPA oder Richtlinien-Sidecar auf derselben Maschine | `socket_path` + `url` |
| `grpc` | Richtliniendienst mit hohem Durchsatz am selben Ort | `url` (host:port) |

Der gRPC-Transport nutzt den Vertrag `provisa.auth.ApprovalService`, definiert in `provisa/auth/approval.proto`. Implementieren Sie diesen Service in Ihrer Policy-Engine:

```proto
service ApprovalService {
  rpc Evaluate (ApprovalRequest) returns (ApprovalResponse);
}

message ApprovalRequest {
  string user = 1;
  repeated string roles = 2;
  repeated string tables = 3;
  repeated string columns = 4;
  string operation = 5;
  map<string, string> session_vars = 6;
  string command = 7;         // a command call's name; empty for a query
  string arguments_json = 8;  // a command call's arguments as a JSON object
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```

Der gRPC-Kanal ist persistent: ein Kanal pro Provisa-Instanz, wiederverwendet für alle Aufrufe an diesen Hook-Endpunkt.

### Anfrage / Antwort {: #request-response }

Alle drei Transporte tragen dieselbe Payload:

| Feld | Typ | Beschreibung |
| ------- | ------ | ------------- |
| `user` | string | Authentifizierte Benutzeridentität |
| `roles` | string[] | Provisa-Rollen des Benutzers |
| `tables` | string[] | In der Abfrage referenzierte Tabellen-IDs |
| `columns` | string[] | In der Abfrage ausgewählte Spalten |
| `operation` | string | `"query"` oder `"mutation"`; `"command"` für einen Command-Aufruf |
| `command` | string | Der Name des Commands bei einem Command-Aufruf; leer bei einer Abfrage |
| `arguments` | object | Die Argumente, mit denen ein Command aufgerufen wird; leer bei einer Abfrage |

Die Webhook- und Unix-Socket-Transporte tauschen JSON aus, mit `command` und `arguments` als Schlüsseln. Bei gRPC reisen die Argumente als JSON-Text `arguments_json`. Die Antwort muss `approved` (bool) und optional `reason` (string) enthalten.

### Command-Aufrufe {: #command-calls }

Ein Command mit gesetztem `requires_approval` (der Schalter **Erfordert Genehmigung** im Command-Formular) geht vor jedem Aufruf an den Hook, auf jeder Oberfläche. Er läuft nur, wenn der Hook genehmigt. Die Anfrage enthält `operation: "command"`, den Namen des Commands und seine Argumente; `tables` und `columns` sind leer.

```json
{
  "user": "analyst",
  "roles": ["analyst"],
  "tables": [],
  "columns": [],
  "operation": "command",
  "command": "create_issue",
  "arguments": {"input": {"title": "Crash on save"}}
}
```

Zwei Ablehnungen, beide 403:

- `functions.approval_unavailable`: Kein Hook ist konfiguriert. Anders als eine Abfrage wird ein Command, der eine Genehmigung braucht, ohne einen Hook nie durchgelassen.
- `functions.approval_denied`: Der Hook hat `approved: false` geantwortet. Der Grund steht in der Meldung.

### Timeout und Fallback {: #timeout-and-fallback }

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

Bei einem Timeout oder Transportfehler gilt die Richtlinie `fallback`. Ein Circuit Breaker (Standard: offen nach 5 aufeinanderfolgenden Fehlern, halboffen nach 30 s) verhindert kaskadierende Ausfälle durch einen langsamen Hook-Endpunkt.

### Konfigurationsbeispiel {: #configuration-example }

```yaml
auth:
  approval_hook:
    type: webhook
    url: "http://opa.internal:8181/v1/data/provisa/allow"
    timeout_ms: 300
    fallback: deny

sources:
  - id: analytics_pg
    approval_hook: true   # all tables on this source require hook approval

tables:
  - id: salary_data
    approval_hook: true   # this table always requires hook approval
```

## Secrets

Anmeldedaten verwenden die Syntax `${env:VAR_NAME}`, zur Laufzeit aufgelöst. Passwörter werden nie in der Konfigurations-DB gespeichert.

Den vollständigen Secrets-Dienst (Vaults, Referenzsyntax und Provider) beschreibt [Secrets](secrets.md).
