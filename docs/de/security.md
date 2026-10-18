# Sicherheitsmodell {: #security-model }

Provisa setzt ein mehrschichtiges Sicherheitsmodell über jede Abfragesprache (GraphQL, SQL, Cypher) und jeden Transport (REST, gRPC, Arrow Flight, JDBC, WebSocket) hinweg durch. (REQ-001, REQ-266) Governance wird einheitlich angewendet — es gibt keinen Abfragepfad, der sie umgeht. (REQ-002, REQ-266)

Die Schichten gelten der Reihe nach. Eine Anfrage muss jede Schicht durchlaufen, bevor die nächste ausgewertet wird.

## Schichtenmodell {: #layered-model }

### Schicht 0 — Introspektionsfilterung {: #layer-0-introspection-filtering }

Das Schema und der Katalog, die einer Rolle angezeigt werden, enthalten nur die Tabellen aus ihrer `domain_access`-Liste und die Spalten, die die spaltenspezifischen `visible_to`-Regeln erfüllen. (REQ-039) Objekte außerhalb des Zugriffs einer Rolle sind zum Zeitpunkt der Erkennung unsichtbar — sie können weder abgefragt noch autovervollständigt noch als existent abgeleitet werden. (REQ-039) Das gilt für das GraphQL-Schema, den SQL-Katalog und den Schema-Browser des Abfrage-Editors. (REQ-039, REQ-363)

Siehe [Schema-Sichtbarkeit](#schema-visibility).

### Schicht 1 — Öffentlicher Zugriff {: #layer-1-public-access }

Eine mit dem Geltungsbereich `public` veröffentlichte Spalte wird jeder Rolle der Org bereitgestellt, auch Rollen, deren `domain_access` die Domäne der Tabelle nicht erreicht; eine solche Rolle sieht die Tabelle nur mit ihren öffentlichen Spalten. Nichts ist öffentlich, solange der Registrierer einer Tabelle es nicht sagt. (REQ-1959) Siehe [Öffentliche Spalten](#public-columns).

### Schicht 2 — Domänenzugriff {: #layer-2-domain-access }

Jede Rolle trägt eine `domain_access`-Liste von Domänen-IDs. Eine Abfrage, die eine Tabelle außerhalb dieser Domänen berührt, wird vor der Ausführung abgelehnt. (REQ-038, REQ-039) Das ist die grobe Eigentumsgrenze — eine HR-Rolle erreicht keine Finanztabellen, wie auch immer das SQL geschrieben ist. (REQ-002)

Siehe [Rechtemodell](#rights-model).

### Schicht 3 — Sicherheit auf Zeilenebene {: #layer-3-row-level-security }

Nachdem der Domänenzugriff bestätigt ist, werden tabellen- und rollenspezifische `WHERE`-Prädikate zur Ausführungszeit in jedes `SELECT` eingefügt. (REQ-041, REQ-263) Die Prädikate werden gegen die Rohdaten ausgewertet. Ein regionaler Manager, der eine gemeinsame Auftragstabelle abfragt, sieht selbst bei `SELECT *` nur die Zeilen seiner Region. (REQ-264)

Siehe [Sicherheit auf Zeilenebene (RLS)](#row-level-security-rls).

### Schicht 4 — Spaltensichtbarkeit und Maskierung {: #layer-4-column-visibility-and-masking }

Spalten mit einer `visible_to`-Liste, die die anfragende Rolle ausschließt, werden aus der Abfrageausgabe entfernt. (REQ-040, REQ-263) Bei Spalten mit einer Maskierungsregel werden die Werte ersetzt — Regex-Schwärzung, konstanter Ersatz oder Kürzung —, bevor die Ergebnisse den Server verlassen. (REQ-263) Die Maskierung gilt in allen Abfragesprachen und Ausgabeformaten. (REQ-263)

Siehe [Spaltenberechtigungsmodell](#column-permission-model) und [Maskierung auf Spaltenebene](#column-level-masking).

### Schicht 5 — Prädikatsschutz {: #layer-5-predicate-guard }

Maskierte Spalten werden in `WHERE`- und `HAVING`-Klauseln abgelehnt. (REQ-263) Ohne das könnte ein Aufrufer den unmaskierten Wert durch binäre Suche in einem Filter erschließen, obwohl die Ausgabe maskiert ist. Die Ablehnung wird beim Parsen der Abfrage durchgesetzt, vor der Ausführung. (REQ-531)

### Governance der Beziehungen (V002) {: #relationship-governance-v002 }

JOIN-Bedingungen in SQL müssen einer registrierten, genehmigten Beziehung zwischen Tabellen entsprechen. (REQ-001) Nicht genehmigte Joins werden abgelehnt. Jede Beziehung trägt einen menschenlesbaren Grund und eine Beschreibung — eine Orientierung für Benutzer und autonome Agenten, warum ein Traversierungspfad existiert. Das ist eine Governance-Richtlinie, keine harte Sicherheitsgrenze: Die Schichten 2–5 gelten unabhängig von der Join-Struktur, sodass eine bewusste Umgehung keine Daten offenlegt, die die Rolle nicht auch über zwei getrennte Abfragen erreichen könnte. Umgehungsversuche werden protokolliert und sind prüfbar.

**Umgehungsmechanismen** — V002 kann auf zwei Wegen umgangen werden. Der erste ist eine Capability: Eine Rolle mit `ignore_relationships` joint über Relationen, die der Katalog nicht abdeckt. Unter den vorab angelegten Systemrollen hält nur `modeler` sie — die Erkundungsrolle, deren Aufgabe es ist, das Modell zu bestimmen statt es durchzusetzen. (REQ-1297) `analyst` hält sie nicht.

Der zweite ist ein Opt-out mit zwei Bedingungen, die beide erfüllt sein müssen:

1. **Rollen-Flag** — `relationship_guard: false` in der Rollendefinition (Standard: `true`).
2. **Opt-out pro Abfrage** — das SQL enthält den Kommentar `--relationship-guard=false`.

Das Rollen-Flag allein umgeht V002 nicht; der Kommentar allein umgeht V002 nicht.

**Der Hochsicherheitsmodus fixiert den Schutz.** Unter `security.mode: high` greift keine der beiden Umgehungen: `ignore_relationships` wird ignoriert, `relationship_guard: false` wird ignoriert, und jeder Join muss im Katalog genehmigter Beziehungen stehen. (REQ-693) Das ist bewusste Redundanz — eine Produktionsrolle, der die Capability versehentlich erteilt wurde, kann trotzdem nicht aus dem Modell ausbrechen.

**GraphQL-Pfad** — V002 wird bei GraphQL-Abfragen bedingungslos übersprungen. In der SDL definierte Beziehungen sind konstruktionsbedingt vorab genehmigt; die Prüfung ist redundant und wird nicht angewendet.

**SQL- und Cypher-Pfade** — V002 ist standardmäßig aktiv. Sowohl `endpoint_dev.py` als auch `cypher_router.py` wenden die Prüfung mit zwei Bedingungen an, bevor sie `validate_sql` aufrufen.

**pgwire-Pfad** — dieselbe Prüfung mit zwei Bedingungen wie bei SQL. Der Kommentar `--relationship-guard=false` wird vor der Ausführung aus der Abfrage entfernt; er erreicht die Datenbank nicht.

---

Diese Schichten wirken zusammen. Eine Rolle mit Domänenzugriff, RLS und maskierten Spalten hat alle fünf Beschränkungen gleichzeitig aktiv. Eine neue Datenquelle, Spalte oder Beziehung hinzuzufügen erfordert nicht, jede Regel anzupassen — jede Schicht wird unabhängig konfiguriert und gilt automatisch für jede Abfrage, die regulierte Objekte berührt.

---

## Rechtemodell {: #rights-model }

Unabhängig vergebene Capabilities mit optionaler Rollenhierarchie über `parent_role_id`. `admin` gewährt alles. (REQ-042)

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

Rollen können über `parent_role_id` von einer übergeordneten Rolle erben, gesetzt in der Konfiguration oder auf der Seite Sicherheit als „Erbt von". (REQ-215) Die Kette wird beim Start eingefaltet: Ein Kind hält die Vereinigung der Capabilities und des Domänenzugriffs seiner Vorfahren, jede Spalten-, Metrik-, Funktions- und Webhook-Freigabe, die einen Vorfahren nennt, und die RLS-Regeln der Vorfahren pro Tabelle, wobei das Kind Vorrang hat — die nächstgelegene Rolle in der Kette mit einer Regel für eine Tabelle liefert das Prädikat dieser Tabelle, und eine für das Kind gespeicherte Regel ersetzt die des Elternteils für diese Tabelle. (REQ-1677)

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

Eine gewährte Rolle wird einem Benutzer direkt zugewiesen. Sie hält Rechte: Capabilities, Domänenzugriff, Maskierungseinträge, Zeilenfilter. Die Vererbung (`parent_role_id`) faltet beim Start die Rechte eines Vorfahren in das Kind. Rechte kommen nur hinzu; es gibt keine Subtraktion. Um einen Leser einzuschränken, gewähren Sie einem Kompositum, das sowohl die Rolle als auch eine Bedingung nennt, oder setzen Sie einen Maskeneintrag vor den freizügigen. (REQ-215, REQ-1677)

### Abgeleitete Rollen {: #derived-roles }

Eine abgeleitete Rolle stammt aus den Attributen eines Benutzers. Die Org deklariert, welche Attribute zu Rollen werden (`derived_roles`), jedes mit einer Beschreibung. Für jedes deklarierte Attribut `a`, dessen Wert `v` die Attribute des Benutzers liefern, hält der Benutzer die Elternrolle `a` und ihre Wertrolle `a__v`: `region = eu` ergibt `region` und `region__eu`. Kein Wert bedeutet keine der beiden Rollen.

Die Auflösung der Attribute zählt. Die eigene Aufzeichnung der Org über ein Mitglied hat Vorrang vor dem, was der Identity Provider liefert: Nennen beide dasselbe Attribut, wird der Wert der Org verwendet, und der des Providers wird als überschrieben markiert.

Abgeleitete Rollen tragen keine Rechte und werden nie von Hand zugewiesen. Eine Freigabeliste, eine Maske oder eine Zeilenregel nennt sie genau so, wie sie eine gewährte Rolle nennt; eine Anfrage nennt sie in `X-Provisa-Role` und handelt als sie. (REQ-2057)

Auf der Seite Rollen ist jedes deklarierte Attribut eine Elternrolle mit der Markierung *abgeleitet*; ihre Wertrollen werden darunter aufgelistet, sobald ihre Werte auftreten. **+ Rolle → Aus Attribut abgeleitet** deklariert eine. Ihr Panel zeigt das Attribut, seine Beschreibung, die bisher gesehenen Werte und jede Maske, Zeilenregel und Freigabe, die es oder eine seiner Wertrollen nennt. Es hat keine Freigabe-Steuerelemente. Die Beschreibung einer Wertrolle wird erzeugt: „Gehalten, wenn region des Benutzers eu ist“. (REQ-2078, REQ-2079)

Deklarieren Sie sie in der Konfiguration als Liste:

```yaml
derived_roles:
- attribute: region
  description: The subsidiary a member works for; region__eu marks EU staff.
```

Denken Sie an ein JWT: Seine Claims sind Fakten über die Person. Abgeleitete Rollen machen ausgewählte Fakten zu Rollen, sodass eine Freigabe sie ansprechen kann, ohne für jede Kombination eine Rolle anzulegen.

### Meta-Rollen {: #meta-roles }

Wenn eine Anfrage mehr als eine Rolle nennt, baut Provisa eine flüchtige Meta-Rolle mit der Kennung `meta:<ids sorted, joined by +>`. Sie ist ein einziges Kind jeder Rolle der genannten Menge. Die ganze Pipeline läuft dann mit ihr als gewöhnlicher einzelner Rolle.

Die Rechte sind die Vereinigung der Rechte aller Mitglieder. Zeilenfilter sind das OR des aufgelösten Filters jedes Mitglieds; hat irgendein Mitglied, das eine Tabelle liest, keinen Filter, hat auch die Meta-Rolle keinen — kein Filter bedeutet alle Zeilen. Eine Meta-Rolle wird nie gespeichert, gelistet oder exportiert. Sie wird aus ihren Mitgliedern neu gebaut, wenn sich das Modell ändert. (REQ-1620)

Die Spalten jeder Zeile werden nach den Rollen maskiert und gezeigt, deren Zeilenfilter diese Zeile zugelassen hat: Von diesen Rollen gilt die am wenigsten einschränkende Lesart der Spalte, und eine Rolle, die die Zeile nicht zugelassen hat, trägt nichts dazu bei, wie sie gelesen wird. Masken greifen dort, wo die Tabelle gelesen wird, vor jedem Join und jeder Aggregation, sodass eine Aggregation die maskierten Werte liest. Schreibvorgänge werden ebenso geprüft: Eine Spalte wird in einer Zeile nur geschrieben, wo eine Rolle, die diese Spalte gewährt, die Zeile zulässt. (REQ-1620)

Der Audit-Eintrag nennt jede handelnde Rolle, nicht die ID der Meta-Rolle. Da eine Meta-Rolle nie gespeichert wird, hat sie keine eigene Beschreibung: Wo immer eine angezeigt wird, ist ihre Beschreibung die ihrer Mitglieder, jede ID mit ihrer eigenen. (REQ-2080)

### Beschreibungen {: #descriptions }

Jede Rolle und jede Zeilenregel sagt, wofür sie da ist. Eine Zugriffsprüfung liest, wer was sehen darf und warum. Eine Rolle oder Regel, die niemand erklären kann, kann auch niemand prüfen; deshalb ist die Beschreibung Pflicht: Eine Rolle, eine abgeleitete Elternrolle oder eine Zeilenregel mit leerer Beschreibung wird vom Laden der Konfiguration, von der Admin-API und von der UI abgelehnt, mit Begründung. Die Listen Rollen und Zeilenregeln zeigen jede Beschreibung, und ihre Detail-Panels zeigen und bearbeiten sie. (REQ-2080, REQ-2081)

Eine Rolle hat eine Beschreibung, nicht eine pro Domäne. Beschreibt eine allgemeine Rolle ihre Aufgaben in einer bestimmten Domäne nicht, braucht diese Domäne eine eigene Rolle, meist ein Kind der allgemeinen Rolle (`parent_role_id`), das deren Rechte erbt und beschreibt, was es dort tut.

### Wie eine Anfrage Rollen nennt {: #how-a-request-names-roles }

Ein Client sendet eine Rolle oder eine kommagetrennte Menge in `X-Provisa-Role`. Jede genannte Rolle muss gehalten werden. Eine Rolle, die der Identität nicht zugewiesen ist, wird namentlich abgelehnt.

Rechte und Daten folgen nur den Rollen, als die die Anfrage handelt: Eine Rolle zu halten heißt nicht, als sie zu handeln. Eine Rolle ist diese Rolle; zwei oder mehr handeln als ihre Meta-Rolle. Eine handelnde Rolle, die die Rechte jeder gehaltenen Rolle mitbrächte, bedeutete nichts; so erhält eine Anfrage das geringste Privileg, und der Audit-Eintrag nennt genau, was gehandelt hat.

Genannte abgeleitete Rollen gehen als Qualifikatoren ein. Ein Qualifikator hat keine eigenen Rechte, keinen Domänenzugriff, keine Masken und keine Zeilenfilter, also ist er kein Mitglied der Rechte der Meta-Rolle: Als Mitglied ohne Masken würde er jede Spalte demaskieren, und als eines, das keine Tabelle liest, würde er jedes OR von Zeilenfiltern beenden. Er geht nur in die Freigaben ein: Eine Freigabe oder ein Maskeneintrag, der `region__eu` nennt, erreicht eine Anfrage, die diesen Qualifikator neben einer gewährten Rolle nennt. (REQ-2057)

Ein MCP-Agent handelt nie über die Rollen hinaus, die ihm gegeben wurden. Ein entfernter Agent nennt nur Rollen, die der Prinzipal seines Tokens hält. Ein lokaler stdio-Agent nennt nur Rollen innerhalb der in `mcp.role` (`PROVISA_MCP_ROLE`) festgelegten Menge und handelt als diese Menge, wenn er keine nennt; jede andere Rolle wird namentlich abgelehnt. (REQ-2075)

Ein Agent fragt mit dem MCP-Werkzeug `describe_role`, als welche Rolle er handelt. Die Antwort ist diese Rolle und wofür sie da ist (eine Meta-Rolle mit jedem ihrer Mitglieder) sowie die Rollen, zu denen der Agent wechseln darf, jede mit ihrer Beschreibung. Sie enthält nie eine Zeilenregel, Maske, Freigabe oder Bedingung. Provisa sagt einem Agenten nie, warum ein Ergebnis eingeschränkt wurde, und ein Ergebnis trägt kein „gefiltert“-Kennzeichen: Die Daten einer Rolle sind ihre ganze Sicht, und ein Grund für das Fehlende würde dazu einladen, danach zu sondieren. (REQ-2082)

### Was `+` bedeutet {: #what-means }

`+` erscheint an zwei Stellen, mit jeweils anderer Bedeutung.

**In einem Freigabeeintrag.** `analyst+region__eu` in einer `visible_to`- oder `to`-Liste bedeutet, dass der Leser alle davon hält — `analyst` UND `region__eu` gleichzeitig. Ein Eintrag, der kein Kompositum ist, ist eine einzelne Rolle. Über die Einträge einer Liste hinweg gilt die Regel „irgendeiner": Ein Leser hält die Freigabe, wenn er irgendeinen Eintrag hält. UND innerhalb, ODER übergreifend.

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
- Setzen Sie die spezifischsten Maskeneinträge an den Anfang — ein früherer Eintrag gewinnt, auch wenn der Leser einen späteren hält.
- Prüfen Sie den wirksamen Zugriff mit `POST /admin/masks/check`, bevor Sie eine Kombination in Produktion bringen.

## Spaltenberechtigungsmodell {: #column-permission-model }

Jede Spalte hat ein Berechtigungsmodell, das Lese-, Schreib- und Maskierungszugriff pro Rolle steuert. (REQ-042, REQ-249)

### Drei Sichtbarkeitsstufen {: #three-tier-visibility }

| Stufe | Bedingung | Ergebnis |
| ------ | ----------- | -------- |
| **Verborgen** | Rolle nicht in `visible_to` | Spalte fehlt in Schema und Katalog |
| **Maskiert** | Rolle in `visible_to`; der erste Maskeneintrag, den sie hält, hat eine `mask` | Spalte sichtbar, Wert in der Ausgabe ersetzt |
| **Unmaskiert** | Rolle in `visible_to`; der erste Maskeneintrag, den sie hält, hat keine `mask`, oder sie hält keinen | Voller Lesezugriff |

### Öffentliche Spalten {: #public-columns }

Jede Spalte hat einen `scope`, gesetzt in der Modelldatei, über die Admin-API oder auf der Seite Tabellen. Die drei Geltungsbereiche sind danach geordnet, wem sie dienen: `restricted`, dann `domain`, dann `public`. Ein Tabellenbearbeiter (`table_registration` in der Domäne der Tabelle) darf den Geltungsbereich einer Spalte verengen. Ihn zu erweitern — `restricted` zu `domain` oder `public`, `domain` zu `public` — stellt die Spalte mehr Rollen bereit und verlangt das Recht, das ihre Freigabe verlangt: `column_grant` oder `access_config` in der Domäne der Tabelle. Eine Spalte, die registriert wird, wird mit `domain` verglichen, daher ist eine Registrierung als `public` eine Erweiterung. Bei einer Spalte mit einem sensiblen Tag verlangt jede Änderung des Geltungsbereichs `sensitive_data`. Eine abgelehnte Änderung nennt die Spalte und das Recht. (REQ-1959, REQ-1943, REQ-1944)

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
- Die Domäne `ops` braucht für jede Spalte eine ausdrückliche Freigabe (REQ-1133); nichts darin wird domänenübergreifend bereitgestellt, gleich welcher Geltungsbereich.

Die Regel ist auf jeder Oberfläche dieselbe, weil jede das kompilierte Schema der Rolle liest: GraphQL, SQL (HTTP, pgwire, Flight, JDBC), Cypher und Bolt, gRPC, REST und JSON:API, die Flight- und MCP-Kataloge und die Admin-Listen. Ein Geltungsbereich, der keiner der drei Werte ist, wird beim Speichern einer Spalte abgelehnt.

### Schreibberechtigungen {: #write-grants }

Das Lesen einer Spalte steuert `visible_to` (leer bedeutet, alle Rollen können lesen). Das Schreiben wird je Tabelle über `write_grants` gesteuert, nicht je Spalte. Eine Tabelle trägt standardmäßig keine Berechtigungen, daher kann keine Rolle in sie schreiben. (REQ-2074)

Eine Schreibberechtigung legt fest, was eine Rolle mit einer Operation (`insert`, `update` oder `delete`) in eine Tabelle schreiben darf:

| Feld | Zweck |
| ------- | --------- |
| `columns` | Die Spalten, die ein `INSERT` auflisten oder ein `UPDATE` setzen darf |
| `rows` | Die Zeilen, die ein `UPDATE` oder `DELETE` berühren darf (ein SQL-Prädikat) |
| `check` | Was jede von einem `INSERT` geschriebene oder von einem `UPDATE` hinterlassene Zeile erfüllen muss (ein SQL-Prädikat) |
| `presets` | Spalten, die die Berechtigung selbst setzt, aus einer Sitzungsvariable, einem Literal oder dem Zeitpunkt des Schreibvorgangs. Die Anweisung darf sie nicht liefern. |

Lassen Sie `rows` oder `check` weg, übernimmt die Berechtigung den Lese-Zeilenfilter der Rolle auf der Tabelle, sodass eine Rolle die Zeilen schreibt, die sie liest. `"TRUE"` steht für jede Zeile.

Eine einzige Zulassung entscheidet über jeden Schreibvorgang auf jeder Oberfläche, in dieser Reihenfolge: Die Rolle besitzt das Recht `write`; sie besitzt eine Berechtigung für die Operation auf der Tabelle; jede Spalte, die die Anweisung schreibt, steht in der Berechtigung; die Presets werden hinzugefügt; `rows` wird per AND in das `WHERE` aufgenommen; und `check` wird auf die neuen Zeilen ausgewertet, bevor etwas an die Quelle gesendet wird. Eine Ablehnung nennt Tabelle, Rolle und Operation. (REQ-2074)

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

Registrierte Mutationen (Remote-GraphQL, OpenAPI, gRPC, Hasura) werden durch zwei unabhängige Prüfungen geschützt. (REQ-868) Eine Rolle darf eine Mutation nur aufrufen, wenn sie die globale Capability `write` besitzt UND der Befehl ihr über `visible_to` zugewiesen ist (eine leere Liste weist ihn jeder Rolle zu). (REQ-868) Ein Befehl hat keine Liste `writable_by`; die Konfiguration lehnt eine solche namentlich ab. [tool-verified: `provisa/security/mutation_authz.py` `command_assigned`, `admit_command`; `provisa/core/models.py` `_one_role_list`]

Mutationen werden per Vertrag als Schreibvorgänge eingestuft, nicht nach Angabe des Aufrufers. (REQ-869) Ein `SELECT`, das eine Funktion der Art Mutation referenziert, wird zum Schreibvorgang heraufgestuft und unterliegt derselben Prüfung mit zwei Gates, sodass ein Aufrufer eine Mutation nicht als Lesevorgang getarnt aufrufen kann. (REQ-869) Eine Mutation als lesesicher umzustufen erfordert die Capability `access_config` und wird als Governance-Entscheidung festgehalten; es gibt kein Opt-out pro Anfrage. (REQ-870)

## Schema-Sichtbarkeit {: #schema-visibility }

Rollenspezifische GraphQL-Schemas verbergen nicht autorisierte Inhalte: (REQ-039)

- **Domänenzugriff**: Eine Rolle sieht Tabellen nur in ihren `domain_access`-Domänen (`"*"` = alle) (REQ-039)
- **Spaltensichtbarkeit**: Spalten, die für eine Rolle nicht in `visible_to` stehen, fehlen in der SDL (REQ-039)
- Nicht autorisierte Tabellen/Spalten erscheinen nicht im Schema (REQ-039)

## Sicherheit auf Zeilenebene (RLS) {: #row-level-security-rls }

Tabellen- und rollenspezifische Injektion von SQL-WHERE-Klauseln. Angewendet nach der Kompilierung, vor der Ausführung. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```
Der Filter wird per AND in die WHERE-Klausel der Abfrage eingefügt. Funktioniert für Abfragen und Mutationen (UPDATE/DELETE). (REQ-035, REQ-041)

Ein Term `current_setting('provisa.<name>')` wird gegen die Sitzungsvariablen aufgelöst, die die Anfrage bindet: `user_id` aus der authentifizierten Identität, `role` aus der handelnden Rolle und jeden skalaren Claim des Tokens unter seinem kleingeschriebenen Namen, Bindestriche als Unterstriche. Eine Hasura-Sitzungsvariable behält ihren eigenen Namen: `X-Hasura-User-Id` ist `hasura_user_id`, gelesen aus dem Claim `x-hasura-user-id` des Tokens (auf oberster Ebene oder im Hasura-Claims-Objekt wie `https://hasura.io/jwt/claims`) oder als Attribut `hasura_user_id` für das Mitglied erfasst. Ein importierter Hasura-Filter auf `X-Hasura-User-Id` liest daher `provisa.hasura_user_id`. `provisa.user_id` ist immer die angemeldete Identität; kein Claim ersetzt sie. Die konfigurierten `session_vars` einer Rolle sind die darunterliegenden Konstanten; die Bindungen der Anfrage überlagern sie. Eine Rolle, die von einer anderen erbt, übernimmt die Konstanten ihrer Vorfahren unter ihren eigenen, für jeden Namen den Wert der nächstgelegenen Rolle. `user_id`, `role`, `region` und `org` werden von der Anfrage oder vom Node gebunden und können nicht als Konstanten konfiguriert werden. Im ungesicherten Modus bindet ein Header `x-provisa-session-<name>` den Namen `<name>`. Eine nirgends gebundene Variable wird zu NULL aufgelöst und trifft keine Zeilen. (REQ-1682)

## Maskierung auf Spaltenebene {: #column-level-masking }

Die Maskierung wird als geordnete Liste an jeder Spalte definiert. Der erste Eintrag, den der Leser hält, entscheidet, was er sieht. (REQ-2059, REQ-263)

| Maskentyp | Unterstützte Spaltentypen | SQL-Ausdruck |
| ----------- | ----------------------- | --------------- |
| `regex` | Zeichenkette (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Beliebig | Literalwert (`NULL`, `0`, eine eigene Zeichenkette) |
| `truncate` | Datum/Zeitstempel | `DATE_TRUNC(precision, col)` |

Jeder Eintrag hat eine `to`-Liste, die die Leser nennt, auf die er zielt, und eine optionale `mask`. Ein Eintrag ohne `mask` bedeutet unmaskiert für diese Leser. `*` in `to` trifft jeden Leser.

Der Maskenausdruck wird in die SQL-SELECT-Projektion verschoben. Die Datenbank liefert den maskierten Wert; unmaskierte Daten gehen für maskierte Rollen nie über die Leitung. (REQ-263) Maskierte Spalten werden außerdem in `WHERE`- und `HAVING`-Klauseln gesperrt (Prädikatsschutz der Schicht 5), um das Erschließen von Werten über Filter zu verhindern. (REQ-263, REQ-531)

**Eine Maske gilt für jeden Namen der Spalte, nicht für eine verknüpfte Spalte.** Eine Spalte wird unter ihrem Alias und unter ihrem physischen Namen gleich maskiert. Eine Maske verbirgt keinen Schlüssel, den eine genehmigte Beziehung mit einer Spalte verknüpft, die die Rolle sehen kann: Der Join vergleicht die echten Werte, sodass die verknüpfte Spalte zeigt, was der maskierte Schlüssel enthält. Maskieren Sie die verknüpfte Spalte ebenfalls, oder genehmigen Sie die Beziehung für diese Rolle nicht. (REQ-263, REQ-019)

**Kein gehaltener Eintrag bedeutet unmaskiert.** Ein Leser, der keinen Eintrag hält, liest die Spalte unmaskiert. Beenden Sie eine Liste mit einem `*`-Eintrag, wenn auf jeden Leser irgendeine Maske angewendet werden soll. Der Spalteneditor warnt, wenn einer Liste ein `*`-Eintrag fehlt (`unlisted_unmasked`).

**Verweigern durch Reihenfolge.** Ein früherer Eintrag gewinnt, auch wenn der Leser zusätzlich einen späteren, freizügigeren hält. Damit Auftragnehmer ein Gehalt immer maskiert sehen, gleich welche anderen Rollen sie haben, setzen Sie den Eintrag für Auftragnehmer vor den unmaskierten Eintrag. Eine gewährte Rolle verliert nie, was ihr Elternteil über die Vererbung gewährt, aber ein früher in der Liste stehender Maskeneintrag gilt zuerst — so funktioniert Einschränkung hier.

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

Die Antwort einer getrackten Funktion oder eines Webhooks wird wie die Zeilen einer Tabelle reguliert. (REQ-1679) Die Zeilen, die eine Aktion liefert, werden als Relation gebunden, deren Spalten der deklarierte Ausgabevertrag der Aktion sind — `output_columns` bei einer Funktion, `inline_return_type` bei einem Webhook oder die Objekteigenschaften des Array-`return_schema` einer Funktion —, und diese Relation durchläuft dieselbe Governance-Stufe wie jeder Tabellenlesevorgang, über eine VALUES-CTE mit den Zeilen. (REQ-1679) Nichts an RLS, Maskierung oder Sichtbarkeit wird für Aktionen neu implementiert.

Drei Dinge gelten, in derselben Reihenfolge wie bei einer Tabelle. (REQ-1679)

- Ein Zeilenfilter: die RLS-Regel der Rolle, unter dem Namen der Aktion gespeichert (`upsertRlsRule` mit `actionName`), oder, wenn die Aktion keine eigene hat, die Domänenregel der Domäne der Aktion. Das Prädikat wird beim Speichern gegen den Vertrag validiert, so wie eine Tabellenregel. (REQ-1676)
- Spaltensichtbarkeit: Eine Vertragsspalte, die `visible_to` deklariert, wird für eine Rolle entfernt, die die Liste nicht nennt; eine Spalte, die keines deklariert, gehört zur öffentlichen Form der Aktion und bleibt.
- Masken: Eine Vertragsspalte trägt `masks`, dieselbe geordnete Maskenliste wie eine Tabellenspalte. (REQ-2059)

Die Rollenvererbung löst die Kette für alle drei auf. (REQ-1677) Eine Antwort, die eine Spalte außerhalb des deklarierten Vertrags liefert, wird mit 502 abgelehnt statt unreguliert durchgereicht. Eine Aktion, die keinen Spaltenvertrag deklariert, liefert einen Skalar und hat nichts zu binden; sie wird unverändert geliefert. Der Admin-Testaufruf berichtet die angewendeten Filter, Ausschlüsse und Masken, und die regulierte Anweisung wird ins Abfrage-Audit-Log geschrieben.

## Stichproben {: #sampling }

Alle Rollen sehen Stichprobenergebnisse (Standard: 100 Zeilen), sofern sie nicht die Capability `full_results` haben. (REQ-554) Gesteuert über die Umgebungsvariable `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Audit-Protokollierung {: #audit-logging }

Jede Abfrage, die ein Domänen-Asset berührt, wird in `query_audit_log` erfasst. (REQ-596, REQ-613) Das Audit-Log gehört der Org, nicht der Plattform: Das eigene Register der Plattform für Orgs, Benutzer und Einladungen enthält keine Audit-Einträge. (REQ-1922) Das `query_audit_log` jeder Org ist eine Tabelle im eigenen Schema dieser Org, in dem Speicher, den die Org für ihre Aufzeichnung in jeder von ihr gewählten Region benennt. Eine Org, die keine Regionen deklariert, behält dieses Schema in der Control-Plane-Datenbank des Deployments, neben ihrem Modell.

Jede Zeile hält `tenant_id` (die Org), `user_id`, `role_id`, einen SHA-256-Hash des Abfragetexts, den Abfragetext selbst verschlüsselt (REQ-689), `table_ids` und `table_names`, `source`, `status_code`, `duration_ms`, die Route und die Zeilenanzahl, was auf die Anweisung durchgesetzt wurde, und `logged_at`. Der Abfragetext wird mit dem Schlüssel der Org verschlüsselt, wenn die Org einen gesetzt hat, sonst mit dem Schlüssel des Deployments (REQ-1574). Ein Deployment, das keinen Verschlüsselungs-Provider konfiguriert, speichert ihn unverschlüsselt. Der Text wird in den abfragbaren Ops-Tabellen nie offengelegt und nur bei autorisierten Admin-Lesevorgängen entschlüsselt.

Das Log ist auf Datenbankebene nur anhängbar: PostgreSQL-Regeln machen `DELETE` und `UPDATE` auf `query_audit_log` zu No-ops. (REQ-596, REQ-613) Zwei Indizes, `(tenant_id, logged_at)` und `(user_id, logged_at)`, unterstützen org-bezogene und benutzerbezogene Compliance-Abfragen über Zeiträume. (REQ-596, REQ-613)

Aufbewahrung: Zeilen werden unbegrenzt behalten. Provisa hat keine Einstellung für die Aufbewahrung und löscht das Audit-Log nie; da `DELETE` ein No-op ist, bedeutet das Entfernen alter Zeilen, dass ein Operator die Daten der Tabelle außerhalb von Provisa löscht oder archiviert.

## Ratenbegrenzung {: #rate-limiting }

Rollenspezifische Ratenlimits werden in `provisa.yaml` konfiguriert: maximale Anfragen pro Sekunde, maximale gleichzeitige SSE-Subscriptions und maximale gleichzeitige Arrow-Flight-Streams. (REQ-369) Die Limits werden auf der API-Schicht vor Kompilierung und Ausführung durchgesetzt; Anfragen über dem Limit werden mit HTTP 429 und einem `Retry-After`-Header abgelehnt. (REQ-369)

Der NL-Abfragedienst (`POST /query/nl`) hat über `nl.rate_limit` ein eigenes Limit (Anfragen pro Minute und Rolle). Anfragen über dem Limit werden abgelehnt, bevor ein LLM-Aufruf erfolgt. (REQ-370)

Der Zustand der Ratenlimits liegt in Redis (`cache.redis_url`) als Zähler mit gleitendem Fenster — kein Zustand pro Instanz —, sodass die Limits über alle horizontalen Provisa-Instanzen gelten. (REQ-371)

## Limit für Abfragekomplexität {: #query-complexity-limit }

Ein Ratenlimit begrenzt, wie viele Anfragen eine Rolle sendet. Das Komplexitätslimit begrenzt, was eine Anweisung verlangen darf. (REQ-1174)

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

Austauschbare Auth-Provider: (REQ-120)

| Provider | Credential | Einsatzfall |
| ---------- | ----------- | ---------- |
| `none` | Header X-Provisa-Role | Nur Entwicklung |
| `basic` | Lokale Konten (bcrypt) + Sitzungs-JWT | Eigenständige Deployments |
| `firebase` | Firebase-ID-Token | Produktion |
| `keycloak` | Keycloak-JWT | Unternehmen |
| `oauth` / `oidc` | OIDC-JWT | PingFed, Okta, Azure AD, Auth0 |
| `ldap` | Verzeichnis-Benutzername und -Passwort (LDAP oder Active Directory) | Unternehmensverzeichnisse (REQ-1265) |
| `saml` | Signierte SAML-2.0-Assertion, dann ein Sitzungs-JWT | Unternehmensweites Single Sign-on (REQ-1265) |
| `simple` | bcrypt + JWT | Tests |

`ldap` meldet einen Benutzer per Search-then-Bind an: Provisa bindet als Service-Konto, findet den einen Eintrag, den der Benutzername benennt, und bindet dann als dieser Eintrag mit dem vorgelegten Passwort, sodass das Verzeichnis es prüft. Die Gruppen des Benutzers werden zu seinen Rollen. Das Konto und seine Gruppen werden bei jeder Rückkehr eines Sitzungs-Tokens neu gelesen, sodass ein aus dem Verzeichnis oder aus einer Gruppe entfernter Benutzer diesen Zugriff verliert, bevor das Token abläuft. `ldaps://` und StartTLS werden beide unterstützt.

`saml` ist SP-initiiertes Web Browser SSO. Die XML-Signatur der Response wird gegen die Metadaten des Identity Providers geprüft, und Claims werden nur aus dem signierten Element gelesen. Issuer, Audience, die ACS-URL, Ablauf und `InResponseTo` werden alle geprüft. Unaufgeforderte (IdP-initiierte) Responses, verschlüsselte Assertions, Dokumente mit DOCTYPE, mehrere Assertions und SHA-1-Signaturen werden abgelehnt. Eine SAML-Sitzung wird nicht erneut beim Identity Provider geprüft: Ein dort entfernter Benutzer behält den Zugriff, bis das Sitzungs-Token abläuft.

Rollenzuordnung: Identitäts-Claims → Provisa-Rolle über konfigurierbare Regeln. (REQ-120) Das Feld `assignments_source` steuert, woher Rollenzuweisungen kommen: `claims` liest sie aus den Claims des JWT (Standard), `provisa` liest sie aus dem internen Zuweisungsspeicher von Provisa. (REQ-551) Ein mandantenfähiges Deployment (`multitenancy: true`) vergibt Rollen selbst, pro Org, und muss daher `assignments_source: provisa` nutzen: Mit `claims` startet es nicht, und eine Einstellungsänderung, die diese Kombination ergäbe, wird abgelehnt. Ein Deployment mit einem Mandanten darf beides nutzen.

Ein in `provisa.yaml` konfigurierter Superuser (Benutzername plus ein Passwort aus einem Umgebungs-Secret) erhält immer die Admin-Rolle und alle Capabilities, unabhängig vom konfigurierten Provider — ein Bootstrap-Pfad für die Ersteinrichtung. (REQ-125)

### Oberflächen und Anmeldedaten {: #surfaces-and-credentials }

Jede Oberfläche authentifiziert über denselben Provider-Vertrag, sodass ein Credential, das auf einer funktioniert, auf allen funktioniert, wo immer das Protokoll es tragen kann. (REQ-124, REQ-1263) Diese Tabelle ist die einzige Referenz; die Dokumente der einzelnen Oberflächen wiederholen sie nicht.

| Oberfläche | Passwort | Provider-Token | Personal Access Token | Client-Zertifikat (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | über einen terminierenden Proxy |
| pgwire | Passwortfeld (Klartext oder SCRAM) | Passwortfeld, OIDC-Deployments | Passwortfeld | ja |
| Bolt | Schema `basic` | Schema `bearer` | Schema `bearer` | ja |
| Arrow Flight | — | `token` im Handshake oder in der Ticket-Payload; Aufruf-Header `authorization: Bearer` bei `list_flights`, `get_flight_info` und `get_schema` | dasselbe | ja |
| gRPC | — | Metadaten `authorization` | Metadaten `authorization` | ja |
| Airport (DuckDB-Erweiterung `airport`, Opt-in) | — | `authorization: Bearer` (das `auth_token` des Secrets) | dasselbe | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | über einen terminierenden Proxy |

Die einzige Authentifizierungsoption der DuckDB-Erweiterung `airport` ist das Bearer-`auth_token` ihres Secrets, daher legt ein DuckDB-Client dort ein Provider-Token oder ein Personal Access Token vor und handelt als die Rolle, zu der seine Identität aufgelöst wird. Ein Client, der Aufruf-Header setzen kann, kann in `x-provisa-role` eine gehaltene Rolle oder eine kommagetrennte Menge davon anfordern. (REQ-1120, REQ-1263)

Eine Ablehnung sagt auf jeder dieser Oberflächen, welcher Art sie ist. Ein fehlendes, abgelehntes oder unbrauchbares Credential ist eine Authentifizierungsablehnung: HTTP 401, gRPC `UNAUTHENTICATED`, Arrow Flight und Airport `Unauthenticated`. Ein gültiges Credential, das als eine Rolle handeln will, die seine Identität nicht hält, ist eine Autorisierungsablehnung, mit der Rolle benannt: HTTP 403, gRPC `PERMISSION_DENIED`, Arrow Flight und Airport `Unauthorized`. (REQ-273, REQ-1263) Eine Ausnahme liegt in der Arrow-Client-Bibliothek, nicht im Server: Eine Ablehnung des Flight-Aufrufs `get_schema` erreicht einen Python-Client als allgemeiner Arrow-Fehler mit dem Grund des Servers, ohne seine Art; `list_flights`, `get_flight_info` und `do_get` liefern die Art.

Wo eine Zelle `—` zeigt, trägt das Protokoll kein Benutzernamenfeld, mit dem ein Passwort gepaart werden könnte; die Token-Formen decken es ab. pgwire ist der Spiegelfall: Das Startup-Paket hat ein Secret-Feld und kein Schema, daher bestimmt, was das Secret *ist*, die Methode — ein PAT wird an seinem Präfix erkannt, das Secret wird als Bearer-Token gelesen, wenn der konfigurierte Provider ein Token-Provider ist, und alles andere ist ein Passwort. Die Wahl wird einmal getroffen — ein Credential, das der gewählte Validator ablehnt, wird nicht gegen einen anderen erneut versucht.

Die Matrix wird durch `tests/unit/test_auth_surface_conformance.py` durchgesetzt, das den echten Validierungseinstieg jeder Oberfläche ansteuert und fehlschlägt, wenn eine neue Oberfläche ohne Zeile hinzukommt.

### Personal Access Tokens {: #personal-access-tokens }

Ein PAT ist ein langlebiges Bearer-Secret, das ein Benutzer für einen Client erzeugt, der keine interaktive Anmeldung durchlaufen kann — ein Skript, ein BI-Tool, ein Treiber. (REQ-1263) Es trägt seine eigene Org und Rolle, und jede Oberfläche löst es über denselben Validator auf, sodass keine Oberfläche wissen muss, was ein PAT ist.

Die Wire-Form ist `provisa_pat_`, gefolgt von 43 URL-sicheren Base64-Zeichen. Das Präfix leitet ein vorgelegtes Secret zum Token-Speicher statt zum Identity Provider, und es macht ein geleaktes Token in Logs und Repositorys auffindbar.

- **Speicherung** — nur der SHA-256 des Secrets wird gehalten. Das Secret selbst wird genau einmal angezeigt, bei der Erstellung, und lässt sich nicht wiederherstellen. Die Liste trägt das Anzeigepräfix und die Lebenszyklus-Zeitstempel, nie ein funktionierendes Credential.
- **Ausgabe und Widerruf** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}` sowie der Selbstbedienungsbereich im eigenen Profil des Benutzers in der Admin-UI. Ein Credential zu erzeugen und zu widerrufen ist ein Akt des Token-Inhabers.
- **Zuordnung** — ein validiertes PAT wird zum Konto seines Eigentümers aufgelöst: Benutzer-ID, E-Mail und Anzeigename. Ein unter einem PAT geschriebener Audit-Eintrag oder Nutzungsbericht nennt daher die Person, nicht das Credential. Welches Token dieser Person gehandelt hat, wird getrennt in `raw_claims["token_name"]` geführt.
- **Ablauf** — ein Token kann ein Ablaufdatum tragen; ein abgelaufenes Token wird bei der Validierung abgelehnt. Das Löschen der Mitgliedschaft eines Benutzers widerruft seine Tokens mit.

### SCRAM-SHA-256 auf pgwire {: #scram-sha-256-on-pgwire }

Unter dem Provider `basic` lässt `auth.scram: true` pgwire SASL (Authentifizierungscode 10) mit dem Mechanismus `SCRAM-SHA-256` anbieten, sodass ein Passwort nachgewiesen statt gesendet wird. (REQ-1394) Channel Binding (`SCRAM-SHA-256-PLUS`) wird nicht angeboten.

SCRAM braucht einen Verifier nach RFC 5802, der sich nicht aus einem bcrypt-Hash ableiten lässt. Ein Verifier wird immer dann geschrieben, wenn ein Passwort im Klartext durchläuft — Registrierung, Anmeldung, Passwortänderung, Admin-Zurücksetzung —, sodass ein Deployment, das SCRAM einschaltet, Verifier sammelt, wenn sich seine Benutzer das nächste Mal authentifizieren, und die erste SCRAM-Verbindung jedes Benutzers auf seine nächste Passworteingabe folgt. Einem Benutzer ohne Verifier wird mit einem Schein-Austausch geantwortet, der von einem echten nicht zu unterscheiden ist, sodass die Leitung nicht verrät, wer schon umgestellt ist.

### Mutual TLS {: #mutual-tls }

Die Prüfung des Client-Zertifikats verlegt die erste Prüfung in den TLS-Handshake: Ein Aufrufer ohne ein von der CA des Deployments signiertes Zertifikat erreicht die Credential-Schicht nie. (REQ-1228) Sie ist auf pgwire, Bolt, gRPC und Arrow Flight verfügbar — den vier Transporten, die ihr eigenes TLS terminieren.

| Variable | Bedeutung |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | PEM-Bundle der CA(s), die Client-Zertifikate signieren dürfen |
| `PROVISA_MTLS_MODE` | `required` (Standard, sobald eine CA gesetzt ist) oder `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Wenn true, muss der Common Name des Zertifikats dem Benutzernamen entsprechen, als der sich die Verbindung dann authentifiziert |

Protokollspezifische Überschreibungen folgen derselben Benennung wie die TLS-Einstellungen. Nichts wird abgeleitet: Ein ohne CA gesetzter Modus verweigert den Start, und ein unbekannter Modus verweigert den Start, statt als sicherster Nachbar gelesen zu werden — ein Deployment, das glaubt, Client-Zertifikate zu verlangen, es aber nicht tut, ist schlechter dran als eines, das nicht startet.

### TLS-Versionen {: #tls-versions }

Jeder Listener, den Provisa terminiert (die HTTP-API und die UI, pgwire, Bolt, MCP, gRPC und Arrow Flight), akzeptiert TLS 1.2 und TLS 1.3 und lehnt TLS 1.0 und 1.1 ab. Provisa legt weder eine Protokollversion noch eine Cipher-Liste selbst fest. Die HTTP-, pgwire- und Bolt-Listener verwenden die `ssl`-Server-Standards von Python (Minimum TLS 1.2). gRPC und Arrow Flight verwenden die Standards von gRPC (Minimum TLS 1.2). Die Versionen sind nicht konfigurierbar.

### Anmelde-Drosselung {: #login-throttling }

Passwortraten ist protokollunabhängig: Dasselbe Konto kann über HTTP, pgwire und Bolt bearbeitet werden. Der Zähler liegt daher auf der Credential-Validierungsschicht, nicht auf einer einzelnen Oberfläche, sodass eine irgendwo verdiente Sperre überall durchgesetzt wird. (REQ-1393)

Sie ist standardmäßig an — fünf Fehlschläge in fünf Minuten sperren das Subjekt für fünfzehn Minuten — und wird unter `auth.login_throttle` eingestellt. Ein gesperrtes Subjekt wird abgelehnt, bevor das Credential überhaupt geprüft wird, und eine erfolgreiche Authentifizierung löscht die Historie dieses Subjekts.

Der Schlüssel ist der Principal, den das Protokoll trägt. Eine reine Bearer-Oberfläche trägt keinen Principal, daher ist der Schlüssel ein Digest des Credentials selbst; das verhindert, dass ein schlechtes Token unbegrenzt wiederholt wird. Der Speicher ist pro Prozess, daher erlaubt ein Deployment mit mehreren API-Workern bis zu `max_attempts` pro Worker — die Drosselung bremst das Raten, sie ist kein verteiltes Kontingent.

### Rollen-Sondierung {: #role-probing }

Das Nennen einer Rolle, die der Aufrufer nicht hält, wird auf jeder Oberfläche als erkanntes Sicherheitsereignis hoher Schwere behandelt: der HTTP-API, MCP, pgwire, Bolt, Arrow Flight, dem Airport-Flight-Dienst und gRPC. Ein legitimer Client nennt die Rollen, die ihm gegeben wurden; einer, der eine andere nennt, ist entweder fehlerhaft oder sondiert. (REQ-2076)

Die Anfrage wird abgelehnt. Der Aufrufer erfährt nur „not permitted“, nichts darüber, ob die Rolle existiert, und die Verbindung oder Sitzung endet, wo es eine gibt: die pgwire-Verbindung, die Bolt-Sitzung, der gRPC-Aufruf, die MCP-Sitzung. Die einzige Ausnahme ist eine angemeldete Sitzung auf der HTTP-API, die die Ablehnung mit dem Rollennamen behält.

Was sonst geschieht, hängt von Anmeldedaten und Transport ab:

| Anmeldedaten | Transport | Gesperrt |
|---|---|---|
| Persönliches Zugriffstoken | jeder | das Token |
| Von einem externen Identitätsanbieter signiertes Token | MCP, pgwire, Bolt, Flight, gRPC | der Token-Zugriff des Prinzipals auf die Organisation |
| Von einem externen Identitätsanbieter signiertes Token | die HTTP-API | nichts |
| Eine Sitzung oder ein Passwort, das dieses Deployment selbst prüft | jeder | nichts |
| Eine lokale MCP-Sitzung (kein Bearer) | MCP | nichts; das Ereignis geht in das Protokoll des Betreibers |

Die HTTP-API ist die Ausnahme, weil der Browser dasselbe Bearer-Token vorlegt wie ein API-Client, sodass der Server sie nicht unterscheiden kann, und ein Tab, der noch eine Rolle nennt, die der Benutzer inzwischen verloren hat (nachdem ein Administrator seine Rollen geändert hat oder nach einem Organisationswechsel), alltäglich ist. MCP und die anderen Transporte sind nie eine Browser-Sitzung. Dort wird das Ereignis wie jedes andere abgelehnt, aufgezeichnet und gemeldet; nur die Sperre entfällt.

Ein gesperrtes Token wird wie ein widerrufenes abgelehnt. Ein Prinzipal, dessen Token-Zugriff gesperrt ist, wird in dieser Organisation auf MCP, pgwire, Bolt, Flight und gRPC abgelehnt und kann sich weiterhin in der UI anmelden. Nur ein Administrator hebt eine Sperre auf; nichts hebt sie nach Zeitablauf auf. Team → Gesperrter Zugriff listet jede Sperre mit dem Ereignis dahinter und hebt sie auf (`GET /admin/orgs/{org}/suspensions`, `POST /admin/orgs/{org}/suspensions/lift`).

Jedes Ereignis wird in das Administrationsprotokoll der Organisation geschrieben (Aktion `security.role_probe`), mit dem Prinzipal, der Kennung der Anmeldedaten, der Client-Adresse, dem Transport, dem Endpunkt oder Werkzeug, der genannten Rolle, den gehaltenen Rollen, der Zeit und den Kennungen der jüngsten geprüften Anweisungen des Prinzipals. Ihr Text bleibt im Abfrage-Auditprotokoll und wird mit dem Recht `view_governance` gelesen.

Die Administratoren der Organisation erhalten eine E-Mail, wo das Deployment E-Mails versendet. Das Ereignis wird außerdem als JSON an den Sicherheits-Webhook des Betreibers gesendet, wenn `security.webhook_url` gesetzt ist. Was die Organisation verlässt, nennt den Prinzipal, die Kennung der Anmeldedaten, den Transport, den Endpunkt, die genannte Rolle, die Zeit und die Schwere, mit einem Link zur Administrationsansicht; es enthält keine Anweisung und keine gehaltene Rolle. Eine Meldung, die nicht zugestellt werden kann, wird protokolliert und macht die Ablehnung oder die Sperre nie rückgängig.

### Die Org benennen {: #naming-the-org }

Unter Mandantenfähigkeit nennt jede Anfrage die Org, für die sie ist. Genau einer Org anzugehören benennt sie nicht: Eine Anfrage, die keine nennt, wird abgelehnt, und die Ablehnung sagt, was zu senden ist. (REQ-1235) Jede Oberfläche nennt eine Org so:

| Oberfläche | Wie ein Client die Org nennt |
| ------- | -------------------------- |
| HTTP (UI, REST, GraphQL) | Der eigene Hostname der Org, `acme.provisa.dev`, oder der Header `X-Org-Provisa` auf dem Control-Plane-Host |
| pgwire | Der Datenbankname (`psql -d acme`, das Datenbankfeld eines BI-Tools) oder der eigene Hostname der Org, über TLS gewählt |
| Bolt | Der Datenbankname, `<org>.provisa_<role>` (oder `<org>.provisa_ops_<role>`), oder der eigene Hostname der Org, über TLS gewählt |
| Arrow Flight | `"org"` im Ticket |
| gRPC | Der Metadaten-Header `x-provisa-org` |
| MCP (HTTP-Transport) | Der eigene Hostname der Org oder der Header `X-Org-Provisa` |
| Python-Client | `org=` bei `ProvisaClient`, `connect()` und `adbc_connect()`; `?org=` in einer SQLAlchemy-URL |

Ein pgwire-Client, der einen Hostnamen über TLS wählt, sendet ihn im TLS-ClientHello, und Provisa liest die Org daraus (REQ-1234). Nennen Hostname und Datenbankname beide eine Org und unterscheiden sich, wird die Verbindung abgelehnt; keiner wird bevorzugt. Der Datenbankname `provisa`, der Standard, nennt keine Org, und ein mit einer Org verbundenes BI-Tool zeigt
diese Org als seine Datenbank. Auf Bolt listet `SHOW DATABASES` ein `<org>.provisa_<role>` pro Org, der der
Benutzer angehört; ein bloßes `provisa_<role>` wird unter Mandantenfähigkeit abgelehnt, und eine Sitzung bleibt in der
Org, die sie zuerst genannt hat.

Ein Name ist eine Anfrage, keine Freigabe. Er erreicht überall denselben Resolver, der eine Org ablehnt, in der der authentifizierte Principal weder Mitglied ist noch das orgübergreifende Recht hält.

Ein Personal Access Token wird für eine Org ausgestellt und nennt sie selbst. Es öffnet nur diese Org: Eine Anfrage, die eine andere Org nennt, wird abgelehnt, auch wenn der Eigentümer des Tokens beiden angehört.

Ein Deployment mit einem Mandanten hat keine Org zu benennen. Keine Oberfläche liest dort einen Hostnamen, einen Datenbanknamen oder einen Header dafür.

## Secrets über Organisationen hinweg {: #secrets-across-organizations }

Die Secrets einer Organisation werden nur von dieser Organisation gelesen. Im eigenen Speicher von Provisa wird jedes Secret unter seiner Organisation gehalten. In einem zentralen Dienst (Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) auf einem Deployment mit vielen Organisationen erfolgt jeder Lesevorgang innerhalb des Präfixes der lesenden Organisation, und eine Referenz, die es verlassen würde, wird abgelehnt. (REQ-1557) Die Präfixe und die Schritte, bestehende Secrets darunter zu verschieben, stehen unter [Secrets](secrets.md#one-organizations-secrets-in-a-central-service).

## Telemetrie über Organisationen hinweg {: #telemetry-across-organizations }

Auf einem Deployment, das sich viele Organisationen teilen, sind die Betriebssignale (`"ops"."traces"` und auf der Trino-Engine die Metriken und Logs daneben) ein Store, in den die Arbeit jeder Organisation einfließt. Jeder Span hält die Organisation fest, für die er gestartet wurde, gespeichert als `tenant_id`, und jeder Lesezugriff auf die Signale liefert nur die Zeilen der lesenden Organisation, unabhängig von der Rolle. Ein Span, der außerhalb der Arbeit einer Organisation gestartet wurde (etwa der Start des Servers), gehört keiner und wird von keiner gelesen. Der Plattformadministrator hat in keiner Organisation Datenzugriff und liest keine davon über Provisa. (REQ-1349, REQ-1327)

Die Betriebssignale werden auf beiden Engines unbegrenzt aufbewahrt, wie das Audit-Log: Die Kompaktierung schreibt die Dateien, in denen sie liegen, neu und entfernt keine ihrer Zeilen. Auf Trino sind sie die Iceberg-Tabellen des Katalogs `otel`. Auf einer nativen Engine (DuckDB) legt der Server seine eigenen Trace-Datensätze als Parquet-Dateien in seinem Telemetrie-Ordner ab, und die Engine liest sie unter demselben Namen, `"ops"."traces"`, mit der Refresh-Lineage-Sicht `"ops"."mv_refresh_lineage"` darüber; beide werden nach derselben Regel pro Organisation gelesen. (REQ-862)

Die Signale liegen in ihrem Store im Klartext. Eine Workload, die mehr als eine Grenze pro Organisation innerhalb eines Deployments braucht, läuft auf einem dedizierten Deployment; eine hochsensible oder regulierte wird selbst gehostet.

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

- **Zwischen Nodes.** Ein Koordinator und seine Worker authentifizieren sich gegenseitig mit dem generierten gemeinsamen Secret des Deployments (`internal-communication.shared-secret`). Ihren Datenverkehr zu verschlüsseln ist eine Option des Betreibers, standardmäßig aus: Aus bleibt er im privaten Netz oder Cluster des Deployments. Schalten Sie sie ein, wo eine Compliance- oder Zero-Trust-Vorgabe Verschlüsselung innerhalb des Deployments verlangt. Trino dokumentiert, dass sie Shuffle-lastige Abfragen (verteilte Joins, Aggregationen, Fensterfunktionen) um 10 % bis über 100 % verlangsamt. Auf einem Compose-Deployment ist es die Einstellung `engine.trino_internal_tls` (Deployment-Einstellungen → Engine), daneben das Engine-Netz und die festgelegte Adresse des Koordinators, und sie greift beim nächsten Start. Unter dem Helm-Chart ist es der Wert `trino.internalTls`; das Chart erzeugt die CA und das Zertifikat, die die Nodes verwenden, behält sie über Upgrades hinweg und adressiert jeden Node über seinen Pod-DNS-Namen. Die Engines der gehosteten Plattform sind Einzel-Node-Koordinatoren ohne Datenverkehr zwischen Nodes und lehnen sie ab. (REQ-2072)
- **Ausgelagerte Daten.** Die fehlertolerante Ausführung verschlüsselt die Exchange-Daten, die sie auslagert, mit einem pro Abfrage generierten Schlüssel, der verworfen wird, wenn die Abfrage endet (`fault-tolerant-execution.exchange-encryption-enabled=true`, ausdrücklich gesetzt). Das Legacy-Spilling auf Festplatte wird nicht verwendet.
- **Organisationen auf einer Engine.** Die Kataloge jeder Organisation gehören ihr, sind nach der Organisation benannt, und die Anweisungen jeder Organisation laufen in ihrer eigenen Ressourcengruppe.
- **Kataloge und die Zugangsdaten darin liegen nur im Speicher der Engine.** Provisa erzeugt jeden Trino-Katalog zur Laufzeit, und jeder enthält die Verbindungseigenschaften seiner Quelle, Passwörter eingeschlossen. Jede Engine, die Provisa betreibt, hält sie im Speicher (`catalog.store=memory`): Kein Katalog wird auf die Festplatte der Engine geschrieben, und keiner wird aus einer Datei eingebunden. Ein Coordinator, der neu startet, kommt ohne Kataloge zurück; jeder Provisa-Prozess bemerkt das bei seiner nächsten Abfrage, der Engine-Watcher innerhalb einer Minute, und erzeugt die Kataloge, die er bedient, neu. Eine Kafka-Quelle mit SASL-Zugangsdaten braucht eine Datei: Der Kafka-Connector von Trino liest seine Client-Einstellungen nur aus einer Datei, daher schreibt Provisa sie, Passwort eingeschlossen, nach `kafka-client/<catalog>.properties`. Die Datei ist nur für den Benutzer von Trino lesbar (Modus 0600 in einem Verzeichnis mit 0700). Im Container-Stack ist das Verzeichnis ein speicherbasiertes Volume, das sich API und Engine teilen. Auf den Engines der gehosteten Plattform ist es das eigene Kubernetes-Secret der Engine, im Speicher des Nodes gehalten und nur für die Gruppe von Trino lesbar (Modus 0440). In beiden Fällen erreicht die Datei nie eine Festplatte. Auf den gehosteten Engines muss eine Kafka-Quelle eine Schema-Registry nennen: Eine, deren Topic-Spalten nur das Modell deklariert, wird beim Speichern oder Anwenden mit Namen abgelehnt, da ihre Tabellenbeschreibungen ein Verzeichnis bräuchten, das der laufenden Engine nicht gegeben werden kann. Beim Start des Stacks werden die Katalog- und Kafka-Client-Dateien entfernt, die frühere Releases auf der Festplatte hinterlassen haben.

### Quellen auf einem gemeinsamen Deployment {: #sources-on-a-shared-deployment }

Auf einem Deployment, das viele Organisationen hält, dürfen die Quellen einer Organisation nur öffentliche Adressen erreichen: nie das private Netz der Plattform, in dem ihre Control Plane, ihr Cache und ihr Objektspeicher liegen. Die Prüfung greift, wenn eine Quelle gespeichert wird, durch die Oberfläche, die Admin-API oder ein Anwenden der Konfiguration, und erneut bei jeder Verbindung und jeder HTTP-Weiterleitung. Die Quellen einer Demo-Sandbox sind zusätzlich an die Adresse gebunden, bei der sie geprüft wurden. Die eigene Organisation des Deployments, deren Quellen der Betreiber konfiguriert, unterliegt dem nicht.

## Verschlüsselung ruhender Daten und die eigenen Schlüssel der Organisation {: #encryption-at-rest-and-the-organizations-own-keys }

Die Verschlüsselung unter dem eigenen Schlüssel einer Organisation ist verfügbar und bleibt aus, bis die Organisation sie einschaltet. Nichts wird unter einem Schlüssel der Organisation verschlüsselt, solange kein Org-Admin einen anlegt.

**Der Verschlüsselungsschlüssel der Organisation** (REQ-1574) umhüllt die Secrets der Organisation, ihre verschlüsselten Spalten und ihren Audit-Abfragetext. Ein Org-Admin legt ihn unter **Admin → Org Secrets → Org Key** an oder mit `PUT /admin/org-encryption`: Ohne Body generiert Provisa den Schlüssel, der nie angezeigt wird, und ein `key_b64`-Body liefert 32 Bytes Ihres eigenen. `GET /admin/org-encryption` meldet nur seinen Fingerabdruck. Ohne Organisationsschlüssel werden die Daten der Organisation mit dem Schlüssel des Deployments umhüllt (`encryption.provider` in `provisa.yaml`). Ein Deployment, das keinen Verschlüsselungs-Provider konfiguriert, speichert verschlüsselte Spalten und Audit-Abfragetext unverschlüsselt; Secrets werden nie im Klartext gespeichert (siehe [Secrets](secrets.md#provisas-built-in-store)).

**Der Ordner der Organisation** (REQ-1992) enthält ihre hochgeladenen und gelandeten Dateien, die Spill-Dateien ihrer Engine und ihre temporären Dateien. Ein Org-Admin schaltet seine Verschlüsselung unter **Admin → Org Secrets → File folder encryption** ein oder mit `PUT /admin/org-files` (`{"encrypted": true, "key_reference": "${secret:NAME}"}`). `POST /admin/org-files/generate-key` antwortet einmalig mit einem neuen 256-Bit-Schlüssel, der im Vault abzulegen ist. Der Schlüssel bleibt im eigenen Vault der Organisation (AWS Secrets Manager, GCP Secret Manager, Azure Key Vault oder HashiCorp Vault): Provisa speichert nur die Referenz, und gocryptfs hält die Dateien des Ordners auf der Festplatte unter dem Schlüssel verschlüsselt. Das erfordert FUSE, also einen Linux-Host oder -Container mit `/dev/fuse`; unter macOS wird die Einstellung abgelehnt.

Auf einem selbst gehosteten Deployment, das typischerweise eine Organisation auf eigenen Engines betreibt, verschlüsselt das Einschalten beider die Secrets, Dateien, Spill-Dateien und temporären Dateien dieser Organisation unter ihren Schlüsseln:

- Die DuckDB-Engine, die eine Organisation betreibt, lagert in den Ordner der Organisation aus.
- Zeilen, die ein Zeilenfilter abruft, und hochgeladener Code werden im Ordner der Organisation bereitgestellt.
- Die fehlertolerante Ausführung von Trino verschlüsselt ausgelagerte Exchange-Daten mit einem pro Abfrage generierten Schlüssel, der verworfen wird, wenn die Abfrage endet (`fault-tolerant-execution.exchange-encryption-enabled=true`). Provisa schaltet das Legacy-Spilling von Trino auf Festplatte nicht ein.
- Trino hält die Kataloge der Organisation mit ihren Zugangsdaten im Speicher und schreibt keinen auf die Festplatte (`catalog.store=memory`); die SASL-Client-Einstellungen einer Kafka-Quelle liegen in einer Datei, die nur der Benutzer von Trino liest, im Container-Stack im Speicher (siehe [Die Föderations-Engine](#the-federation-engine-trino)).
- Eine PostgreSQL-Föderations-Engine schreibt ihre temporären Dateien dorthin, wo dieser PostgreSQL-Server sie ablegt, unter der eigenen Speicherverschlüsselung des Servers, nicht im Ordner der Organisation.

### Gemeinsam und dediziert gehostete Plattformen {: #shared-and-dedicated-hosted-platforms }

Die gemeinsam gehostete Plattform isoliert Organisationen logisch. Ein Deployment bedient alle, und die Daten jeder Organisation bleiben ihre eigenen durch Governance, ihre eigenen Schemas, Kataloge und Ressourcengruppen auf der Engine, ihre eigenen Schlüssel, Schlüssel pro Abfrage für ausgelagerte Engine-Daten und die Ablehnung ihrer Quellen an den privaten Adressen der Plattform. Sie wird für Experimente und kleine Unternehmen empfohlen. Eine dediziert gehostete Plattform ist physisch getrennt: eigene Maschinen, eigenes Netz und eigene Stores, die einen Kunden bedienen. Für hochsensible oder regulierte Workloads hosten Sie Provisa selbst innerhalb Ihrer eigenen Grenze, wo Ihre eigenen Betreiber jeden Schlüssel und jeden Host halten. Die gehosteten Plattformen haben heute keinen SOC-2-Bericht. Wir prüfen SOC 2 und informieren über Fortschritte. Wenn Sie SOC 2 benötigen, empfehlen wir das Selbsthosting.

### Python im Browser {: #python-in-the-browser }

Die Tabs Python und Notebook von Explore führen Python im Browser des Besuchers aus. Jeder Lesezugriff geht als angemeldeter Benutzer an den Server, mit seiner handelnden Rolle, Organisation und Umgebung, und wird genau wie dieselbe Anweisung von jedem anderen Client gesteuert.

## Hochsicherheitsmodus {: #high-security-mode }

`security.mode: high` in `provisa.yaml` sichert eine Garantie zu: Das Provisa-Backend verarbeitet nie Klartextdaten. (REQ-693) Jede relevante Spalte ist an der Quelle verschlüsselt, und nur ein Client mit dem Entschlüsselungsschlüssel kann sie lesen. Diese Garantie hat Folgen, die ein Deployment einplanen muss.

**Was der Modus tut:**

- **Datenendpunkte verlangen den Nachweis clientseitiger Entschlüsselung.** Alles unter `/data/` liefert 403, sofern der Aufrufer nicht den Header `X-Provisa-KMS-Key` vorlegt — das Kennzeichen eines JDBC- oder Python-Clients, der lokal entschlüsselt. Ein Browser oder ein REST-Konsument im Klartext trägt keinen solchen Schlüssel und wird abgelehnt. Das Gate ist ein Default-Deny über den ganzen Baum: Eine morgen hinzugefügte Route ist am Tag ihrer Auslieferung geschützt, und eine Ausnahme muss begründet werden.
- **Schema-Metadaten-Endpunkte bleiben offen.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` und `/data/compile` liefern keine Zeilendaten, und ein Client muss das Schema lesen — einschließlich der Felder, die `@encrypted` sind —, bevor er sich überhaupt verbinden kann.
- **gRPC und Arrow Flight bedienen weiter, unter demselben Nachweis.** Sie sind die Transporte, die verschlüsselnde Clients tatsächlich nutzen; sie zu schließen ließe ein Hochsicherheits-Deployment ohne Wire-Protokoll. Ein Datenaufruf auf einem von beiden muss denselben KMS-Schlüssel als Aufruf-Metadaten tragen.
- **pgwire, Bolt und MCP starten nicht.** Keiner der drei hat einen Handshake pro Verbindung, der einen Entschlüsselungskontext tragen könnte: Eine pgwire-Zeilenmenge und ein Cypher-Ergebnis sind auf der Leitung Klartext, und ein MCP-Werkzeugaufruf übergibt seine Ergebnisse einem Modell als Text. Ein konfigurierter Port für einen von ihnen wird beim Start abgelehnt statt bedient.
- **Der Beziehungsschutz lässt sich nicht umgehen.** `ignore_relationships` und `relationship_guard: false` werden beide ignoriert; siehe [Governance der Beziehungen](#relationship-governance-v002).

**Prüfen, ob ein Deployment im Modus läuft:** Das Startlog nennt ihn, eine `/data/sql`-Anfrage ohne KMS-Schlüssel antwortet mit 403 und einer Meldung, die REQ-693 nennt, und die Ports für pgwire, Bolt und MCP lauschen nicht.

## ABAC-Genehmigungs-Hook {: #abac-approval-hook }

Ein optionaler externer Richtlinien-Hook, der vor der Abfrageausführung auslöst. (REQ-203) Ist er konfiguriert, ruft Provisa Ihre Policy-Engine mit Benutzeridentität, Rollen, Tabellen, Spalten und Operation auf. Die Antwort bestimmt, ob die Abfrage fortfährt. (REQ-203) Ein mit **Erfordert Genehmigung** registrierter Command wird vor jedem Aufruf demselben Hook vorgelegt; siehe [Command-Aufrufe](#command-calls). (REQ-1924)

### Geltungsbereich {: #scoping }

Der Hook löst nur aus, wenn die Abfrage eine einbezogene Tabelle oder Quelle berührt — kein Overhead für alles andere. (REQ-204)

| Konfiguration | Wirkung |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Jede Abfrage löst den Hook aus |
| `sources[].approval_hook: true` | Alle Tabellen dieser Quelle lösen den Hook aus |
| `tables[].approval_hook: true` | Diese Tabelle löst den Hook aus |

### Protokolle {: #protocols }

Drei Transporte werden unterstützt: (REQ-246)

| Typ | Einsatzfall | Konfigurationsfeld |
| ------ | ---------- | ------------- |
| `webhook` | Jeder HTTP-fähige Richtliniendienst (OPA, eigene) | `url` |
| `unix_socket` | OPA oder Richtlinien-Sidecar auf derselben Maschine | `socket_path` + `url` |
| `grpc` | Richtliniendienst mit hohem Durchsatz am selben Ort | `url` (host:port) |

Der gRPC-Transport nutzt den Vertrag `provisa.auth.ApprovalService`, definiert in `provisa/auth/approval.proto`. Implementieren Sie diesen Service in Ihrer Policy-Engine: (REQ-246)

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
Der gRPC-Kanal ist persistent — ein Kanal pro Provisa-Instanz, wiederverwendet für alle Aufrufe an diesen Hook-Endpunkt. (REQ-555)

### Anfrage / Antwort {: #request-response }

Alle drei Transporte tragen dieselbe Payload: (REQ-246)

| Feld | Typ | Beschreibung |
| ------- | ------ | ------------- |
| `user` | string | Authentifizierte Benutzeridentität |
| `roles` | string[] | Provisa-Rollen des Benutzers |
| `tables` | string[] | In der Abfrage referenzierte Tabellen-IDs |
| `columns` | string[] | In der Abfrage ausgewählte Spalten |
| `operation` | string | `"query"` oder `"mutation"`; `"command"` für einen Command-Aufruf |
| `command` | string | Der Name des Commands bei einem Command-Aufruf; leer bei einer Abfrage |
| `arguments` | object | Die Argumente, mit denen ein Command aufgerufen wird; leer bei einer Abfrage |

Die Webhook- und Unix-Socket-Transporte tauschen JSON aus, mit `command` und `arguments` als Schlüsseln. Bei gRPC reisen die Argumente als JSON-Text `arguments_json`. Die Antwort muss `approved` (bool) und optional `reason` (string) enthalten. (REQ-246)

### Command-Aufrufe {: #command-calls }

Ein Command mit gesetztem `requires_approval` (der Schalter **Erfordert Genehmigung** im Command-Formular) geht vor jedem Aufruf an den Hook, auf jeder Oberfläche. Er läuft nur, wenn der Hook genehmigt. Die Anfrage enthält `operation: "command"`, den Namen des Commands und seine Argumente; `tables` und `columns` sind leer. (REQ-1924) [tool-verified: `provisa/api/data/action_exec.py` `_require_approval` (`operation="command"`, `tables=[]`, `columns=[]`, `command=fn["name"]`, `arguments=args`)]

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
[inferred: the payload also carries `session_vars`; values shown are placeholders]

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
Bei einem Timeout oder Transportfehler gilt die Richtlinie `fallback`. (REQ-247) Ein Circuit Breaker (Standard: offen nach 5 aufeinanderfolgenden Fehlern, halboffen nach 30 s) verhindert kaskadierende Ausfälle durch einen langsamen Hook-Endpunkt. (REQ-556)

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

Anmeldedaten verwenden die Syntax `${env:VAR_NAME}`, zur Laufzeit aufgelöst. (REQ-557) Passwörter werden nie in der Konfigurations-DB gespeichert. (REQ-557)

Den vollständigen Secrets-Dienst — Vaults, Referenzsyntax und Provider — beschreibt [Secrets](secrets.md).
