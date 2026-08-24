# Sicherheitsmodell
Provisa setzt ein mehrschichtiges Sicherheitsmodell über alle Abfragesprachen (GraphQL, SQL, Cypher) und alle Transporte (REST, gRPC, Arrow Flight, JDBC, WebSocket) hinweg durch. (REQ-001, REQ-266) Governance wird einheitlich angewendet — es gibt keinen Abfragepfad, der sie umgeht. (REQ-002, REQ-266)

Die Schichten gelten der Reihe nach. Eine Anfrage muss jede Schicht durchlaufen, bevor die nächste ausgewertet wird.

## Schichtenmodell
### Schicht 0 — Introspektionsfilterung
Das Schema und der Katalog, die einer Rolle angezeigt werden, enthalten nur die Tabellen aus ihrer `domain_access`-Liste und die Spalten, die die spaltenspezifischen `visible_to`-Regeln erfüllen. (REQ-039) Objekte außerhalb des Zugriffsbereichs einer Rolle sind zum Zeitpunkt der Erkennung unsichtbar — sie können weder abgefragt noch autovervollständigt noch als existent abgeleitet werden. (REQ-039) Dies gilt für das GraphQL-Schema, den SQL-Katalog und den Schema-Browser des Abfrage-Editors. (REQ-039, REQ-363)

Siehe [Schema-Sichtbarkeit](#schema-sichtbarkeit).

### Schicht 1 — Öffentlicher Zugriff
Tabellen in Domänen ohne `domain_access`-Beschränkung sind für alle authentifizierten Identitäten ohne zusätzliche Konfiguration sichtbar. Keine Reibungsverluste für tatsächlich öffentliche Daten.

### Schicht 2 — Domänenzugriff
Jede Rolle besitzt eine `domain_access`-Liste von Domänen-IDs. Eine Abfrage, die eine Tabelle außerhalb dieser Domänen berührt, wird vor der Ausführung abgelehnt. (REQ-038, REQ-039) Dies ist die grobkörnige Eigentumsgrenze — eine HR-Rolle kann unabhängig davon, wie das SQL geschrieben ist, nicht auf Finanztabellen zugreifen. (REQ-002)

Siehe [Rechtemodell](#rechtemodell).

### Schicht 3 — Sicherheit auf Zeilenebene
Nachdem der Domänenzugriff bestätigt wurde, werden `WHERE`-Prädikate pro Tabelle und Rolle zur Ausführungszeit in jedes `SELECT` injiziert. (REQ-041, REQ-263) Die Prädikate werden gegen die Rohdaten ausgewertet. Ein Regionalleiter, der eine gemeinsam genutzte Bestelltabelle abfragt, sieht selbst bei einem `SELECT *` nur die Zeilen seiner Region. (REQ-264)

Siehe [Sicherheit auf Zeilenebene (RLS)](#sicherheit-auf-zeilenebene-rls).

### Schicht 4 — Spaltensichtbarkeit und Maskierung
Spalten mit einer `visible_to`-Liste, die die anfragende Rolle ausschließt, werden aus der Abfrageausgabe entfernt. (REQ-040, REQ-263) Bei Spalten mit einer Maskierungsregel werden die Werte ersetzt — durch Redaktion per regulärem Ausdruck, Ersetzung durch eine Konstante oder Kürzung — bevor die Ergebnisse den Server verlassen. (REQ-263) Die Maskierung gilt in allen Abfragesprachen und Ausgabeformaten. (REQ-263)

Siehe [Spaltenberechtigungsmodell](#spaltenberechtigungsmodell) und [Maskierung auf Spaltenebene](#maskierung-auf-spaltenebene).

### Schicht 5 — Prädikatsschutz
Maskierte Spalten werden in `WHERE`- und `HAVING`-Klauseln abgelehnt. (REQ-263) Ohne diesen Schutz könnte ein Aufrufer den unmaskierten Wert durch binäre Suche in einem Filter ableiten, selbst wenn die Ausgabe maskiert ist. Die Ablehnung erfolgt zum Zeitpunkt des Abfrage-Parsens, vor der Ausführung. (REQ-531)

### Governance der Beziehungen (V002)
JOIN-Bedingungen in SQL müssen einer registrierten, genehmigten Beziehung zwischen Tabellen entsprechen. (REQ-001) Nicht genehmigte Joins werden abgelehnt. Jede Beziehung trägt einen für Menschen lesbaren Grund und eine Beschreibung — eine Orientierungshilfe sowohl für Benutzer als auch für autonome Agenten, warum ein Traversierungspfad existiert. Dies ist eine Governance-Richtlinie, keine harte Sicherheitsgrenze: Die Schichten 2–5 gelten unabhängig von der Join-Struktur, sodass eine gezielte Umgehung keine Daten offenlegt, die die Rolle nicht auch über zwei separate Abfragen erreichen könnte. Umgehungsversuche werden protokolliert und sind auditierbar.

**Umgehungsmechanismen** — V002 kann auf zwei Wegen umgangen werden. Der erste ist eine Fähigkeit: Eine Rolle mit `ignore_relationships` verknüpft Relationen, die der Katalog nicht abdeckt. Unter den vorkonfigurierten Systemrollen besitzt nur `modeler` sie — die Erkundungsrolle, deren Aufgabe es ist, das Modell zu bestimmen, statt es durchzusetzen. (REQ-1297) `analyst` besitzt sie nicht. [tool-verified: `provisa/core/db.py:84`]

Der zweite ist ein Opt-out mit zwei Bedingungen, die beide erfüllt sein müssen:

1. **Rollen-Flag** — `relationship_guard: false` in der Rollendefinition (Standard: `true`). [tool-verified: `provisa/core/models.py:349`]
2. **Opt-out pro Abfrage** — das SQL enthält den Kommentar `--relationship-guard=false`. [tool-verified: `provisa/compiler/params.py:80`]

Das Rollen-Flag allein umgeht V002 nicht; der Kommentar allein umgeht V002 nicht.

**Der Hochsicherheitsmodus fixiert den Schutz.** Unter `security.mode: high` greift keine der beiden Umgehungen: `ignore_relationships` wird ignoriert, `relationship_guard: false` wird ignoriert, und jeder Join muss im Katalog der genehmigten Beziehungen vorhanden sein. (REQ-693) Das ist bewusste Redundanz — eine Produktionsrolle, der die Fähigkeit versehentlich gewährt wurde, kann trotzdem nicht aus dem Modell ausbrechen. [tool-verified: `provisa/pgwire/_pipeline.py:377`]

**GraphQL-Pfad** — V002 wird bei GraphQL-Abfragen bedingungslos übersprungen. In SDL definierte Beziehungen sind konstruktionsbedingt bereits genehmigt; die Prüfung ist redundant und wird nicht angewendet. [tool-verified: `provisa/api/data/endpoint.py:468`]

**SQL- und Cypher-Pfade** — V002 ist standardmäßig aktiv. Sowohl `endpoint_dev.py` als auch `cypher_router.py` wenden die Zwei-Bedingungen-Prüfung an, bevor `validate_sql` aufgerufen wird. [tool-verified: `provisa/api/data/endpoint_dev.py:127`, `provisa/api/rest/cypher_router.py:260`]

**pgwire-Pfad** — dieselbe Zwei-Bedingungen-Prüfung wie bei SQL. Der Kommentar `--relationship-guard=false` wird vor der Ausführung aus der Abfrage entfernt; er erreicht die Datenbank nicht. [tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

Diese Schichten kombinieren sich. Eine Rolle mit Domänenzugriff, RLS und maskierten Spalten hat alle fünf Einschränkungen gleichzeitig aktiv. Das Hinzufügen einer neuen Datenquelle, Spalte oder Beziehung erfordert keine Aktualisierung jeder einzelnen Regel — jede Schicht wird unabhängig konfiguriert und gilt automatisch für jede Abfrage, die gesteuerte Objekte berührt.

---

## Rechtemodell
Unabhängig zugewiesene Fähigkeiten mit optionaler Rollenhierarchie über `parent_role_id`. `admin` gewährt alle. (REQ-042)

| Fähigkeit | Beschreibung |
| ----------- | ------------- |
| `source_registration` | Datenquellen registrieren |
| `table_registration` | Tabellen, Spalten registrieren |
| `create_relationship` | Fremdschlüsselbeziehungen definieren |
| `access_config` | RLS, Maskierung konfigurieren |
| `query_development` | Abfragen ausführen |
| `write` | Registrierte Mutationen aufrufen (grobkörnige Schranke; siehe Mutationsautorisierung) |
| `full_results` | Stichprobenlimits umgehen |
| `ignore_relationships` | Beziehungs-Governance umgehen (V002). Unter den Systemrollen nur von `modeler` gehalten und im Hochsicherheitsmodus vollständig ignoriert |
| `admin` | Superuser — gewährt alle Fähigkeiten |

### Rollenvererbung
Rollen können Fähigkeiten und Domänenzugriff von einer übergeordneten Rolle über `parent_role_id` erben. (REQ-215) Die Hierarchie wird beim Start abgeflacht — untergeordnete Rollen führen die Fähigkeiten und den Domänenzugriff ihrer übergeordneten Rolle mit ihren eigenen zusammen. (REQ-215)

```yaml
roles:
  - id: basic_user
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```

## Spaltenberechtigungsmodell
Jede Spalte verfügt über ein Berechtigungsmodell mit vier Feldern, das Lese-, Schreib- und Maskierungszugriff pro Rolle steuert. (REQ-042, REQ-249)

### Drei Sichtbarkeitsstufen
| Stufe | Bedingung | Ergebnis |
| ------ | ----------- | -------- |
| **Verborgen** | Rolle nicht in `visible_to` | Spalte fehlt im GraphQL-SDL |
| **Maskiert** | Rolle in `visible_to`, hat Maskierungsregel, Rolle nicht in `unmasked_to` | Spalte sichtbar, aber Daten in SQL maskiert |
| **Unmaskiert** | Rolle in `visible_to` UND Rolle in `unmasked_to` (oder keine Maskierungsregel) | Vollständiger Lesezugriff |

### Schreibberechtigungen
| Feld | Leer bedeutet | Zweck |
| ------- | ------------ | --------- |
| `visible_to` | Alle Rollen können lesen | Steuert, wer die Spalte sieht (maskiert oder unmaskiert) |
| `unmasked_to` | Keine Rolle sieht unmaskierte Werte | Steuert, wer die Maskierung umgeht |
| `writable_by` | Keine Rolle kann schreiben | Steuert, wer ändern darf (INSERT/UPDATE) |

Die Schreibberechtigung wird in der Mutationspipeline durchgesetzt. Eine Rolle, die nicht in `writable_by` enthalten ist, erhält beim Versuch, in eine eingeschränkte Spalte zu schreiben, einen 403-Fehler. (REQ-033, REQ-034)

### Beispiel
```yaml
columns:
  - name: email
    visible_to: [admin, analyst, viewer]
    writable_by: [admin]
    unmasked_to: [admin]
    mask_type: regex
    mask_pattern: "(.).*@"
    mask_replace: "$1***@"
  - name: salary
    visible_to: [admin, hr]
    writable_by: [hr]
    unmasked_to: [admin, hr]
    mask_type: constant
    mask_value: "0"
  - name: created_at
    visible_to: []           # all can read
    writable_by: []          # nobody can write (auto-set)
```

In diesem Beispiel:

- `email`: admin sieht `alice@example.com` und kann bearbeiten; analyst/viewer sehen `a***@example.com`
- `salary`: admin und hr sehen den echten Wert; hr kann bearbeiten; alle anderen Rollen sehen die Spalte überhaupt nicht
- `created_at`: alle können lesen, niemand kann schreiben

## Mutationsautorisierung
Registrierte Mutationen (Remote-GraphQL, OpenAPI, gRPC, Hasura) unterliegen zwei unabhängigen Prüfungen. (REQ-867, REQ-868) Eine Rolle darf eine Mutation nur aufrufen, wenn sie die globale Fähigkeit `write` besitzt UND in der `writable_by`-Liste dieser Mutation aufgeführt ist. (REQ-868) Ein leeres `writable_by` bedeutet standardmäßige Ablehnung — keine Rolle kann sie aufrufen. (REQ-867)

Mutationen werden vertraglich als Schreibvorgänge klassifiziert, nicht durch die Angabe des Aufrufers. (REQ-869) Ein `SELECT`, das auf eine Funktion vom Mutationstyp verweist, wird zu einem Schreibvorgang hochgestuft und unterliegt derselben Zwei-Schranken-Prüfung, sodass ein Aufrufer eine Mutation nicht als Lesevorgang tarnen kann. (REQ-869) Eine Neuklassifizierung einer Mutation als lesesicher erfordert die Fähigkeit `access_config` und wird als Governance-Entscheidung protokolliert; es gibt kein Opt-out pro Anfrage. (REQ-870)

## Schema-Sichtbarkeit
Rollenspezifische GraphQL-Schemas verbergen nicht autorisierte Inhalte: (REQ-039)

- **Domänenzugriff**: Die Rolle sieht Tabellen nur in ihren `domain_access`-Domänen (`"*"` = alle) (REQ-039)
- **Spaltensichtbarkeit**: Spalten, die nicht in `visible_to` für eine Rolle enthalten sind, werden aus dem SDL ausgelassen (REQ-039)
- Nicht autorisierte Tabellen/Spalten erscheinen nicht im Schema (REQ-039)

## Sicherheit auf Zeilenebene (RLS)
Injektion von SQL-WHERE-Klauseln pro Tabelle und Rolle. Wird nach der Kompilierung, vor der Ausführung angewendet. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

Der Filter wird per UND-Verknüpfung in die WHERE-Klausel der Abfrage eingefügt. Funktioniert sowohl für Abfragen als auch für Mutationen (UPDATE/DELETE). (REQ-035, REQ-041)

## Maskierung auf Spaltenebene
Die Maskierung wird einmal pro Spalte definiert — sie ist eine Eigenschaft der Spalte, nicht der Rolle. Das Feld `unmasked_to` steuert, welche Rollen sie umgehen. (REQ-249)

| Maskierungstyp | Unterstützte Typen | SQL-Ausdruck |
| ----------- | ---------------- | ---------------- |
| `regex` | Zeichenkette (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Beliebig | Literalwert (NULL, 0, benutzerdefiniert) |
| `truncate` | Datum/Timestamp | `DATE_TRUNC(precision, col)` |

Die Maskierung wird in die SQL-SELECT-Projektion verlagert — die Datenbank liefert maskierte Daten zurück. (REQ-263) Unmaskierte Daten gelangen für maskierte Rollen nie über die Leitung. (REQ-263) Maskierte Spalten werden außerdem in `WHERE`- und `HAVING`-Klauseln blockiert (Prädikatsschutz der Schicht 5), um eine Ableitung des unmaskierten Werts durch Filterung zu verhindern. (REQ-263, REQ-531)

## Stichprobenerhebung
Alle Rollen sehen stichprobenartige Ergebnisse (Standard: 100 Zeilen), sofern sie nicht über die Fähigkeit `full_results` verfügen. (REQ-554) Gesteuert über die Umgebungsvariable `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Audit-Protokollierung
Jede Abfrage, die ein Domänen-Asset berührt, wird im Nur-Anhängen-Protokoll `query_audit_log` erfasst. (REQ-596, REQ-613) Jede Zeile erfasst `tenant_id`, `user_id`, `role_id`, einen SHA-256-Hash des Abfragetexts, `table_ids`, `source`, `status_code`, `duration_ms` und `logged_at`. (REQ-596) Der Abfragetext wird niemals im Klartext gespeichert — nur sein Hash. (REQ-596)

Das Protokoll ist auf Datenbankebene nur anhängend: PostgreSQL-Regeln blockieren `DELETE` und `UPDATE`. (REQ-596, REQ-613) Zwei Indizes — `(tenant_id, logged_at)` und `(user_id, logged_at)` — unterstützen mandantenbezogene und benutzerbezogene Compliance-Abfragen über Zeiträume. (REQ-596, REQ-613)

Wenn die Verschlüsselung aktiviert ist, wird die Spalte mit dem Hash des Abfragetexts verschlüsselt gespeichert und nur bei autorisierten Administratorzugriffen entschlüsselt. (REQ-689)

## Ratenbegrenzung
Ratenlimits pro Rolle werden in `provisa.yaml` konfiguriert: maximale Anfragen pro Sekunde, maximale Anzahl gleichzeitiger SSE-Abonnements und maximale Anzahl gleichzeitiger Arrow-Flight-Streams. (REQ-369) Die Limits werden auf der API-Schicht vor der Kompilierung oder Ausführung durchgesetzt; Anfragen über dem Limit werden mit HTTP 429 und einem `Retry-After`-Header abgelehnt. (REQ-369)

Der NL-Abfragedienst (`POST /query/nl`) hat ein unabhängiges Limit über `nl.rate_limit` (Anfragen pro Minute pro Rolle). Anfragen über dem Limit werden abgelehnt, bevor ein LLM-Aufruf erfolgt. (REQ-370)

Der Zustand der Ratenbegrenzung liegt in Redis (`cache.redis_url`) als gleitender Fensterzähler vor — kein Zustand pro Instanz — sodass die Limits über alle horizontal skalierten Provisa-Instanzen hinweg gelten. (REQ-371)

## Authentifizierung
Austauschbare Authentifizierungsanbieter: (REQ-120)

| Anbieter | Token-Typ | Anwendungsfall |
| ---------- | ----------- | ---------- |
| `none` | X-Provisa-Role-Header | Entwicklung |
| `basic` | bcrypt-basierte lokale Konten + JWT | Eigenständige Deployments |
| `firebase` | Firebase-ID-Token | Produktion |
| `keycloak` | Keycloak-JWT | Unternehmen |
| `oauth` | OIDC-JWT | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | Tests |

Rollenzuordnung: Identitätsansprüche (Claims) → Provisa-Rolle über konfigurierbare Regeln. (REQ-120) Das Feld `assignments_source` bestimmt, woher die Rollenzuweisungen stammen: `claims` liest sie aus den Claims des JWT-Tokens (Standard), `provisa` liest sie aus dem internen Zuweisungsspeicher von Provisa. (REQ-551)

Ein in `provisa.yaml` konfigurierter Superuser (Benutzername plus ein Passwort aus einem Umgebungssecret) erhält unabhängig vom konfigurierten Anbieter immer die Admin-Rolle und alle Fähigkeiten — ein Bootstrap-Pfad für die Ersteinrichtung. (REQ-125)

### Oberflächen und Anmeldeinformationen
Jede Oberfläche authentifiziert über denselben Anbietervertrag, sodass eine Anmeldeinformation, die auf einer funktioniert, auf allen funktioniert, wo das Protokoll sie transportieren kann. (REQ-124, REQ-1263) Diese Tabelle ist die einzige Referenz; die Dokumente der einzelnen Oberflächen wiederholen sie nicht.

| Oberfläche | Passwort | Anbieter-Token | Personal Access Token | Client-Zertifikat (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | über terminierenden Proxy |
| pgwire | Passwortfeld (Klartext oder SCRAM) | Passwortfeld, OIDC-Deployments | Passwortfeld | ja |
| Bolt | Schema `basic` | Schema `bearer` | Schema `bearer` | ja |
| Arrow Flight | — | `token` im Handshake oder in der Ticket-Payload | dasselbe | ja |
| gRPC | — | Metadaten `authorization` | Metadaten `authorization` | ja |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | über terminierenden Proxy |

Wo eine Zelle `—` zeigt, führt das Protokoll kein Benutzernamensfeld mit, mit dem sich ein Passwort paaren ließe; die Token-Formen decken es ab. pgwire ist der Spiegelfall: Das Startup-Paket hat ein einziges Geheimnisfeld und kein Schema, sodass die Methode davon abhängt, *was* das Geheimnis ist — ein PAT wird an seinem Präfix erkannt, das Geheimnis wird als Bearer-Token gelesen, wenn der konfigurierte Anbieter ein Token-Anbieter ist, und alles andere ist ein Passwort. Die Wahl wird einmal getroffen — eine Anmeldeinformation, die der gewählte Validator ablehnt, wird nicht gegen einen anderen erneut versucht.

Die Matrix wird von `tests/unit/test_auth_surface_conformance.py` durchgesetzt, das den echten Validierungseinstiegspunkt jeder Oberfläche ansteuert und fehlschlägt, wenn eine neue Oberfläche ohne Zeile hinzugefügt wird.

### Personal Access Tokens

Ein PAT ist ein langlebiges Bearer-Geheimnis, das ein Benutzer für einen Client erzeugt, der keine interaktive Anmeldung durchführen kann — ein Skript, ein BI-Tool, ein Treiber. (REQ-1263) Es trägt seine eigene Organisation und Rolle, und jede Oberfläche löst es über denselben Validator auf, sodass keine Oberfläche wissen muss, was ein PAT ist.

Die Wire-Form ist `provisa_pat_` gefolgt von 43 URL-sicheren Base64-Zeichen. Das Präfix leitet ein vorgelegtes Geheimnis an den Token-Speicher statt an den Identitätsanbieter und macht ein geleaktes Token in Protokollen und Repositories auffindbar.

- **Speicherung** — nur der SHA-256 des Geheimnisses wird aufbewahrt. Das Geheimnis selbst wird genau einmal bei der Erstellung angezeigt und kann nicht wiederhergestellt werden. Die Auflistung führt das Anzeigepräfix und die Lebenszyklus-Zeitstempel mit, niemals eine funktionierende Anmeldeinformation.
- **Ausstellung und Widerruf** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}` sowie der Self-Service-Bereich im eigenen Profil des Benutzers in der Admin-Oberfläche. Das Erzeugen und Widerrufen einer Anmeldeinformation ist die Handlung des Token-Inhabers.
- **Zuordnung** — ein validiertes PAT löst sich auf das Konto seines Eigentümers auf: Benutzer-ID, E-Mail und Anzeigename. Eine unter einem PAT geschriebene Audit-Zeile oder ein Nutzungsbericht nennt daher die Person, nicht die Anmeldeinformation. Welches Token dieser Person gehandelt hat, wird getrennt in `raw_claims["token_name"]` geführt.
- **Ablauf** — ein Token kann ein Ablaufdatum tragen; ein abgelaufenes Token wird bei der Validierung abgelehnt. Das Löschen der Mitgliedschaft eines Benutzers widerruft dessen Tokens mit.

### SCRAM-SHA-256 auf pgwire
Unter dem Anbieter `basic` bewirkt `auth.scram: true`, dass pgwire SASL (Authentifizierungscode 10) mit dem Mechanismus `SCRAM-SHA-256` anbietet, sodass ein Passwort nachgewiesen statt gesendet wird. (REQ-1394) Channel Binding (`SCRAM-SHA-256-PLUS`) wird nicht angeboten.

SCRAM benötigt einen Verifier nach RFC 5802, der sich nicht aus einem bcrypt-Hash ableiten lässt. Ein Verifier wird geschrieben, wann immer ein Passwort im Klartext durchläuft — Registrierung, Anmeldung, Passwortänderung, Admin-Zurücksetzung — sodass ein Deployment, das SCRAM einschaltet, Verifier sammelt, während sich seine Benutzer das nächste Mal authentifizieren, und die erste SCRAM-Verbindung jedes Benutzers auf dessen nächste Passworteingabe folgt. Einem Benutzer ohne Verifier wird mit einem Schein-Austausch geantwortet, der von einem echten nicht zu unterscheiden ist, sodass die Leitung nicht verrät, wer bereits migriert ist.

### Mutual TLS

Die Client-Zertifikatsprüfung verlegt die erste Kontrolle in den TLS-Handshake: Ein Aufrufer ohne ein von der CA des Deployments signiertes Zertifikat erreicht die Anmeldeinformationsschicht nie. (REQ-1228) Sie ist auf pgwire, Bolt, gRPC und Arrow Flight verfügbar — den vier Transporten, die ihr TLS selbst terminieren.

| Variable | Bedeutung |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | PEM-Bündel der CA(s), die Client-Zertifikate signieren dürfen |
| `PROVISA_MTLS_MODE` | `required` (Standard, sobald eine CA gesetzt ist) oder `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Wenn wahr, muss der Common Name des Zertifikats dem Benutzernamen entsprechen, als der sich die Verbindung anschließend authentifiziert |

Protokollspezifische Überschreibungen folgen derselben Benennung wie die TLS-Einstellungen. Nichts wird erschlossen: Ein ohne CA gesetzter Modus verweigert den Start, und ein unbekannter Modus verweigert den Start, statt als der sicherste Nachbar gelesen zu werden — ein Deployment, das glaubt, Client-Zertifikate zu verlangen, und es nicht tut, ist schlechter dran als eines, das nicht startet.

### Anmelde-Drosselung
Passwortraten ist protokollunabhängig: Dasselbe Konto kann über HTTP, pgwire und Bolt bearbeitet werden. Der Zähler sitzt daher auf der Ebene der Anmeldeinformationsvalidierung und nicht auf einer einzelnen Oberfläche, sodass eine irgendwo verdiente Sperre überall durchgesetzt wird. (REQ-1393)

Sie ist standardmäßig aktiv — fünf Fehlversuche in fünf Minuten sperren das Subjekt für fünfzehn Minuten — und wird unter `auth.login_throttle` eingestellt. Ein gesperrtes Subjekt wird abgewiesen, bevor die Anmeldeinformation überhaupt geprüft wird, und eine erfolgreiche Authentifizierung löscht die Historie dieses Subjekts.

Der Schlüssel ist der Principal, den das Protokoll mitführt. Eine reine Bearer-Oberfläche führt keinen Principal mit, daher ist der Schlüssel ein Digest der Anmeldeinformation selbst; das verhindert, dass ein einzelnes fehlerhaftes Token unbegrenzt wiederholt wird. Der Speicher ist prozesslokal, sodass ein Deployment mit mehreren API-Workern bis zu `max_attempts` pro Worker zulässt — die Drosselung ist eine Bremse für das Raten, keine verteilte Quote.

### Adressierung einer Organisation auf einem Wire-Protokoll
Unter Mandantenfähigkeit wird eine Organisation über den Hostnamen adressiert: `acme.provisa.dev` ist die Organisation `acme`. Über HTTP kommt dieser Name im `Host`-Header an. Ein pgwire- oder Bolt-Client sendet keinen solchen Header, wohl aber den gewählten Hostnamen im TLS-ClientHello, und Provisa liest die Organisation von dort. (REQ-1234) Am Client ändert sich nichts — die Verbindung zu `acme.provisa.dev` genügt.

Der Hostname ist eine Anfrage, keine Gewährung. Er erreicht denselben Resolver wie der `Host`-Header, der jede Organisation ablehnt, in der der authentifizierte Principal weder Mitglied ist noch das organisationsübergreifende Recht besitzt. Das Wählen eines Hostnamens, in dem Sie keine Mitgliedschaft haben, erreicht keine Daten. Ein Client, der sich über eine IP-Adresse verbunden hat, sendet keinen Hostnamen und löst seine Organisation allein aus dem Principal auf — was bei einem Einzel-Organisations-Deployment jede Verbindung ist.

gRPC, Arrow Flight und MCP übergeben ihre Zertifikate an Bibliotheken, die keinen Hostnamen-Callback bereitstellen; diese Transporte benennen eine Organisation stattdessen mit dem Metadaten-Header `x-provisa-org`.

## Hochsicherheitsmodus
`security.mode: high` in `provisa.yaml` sichert eine Zusage zu: Das Provisa-Backend verarbeitet niemals Klartextdaten. (REQ-693) Jede relevante Spalte ist an der Quelle verschlüsselt, und nur ein Client mit dem Entschlüsselungsschlüssel kann sie lesen. Diese Zusage hat Folgen, die ein Deployment einplanen muss.

**Was der Modus bewirkt:**

- **Datenendpunkte verlangen den Nachweis clientseitiger Entschlüsselung.** Alles unter `/data/` liefert 403, sofern der Aufrufer nicht den Header `X-Provisa-KMS-Key` mitführt — das Kennzeichen eines JDBC- oder Python-Clients, der lokal entschlüsselt. Ein Browser oder ein Klartext-REST-Konsument führt keinen solchen Schlüssel mit und wird abgewiesen. Die Sperre ist ein Default-Deny über den gesamten Baum: Eine morgen hinzugefügte Route ist am Tag ihrer Auslieferung gesperrt, und eine Ausnahme muss begründet werden.
- **Schema-Metadaten-Endpunkte bleiben offen.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` und `/data/compile` liefern keine Zeilendaten, und ein Client muss das Schema lesen — einschließlich der Frage, welche Felder `@encrypted` sind — bevor er sich überhaupt verbinden kann.
- **gRPC und Arrow Flight liefern weiter, unter demselben Nachweis.** Sie sind die Transporte, die verschlüsselnde Clients tatsächlich nutzen; sie zu schließen ließe ein Hochsicherheits-Deployment ohne Wire-Protokoll zurück. Ein Datenaufruf über beide muss denselben KMS-Schlüssel als Aufrufmetadaten mitführen.
- **pgwire, Bolt und MCP starten nicht.** Keines der drei hat einen Handshake pro Verbindung, der einen Entschlüsselungskontext transportieren könnte: Ein pgwire-Ergebnissatz und ein Cypher-Ergebnis sind auf der Leitung Klartext, und ein MCP-Tool-Aufruf übergibt seine Ergebnisse als Text an ein Modell. Ein konfigurierter Port für eines von ihnen wird beim Start abgelehnt statt bedient.
- **Der Beziehungsschutz kann nicht umgangen werden.** `ignore_relationships` und `relationship_guard: false` werden beide ignoriert; siehe [Governance der Beziehungen](#governance-der-beziehungen-v002).

**So prüfen Sie, ob ein Deployment im Modus läuft:** Das Startprotokoll nennt ihn, eine `/data/sql`-Anfrage ohne KMS-Schlüssel antwortet mit 403 und einer Meldung, die REQ-693 nennt, und die Ports für pgwire, Bolt und MCP lauschen nicht.

## ABAC-Genehmigungs-Hook
Ein optionaler externer Richtlinien-Hook, der vor der Ausführung der Abfrage ausgelöst wird. (REQ-203) Bei entsprechender Konfiguration ruft Provisa Ihre Policy-Engine mit der Benutzeridentität, den Rollen, den Tabellen, den Spalten und der Operation auf. Die Antwort bestimmt, ob die Abfrage fortgesetzt wird. (REQ-203)

### Geltungsbereich
Der Hook wird nur ausgelöst, wenn die Abfrage eine Tabelle oder Quelle im festgelegten Geltungsbereich berührt — kein Overhead für alles andere. (REQ-204)

| Konfiguration | Effekt |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Jede Abfrage löst den Hook aus |
| `sources[].approval_hook: true` | Alle Tabellen dieser Quelle lösen den Hook aus |
| `tables[].approval_hook: true` | Diese Tabelle löst den Hook aus |

### Protokolle
Drei Transporte werden unterstützt: (REQ-246)

| Typ | Anwendungsfall | Konfigurationsfeld |
| ------ | ---------- | ------------- |
| `webhook` | Jeder HTTP-fähige Policy-Dienst (OPA, benutzerdefiniert) | `url` |
| `unix_socket` | OPA oder Policy-Sidecar auf derselben Maschine | `socket_path` + `url` |
| `grpc` | Hochdurchsatz-Policy-Dienst am selben Standort | `url` (Host:Port) |

Der gRPC-Transport verwendet den Vertrag `provisa.auth.ApprovalService`, der in `provisa/auth/approval.proto` definiert ist. Implementieren Sie diesen Dienst in Ihrer Policy-Engine: (REQ-246)

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
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```

Der gRPC-Kanal ist dauerhaft — ein Kanal pro Provisa-Instanz, der für alle Aufrufe an diesen Hook-Endpunkt wiederverwendet wird. (REQ-555)

### Anfrage / Antwort
Alle drei Transporte übertragen dieselbe Nutzlast: (REQ-246)

| Feld | Typ | Beschreibung |
| ------- | ------ | ------------- |
| `user` | string | Identität des authentifizierten Benutzers |
| `roles` | string[] | Provisa-Rollen des Benutzers |
| `tables` | string[] | In der Abfrage referenzierte Tabellen-IDs |
| `columns` | string[] | In der Abfrage ausgewählte Spalten |
| `operation` | string | `"query"` oder `"mutation"` |

Die Transporte Webhook und Unix-Socket tauschen JSON aus. Die Antwort muss `approved` (bool) enthalten und optional `reason` (string). (REQ-246)

### Timeout und Fallback
```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

Bei einem Timeout oder Transportfehler greift die `fallback`-Richtlinie. (REQ-247) Ein Circuit Breaker (Standard: öffnet nach 5 aufeinanderfolgenden Fehlern, halb offen nach 30 s) verhindert kaskadierende Ausfälle durch einen langsamen Hook-Endpunkt. (REQ-556)

### Konfigurationsbeispiel
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

Anmeldedaten verwenden die Syntax `${env:VAR_NAME}`, die zur Laufzeit aufgelöst wird. (REQ-557) Passwörter werden niemals in der Konfigurationsdatenbank gespeichert. (REQ-557)

Den vollständigen Secrets-Dienst — Vaults, Referenzsyntax und Anbieter — beschreibt [Secrets](secrets.md).
