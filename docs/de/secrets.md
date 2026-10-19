# Secrets

**Namen gehen hinein. Werte kommen nie wieder heraus.**

Kein API-Endpunkt gibt einen gespeicherten Secret-Wert zurück, und keine UI bietet eine Schaltfläche „Anzeigen“. Wer einen Wert verloren hat, ersetzt ihn, mit demselben Aufruf und demselben Formular, mit denen er angelegt wurde. Das ist keine Richtlinienentscheidung: Den Leseweg gibt es im Code schlicht nicht.

---

## Referenzsyntax {: #reference-syntax }

Überall dort, wo Provisa Anmeldedaten auflöst, sind drei Referenzformen gültig:

| Form | Wird aufgelöst aus | Wer sie verwenden kann |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | Der Umgebung des Serverprozesses | Nur Deployment-Konfiguration |
| `${secret:NAME}` | Dem Org-Vault — geteilt von allen Mitgliedern | Jedes Feld, das eine Anmeldedaten-Referenz akzeptiert |
| `${user:NAME}` | Dem persönlichen Vault der handelnden Person | Jedes Feld, das eine Anmeldedaten-Referenz akzeptiert |

In einem mandantenfähigen Deployment gehören Plattform-Anmeldedaten in einen Secrets-Dienst und nicht in Umgebungsvariablen. Ein Mandanten-Admin kann `${env:...}` in ein eigenes Anmeldefeld schreiben, also ist jeder Wert in der Prozessumgebung für jeden Mandanten erreichbar. Lassen Sie nur die eigene Anmeldung des Secrets-Backends in der Umgebung.

Die Auflösung ist durchgängig fail-closed. Ein unbekannter Anbietername, ein nicht gesetzter Name und ein nicht erreichbares Backend lösen jeweils einen Fehler aus. Eine Referenz, die nicht aufgelöst werden konnte, wird nie stillschweigend durch eine leere Zeichenkette ersetzt.

### Namensformat {: #name-format }

Secret-Namen müssen `[A-Za-z_][A-Za-z0-9_]*` entsprechen: Buchstaben, Ziffern und Unterstriche, beginnend mit einem Buchstaben oder Unterstrich. Der Grund ist praktisch. `${secret:NAME}` wird von der Referenzgrammatik geparst, die bis zur schließenden `}` liest, also ergäbe ein Name mit Klammer, Leerzeichen oder Doppelpunkt eine Referenz, die als etwas anderes geparst wird.

---

## Zwei Vaults, ein Dienst {: #two-vaults-one-service }

Jede Org hat zwei Vaults. Beide liegen innerhalb desselben Secrets-Dienstes.

**Org-Vault.** Anmeldedaten, die ein Org-Admin hier ablegt, sind geteilt, also erhält jedes Mitglied, das `${secret:DATABASE_TOKEN}` referenziert, denselben Wert. Nutzen Sie ihn für Anmeldedaten, die der *Organisation* gehören, etwa ein geteiltes Datenbankpasswort, einen Service-Account-Schlüssel oder ein Deployment-Token. Lesen und Schreiben im Org-Vault erfordert die Capability `org_settings`.

**Persönlicher Vault.** Eine hier gespeicherte Anmeldung gehört genau einer Person. Haben zwei Personen je ein `GIT_TOKEN`, wird `${user:GIT_TOKEN}` zu dem der gerade handelnden Person aufgelöst, sodass derselbe Referenztext jeder Person ihre eigene Anmeldung gibt. Wer nichts gespeichert hat, bekommt einen Fehler und nie den Wert eines anderen. Keine Fähigkeit schützt den persönlichen Vault, denn die eigene Anmeldung zu halten, ist kein Privileg, das ein Administrator vergibt, und keine Anfragesyntax nennt den Vault einer anderen Person.

Der Geltungsbereich ist Teil der Referenz selbst, keine Berechtigung legt sich um sie. `${secret:NAME}` und `${user:NAME}` antworten nie füreinander.

---

## Einen Secrets-Dienst wählen {: #choosing-a-secrets-service }

**Admin → Sicherheit → Secrets-Dienst.** Jeder mit der Fähigkeit `platform_settings` sieht das Panel. Es listet jedes Backend, das der Build kennt, ob sein SDK installiert ist oder nicht, und eine ausgegraute Zeile nennt das fehlende Python-Paket, statt die Option zu verstecken.

Fünf Backends werden mitgeliefert:

| Schlüssel | Bezeichnung | Benötigt |
| ----- | ------- | ------- |
| `provisa` | Provisa (integriert, verschlüsselt) | Nichts; das ist die Voreinstellung |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (Secrets) | `azure-keyvault-secrets` |

Die Auswahl schlägt sicher fehl: Ein unbekanntes oder nicht verfügbares Backend bricht den Start mit einem Fehler ab, und Provisa weicht nie stillschweigend auf ein anderes aus.

### Die eigenen Anmeldedaten des Backends {: #the-backends-own-credential }

Die Verbindungsanmeldung eines zentralen Backends ist Prozesskonfiguration und kommt allein aus `${env:...}`. `${secret:...}` kann sie nicht liefern, denn ein Secrets-Dienst, dessen eigene Anmeldung in ihm selbst liegt, ließe sich nie öffnen. Die Vertrauenskette endet absichtlich in der Host-Umgebung. Provisa löst jeden Konfigurationswert eines Backends aus der Umgebung auf, bevor es das Backend baut.

Zum Beispiel eine Vault-Konfiguration in `provisa.yaml`:

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Zentraler Dienst vs. integrierter Speicher {: #central-service-vs-built-in }

Ist ein zentraler Dienst konfiguriert, liest Provisa aus ihm und schreibt nie hinein. Einträge anlegen und löschen ist Sache der eigenen Werkzeuge des Dienstes, also sagt die Secrets-Seite das und bietet keine Schaltfläche zum Anlegen.

Mit dem eingebauten Backend `provisa` ist die Secrets-Seite voll beschreibbar. Sie legen Secrets in der UI oder über die API an, ersetzen und löschen sie.

---

### Die Secrets einer Organisation in einem zentralen Dienst {: #one-organizations-secrets-in-a-central-service }

Ein Deployment, das viele Organisationen hält und einen zentralen Dienst liest, liest ihn für jede Organisation innerhalb des eigenen Präfixes dieser Organisation. Eine Referenz bleibt der kurze Name (`${secret:db#password}`); das Präfix wird beim Lesen hinzugefügt.

| Dienst | Wo das Secret `db` der Organisation `acme` liegt |
|---|---|
| HashiCorp Vault (KV v2) | `orgs/acme/db` |
| AWS Secrets Manager | `orgs/acme/db` |
| GCP Secret Manager | `org-acme--db` |
| Azure Key Vault | `o4-acme-db` (`o`, die Länge der Organisations-ID, die ID mit `_` als `-` geschrieben, dann der Name) |

Eine Referenz wird namentlich abgelehnt, wenn:

- ihr Name das Präfix verlassen würde: Er beginnt mit `/`, enthält ein `..`-Segment oder einen Backslash, oder enthält `/` bei GCP oder Azure, deren Namen flach sind;
- ihr Name mit Präfix für den Dienst zu lang ist (GCP 255 Zeichen, Azure 127); das wird beim Speichern der Referenz abgelehnt, mit Angabe der Grenze;
- an die Operation, die sie liest, keine Organisation gebunden ist.

Ein Deployment mit einer Organisation liest eine Referenz genau so, wie sie geschrieben ist, ohne Präfix: Der Dienst, an den es angebunden ist, gehört dieser Organisation, und nichts muss verschoben werden.

**Umzug unter das Präfix.** Auf einem Deployment mit vielen Organisationen muss jedes Secret, auf das eine Organisation verweist, unter dem Präfix dieser Organisation liegen. Kopieren Sie jedes dorthin mit dem eigenen Werkzeug des Dienstes und kürzen Sie dann die Referenz auf den Namen innerhalb des Präfixes (`${secret:teams/data/db}`, abgelegt unter `orgs/acme/db`, wird zu `${secret:db}`). Eine Referenz, deren Secret nicht verschoben wurde, schlägt als nicht gefunden fehl; sie wird nicht von dort gelesen, wo es lag.

## Der integrierte Speicher von Provisa {: #provisas-built-in-store }

Das ist der Standard, wenn kein zentraler Dienst konfiguriert ist. Jede Zeile in `secrets_store` enthält einen verschlüsselten Envelope-Blob. Die Spalte `value` ist binär, und der Entschlüsselungsschlüssel liegt in der Prozessumgebung, außerhalb der Datenbank. Eine Kopie der Steuerungsebene ohne den Master-Schlüssel des Deployments enthält Chiffretext und sonst nichts.

Verschlüsselung ist nie optional. Ist kein prozessweiter Verschlüsselungsschlüssel konfiguriert, nutzt der Speicher einen lokalen Schlüsselbund. Hat der Host keinen Schlüsselbund für einen Schlüssel, verweigert der Speicher das Schreiben, und der Wert wird nie im Klartext abgelegt.

**Speicherform**:

| Spalte | Typ | Zweck |
| -------- | ------ | --------- |
| `org_id` | Text | Die Org, der dieses Secret gehört |
| `owner_id` | Text | `"*"` für den Org-Vault; Benutzer-ID für den persönlichen Vault |
| `name` | Text | Der Referenzname |
| `value` | LargeBinary | Verschlüsselter Envelope-Blob |
| `description` | Text | Wofür das Secret da ist — nie aus dem Wert abgeleitet |
| `updated_by` | Text | Wer es zuletzt gesetzt hat |

Eine Organisation, die einen eigenen Verschlüsselungsschlüssel angelegt hat, hat ihre Secrets unter diesem Schlüssel umhüllt; andernfalls sind sie unter dem Schlüssel des Deployments umhüllt. Siehe [Verschlüsselung ruhender Daten](security.md#encryption-at-rest-and-the-organizations-own-keys).

Die Spalte `value` wird in keiner Listenabfrage selektiert.

---

## API-Endpunkte {: #api-endpoints }

Alle Routen liegen unter `/admin/orgs/{org_id}`. Der Org-Vault erfordert `org_settings` in dieser Org. Der persönliche Vault erfordert keine Capability, denn der Besitzer wird aus der authentifizierten Identität gelesen, und kein Anfrageparameter nennt den Vault einer anderen Person.

| Methode | Pfad | Was sie tut |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | Namen und Referenzen des Org-Vaults auflisten |
| `PUT` | `/secrets/{name}` | Ein Org-Secret anlegen oder ersetzen |
| `DELETE` | `/secrets/{name}` | Ein Org-Secret löschen |
| `GET` | `/my-secrets` | Die persönlichen Namen und Referenzen der aufrufenden Person auflisten |
| `PUT` | `/my-secrets/{name}` | Ein Secret der aufrufenden Person anlegen oder ersetzen |
| `DELETE` | `/my-secrets/{name}` | Ein Secret der aufrufenden Person löschen |

Jede Antwort liefert Metadaten und nie den Wert: Name, Beschreibung, `updated_at`, `updated_by` und den einzufügenden `reference`-String. Der `PUT`-Body enthält `value` (Pflicht) und `description` (optional). Ersetzen ist derselbe Aufruf wie Anlegen, denn der Name ist die Identität, und es gibt keine separate ID.

Jeder Schreibvorgang wird im Audit-Log festgehalten. Der Eintrag nennt den Handelnden und das Secret. Weder der Wert noch seine Länge werden festgehalten.

---

## Wo `${secret:NAME}` aufgelöst wird {: #where-secretname-resolves }

Die Auflösung geschieht innerhalb einer gebundenen Operation. Nichts wird beim Import oder beim Start aufgelöst. Der Speicher liest und entschlüsselt die Secrets der Org einmal, wenn die Operation beginnt, und hält die Zuordnung, solange sie läuft. Außerhalb einer gebundenen Operation löst `${secret:NAME}` einen Fehler aus.

Zwei Aufrufstellen stellen die Bindung her:

**Git-Remote-Operationen.** Die Remote-URL des Repositorys einer Org kann eine Referenz `${secret:...}` oder `${user:...}` enthalten, etwa ein in die URL eingebettetes Push-Token. Der Environments-Router bindet dann den Org-Vault und den persönlichen Vault des handelnden Benutzers um den Git-Aufruf. Mit der Form `${user:GIT_TOKEN}` landet ein Commit unter der Anmeldung dessen, der gepusht hat. Kein geteiltes Dienstkonto ist beteiligt.

**Lesen von KI-Anbieter-API-Schlüsseln.** Liest Provisa den LLM-Anbieterschlüssel einer Org und ist dieser als Referenz `${secret:NAME}` gespeichert, bindet es den Org-Vault für diese Anfrage. Die Referenz wird beim Hinausgehen aufgelöst, und der Referenztext selbst erreicht den Anbieter nie.

---

## KI-Anbieterschlüssel einer Org als Secret-Referenzen {: #org-ai-vendor-keys-as-secret-references }

Der KI-Anbieterschlüssel einer Org (Anthropic, OpenAI und andere) kann statt als literaler Schlüssel als `${secret:NAME}`-Referenz abgelegt werden.

Legen Sie den Schlüssel zuerst im Org-Vault ab:

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Setzen Sie dann die KI-Konfiguration der Org so, dass sie darauf verweist:

```
vendor key field → ${secret:OPENAI_KEY}
```

Die Referenz wird verschlüsselt in `org_secrets` gespeichert. Zur Abfragezeit löst Provisa `${secret:OPENAI_KEY}` gegen den Org-Vault auf und übergibt den eigentlichen Schlüssel an das SDK des Anbieters. Eine Rotation des Vault-Eintrags wirkt sofort, ohne Konfigurationsänderung in den Org-Einstellungen.

---

## Die eigenen Store-Anmeldedaten des Deployments {: #the-deployments-own-store-credentials }

Die Passwörter der Stores, die Provisa betreibt (PostgreSQL, Redis, MinIO, das Admin-Konto von Grafana und das gemeinsame Secret des Trino-Clusters), sind in keinem der beiden Vaults Secrets. Sie werden pro Deployment generiert und vom Deployment verwahrt: in `~/.provisa/store-credentials.env` (Modus 600), im Terraform-State oder im Kubernetes-Secret, das `credentials.existingSecret` des Helm-Charts benennt. Siehe [Deployment-Härtung](security.md#deployment-hardening).

---

## Zugriff durch Plattform-Admins {: #platform-admin-access }

Ein Plattform-Admin, der die Steuerungsebene betreibt, kann die Secret-Werte keiner Org lesen. Der Guard `org_settings` lehnt `cross_org` und den Plattform-Bypass rundweg ab, denn den Lebenszyklus einer Org zu verwalten, gibt keinen Lesezugriff auf die Anmeldedaten dieser Org. Der Server setzt das selbst durch, unabhängig von der UI.

---

## Siehe auch {: #see-also }

- [Sicherheitsmodell](security.md) — mehrschichtige Zugriffssteuerung, Authentifizierung und Audit-Protokollierung
- [Konfigurationsreferenz](configuration.md) — `${env:VAR}`-Syntax für Anmeldedaten auf Prozessebene
