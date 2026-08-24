# Secrets

**Namen gehen hinein. Werte kommen nie wieder heraus.**

Kein API-Endpunkt gibt einen gespeicherten Secret-Wert zurück. Keine Oberfläche bietet eine Schaltfläche „Anzeigen“. Wer einen Wert verloren hat, ersetzt ihn — das ist derselbe Aufruf, der ihn angelegt hat, über dasselbe Formular. Das ist keine Richtlinienentscheidung: Der Lesepfad existiert im Code schlicht nicht. (REQ-1558)

---

## Referenzsyntax

Überall dort, wo Provisa Anmeldedaten auflöst, sind drei Referenzformen gültig:

| Form | Wird aufgelöst aus | Wer sie verwenden kann |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | Der Umgebung des Serverprozesses | Nur Deployment-Konfiguration |
| `${secret:NAME}` | Dem Org-Vault — geteilt von allen Mitgliedern | Jedes Feld, das eine Anmeldedaten-Referenz akzeptiert |
| `${user:NAME}` | Dem persönlichen Vault der handelnden Person | Jedes Feld, das eine Anmeldedaten-Referenz akzeptiert |

Die Auflösung ist durchgängig fail-closed. Ein unbekannter Anbietername, ein nicht gesetzter Name und ein nicht erreichbares Backend lösen jeweils einen Fehler aus. Eine Referenz, die nicht aufgelöst werden konnte, wird nie stillschweigend durch eine leere Zeichenkette ersetzt. (REQ-1557) [tool-verified: `provisa/core/secrets.py:92-117`]

### Namensformat

Secret-Namen müssen `[A-Za-z_][A-Za-z0-9_]*` entsprechen — Buchstaben, Ziffern und Unterstriche, beginnend mit einem Buchstaben oder Unterstrich. Die Einschränkung ist praktischer Natur: `${secret:NAME}` wird von der Referenzgrammatik geparst, die bis zur schließenden `}` liest. Ein Name mit geschweifter Klammer, Leerzeichen oder Doppelpunkt ergäbe eine Referenz, die als etwas anderes geparst wird. [tool-verified: `provisa/core/secrets_store.py:61`]

---

## Zwei Vaults, ein Dienst

Jede Org hat zwei Vaults. Beide liegen innerhalb desselben Secrets-Dienstes. (REQ-1560)

**Org-Vault** — Anmeldedaten, die ein Org-Admin hier ablegt, sind geteilt. Jedes Mitglied, das `${secret:DATABASE_TOKEN}` referenziert, erhält denselben Wert. Das ist für Anmeldedaten gedacht, die der *Organisation* gehören: ein geteiltes Datenbankpasswort, ein Service-Account-Schlüssel, ein Deployment-Token. Der Org-Vault erfordert die Capability `org_settings` zum Lesen und Schreiben.

**Persönlicher Vault** — Anmeldedaten, die hier abgelegt sind, gehören genau einer Person. Wenn zwei Personen je ein `GIT_TOKEN` halten, löst `${user:GIT_TOKEN}` auf diejenige auf, die gerade handelt. Derselbe Referenztext gibt jeder Person ihre eigenen Anmeldedaten. Wer nichts abgelegt hat, bekommt einen Fehler und nicht den Wert einer anderen Person. Keine Capability schützt den persönlichen Vault — die eigenen Anmeldedaten zu halten ist kein Privileg, das eine Administratorin vergibt. Und es gibt keine Anfragesyntax, um den Vault einer anderen Person zu benennen. [tool-verified: `provisa/api/admin/secrets_router.py:86-103`]

Der Geltungsbereich ist Teil der Referenz, nicht eine Berechtigung darum herum. `${secret:NAME}` und `${user:NAME}` antworten nie füreinander.

---

## Einen Secrets-Dienst wählen

**Admin → Sicherheit → Secrets-Dienst.** Das Panel ist für alle sichtbar, die die Capability `platform_settings` halten. Jedes Backend, das der Build kennt, wird aufgeführt, unabhängig davon, ob das SDK installiert ist. Eine ausgegraute Zeile nennt das fehlende Python-Paket — das Panel benennt es, statt die Option ganz zu verbergen.

Fünf Backends werden mitgeliefert:

| Schlüssel | Bezeichnung | Benötigt |
| ----- | ------- | ------- |
| `provisa` | Provisa (integriert, verschlüsselt) | Nichts; das ist die Voreinstellung |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (Secrets) | `azure-keyvault-secrets` |

[tool-verified: `provisa/core/secrets_registry.py:161-299`]

Die Auswahl ist fail-closed: Ein unbekanntes oder nicht verfügbares Backend löst beim Start einen Fehler aus, statt stillschweigend auf ein anderes zurückzufallen. (REQ-1557)

### Die eigenen Anmeldedaten des Backends

Die Verbindungs-Anmeldedaten eines zentralen Backends sind Prozesskonfiguration. Sie kommen ausschließlich aus `${env:...}` — nie aus `${secret:...}`. Ein Secrets-Dienst, dessen eigene Anmeldedaten in ihm selbst liegen, lässt sich nicht öffnen; die Vertrauenskette endet daher konstruktionsbedingt in der Host-Umgebung. Die Registry erzwingt das: Jeder Konfigurationswert einer Backend-Spezifikation wird mit `providers=("env",)` aufgelöst, bevor das Backend konstruiert wird. [tool-verified: `provisa/core/secrets_registry.py:128-141`]

Beispiel — Vault-Konfiguration in `provisa.yaml`:

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Zentraler Dienst vs. integrierter Speicher

Ist ein zentraler Dienst konfiguriert, liest Provisa aus ihm, schreibt aber nicht in ihn. Der zentrale Dienst besitzt das Anlegen und Löschen von Einträgen — diese Operationen gehören zu seinem eigenen Werkzeug. Die Secrets-Seite sagt das und bietet keine Schaltfläche zum Anlegen. (REQ-1557)

Ist das integrierte Backend `provisa` aktiv, ist die Secrets-Seite voll schreibbar: anlegen, ersetzen und löschen über die Oberfläche oder die API.

---

## Der integrierte Speicher von Provisa

Die Voreinstellung, wenn kein zentraler Dienst konfiguriert ist. Jede Zeile in `secrets_store` hält einen verschlüsselten Envelope-Blob — die Spalte `value` ist binär, nicht Text, und der Entschlüsselungsschlüssel liegt in der Prozessumgebung, nicht in der Datenbank. Eine Kopie der Control Plane ohne den Master-Schlüssel des Deployments hält Chiffretext und sonst nichts. (REQ-1558)

Verschlüsselung ist nie optional. Ist kein prozessweiter Verschlüsselungsschlüssel konfiguriert, weicht der Speicher auf einen lokalen Schlüsselbund aus. Hat der Host keinen Schlüsselbund, der einen Schlüssel halten kann, verweigert der Speicher den Schreibvorgang, statt den Wert im Klartext abzulegen. [tool-verified: `provisa/core/secrets_store.py:130-159`]

**Speicherform** [tool-verified: `provisa/core/schema_admin.py:493-505`]:

| Spalte | Typ | Zweck |
| -------- | ------ | --------- |
| `org_id` | Text | Die Org, der dieses Secret gehört |
| `owner_id` | Text | `"*"` für den Org-Vault; Benutzer-ID für den persönlichen Vault |
| `name` | Text | Der Referenzname |
| `value` | LargeBinary | Verschlüsselter Envelope-Blob |
| `description` | Text | Wofür das Secret da ist — nie aus dem Wert abgeleitet |
| `updated_by` | Text | Wer es zuletzt gesetzt hat |

Die Spalte `value` wird in keiner Listenabfrage selektiert. [tool-verified: `provisa/core/secrets_store.py:214-235`]

---

## API-Endpunkte

Alle Routen liegen unter `/admin/orgs/{org_id}`. Der Org-Vault erfordert `org_settings` in dieser Org. Der persönliche Vault erfordert keine Capability — der Eigentümer wird aus der authentifizierten Identität gelesen; es gibt keinen Anfrageparameter, um den Vault einer anderen Person zu benennen.

| Methode | Pfad | Was sie tut |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | Namen und Referenzen des Org-Vaults auflisten |
| `PUT` | `/secrets/{name}` | Ein Org-Secret anlegen oder ersetzen |
| `DELETE` | `/secrets/{name}` | Ein Org-Secret löschen |
| `GET` | `/my-secrets` | Die persönlichen Namen und Referenzen der aufrufenden Person auflisten |
| `PUT` | `/my-secrets/{name}` | Ein Secret der aufrufenden Person anlegen oder ersetzen |
| `DELETE` | `/my-secrets/{name}` | Ein Secret der aufrufenden Person löschen |

Jede Antwort liefert Metadaten — Name, Beschreibung, `updated_at`, `updated_by` und die einzufügende `reference`-Zeichenkette —, aber nie den Wert. Der `PUT`-Body trägt `value` (erforderlich) und `description` (optional). Ein Ersetzen ist derselbe Aufruf wie ein Anlegen: Der Name ist die Identität, keine separate ID.

Jeder Schreibvorgang wird im Audit-Log festgehalten. Der Eintrag nennt die handelnde Person und den Secret-Namen. Der Wert wird nicht festgehalten, nicht einmal seine Länge. [tool-verified: `provisa/api/admin/secrets_router.py:106-117`]

---

## Wo `${secret:NAME}` aufgelöst wird

Die Auflösung geschieht innerhalb einer kontextgebundenen Operation, nicht zur Importzeit und nicht beim Start. Der Speicher liest und entschlüsselt die Secrets der Org einmal zu Beginn dieser Operation und hält die Zuordnung für ihre Dauer in einer `ContextVar`. Außerhalb einer gebundenen Operation löst `${secret:NAME}` einen Fehler aus. (REQ-1557) [tool-verified: `provisa/core/secrets_store.py:269-290`]

Zwei Aufrufstellen stellen die Bindung her:

**Git-Remote-Operationen.** Enthält die Remote-URL des Repositorys einer Org eine `${secret:...}`- oder `${user:...}`-Referenz — etwa ein in der URL eingebettetes Push-Token —, bindet der Environments-Router sowohl den Org-Vault als auch den persönlichen Vault der handelnden Person um den Git-Aufruf. Die Form `${user:GIT_TOKEN}` bedeutet, dass ein Commit unter den Anmeldedaten derjenigen Person landet, die ihn gepusht hat, und nicht unter einem geteilten Service-Account. [tool-verified: `provisa/api/admin/environments_router.py:1263`]

**Lesen von KI-Anbieter-API-Schlüsseln.** Liest Provisa den LLM-Anbieterschlüssel einer Org und ist dieser Schlüssel als `${secret:NAME}`-Referenz abgelegt, stellt `bound_to_request_org` den Org-Vault für diese Anfrage her. Die Referenz wird auf dem Weg nach draußen aufgelöst; der Referenztext selbst wird nie an den Anbieter gesendet. (REQ-1580) [tool-verified: `provisa/core/org_secrets.py:76-79`]

---

## KI-Anbieterschlüssel einer Org als Secret-Referenzen

Der KI-Anbieterschlüssel einer Org (Anthropic, OpenAI und andere) kann statt als literaler Schlüssel als `${secret:NAME}`-Referenz abgelegt werden. (REQ-1580)

Legen Sie den Schlüssel zuerst im Org-Vault ab:

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Setzen Sie dann die KI-Konfiguration der Org so, dass sie darauf verweist:

```
vendor key field → ${secret:OPENAI_KEY}
```

Die Referenz wird verschlüsselt in `org_secrets` abgelegt. Zur Abfragezeit löst Provisa `${secret:OPENAI_KEY}` gegen den Org-Vault auf und übergibt den literalen Schlüssel an das Anbieter-SDK. Ein Rotieren des Vault-Eintrags wirkt sofort — ohne Konfigurationsänderung auf der Seite der Org-Einstellungen. [tool-verified: `provisa/core/org_secrets.py:64-79`]

---

## Zugriff durch Plattform-Admins

Ein Plattform-Admin, der die Control Plane betreibt, hat keinen Lesezugriff auf die Secret-Werte irgendeiner Org. Der `org_settings`-Wächter verweigert `cross_org` und den Plattform-Bypass ausdrücklich: den Lebenszyklus einer Org zu verwalten ist kein Lesen der Anmeldedaten, die diese Org verwahrt. Der Server erzwingt das unabhängig von der Oberfläche. (REQ-1361) [tool-verified: `provisa/api/admin/secrets_router.py:53-83`]

---

## Siehe auch

- [Sicherheitsmodell](security.md) — mehrschichtige Zugriffssteuerung, Authentifizierung und Audit-Protokollierung
- [Konfigurationsreferenz](configuration.md) — `${env:VAR}`-Syntax für Anmeldedaten auf Prozessebene
