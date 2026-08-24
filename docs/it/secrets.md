# Segreti

**I nomi entrano. I valori non escono mai.**

Nessun endpoint API restituisce il valore di un segreto memorizzato. Nessuna interfaccia offre un pulsante «mostra». Chi ha perso un valore lo sostituisce — è la stessa chiamata che lo ha creato, attraverso lo stesso modulo. Non è una decisione di policy: nel codice il percorso di lettura semplicemente non esiste. (REQ-1558)

---

## Sintassi dei riferimenti

Tre forme di riferimento sono valide ovunque Provisa risolva credenziali:

| Forma | Si risolve da | Chi può usarla |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | L'ambiente del processo server | Solo configurazione di deployment |
| `${secret:NAME}` | Il vault dell'org — condiviso da tutti i membri | Qualsiasi campo che accetta un riferimento a credenziale |
| `${user:NAME}` | Il vault personale di chi sta agendo | Qualsiasi campo che accetta un riferimento a credenziale |

La risoluzione è fail-closed ovunque. Un nome di provider sconosciuto, un nome non impostato e un backend irraggiungibile sollevano tutti un errore. Un riferimento che non è stato possibile risolvere non viene mai sostituito silenziosamente con una stringa vuota. (REQ-1557) [tool-verified: `provisa/core/secrets.py:92-117`]

### Formato del nome

I nomi dei segreti devono corrispondere a `[A-Za-z_][A-Za-z0-9_]*` — lettere, cifre e trattini bassi, con lettera o trattino basso iniziale. Il vincolo è pratico: `${secret:NAME}` viene analizzato dalla grammatica dei riferimenti, che legge fino alla `}` di chiusura. Un nome contenente una parentesi graffa, uno spazio o due punti produrrebbe un riferimento che viene interpretato come qualcos'altro. [tool-verified: `provisa/core/secrets_store.py:61`]

---

## Due vault, un solo servizio

Ogni org ha due vault. Entrambi risiedono all'interno dello stesso servizio dei segreti. (REQ-1560)

**Vault dell'org** — La credenziale che un amministratore dell'org memorizza qui è condivisa. Ogni membro che fa riferimento a `${secret:DATABASE_TOKEN}` ottiene lo stesso valore. È pensato per le credenziali di proprietà dell'*organizzazione*: una password di database condivisa, la chiave di un account di servizio, un token di deployment. Il vault dell'org richiede la capability `org_settings` in lettura e in scrittura.

**Vault personale** — Una credenziale memorizzata qui appartiene a una sola persona. Quando due persone detengono ciascuna un `GIT_TOKEN`, `${user:GIT_TOKEN}` si risolve in quello di chi sta agendo. Lo stesso testo di riferimento consegna a ciascuna persona la propria credenziale. Chi non ha memorizzato nulla riceve un errore, non il valore di qualcun altro. Nessuna capability protegge il vault personale — detenere una credenziale propria non è un privilegio concesso da un amministratore. E non esiste alcuna sintassi di richiesta per nominare il vault di un'altra persona. [tool-verified: `provisa/api/admin/secrets_router.py:86-103`]

L'ambito fa parte del riferimento, non è un'autorizzazione che lo circonda. `${secret:NAME}` e `${user:NAME}` non rispondono mai l'uno per l'altro.

---

## Scegliere un servizio dei segreti

**Admin → Sicurezza → Servizio dei segreti.** Il pannello è visibile a chiunque detenga la capability `platform_settings`. Ogni backend noto alla build è elencato, che il relativo SDK sia installato o meno. Una riga in grigio indica quale pacchetto Python manca — il pannello lo nomina invece di nascondere del tutto l'opzione.

Cinque backend sono inclusi:

| Chiave | Etichetta | Richiede |
| ----- | ------- | ------- |
| `provisa` | Provisa (integrato, cifrato) | Nulla; è l'impostazione predefinita |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (secrets) | `azure-keyvault-secrets` |

[tool-verified: `provisa/core/secrets_registry.py:161-299`]

La selezione è fail-closed: un backend sconosciuto o non disponibile solleva un errore all'avvio invece di ripiegare silenziosamente su un altro. (REQ-1557)

### La credenziale del backend stesso

La credenziale di connessione di un backend centrale è configurazione di processo. Proviene solo da `${env:...}` — mai da `${secret:...}`. Un servizio dei segreti la cui credenziale risiede al suo interno non può essere aperto, quindi per progettazione la catena di fiducia termina nell'ambiente host. Il registro lo impone: qualsiasi valore di configurazione in una specifica di backend viene risolto con `providers=("env",)` prima che il backend venga costruito. [tool-verified: `provisa/core/secrets_registry.py:128-141`]

Esempio — configurazione di Vault in `provisa.yaml`:

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Servizio centrale e store integrato

Quando è configurato un servizio centrale, Provisa vi legge ma non vi scrive. La creazione e l'eliminazione delle voci appartengono al servizio centrale — quelle operazioni spettano ai suoi strumenti. La pagina Segreti lo dichiara e non offre un pulsante di creazione. (REQ-1557)

Quando è attivo il backend integrato `provisa`, la pagina Segreti è completamente scrivibile: creazione, sostituzione ed eliminazione dalla UI o tramite l'API.

---

## Lo store integrato di Provisa

È l'impostazione predefinita quando non è configurato alcun servizio centrale. Ogni riga di `secrets_store` contiene un blob di busta cifrato — la colonna `value` è binaria, non testuale, e la chiave di decifratura risiede nell'ambiente del processo, non nel database. Una copia del control plane priva della chiave master del deployment contiene testo cifrato e nient'altro. (REQ-1558)

La cifratura non è mai facoltativa. Quando non è configurata alcuna chiave di cifratura a livello di processo, lo store ricorre a un portachiavi locale. Se l'host non dispone di un portachiavi in cui custodire una chiave, lo store rifiuta di scrivere anziché memorizzare il valore in chiaro. [tool-verified: `provisa/core/secrets_store.py:130-159`]

**Forma di archiviazione** [tool-verified: `provisa/core/schema_admin.py:493-505`]:

| Colonna | Tipo | Scopo |
| -------- | ------ | --------- |
| `org_id` | Text | L'org proprietaria di questo segreto |
| `owner_id` | Text | `"*"` per il vault dell'org; id utente per il vault personale |
| `name` | Text | Il nome di riferimento |
| `value` | LargeBinary | Blob di busta cifrato |
| `description` | Text | A che cosa serve il segreto — mai derivato dal valore |
| `updated_by` | Text | Chi lo ha impostato per ultimo |

La colonna `value` non viene selezionata in nessuna query di elenco. [tool-verified: `provisa/core/secrets_store.py:214-235`]

---

## Endpoint API

Tutte le route si trovano sotto `/admin/orgs/{org_id}`. Il vault dell'org richiede `org_settings` in quell'org. Il vault personale non richiede alcuna capability — il proprietario viene letto dall'identità autenticata; non esiste alcun parametro di richiesta per nominare il vault di qualcun altro.

| Metodo | Percorso | Che cosa fa |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | Elenca nomi e riferimenti del vault dell'org |
| `PUT` | `/secrets/{name}` | Crea o sostituisce un segreto dell'org |
| `DELETE` | `/secrets/{name}` | Elimina un segreto dell'org |
| `GET` | `/my-secrets` | Elenca i nomi e i riferimenti personali del chiamante |
| `PUT` | `/my-secrets/{name}` | Crea o sostituisce un segreto del chiamante |
| `DELETE` | `/my-secrets/{name}` | Elimina un segreto del chiamante |

Ogni risposta restituisce metadati — nome, descrizione, `updated_at`, `updated_by` e la stringa `reference` da incollare — ma mai il valore. Il corpo del `PUT` porta `value` (obbligatorio) e `description` (facoltativo). Una sostituzione è la stessa chiamata di una creazione: l'identità è il nome, non un ID separato.

Ogni scrittura viene registrata nel log di audit. La voce di log nomina l'attore e il nome del segreto. Il valore non viene registrato, nemmeno la sua lunghezza. [tool-verified: `provisa/api/admin/secrets_router.py:106-117`]

---

## Dove si risolve `${secret:NAME}`

La risoluzione avviene all'interno di un'operazione legata a un contesto, non al momento dell'import né all'avvio. Lo store legge e decifra i segreti dell'org una sola volta all'inizio di quell'operazione e mantiene la mappa in una `ContextVar` per tutta la sua durata. Al di fuori di un'operazione legata a un contesto, `${secret:NAME}` solleva un errore. (REQ-1557) [tool-verified: `provisa/core/secrets_store.py:269-290`]

Due punti di chiamata stabiliscono il legame:

**Operazioni sul remote Git.** Quando l'URL del remote del repository di un'org contiene un riferimento `${secret:...}` o `${user:...}` — per esempio un token di push incorporato nell'URL — il router degli ambienti lega sia il vault dell'org sia il vault personale dell'utente che sta agendo attorno alla chiamata git. La forma `${user:GIT_TOKEN}` fa sì che un commit venga registrato sotto la credenziale di chi lo ha inviato, non sotto un account di servizio condiviso. [tool-verified: `provisa/api/admin/environments_router.py:1263`]

**Letture della chiave API del fornitore AI.** Quando Provisa legge la chiave del fornitore LLM di un'org e quella chiave è memorizzata come riferimento `${secret:NAME}`, `bound_to_request_org` stabilisce il vault dell'org per quella richiesta. Il riferimento viene risolto in uscita; il testo del riferimento stesso non viene mai inviato al fornitore. (REQ-1580) [tool-verified: `provisa/core/org_secrets.py:76-79`]

---

## Chiavi dei fornitori AI dell'org come riferimenti a segreti

La chiave di un fornitore AI dell'org (Anthropic, OpenAI e altri) può essere memorizzata come riferimento `${secret:NAME}` invece che come chiave letterale. (REQ-1580)

Memorizzare prima la chiave nel vault dell'org:

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Poi impostare la configurazione AI dell'org perché vi faccia riferimento:

```
vendor key field → ${secret:OPENAI_KEY}
```

Il riferimento viene memorizzato cifrato in `org_secrets`. Al momento della query Provisa risolve `${secret:OPENAI_KEY}` sul vault dell'org e consegna la chiave letterale all'SDK del fornitore. La rotazione della voce nel vault ha effetto immediato — nessuna modifica di configurazione sul lato impostazioni dell'org. [tool-verified: `provisa/core/org_secrets.py:64-79`]

---

## Accesso dell'amministratore di piattaforma

Un amministratore di piattaforma che gestisce il control plane non ha alcuna lettura dei valori dei segreti di nessuna org. La protezione `org_settings` rifiuta esplicitamente `cross_org` e il bypass di piattaforma: amministrare il ciclo di vita di un'org non è una lettura delle credenziali che quell'org custodisce. Il server lo impone indipendentemente dalla UI. (REQ-1361) [tool-verified: `provisa/api/admin/secrets_router.py:53-83`]

---

## Vedi anche

- [Modello di sicurezza](security.md) — controllo degli accessi a livelli, autenticazione e registrazione di audit
- [Riferimento alla configurazione](configuration.md) — sintassi `${env:VAR}` per credenziali a livello di processo
