# Segreti {: #secrets }

**I nomi entrano. I valori non escono mai.**

Nessun endpoint dell'API restituisce il valore di un segreto salvato, e nessuna UI offre un pulsante "mostra". Chi ha perso un valore lo sostituisce, con la stessa chiamata e lo stesso modulo con cui è stato creato. Non è una scelta di policy: il percorso di lettura semplicemente non esiste nel codice.

---

## Sintassi dei riferimenti {: #reference-syntax }

Tre forme di riferimento sono valide ovunque Provisa risolva credenziali:

| Forma | Si risolve da | Chi può usarla |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | L'ambiente del processo server | Solo configurazione di deployment |
| `${secret:NAME}` | Il vault dell'org — condiviso da tutti i membri | Qualsiasi campo che accetta un riferimento a credenziale |
| `${user:NAME}` | Il vault personale di chi sta agendo | Qualsiasi campo che accetta un riferimento a credenziale |

In un deployment multi-tenant, tenete le credenziali della piattaforma in un servizio di segreti e fuori dalle variabili d'ambiente. Un amministratore di tenant può scrivere `${env:...}` in un proprio campo credenziale, quindi qualsiasi valore nell'ambiente del processo è alla portata di ogni tenant. Lasciate nell'ambiente solo la credenziale del backend dei segreti stesso.

La risoluzione è fail-closed ovunque. Un nome di provider sconosciuto, un nome non impostato e un backend irraggiungibile sollevano tutti un errore. Un riferimento che non è stato possibile risolvere non viene mai sostituito silenziosamente con una stringa vuota.

### Formato del nome {: #name-format }

I nomi dei segreti devono corrispondere a `[A-Za-z_][A-Za-z0-9_]*`: lettere, cifre e trattini bassi, iniziando con una lettera o un trattino basso. Il motivo è pratico. `${secret:NAME}` viene analizzato dalla grammatica dei riferimenti, che legge fino alla `}` di chiusura, quindi un nome con una graffa, uno spazio o i due punti produrrebbe un riferimento analizzato come qualcos'altro.

---

## Due vault, un solo servizio {: #two-vaults-one-service }

Ogni org ha due vault. Entrambi risiedono all'interno dello stesso servizio dei segreti.

**Vault dell'org.** La credenziale che un amministratore dell'org salva qui è condivisa, quindi ogni membro che fa riferimento a `${secret:DATABASE_TOKEN}` ottiene lo stesso valore. Usatelo per le credenziali di proprietà dell'*organizzazione*, come una password di database condivisa, la chiave di un account di servizio o un token di deployment. Leggere o scrivere nel vault dell'org richiede la capability `org_settings`.

**Vault personale.** Una credenziale salvata qui appartiene a una sola persona. Quando due persone hanno ciascuna un `GIT_TOKEN`, `${user:GIT_TOKEN}` si risolve in quello di chi sta agendo, così lo stesso testo di riferimento dà a ciascuno la propria credenziale. Chi non ha salvato nulla riceve un errore, mai il valore di un altro. Nessuna capacità protegge il vault personale, perché avere la propria credenziale non è un privilegio concesso da un amministratore, e nessuna sintassi di richiesta nomina il vault di un'altra persona.

L'ambito fa parte del riferimento stesso, e nessun permesso lo avvolge. `${secret:NAME}` e `${user:NAME}` non rispondono mai l'uno per l'altro.

---

## Scegliere un servizio dei segreti {: #choosing-a-secrets-service }

**Admin → Sicurezza → Servizio dei segreti.** Chiunque abbia la capacità `platform_settings` vede il pannello. Elenca ogni backend che la build conosce, che il suo SDK sia installato o no, e una riga in grigio nomina il pacchetto Python mancante invece di nascondere l'opzione.

Cinque backend sono inclusi:

| Chiave | Etichetta | Richiede |
| ----- | ------- | ------- |
| `provisa` | Provisa (integrato, cifrato) | Nulla; è l'impostazione predefinita |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (secrets) | `azure-keyvault-secrets` |

La selezione fallisce in modo chiuso: un backend sconosciuto o non disponibile blocca l'avvio con un errore, e Provisa non ripiega mai in silenzio su un altro.

### La credenziale del backend stesso {: #the-backends-own-credential }

La credenziale di connessione di un backend centrale è configurazione del processo e arriva solo da `${env:...}`. `${secret:...}` non può fornirla, perché un servizio di segreti la cui credenziale vive dentro di sé non potrebbe mai essere aperto. La catena di fiducia termina nell'ambiente dell'host per progetto. Provisa risolve ogni valore di configurazione di un backend dall'ambiente prima di costruire il backend.

Per esempio, una configurazione di Vault in `provisa.yaml`:

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Servizio centrale e store integrato {: #central-service-vs-built-in }

Con un servizio centrale configurato, Provisa legge da esso e non vi scrive mai. Creare ed eliminare voci spetta agli strumenti del servizio stesso, quindi la pagina dei segreti lo dice e non offre un pulsante di creazione.

Con il backend integrato `provisa` attivo, la pagina dei segreti è completamente scrivibile. Create, sostituite ed eliminate i segreti dalla UI o tramite l'API.

---

### I segreti di un'organizzazione in un servizio centrale {: #one-organizations-secrets-in-a-central-service }

Un deployment che ospita molte organizzazioni e legge un servizio centrale lo legge, per ogni organizzazione, all'interno del prefisso proprio di quell'organizzazione. Un riferimento resta il nome breve (`${secret:db#password}`); il prefisso viene aggiunto in lettura.

| Servizio | Dove è conservato il segreto `db` dell'organizzazione `acme` |
|---|---|
| HashiCorp Vault (KV v2) | `orgs/acme/db` |
| AWS Secrets Manager | `orgs/acme/db` |
| GCP Secret Manager | `org-acme--db` |
| Azure Key Vault | `o4-acme-db` (`o`, la lunghezza dell'id dell'organizzazione, l'id con `_` scritto come `-`, poi il nome) |

Un riferimento viene rifiutato, per nome, quando:

- il suo nome uscirebbe dal prefisso: inizia con `/`, contiene un segmento `..` o una barra rovesciata, oppure contiene `/` in GCP o Azure, i cui nomi sono piatti;
- il suo nome è troppo lungo per il servizio una volta prefissato (GCP 255 caratteri, Azure 127); questo viene rifiutato quando il riferimento viene salvato, indicando il limite;
- nessuna organizzazione è legata all'operazione che lo legge.

Un deployment con una sola organizzazione legge un riferimento esattamente come è scritto, senza prefisso: il servizio a cui è collegato è quello di quell'organizzazione, e non c'è nulla da spostare.

**Passare al prefisso.** In un deployment con molte organizzazioni, ogni segreto a cui un'organizzazione fa riferimento deve trovarsi sotto il prefisso di quell'organizzazione. Copiate ciascuno lì con gli strumenti propri del servizio, poi accorciate il riferimento al nome all'interno del prefisso (`${secret:teams/data/db}`, conservato in `orgs/acme/db`, diventa `${secret:db}`). Un riferimento il cui segreto non è stato spostato fallisce come non trovato; non viene letto da dove si trovava.

## Lo store integrato di Provisa {: #provisas-built-in-store }

È il default quando non è configurato un servizio centrale. Ogni riga di `secrets_store` contiene un blob di envelope cifrato. La colonna `value` è binaria, e la chiave di decifratura vive nell'ambiente del processo, fuori dal database. Una copia del control plane senza la master key del deployment contiene testo cifrato e nient'altro.

La cifratura non è mai facoltativa. Senza una chiave di cifratura configurata per l'intero processo, l'archivio usa un portachiavi locale. Se l'host non ha un portachiavi che conservi una chiave, l'archivio rifiuta di scrivere, e il valore non viene mai salvato in chiaro.

**Forma di archiviazione**:

| Colonna | Tipo | Scopo |
| -------- | ------ | --------- |
| `org_id` | Text | L'org proprietaria di questo segreto |
| `owner_id` | Text | `"*"` per il vault dell'org; id utente per il vault personale |
| `name` | Text | Il nome di riferimento |
| `value` | LargeBinary | Blob di busta cifrato |
| `description` | Text | A che cosa serve il segreto — mai derivato dal valore |
| `updated_by` | Text | Chi lo ha impostato per ultimo |

Un'organizzazione che ha creato la propria chiave di cifratura ha i propri segreti avvolti sotto quella chiave; altrimenti sono avvolti sotto quella del deployment. Vedi [Cifratura a riposo](security.md#encryption-at-rest-and-the-organizations-own-keys).

La colonna `value` non viene selezionata in nessuna query di elenco.

---

## Endpoint API {: #api-endpoints }

Tutte le route stanno sotto `/admin/orgs/{org_id}`. Il vault dell'org richiede `org_settings` in quell'org. Il vault personale non richiede alcuna capability, perché il proprietario viene letto dall'identità autenticata, e nessun parametro di richiesta nomina il vault di qualcun altro.

| Metodo | Percorso | Che cosa fa |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | Elenca nomi e riferimenti del vault dell'org |
| `PUT` | `/secrets/{name}` | Crea o sostituisce un segreto dell'org |
| `DELETE` | `/secrets/{name}` | Elimina un segreto dell'org |
| `GET` | `/my-secrets` | Elenca i nomi e i riferimenti personali del chiamante |
| `PUT` | `/my-secrets/{name}` | Crea o sostituisce un segreto del chiamante |
| `DELETE` | `/my-secrets/{name}` | Elimina un segreto del chiamante |

Ogni risposta restituisce metadati e mai il valore: il nome, la descrizione, `updated_at`, `updated_by` e la stringa `reference` da incollare. Il body del `PUT` porta `value` (obbligatorio) e `description` (facoltativo). Sostituire è la stessa chiamata del creare, perché il nome è l'identità e non esiste un ID separato.

Ogni scrittura viene registrata nel log di audit. La voce nomina chi ha agito e il segreto. Né il valore né la sua lunghezza vengono registrati.

---

## Dove si risolve `${secret:NAME}` {: #where-secretname-resolves }

La risoluzione avviene dentro un'operazione vincolata. Nulla si risolve all'import o all'avvio. L'archivio legge e decifra i segreti dell'org una volta, all'inizio dell'operazione, e tiene la mappa finché questa dura. Fuori da un'operazione vincolata, `${secret:NAME}` solleva un errore.

Due punti di chiamata stabiliscono il legame:

**Operazioni Git remote.** L'URL remoto del repository di un'org può contenere un riferimento `${secret:...}` o `${user:...}`, come un token di push incorporato nell'URL. Il router degli ambienti vincola allora sia il vault dell'org sia il vault personale dell'utente che agisce attorno alla chiamata git. Con la forma `${user:GIT_TOKEN}`, un commit finisce sotto la credenziale di chi ha fatto il push. Nessun account di servizio condiviso è coinvolto.

**Lettura delle chiavi API dei fornitori di IA.** Quando Provisa legge la chiave del fornitore LLM di un'org e quella chiave è salvata come riferimento `${secret:NAME}`, vincola il vault dell'org per quella richiesta. Il riferimento viene risolto in uscita, e il testo del riferimento non arriva mai al fornitore.

---

## Chiavi dei fornitori AI dell'org come riferimenti a segreti {: #org-ai-vendor-keys-as-secret-references }

La chiave di un fornitore AI dell'org (Anthropic, OpenAI e altri) può essere memorizzata come riferimento `${secret:NAME}` invece che come chiave letterale.

Memorizzare prima la chiave nel vault dell'org:

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Poi impostare la configurazione AI dell'org perché vi faccia riferimento:

```
vendor key field → ${secret:OPENAI_KEY}
```

Il riferimento è salvato cifrato in `org_secrets`. Al momento della query Provisa risolve `${secret:OPENAI_KEY}` nel vault dell'org e passa la chiave letterale all'SDK del fornitore. La rotazione della voce del vault ha effetto subito, senza modificare la configurazione nelle impostazioni dell'org.

---

## Le credenziali degli store del deployment {: #the-deployments-own-store-credentials }

Le password degli store che Provisa esegue (PostgreSQL, Redis, MinIO, l'account admin di Grafana e il segreto condiviso del cluster Trino) non sono segreti in nessuna delle due vault. Sono generate per deployment e conservate dal deployment: in `~/.provisa/store-credentials.env` (modalità 600), nello stato di Terraform, o nel Secret Kubernetes nominato da `credentials.existingSecret` del chart Helm. Vedi [Hardening del deployment](security.md#deployment-hardening).

---

## Accesso dell'amministratore di piattaforma {: #platform-admin-access }

Un amministratore di piattaforma che gestisce il control plane non può leggere i valori segreti di nessuna org. La guardia `org_settings` rifiuta senz'altro `cross_org` e il bypass di piattaforma, perché amministrare il ciclo di vita di un'org non dà lettura delle credenziali che l'org conserva. Il server lo impone da sé, qualunque cosa mostri la UI.

---

## Vedi anche {: #see-also }

- [Modello di sicurezza](security.md) — controllo degli accessi a livelli, autenticazione e registrazione di audit
- [Riferimento alla configurazione](configuration.md) — sintassi `${env:VAR}` per credenziali a livello di processo
