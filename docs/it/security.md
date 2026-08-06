# Modello di sicurezza

Provisa applica un modello di sicurezza a più livelli su ogni linguaggio di query (GraphQL, SQL, Cypher) e ogni trasporto (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) La governance viene applicata in modo uniforme — non esiste alcun percorso di query che la aggiri. (REQ-002, REQ-266)

I livelli si applicano in ordine. Una richiesta deve superare ogni livello prima che venga valutato quello successivo.

## Modello a livelli

### Livello 0 — Filtro dell'introspezione

Lo schema e il catalogo presentati a un ruolo contengono solo le tabelle nella sua lista `domain_access` e le colonne che superano le regole `visible_to` per colonna. (REQ-039) Gli oggetti al di fuori dell'accesso di un ruolo sono invisibili al momento della scoperta — non possono essere interrogati, completati automaticamente o dedotti come esistenti. (REQ-039) Questo vale per lo schema GraphQL, il catalogo SQL e il browser dello schema dell'editor di query. (REQ-039, REQ-363)

Vedere [Visibilità dello schema](#visibilita-dello-schema).

### Livello 1 — Accesso pubblico

Le tabelle di domini senza restrizione `domain_access` sono visibili a tutte le identità autenticate senza configurazione aggiuntiva. Nessun attrito per i dati genuinamente pubblici.

### Livello 2 — Accesso al dominio

Ogni ruolo ha una lista `domain_access` di ID di dominio. Una query che tocca una tabella al di fuori di quei domini viene rifiutata prima dell'esecuzione. (REQ-038, REQ-039) Questo è il confine di proprietà a grana grossa — un ruolo HR non può raggiungere le tabelle finanziarie, indipendentemente da come è scritto l'SQL. (REQ-002)

Vedere [Modello dei diritti](#modello-dei-diritti).

### Livello 3 — Sicurezza a livello di riga

Dopo che l'accesso al dominio è confermato, i predicati `WHERE` per tabella e per ruolo vengono iniettati in ogni `SELECT` al momento dell'esecuzione. (REQ-041, REQ-263) I predicati vengono valutati sui dati grezzi. Un responsabile regionale che interroga una tabella ordini condivisa vede solo le righe della propria regione, anche con un `SELECT *`. (REQ-264)

Vedere [Sicurezza a livello di riga (RLS)](#sicurezza-a-livello-di-riga-rls).

### Livello 4 — Visibilità e mascheramento delle colonne

Le colonne con una lista `visible_to` che esclude il ruolo richiedente vengono rimosse dall'output della query. (REQ-040, REQ-263) Le colonne con una regola di mascheramento hanno i valori sostituiti — tramite redazione con espressione regolare, sostituzione con una costante o troncamento — prima che i risultati lascino il server. (REQ-263) Il mascheramento si applica in tutti i linguaggi di query e formati di output. (REQ-263)

Vedere [Modello delle autorizzazioni di colonna](#modello-delle-autorizzazioni-di-colonna) e [Mascheramento a livello di colonna](#mascheramento-a-livello-di-colonna).

### Livello 5 — Protezione dei predicati

Le colonne mascherate vengono rifiutate nelle clausole `WHERE` e `HAVING`. (REQ-263) Senza questo, chi effettua la chiamata potrebbe dedurre il valore non mascherato tramite ricerca binaria in un filtro, anche se l'output è mascherato. Il rifiuto viene applicato al momento dell'analisi della query, prima dell'esecuzione. (REQ-531)

### Governance delle relazioni (V002)

Le condizioni JOIN in SQL devono corrispondere a una relazione registrata e approvata tra le tabelle. (REQ-001) I join non approvati vengono rifiutati. Ogni relazione porta un motivo e una descrizione leggibili da un essere umano — un orientamento sia per gli utenti sia per gli agenti autonomi sul perché esiste un percorso di attraversamento. Questa è una politica di governance, non un confine di sicurezza rigido: i livelli 2–5 restano validi indipendentemente dalla struttura del join, quindi un'elusione deliberata non espone dati che il ruolo non potrebbe comunque raggiungere tramite due query separate. I tentativi di elusione vengono registrati e sono verificabili tramite audit.

**Meccanismi di bypass** — V002 può essere aggirato in due modi. Il primo è una capacità: un ruolo che possiede `ignore_relationships` esegue join su relazioni che il catalogo non copre. Tra i ruoli di sistema predefiniti solo `modeler` la possiede — il ruolo di scoperta, il cui compito è determinare il modello anziché applicarlo. (REQ-1297) `analyst` non la possiede. [tool-verified: `provisa/core/db.py:84`]

Il secondo è una rinuncia a due condizioni, entrambe necessarie:

1. **Flag del ruolo** — `relationship_guard: false` nella definizione del ruolo (predefinito: `true`). [tool-verified: `provisa/core/models.py:349`]
2. **Esclusione per query** — l'SQL contiene il commento `--relationship-guard=false`. [tool-verified: `provisa/compiler/params.py:80`]

Il flag del ruolo da solo non aggira V002; il commento da solo non aggira V002.

**La modalità ad alta sicurezza blocca la protezione.** Sotto `security.mode: high` non si applica nessuno dei due bypass: `ignore_relationships` viene ignorato, `relationship_guard: false` viene ignorato, e ogni join deve esistere nel catalogo delle relazioni approvate. (REQ-693) È una ridondanza deliberata — un ruolo di produzione a cui la capacità è stata concessa per errore continua a non poter uscire dal modello. [tool-verified: `provisa/pgwire/_pipeline.py:377`]

**Percorso GraphQL** — V002 viene saltato incondizionatamente per le query GraphQL. Le relazioni definite in SDL sono preapprovate per progettazione; il controllo è ridondante e non viene applicato. [tool-verified: `provisa/api/data/endpoint.py:468`]

**Percorsi SQL e Cypher** — V002 è attivo per impostazione predefinita. Sia `endpoint_dev.py` sia `cypher_router.py` applicano il controllo a due condizioni prima di chiamare `validate_sql`. [tool-verified: `provisa/api/data/endpoint_dev.py:127`, `provisa/api/rest/cypher_router.py:260`]

**Percorso pgwire** — stesso controllo a due condizioni dell'SQL. Il commento `--relationship-guard=false` viene rimosso dalla query prima dell'esecuzione; non raggiunge il database. [tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

Questi livelli si combinano tra loro. Un ruolo con accesso al dominio, RLS e colonne mascherate ha tutti e cinque i vincoli attivi contemporaneamente. Aggiungere una nuova origine dati, colonna o relazione non richiede l'aggiornamento di ogni singola regola — ogni livello viene configurato in modo indipendente e si applica automaticamente a qualsiasi query che tocchi oggetti governati.

---

## Modello dei diritti

Capacità assegnate in modo indipendente, con gerarchia di ruoli opzionale tramite `parent_role_id`. `admin` le concede tutte. (REQ-042)

| Capacità | Descrizione |
| ----------- | ------------- |
| `source_registration` | Registrare le origini dati |
| `table_registration` | Registrare tabelle, colonne |
| `create_relationship` | Definire relazioni di chiave esterna |
| `access_config` | Configurare RLS, mascheramento |
| `query_development` | Eseguire query |
| `write` | Invocare le mutazioni registrate (controllo a grana grossa; vedere Autorizzazione delle mutazioni) |
| `full_results` | Bypassare i limiti di campionamento |
| `ignore_relationships` | Bypassare la governance delle relazioni (V002). Posseduta solo da `modeler` tra i ruoli di sistema, e ignorata del tutto in modalità ad alta sicurezza |
| `admin` | Superuser — concede tutte le capacità |

### Ereditarietà dei ruoli

I ruoli possono ereditare capacità e accesso al dominio da un ruolo padre tramite `parent_role_id`. (REQ-215) La gerarchia viene appiattita all'avvio — i ruoli figli uniscono le capacità e l'accesso al dominio del padre con i propri. (REQ-215)

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

## Modello delle autorizzazioni di colonna

Ogni colonna ha un modello di autorizzazioni a quattro campi che controlla l'accesso in lettura, scrittura e mascheramento per ruolo. (REQ-042, REQ-249)

### Visibilità a tre livelli

| Livello | Condizione | Risultato |
| ------ | ----------- | -------- |
| **Nascosta** | Il ruolo non è in `visible_to` | Colonna assente dall'SDL GraphQL |
| **Mascherata** | Il ruolo è in `visible_to`, ha una regola di mascheramento, il ruolo non è in `unmasked_to` | Colonna visibile ma dati mascherati in SQL |
| **Non mascherata** | Il ruolo è in `visible_to` E il ruolo è in `unmasked_to` (oppure nessuna regola di mascheramento) | Accesso in lettura completo |

### Autorizzazioni di scrittura

| Campo | Vuoto significa | Scopo |
| ------- | ------------ | --------- |
| `visible_to` | Tutti i ruoli possono leggere | Controlla chi vede la colonna (mascherata o non mascherata) |
| `unmasked_to` | Nessun ruolo la vede non mascherata | Controlla chi bypassa il mascheramento |
| `writable_by` | Nessun ruolo può scrivere | Controlla chi può modificare (INSERT/UPDATE) |

L'autorizzazione di scrittura viene applicata nella pipeline di mutazione. Un ruolo non presente in `writable_by` riceve un errore 403 quando tenta di scrivere in una colonna con restrizioni. (REQ-033, REQ-034)

### Esempio

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

In questo esempio:

- `email`: admin vede `alice@example.com` e può modificare; analyst/viewer vedono `a***@example.com`
- `salary`: admin e hr vedono il valore reale; hr può modificare; tutti gli altri ruoli non vedono affatto la colonna
- `created_at`: tutti possono leggere, nessuno può scrivere

## Autorizzazione delle mutazioni

Le mutazioni registrate (GraphQL remoto, OpenAPI, gRPC, Hasura) sono soggette a due controlli indipendenti. (REQ-867, REQ-868) Un ruolo può invocare una mutazione solo se possiede la capacità globale `write` E compare nella lista `writable_by` di quella mutazione. (REQ-868) Un `writable_by` vuoto equivale a un rifiuto predefinito — nessun ruolo può invocarla. (REQ-867)

Le mutazioni sono classificate come scritture per contratto, non per dichiarazione di chi effettua la chiamata. (REQ-869) Un `SELECT` che fa riferimento a una funzione di tipo mutazione viene promosso a scrittura ed è soggetto allo stesso controllo a due livelli, in modo che chi effettua la chiamata non possa invocare una mutazione mascherandola da lettura. (REQ-869) Riclassificare una mutazione come sicura per la lettura richiede la capacità `access_config` e viene registrata come decisione di governance; non esiste un'esclusione per singola richiesta. (REQ-870)

## Visibilità dello schema

Gli schemi GraphQL per ruolo nascondono i contenuti non autorizzati: (REQ-039)

- **Accesso al dominio**: il ruolo vede le tabelle solo nei propri domini `domain_access` (`"*"` = tutti) (REQ-039)
- **Visibilità delle colonne**: le colonne non presenti in `visible_to` per un ruolo vengono omesse dall'SDL (REQ-039)
- Le tabelle/colonne non autorizzate non compaiono nello schema (REQ-039)

## Sicurezza a livello di riga (RLS)

Iniezione di clausole SQL WHERE per tabella e per ruolo. Applicata dopo la compilazione, prima dell'esecuzione. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

Il filtro viene combinato con AND nella clausola WHERE della query. Funziona sia per le query sia per le mutazioni (UPDATE/DELETE). (REQ-035, REQ-041)

## Mascheramento a livello di colonna

Il mascheramento viene definito una sola volta per colonna — è una proprietà della colonna, non del ruolo. Il campo `unmasked_to` controlla quali ruoli lo bypassano. (REQ-249)

| Tipo di mascheramento | Tipi supportati | Espressione SQL |
| ----------- | ---------------- | ---------------- |
| `regex` | Stringa (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Qualsiasi | Valore letterale (NULL, 0, personalizzato) |
| `truncate` | Data/Timestamp | `DATE_TRUNC(precision, col)` |

Il mascheramento viene applicato direttamente nella proiezione SQL SELECT — il database restituisce dati mascherati. (REQ-263) I dati non mascherati non attraversano mai la rete per i ruoli mascherati. (REQ-263) Le colonne mascherate sono inoltre bloccate nelle clausole `WHERE` e `HAVING` (protezione dei predicati del livello 5) per impedire di dedurre il valore non mascherato tramite filtraggio. (REQ-263, REQ-531)

## Campionamento

Tutti i ruoli vedono risultati campionati (predefinito: 100 righe) a meno che non abbiano la capacità `full_results`. (REQ-554) Controllato tramite la variabile d'ambiente `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Registrazione di audit

Ogni query che tocca un asset di dominio viene registrata nel `query_audit_log`, di sola aggiunta. (REQ-596, REQ-613) Ogni riga cattura `tenant_id`, `user_id`, `role_id`, un hash SHA-256 del testo della query, `table_ids`, `source`, `status_code`, `duration_ms` e `logged_at`. (REQ-596) Il testo della query non viene mai memorizzato letteralmente — solo il suo hash. (REQ-596)

Il log è di sola aggiunta a livello di database: le regole PostgreSQL bloccano `DELETE` e `UPDATE`. (REQ-596, REQ-613) Due indici — `(tenant_id, logged_at)` e `(user_id, logged_at)` — supportano query di conformità con ambito per tenant e per intervallo temporale per utente. (REQ-596, REQ-613)

Quando la crittografia è abilitata, la colonna dell'hash del testo della query viene memorizzata crittografata e decrittografata solo in lettura da parte di amministratori autorizzati. (REQ-689)

## Limitazione della frequenza (rate limiting)

I limiti di frequenza per ruolo sono configurati in `provisa.yaml`: numero massimo di richieste al secondo, numero massimo di sottoscrizioni SSE concorrenti e numero massimo di stream Arrow Flight concorrenti. (REQ-369) I limiti vengono applicati a livello di API prima della compilazione o dell'esecuzione; le richieste oltre il limite vengono rifiutate con HTTP 429 e un header `Retry-After`. (REQ-369)

Il servizio di query in linguaggio naturale (`POST /query/nl`) ha un limite indipendente tramite `nl.rate_limit` (richieste al minuto per ruolo). Le richieste oltre il limite vengono rifiutate prima di qualsiasi chiamata all'LLM. (REQ-370)

Lo stato del rate limiting risiede in Redis (`cache.redis_url`) come contatore a finestra scorrevole — nessuno stato per istanza — cosicché i limiti si mantengono su tutte le istanze Provisa orizzontali. (REQ-371)

## Autenticazione

Provider di autenticazione collegabili: (REQ-120)

| Provider | Tipo di token | Caso d'uso |
| ---------- | ----------- | ---------- |
| `none` | Header X-Provisa-Role | Sviluppo |
| `basic` | Account locali bcrypt + JWT | Deployment autonomi |
| `firebase` | Token ID Firebase | Produzione |
| `keycloak` | JWT Keycloak | Enterprise |
| `oauth` | JWT OIDC | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | Test |

Mappatura dei ruoli: claim di identità → ruolo Provisa tramite regole configurabili. (REQ-120) Il campo `assignments_source` controlla da dove provengono le assegnazioni dei ruoli: `claims` le legge dai claim del token JWT (predefinito), `provisa` le legge dallo store interno delle assegnazioni di Provisa. (REQ-551)

Un superuser configurato in `provisa.yaml` (nome utente più una password proveniente da un secret d'ambiente) riceve sempre il ruolo admin e tutte le capacità, indipendentemente dal provider configurato — un percorso di bootstrap per la configurazione iniziale. (REQ-125)

### Superfici e credenziali

Ogni superficie si autentica attraverso lo stesso contratto di provider, quindi una credenziale che funziona su una funziona su tutte, ovunque il protocollo possa trasportarla. (REQ-124, REQ-1263) Questa tabella è il riferimento unico; i documenti delle singole superfici non la ripetono.

| Superficie | Password | Token del provider | Token di accesso personale | Certificato client (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | tramite proxy di terminazione |
| pgwire | campo password (in chiaro o SCRAM) | campo password, deployment OIDC | campo password | sì |
| Bolt | schema `basic` | schema `bearer` | schema `bearer` | sì |
| Arrow Flight | — | `token` nell'handshake o nel payload del ticket | idem | sì |
| gRPC | — | metadati `authorization` | metadati `authorization` | sì |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | tramite proxy di terminazione |

Dove una cella riporta `—`, il protocollo non trasporta alcun campo nome utente da abbinare a una password; le forme a token lo coprono. pgwire è il caso speculare: il pacchetto di avvio ha un unico campo segreto e nessuno schema, quindi è *ciò che* il segreto è a scegliere il metodo — un PAT si riconosce dal prefisso, il segreto viene letto come token bearer quando il provider configurato è un provider di token, e qualsiasi altra cosa è una password. La scelta si fa una volta sola — una credenziale che il validatore selezionato rifiuta non viene ritentata su un altro.

La matrice è imposta da `tests/unit/test_auth_surface_conformance.py`, che sollecita il vero punto di ingresso di validazione di ogni superficie e fallisce quando viene aggiunta una nuova superficie senza riga.

### Token di accesso personali

Un PAT è un segreto bearer di lunga durata che un utente conia per un client che non può completare un accesso interattivo — uno script, uno strumento di BI, un driver. (REQ-1263) Porta con sé la propria organizzazione e il proprio ruolo, e ogni superficie lo risolve tramite lo stesso validatore, quindi nessuna superficie deve sapere cosa sia un PAT.

La forma sul filo è `provisa_pat_` seguito da 43 caratteri base64 sicuri per URL. Il prefisso è ciò che instrada un segreto presentato verso l'archivio dei token invece che verso il provider di identità, e rende un token trapelato individuabile con grep in log e repository.

- **Archiviazione** — viene conservato solo lo SHA-256 del segreto. Il segreto stesso viene mostrato esattamente una volta, alla creazione, e non è recuperabile. L'elenco riporta il prefisso di visualizzazione e le marche temporali del ciclo di vita, mai una credenziale utilizzabile.
- **Emissione e revoca** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}`, più la sezione self-service nel profilo dell'utente stesso nella UI di amministrazione. Coniare e revocare una credenziale è atto di chi la detiene.
- **Attribuzione** — un PAT validato si risolve nell'account del proprietario: id utente, email e nome visualizzato. Una riga di audit o un report d'uso scritto sotto un PAT nomina quindi la persona, non la credenziale. Quale dei token di quella persona abbia agito è riportato separatamente, in `raw_claims["token_name"]`.
- **Scadenza** — un token può portare una scadenza; un token scaduto viene rifiutato in validazione. Eliminare l'appartenenza di un utente ne revoca i token insieme.

### SCRAM-SHA-256 su pgwire

Con il provider `basic`, impostare `auth.scram: true` fa sì che pgwire annunci SASL (codice di autenticazione 10) con il meccanismo `SCRAM-SHA-256`, così una password viene dimostrata anziché inviata. (REQ-1394) Il channel binding (`SCRAM-SHA-256-PLUS`) non viene offerto.

SCRAM richiede un verifier RFC 5802, che non può essere derivato da un hash bcrypt. Un verifier viene scritto ogni volta che una password transita in chiaro — registrazione, accesso, cambio password, reimpostazione da amministratore — quindi un deployment che attiva SCRAM raccoglie i verifier man mano che i suoi utenti si autenticano la volta successiva, e la prima connessione SCRAM di ciascun utente segue il suo successivo inserimento della password. A un utente ancora privo di verifier si risponde con uno scambio simulato indistinguibile da uno reale, così il filo non rivela chi ha migrato.

### TLS reciproco

La verifica del certificato client sposta il primo controllo nell'handshake TLS: un chiamante privo di un certificato firmato dalla CA del deployment non raggiunge mai il livello delle credenziali. (REQ-1228) È disponibile su pgwire, Bolt, gRPC e Arrow Flight — i quattro trasporti che terminano il proprio TLS.

| Variabile | Significato |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | Bundle PEM della/e CA autorizzate a firmare i certificati client |
| `PROVISA_MTLS_MODE` | `required` (il valore predefinito una volta impostata una CA) oppure `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Quando è vero, il common name del certificato deve coincidere con il nome utente con cui la connessione si autentica poi |

Gli override per protocollo seguono la stessa denominazione delle impostazioni TLS. Nulla viene dedotto: una modalità impostata senza CA rifiuta di avviarsi, e una modalità non riconosciuta rifiuta di avviarsi anziché essere letta come il vicino più sicuro — un deployment che crede di richiedere certificati client e non lo fa sta peggio di uno che non si avvia.

### Limitazione dei tentativi di accesso

Indovinare password è indipendente dal protocollo: lo stesso account può essere martellato via HTTP, pgwire e Bolt. Il contatore vive perciò al livello di validazione delle credenziali, non su una singola superficie, così che un blocco guadagnato ovunque venga applicato ovunque. (REQ-1393)

È attiva per impostazione predefinita — cinque fallimenti in cinque minuti bloccano il soggetto per quindici minuti — e si regola sotto `auth.login_throttle`. Un soggetto bloccato viene rifiutato prima ancora che la credenziale venga esaminata, e un'autenticazione riuscita azzera la cronologia di quel soggetto.

La chiave è il principal che il protocollo trasporta. Una superficie solo-bearer non trasporta alcun principal, quindi la chiave è un digest della credenziale stessa; ciò che questo ferma è un singolo token compromesso riprodotto senza limiti. L'archivio è per processo, quindi un deployment con più worker API consente fino a `max_attempts` per worker — la limitazione è un freno all'indovinare, non una quota distribuita.

### Indirizzare un'organizzazione su un protocollo di trasporto

In multitenancy un'organizzazione si indirizza per nome host: `acme.provisa.dev` è l'organizzazione `acme`. Su HTTP quel nome arriva nell'header `Host`. Un client pgwire o Bolt non invia un header simile, ma invia il nome host che ha composto nel ClientHello TLS, e Provisa legge di lì l'organizzazione. (REQ-1234) Nulla cambia lato client — connettersi a `acme.provisa.dev` è tutto ciò che serve.

Il nome host è una richiesta, non una concessione. Raggiunge lo stesso resolver dell'header `Host`, che rifiuta qualsiasi organizzazione di cui il principal autenticato non sia membro né detenga il diritto cross-organizzazione. Comporre un nome host in cui non si ha appartenenza non raggiunge alcun dato. Un client connesso per indirizzo IP non invia alcun nome host e risolve la propria organizzazione dal solo principal, che è ogni connessione in un deployment a organizzazione singola.

gRPC, Arrow Flight e MCP consegnano i propri certificati a librerie che non espongono alcun callback sul nome host; quei trasporti nominano un'organizzazione con l'header di metadati `x-provisa-org`.

## Modalità ad alta sicurezza

`security.mode: high` in `provisa.yaml` afferma una garanzia: il backend di Provisa non tratta mai dati in chiaro. (REQ-693) Ogni colonna che conta è cifrata all'origine, e solo un client in possesso della chiave di decifratura può leggerla. Questa garanzia ha conseguenze che un deployment deve pianificare.

**Che cosa fa la modalità:**

- **Gli endpoint dati richiedono la prova della decifratura lato client.** Tutto ciò che sta sotto `/data/` restituisce 403 a meno che il chiamante non presenti l'header `X-Provisa-KMS-Key` — il marcatore di un client JDBC o Python configurato per decifrare localmente. Un browser o un consumatore REST in chiaro non porta alcuna chiave del genere e viene rifiutato. Il varco è un deny-by-default sull'intero albero: una rotta aggiunta domani è protetta il giorno stesso in cui viene rilasciata, e un'esenzione va argomentata.
- **Gli endpoint di metadati dello schema restano aperti.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` e `/data/compile` non restituiscono dati di riga, e un client deve leggere lo schema — compreso quali campi siano `@encrypted` — prima ancora di potersi connettere.
- **gRPC e Arrow Flight continuano a servire, sotto la stessa prova.** Sono i trasporti che i client che cifrano usano davvero; chiuderli lascerebbe un deployment ad alta sicurezza senza protocollo di trasporto. Una chiamata dati su entrambi deve portare la stessa chiave KMS come metadato della chiamata.
- **pgwire, Bolt e MCP non si avviano.** Nessuno dei tre ha un handshake per connessione capace di trasportare un contesto di decifratura: un result set pgwire e un risultato Cypher viaggiano in chiaro, e una chiamata a strumento MCP consegna i risultati a un modello come testo. Una porta configurata per uno qualsiasi di essi viene rifiutata all'avvio anziché servita.
- **La protezione delle relazioni non può essere aggirata.** `ignore_relationships` e `relationship_guard: false` vengono entrambi ignorati; vedi [Governance delle relazioni](#relationship-governance-v002).

**Verificare che un deployment sia in questa modalità:** il log di avvio la nomina, una richiesta `/data/sql` senza chiave KMS risponde 403 con un messaggio che cita REQ-693, e le porte pgwire, Bolt e MCP non sono in ascolto.

## Hook di approvazione ABAC

Un hook di policy esterno opzionale che si attiva prima dell'esecuzione della query. (REQ-203) Quando configurato, Provisa effettua una chiamata al motore di policy con l'identità dell'utente, i ruoli, le tabelle, le colonne e l'operazione. La risposta determina se la query prosegue. (REQ-203)

### Ambito

L'hook si attiva solo quando la query tocca una tabella o un'origine nel proprio ambito — nessun sovraccarico per tutto il resto. (REQ-204)

| Configurazione | Effetto |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Ogni query attiva l'hook |
| `sources[].approval_hook: true` | Tutte le tabelle di quell'origine attivano l'hook |
| `tables[].approval_hook: true` | Quella tabella attiva l'hook |

### Protocolli

Sono supportati tre trasporti: (REQ-246)

| Tipo | Caso d'uso | Campo di configurazione |
| ------ | ---------- | ------------- |
| `webhook` | Qualsiasi servizio di policy compatibile con HTTP (OPA, personalizzato) | `url` |
| `unix_socket` | OPA o sidecar di policy sulla stessa macchina | `socket_path` + `url` |
| `grpc` | Servizio di policy co-localizzato ad alto throughput | `url` (host:porta) |

Il trasporto gRPC utilizza il contratto `provisa.auth.ApprovalService` definito in `provisa/auth/approval.proto`. Implementate questo servizio nel vostro motore di policy: (REQ-246)

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

Il canale gRPC è persistente — un canale per istanza Provisa, riutilizzato per tutte le chiamate a quell'endpoint dell'hook. (REQ-555)

### Richiesta / Risposta

Tutti e tre i trasporti veicolano lo stesso payload: (REQ-246)

| Campo | Tipo | Descrizione |
| ------- | ------ | ------------- |
| `user` | string | Identità dell'utente autenticato |
| `roles` | string[] | Ruoli Provisa dell'utente |
| `tables` | string[] | ID delle tabelle referenziate nella query |
| `columns` | string[] | Colonne selezionate nella query |
| `operation` | string | `"query"` o `"mutation"` |

I trasporti webhook e Unix socket scambiano JSON. La risposta deve includere `approved` (bool) e, facoltativamente, `reason` (string). (REQ-246)

### Timeout e fallback

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

In caso di timeout o errore di trasporto, si applica la policy `fallback`. (REQ-247) Un circuit breaker (predefinito: si apre dopo 5 fallimenti consecutivi, semi-aperto dopo 30s) previene fallimenti a cascata causati da un endpoint dell'hook lento. (REQ-556)

### Esempio di configurazione

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

## Segreti

Le credenziali usano la sintassi `${env:VAR_NAME}`, risolta in fase di runtime. (REQ-557) Le password non vengono mai memorizzate nel database di configurazione. (REQ-557)
