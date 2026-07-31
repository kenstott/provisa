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

**Meccanismi di bypass** — V002 può essere aggirato solo quando entrambe le seguenti condizioni indipendenti sono vere:

1. **Flag del ruolo** — `relationship_guard: false` nella definizione del ruolo (predefinito: `true`). [tool-verified: `provisa/core/models.py:349`]
2. **Esclusione per query** — l'SQL contiene il commento `--relationship-guard=false`. [tool-verified: `provisa/compiler/params.py:80`]

Entrambe devono essere presenti. Il flag del ruolo da solo non aggira V002; il commento da solo non aggira V002.

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
| `ignore_relationships` | Bypassare la governance delle relazioni (V002) |
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
| `firebase` | Token ID Firebase | Produzione |
| `keycloak` | JWT Keycloak | Enterprise |
| `oauth` | JWT OIDC | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | Test |

Mappatura dei ruoli: claim di identità → ruolo Provisa tramite regole configurabili. (REQ-120) Il campo `assignments_source` controlla da dove provengono le assegnazioni dei ruoli: `claims` le legge dai claim del token JWT (predefinito), `provisa` le legge dallo store interno delle assegnazioni di Provisa. (REQ-551)

Un superuser configurato in `provisa.yaml` (nome utente più una password proveniente da un secret d'ambiente) riceve sempre il ruolo admin e tutte le capacità, indipendentemente dal provider configurato — un percorso di bootstrap per la configurazione iniziale. (REQ-125)

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
