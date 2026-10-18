# Modello di sicurezza {: #security-model }

Provisa applica un modello di sicurezza a più livelli su ogni linguaggio di query (GraphQL, SQL, Cypher) e ogni trasporto (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) La governance si applica in modo uniforme — nessun percorso di query la aggira. (REQ-002, REQ-266)

I livelli si applicano in ordine. Una richiesta deve superare ogni livello prima che venga valutato il successivo.

## Modello a livelli {: #layered-model }

### Livello 0 — Filtraggio dell'introspezione {: #layer-0-introspection-filtering }

Lo schema e il catalogo presentati a un ruolo contengono solo le tabelle della sua lista `domain_access` e le colonne che superano le regole `visible_to` di ogni colonna. (REQ-039) Gli oggetti fuori dall'accesso di un ruolo sono invisibili al momento della scoperta — non possono essere interrogati, completati automaticamente né dedotti come esistenti. (REQ-039) Questo vale per lo schema GraphQL, il catalogo SQL e il browser dello schema dell'editor di query. (REQ-039, REQ-363)

Vedere [Visibilità dello schema](#schema-visibility).

### Livello 1 — Accesso pubblico {: #layer-1-public-access }

Una colonna pubblicata con l'ambito `public` viene servita a ogni ruolo dell'organizzazione, compresi i ruoli il cui `domain_access` non raggiunge il dominio della tabella; un tale ruolo vede la tabella con le sole colonne pubbliche. Nulla è pubblico se chi registra una tabella non lo dice. (REQ-1959) Vedere [Colonne pubbliche](#public-columns).

### Livello 2 — Accesso ai domini {: #layer-2-domain-access }

Ogni ruolo porta una lista `domain_access` di id di dominio. Una query che tocca una tabella fuori da quei domini viene rifiutata prima dell'esecuzione. (REQ-038, REQ-039) È il confine grossolano di proprietà — un ruolo HR non raggiunge le tabelle finanziarie, comunque sia scritto l'SQL. (REQ-002)

Vedere [Modello dei diritti](#rights-model).

### Livello 3 — Sicurezza a livello di riga {: #layer-3-row-level-security }

Una volta confermato l'accesso al dominio, predicati `WHERE` per tabella e per ruolo vengono iniettati in ogni `SELECT` al momento dell'esecuzione. (REQ-041, REQ-263) I predicati si valutano sui dati grezzi. Un responsabile regionale che interroga una tabella di ordini condivisa vede solo le righe della sua regione, anche con `SELECT *`. (REQ-264)

Vedere [Sicurezza a livello di riga (RLS)](#row-level-security-rls).

### Livello 4 — Visibilità e mascheramento delle colonne {: #layer-4-column-visibility-and-masking }

Le colonne con una lista `visible_to` che esclude il ruolo richiedente vengono rimosse dall'output della query. (REQ-040, REQ-263) Le colonne con una regola di mascheramento vedono i loro valori sostituiti — oscuramento tramite regex, sostituzione con una costante o troncamento — prima che i risultati lascino il server. (REQ-263) Il mascheramento si applica in tutti i linguaggi di query e formati di output. (REQ-263)

Vedere [Modello dei permessi di colonna](#column-permission-model) e [Mascheramento a livello di colonna](#column-level-masking).

### Livello 5 — Protezione dei predicati {: #layer-5-predicate-guard }

Le colonne mascherate vengono rifiutate nelle clausole `WHERE` e `HAVING`. (REQ-263) Senza questo, un chiamante potrebbe dedurre il valore non mascherato cercandolo per bisezione in un filtro, anche se l'output è mascherato. Il rifiuto viene applicato all'analisi della query, prima dell'esecuzione. (REQ-531)

### Governance delle relazioni (V002) {: #relationship-governance-v002 }

Le condizioni JOIN in SQL devono corrispondere a una relazione registrata e approvata tra tabelle. (REQ-001) I join non approvati vengono rifiutati. Ogni relazione porta un motivo e una descrizione leggibili — una guida per utenti e agenti autonomi sul perché esiste un percorso di attraversamento. È una politica di governance, non un confine di sicurezza rigido: i livelli 2–5 valgono qualunque sia la struttura del join, quindi un aggiramento deliberato non espone dati che il ruolo non potrebbe raggiungere con due query separate. I tentativi di aggiramento vengono registrati e sono verificabili.

**Meccanismi di aggiramento** — V002 può essere aggirato in due modi. Il primo è una capacità: un ruolo che detiene `ignore_relationships` esegue join tra relazioni che il catalogo non copre. Tra i ruoli di sistema predefiniti solo `modeler` la detiene — il ruolo di scoperta il cui compito è determinare il modello anziché applicarlo. (REQ-1297) `analyst` non la detiene.

Il secondo è un'esclusione a due condizioni, che devono essere vere entrambe:

1. **Flag del ruolo** — `relationship_guard: false` nella definizione del ruolo (predefinito: `true`).
2. **Esclusione per query** — l'SQL contiene il commento `--relationship-guard=false`.

Il flag del ruolo da solo non aggira V002; il commento da solo non aggira V002.

**La modalità ad alta sicurezza blocca la protezione.** Con `security.mode: high` nessun aggiramento si applica: `ignore_relationships` viene ignorato, `relationship_guard: false` viene ignorato, e ogni join deve esistere nel catalogo delle relazioni approvate. (REQ-693) È una ridondanza voluta — un ruolo di produzione a cui la capacità è stata concessa per errore non può comunque uscire dal modello.

**Percorso GraphQL** — V002 viene sempre saltato per le query GraphQL. Le relazioni definite nell'SDL sono pre-approvate per progetto; il controllo è ridondante e non viene applicato.

**Percorsi SQL e Cypher** — V002 è attivo per impostazione predefinita. Sia `endpoint_dev.py` sia `cypher_router.py` applicano il controllo a due condizioni prima di chiamare `validate_sql`.

**Percorso pgwire** — lo stesso controllo a due condizioni dell'SQL. Il commento `--relationship-guard=false` viene rimosso dalla query prima dell'esecuzione; non raggiunge il database.

---

Questi livelli si combinano. Un ruolo con accesso ai domini, RLS e colonne mascherate ha tutti e cinque i vincoli attivi contemporaneamente. Aggiungere una sorgente dati, una colonna o una relazione non richiede di aggiornare ogni regola — ogni livello si configura in modo indipendente e si applica automaticamente a qualsiasi query che tocca oggetti governati.

---

## Modello dei diritti {: #rights-model }

Capacità assegnate in modo indipendente, con una gerarchia di ruoli facoltativa tramite `parent_role_id`. `admin` concede tutto. (REQ-042)

| Capacità | Descrizione |
| ----------- | ------------- |
| `source_registration` | Registrare sorgenti dati |
| `table_registration` | Registrare tabelle e colonne |
| `create_relationship` | Definire relazioni FK |
| `access_config` | Configurare RLS e mascheramento |
| `query_development` | Eseguire query |
| `write` | Invocare mutazioni registrate (controllo grossolano; vedere Autorizzazione delle mutazioni) |
| `full_results` | Aggirare i limiti di campionamento |
| `ignore_relationships` | Aggirare la governance delle relazioni (V002). Detenuta solo da `modeler` tra i ruoli di sistema, e del tutto ignorata in modalità ad alta sicurezza |
| `admin` | Superutente — concede tutto |

### Ereditarietà dei ruoli {: #role-inheritance }

I ruoli possono ereditare da un ruolo padre tramite `parent_role_id`, impostato nella configurazione o nella pagina Sicurezza come «Eredita da». (REQ-215) La catena viene ripiegata all'avvio: un figlio detiene l'unione delle capacità e dell'accesso ai domini dei suoi antenati, ogni concessione di colonna, metrica, funzione e webhook che nomina un antenato, e le regole RLS degli antenati per tabella, con precedenza al figlio — il ruolo più vicino nella catena con una regola per una tabella fornisce il predicato di quella tabella, e una regola salvata per il figlio sostituisce quella del padre per quella tabella. (REQ-1677)

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
## Ruoli: concessi, derivati e meta {: #roles-granted-derived-and-meta }

Tre tipi di ruolo lavorano insieme nel modello di accesso di Provisa. La pagina Ruoli elenca insieme i primi due; un meta-ruolo non viene mai elencato.

### Ruoli concessi {: #granted-roles }

Un ruolo concesso viene assegnato direttamente a un utente. Detiene diritti: capacità, accesso ai domini, voci di mascheramento, filtri di riga. L'ereditarietà (`parent_role_id`) ripiega all'avvio i diritti di un antenato nel figlio. I diritti si sommano soltanto; non c'è sottrazione. Per restringere un lettore, concedete a un composto che nomini sia il ruolo sia una condizione, oppure mettete una voce di maschera prima di quella permissiva. (REQ-215, REQ-1677)

### Assegnazioni in scadenza e inutilizzate {: #expiring-and-unused-grants }

Un'assegnazione che Provisa detiene può terminare. Un'assegnazione concessa con una scadenza non è più detenuta da quell'istante, a ogni richiesta. Un ruolo può indicare `inactivity_days`: un'assegnazione di quel ruolo da cui nessuna richiesta è dipesa per più tempo, contato dal più recente tra l'ultimo uso e la creazione, viene rimossa. I ruoli che provengono dai claim o dalle mappature dei ruoli di un provider di identità sono limitati nel tempo dal provider; impostatene lì la scadenza. I ruoli derivati e i meta-ruoli non scadono mai. (REQ-2077)

Una richiesta è dipesa da un ruolo quando quel ruolo l'ha servita da solo o, quando la richiesta agisce come più ruoli, quando al ruolo è concessa una tabella che l'istruzione ha letto. Agire come «Tutti» non conta come usato ogni ruolo detenuto. L'ultimo uso di un'assegnazione è registrato al massimo una volta al giorno.

Ogni 15 minuti la scansione di ogni organizzazione elimina le assegnazioni scadute e inutilizzate e registra ciascuna nel registro amministrativo come `role.expired` o `role.removed_inactive`. Dove la posta in uscita è configurata, anche in un deployment con una sola organizzazione, il titolare e gli amministratori dell'organizzazione ricevono un'email `auth.role_grant_notice_days` giorni (7 di default) prima che un'assegnazione termini, e gli amministratori ne ricevono una quando viene rimossa; dove la posta non è configurata, la voce del registro fa da traccia. Questi avvisi sono inviati qualunque sia la preferenza email dell'utente.

### Ruoli derivati {: #derived-roles }

Un ruolo derivato proviene dagli attributi di un utente. L'organizzazione dichiara quali attributi diventano ruoli (`derived_roles`), ciascuno con una descrizione. Per ogni attributo dichiarato `a` il cui valore `v` è fornito dagli attributi dell'utente, l'utente detiene il ruolo padre `a` e il suo ruolo di valore `a__v`: `region = eu` dà `region` e `region__eu`. Senza valore, nessuno dei due ruoli.

La risoluzione degli attributi conta. Il registro proprio dell'organizzazione su un membro prevale su ciò che fornisce il provider di identità: se entrambi nominano lo stesso attributo, si usa il valore dell'organizzazione e quello del provider viene segnato come sostituito.

I ruoli derivati non portano diritti e non vengono mai assegnati a mano. Una lista di concessione, una maschera o una regola di riga li nomina esattamente come nomina un ruolo concesso; una richiesta li nomina in `X-Provisa-Role` e agisce come essi. (REQ-2057)

Nella pagina Ruoli ogni attributo dichiarato è un ruolo padre contrassegnato come *derivato*, con i suoi ruoli di valore elencati sotto man mano che i loro valori compaiono. **+ Ruolo → Derivato da un attributo** ne dichiara uno. Il suo pannello mostra l'attributo, la sua descrizione, i valori visti finora e ogni maschera, regola di riga e concessione che nomina esso o uno dei suoi ruoli di valore. Non ha controlli di concessione. La descrizione di un ruolo di valore è generata: «Posseduto quando l'attributo region dell'utente è eu». (REQ-2078, REQ-2079)

Dichiarateli nella configurazione come elenco:

```yaml
derived_roles:
- attribute: region
  description: The subsidiary a member works for; region__eu marks EU staff.
```

Pensate a un JWT: i suoi claim sono fatti sulla persona. I ruoli derivati trasformano fatti scelti in ruoli, così che una concessione possa rivolgersi a essi senza creare un ruolo per combinazione.

### Meta-ruoli {: #meta-roles }

Quando una richiesta nomina più di un ruolo, Provisa costruisce un meta-ruolo effimero identificato come `meta:<ids sorted, joined by +>`. È un unico figlio di ogni ruolo dell'insieme nominato. L'intera pipeline viene poi eseguita con esso come un normale ruolo singolo.

I diritti sono l'unione di quelli di ogni membro. I filtri di riga sono l'OR del filtro risolto di ogni membro; se un membro che legge una tabella non ha filtro, non ne ha nemmeno il meta-ruolo — nessun filtro significa tutte le righe. Un meta-ruolo non viene mai memorizzato, elencato né esportato. Viene ricostruito dai suoi membri quando il modello cambia. (REQ-1620)

Le colonne di ogni riga sono mascherate e mostrate secondo i ruoli il cui filtro di righe ha ammesso quella riga: tra questi ruoli si applica la lettura meno restrittiva della colonna, e un ruolo che non ha ammesso la riga non contribuisce in nulla a come viene letta. Le maschere si applicano dove la tabella viene letta, prima di ogni join o aggregazione, così un'aggregazione legge i valori mascherati. Le scritture sono verificate allo stesso modo: una colonna viene scritta su una riga solo dove un ruolo che concede quella colonna ammette la riga. (REQ-1620)

Il record di audit nomina ogni ruolo agente, non l'id del meta-ruolo. Poiché un meta-ruolo non viene mai memorizzato, non ha una descrizione propria: ovunque ne venga mostrata una, la sua descrizione è quella dei suoi membri, ogni id con la propria. (REQ-2080)

### Descrizioni {: #descriptions }

Ogni ruolo e ogni regola di riga dice a cosa serve. Una revisione degli accessi legge chi può vedere cosa e perché. Un ruolo o una regola che nessuno sa spiegare è uno che nessuno può rivedere, quindi la descrizione è obbligatoria: un ruolo, un ruolo padre derivato o una regola di riga con descrizione vuota viene rifiutato dal caricamento della configurazione, dall'API di amministrazione e dalla UI, con il motivo. Gli elenchi Ruoli e Regole di riga mostrano ogni descrizione, e i loro pannelli di dettaglio la mostrano e la modificano. (REQ-2080, REQ-2081)

Un ruolo ha una sola descrizione, non una per dominio. Un ruolo generale che non descrive le sue responsabilità in un particolare dominio significa che quel dominio ha bisogno di un ruolo proprio, di solito un figlio del ruolo generale (`parent_role_id`), che ne eredita i diritti e descrive cosa fa lì.

### Come una richiesta nomina i ruoli {: #how-a-request-names-roles }

Un client invia un ruolo, o un insieme separato da virgole, in `X-Provisa-Role`. Ogni ruolo nominato deve essere detenuto. Un ruolo non assegnato all'identità viene rifiutato per nome.

Diritti e dati seguono solo i ruoli con cui agisce la richiesta: detenere un ruolo non significa agire come esso. Un ruolo è quel ruolo; due o più agiscono come il loro meta-ruolo. Un ruolo agente che portasse con sé i diritti di ogni ruolo detenuto non significherebbe nulla, quindi una richiesta ottiene il privilegio minimo e il record di audit nomina esattamente ciò che ha agito.

I ruoli derivati nominati entrano come qualificatori. Un qualificatore non ha diritti, accesso ai domini, maschere o filtri di riga propri, quindi non è membro dei diritti del meta-ruolo: come membro senza maschere smaschererebbe ogni colonna, e come membro che non legge alcuna tabella porrebbe fine a ogni OR di filtri di riga. Entra solo nelle concessioni: una concessione o una voce di maschera che nomina `region__eu` raggiunge una richiesta che nomina quel qualificatore insieme a un ruolo concesso. (REQ-2057)

Un agente MCP non agisce mai oltre i ruoli che gli sono stati dati. Un agente remoto nomina solo ruoli che il principal del suo token detiene. Un agente stdio locale nomina solo ruoli all'interno dell'insieme fissato in `mcp.role` (`PROVISA_MCP_ROLE`), e agisce come quell'insieme quando non ne nomina nessuno; qualsiasi altro ruolo viene rifiutato per nome. (REQ-2075)

Un agente chiede con quale ruolo agisce tramite lo strumento MCP `describe_role`. La risposta è quel ruolo e a cosa serve (un meta-ruolo con ciascuno dei suoi membri), e i ruoli a cui l'agente può passare, ciascuno con la sua descrizione. Non contiene mai una regola di riga, una maschera, una concessione o una condizione. Provisa non dice mai a un agente perché un risultato è stato ristretto, e un risultato non porta alcun indicatore «filtrato»: i dati di un ruolo sono tutta la sua vista, e un motivo per ciò che manca inviterebbe a sondarlo. (REQ-2082)

### Cosa significa `+` {: #what-means }

`+` compare in due punti, con un significato diverso in ciascuno.

**In una voce di concessione.** `analyst+region__eu` in una lista `visible_to` o `to` significa che il lettore li detiene tutti — `analyst` E `region__eu` contemporaneamente. Una voce che non è un composto è un ruolo singolo. Tra le voci di una lista la regola è una qualsiasi: un lettore detiene la concessione se detiene una voce qualsiasi. E all'interno, O tra le voci.

**In un id di meta-ruolo.** `meta:analyst+engineer` è l'identificatore del server per il meta-ruolo dell'insieme {analyst, engineer}. I suoi diritti sono l'unione di entrambi. Un client non nomina mai direttamente l'id del meta-ruolo; nomina i ruoli che detiene, e il server costruisce il meta-ruolo.

### Regole di colonna ABAC come semplice RBAC {: #abac-column-rules-as-plain-rbac }

Una politica basata sugli attributi chiede: «questo utente, con questi attributi, può vedere questa colonna?» RBAC chiede: «quali ruoli possono vedere questa colonna?» Le concessioni composte e i ruoli derivati le rendono la stessa domanda.

«Smascherare questa email solo per un analista nell'UE» è una voce di maschera `{to: [analyst+region__eu], mask: null}`. Qualsiasi lettore che detenga sia il ruolo `analyst` sia il ruolo derivato `region__eu` legge la colonna non mascherata. Nessun linguaggio di policy. Nessun ruolo per combinazione.

Il compromesso è il calcolo al posto dell'enumerazione. Un piccolo catalogo di ruoli mantiene bassa la manutenzione, ma «chi può vedere questa email UE?» diventa una risposta calcolata: le voci composte vengono valutate rispetto alle assegnazioni e ai claim di ogni utente. Provisa può rispondere in entrambe le direzioni perché il modello è un'unica definizione. Il controllo di accesso dell'editor di colonne (`POST /admin/masks/check`) esegue questa valutazione per un insieme di ruoli scelto.

### Vantaggi {: #benefits }

- Una regola di colonna ABAC diventa una voce di concessione RBAC.
- Un piccolo catalogo di ruoli invece di un ruolo per combinazione.
- Un'unica definizione che coordina le regole di ogni regione.
- Privilegio minimo e un record di audit esatto per richiesta.

### Buone pratiche {: #best-practices }

- Mantenete il catalogo dei ruoli piccolo quanto l'organizzazione lo consente.
- Ereditate all'interno di un asse (anzianità, livello di autorizzazione). Componete tra assi indipendenti (ruolo + regione, ruolo + reparto).
- Mettete per prime le voci di maschera più specifiche — una voce precedente vince anche quando il lettore ne detiene una successiva.
- Verificate l'accesso effettivo con `POST /admin/masks/check` prima di distribuire una combinazione in produzione.

## Modello dei permessi di colonna {: #column-permission-model }

Ogni colonna ha un modello di permessi che controlla l'accesso in lettura, scrittura e mascheramento per ruolo. (REQ-042, REQ-249)

### Visibilità a tre livelli {: #three-tier-visibility }

| Livello | Condizione | Risultato |
| ------ | ----------- | -------- |
| **Nascosta** | Ruolo non in `visible_to` | Colonna assente da schema e catalogo |
| **Mascherata** | Ruolo in `visible_to`; la prima voce di maschera che detiene ha una `mask` | Colonna visibile, valore sostituito nell'output |
| **Non mascherata** | Ruolo in `visible_to`; la prima voce di maschera che detiene non ha `mask`, o non ne detiene nessuna | Accesso completo in lettura |

### Colonne pubbliche {: #public-columns }

Ogni colonna ha uno `scope`, impostato nel file del modello, tramite l'API di amministrazione o nella pagina Tabelle. I tre ambiti sono ordinati in base a chi servono: `restricted`, poi `domain`, poi `public`. Un editor di tabelle (`table_registration` nel dominio della tabella) può restringere l'ambito di una colonna. Allargarlo — da `restricted` a `domain` o `public`, da `domain` a `public` — serve la colonna a più ruoli e richiede il diritto che richiede concederla: `column_grant` o `access_config` nel dominio della tabella. Una colonna in fase di registrazione viene confrontata con `domain`, quindi registrarla come `public` è un allargamento. Su una colonna con un tag sensibile, qualsiasi cambio di ambito richiede `sensitive_data`. Un cambio rifiutato nomina la colonna e il diritto. (REQ-1959, REQ-1943, REQ-1944)

| Ambito | Servita a |
| ------- | ----------- |
| `domain` (predefinito) | I ruoli che raggiungono il dominio della tabella, come dice `visible_to`. Un `visible_to` vuoto è ognuno di tali ruoli. |
| `public` | Gli stessi ruoli, e anche i ruoli che non raggiungono il dominio della tabella. |
| `restricted` | Solo i ruoli che `visible_to` nomina. Un `visible_to` vuoto è nessuno. |

Pubblicare una colonna rimuove la condizione di dominio per quella colonna e nient'altro:

- A un ruolo fuori dal dominio viene servita la tabella con le sole colonne pubbliche. Le altre colonne della tabella, e le tabelle che non pubblicano nulla, restano fuori dal suo schema, dal suo catalogo e dal suo SQL.
- Il `visible_to` proprio della colonna decide ancora chi: una colonna pubblica concessa a un ruolo è pubblica solo per quel ruolo.
- Maschere, filtri di riga, limiti di riga e protezione delle relazioni si applicano come all'interno del dominio. Senza filtro di riga per il ruolo, le colonne pubblicate vengono lette senza filtro; un filtro di riga sulla tabella o sul suo dominio si applica comunque.
- Una relazione viene servita quando lo sono le colonne di entrambe le estremità.
- Una colonna di parametro di una tabella API (un argomento di percorso o di query) non è un dato e non pubblica nulla.
- Il dominio `ops` richiede una concessione esplicita per ogni colonna (REQ-1133); nulla in esso viene servito tra domini, qualunque sia il suo ambito.

La regola è la stessa su ogni superficie, perché ognuna legge lo schema compilato del ruolo: GraphQL, SQL (HTTP, pgwire, Flight, JDBC), Cypher e Bolt, gRPC, REST e JSON:API, i cataloghi Flight e MCP e gli elenchi di amministrazione. Un ambito che non è uno dei tre valori viene rifiutato al salvataggio di una colonna.

### Autorizzazioni di scrittura {: #write-grants }

`visible_to` controlla la lettura di una colonna (vuoto significa che tutti i ruoli possono leggere). La scrittura è controllata per tabella con `write_grants`, non per colonna. Per impostazione predefinita una tabella non ha alcuna autorizzazione, quindi nessun ruolo può scriverci. (REQ-2074)

Un'autorizzazione di scrittura indica che cosa un ruolo può scrivere in una tabella con un'operazione (`insert`, `update` o `delete`):

| Campo | Scopo |
| ------- | --------- |
| `columns` | Le colonne che un `INSERT` può elencare o un `UPDATE` può impostare |
| `rows` | Le righe che un `UPDATE` o un `DELETE` può toccare (un predicato SQL) |
| `check` | Ciò che ogni riga scritta da un `INSERT`, o lasciata da un `UPDATE`, deve soddisfare (un predicato SQL) |
| `presets` | Colonne che l'autorizzazione imposta da sé, da una variabile di sessione, da un letterale o dal momento della scrittura. L'istruzione non può fornirle. |

Se si omette `rows` o `check`, l'autorizzazione prende il filtro di righe in lettura del ruolo sulla tabella, così un ruolo scrive le righe che legge. `"TRUE"` significa ogni riga.

Un'unica ammissione decide ogni scrittura su ogni superficie, in quest'ordine: il ruolo detiene il diritto `write`; detiene un'autorizzazione per l'operazione sulla tabella; ogni colonna scritta dall'istruzione è nell'autorizzazione; i preset vengono aggiunti; `rows` viene combinato con AND nel `WHERE`; e `check` viene valutato sulle nuove righe prima che venga inviato qualcosa alla sorgente. Un rifiuto nomina la tabella, il ruolo e l'operazione. (REQ-2074)

Le istruzioni più ampie richiedono autorizzazioni più ampie. `TRUNCATE` richiede un'autorizzazione `delete` su ogni riga. `MERGE` richiede un'autorizzazione `insert` e una `update` su ogni riga. `INSERT ... ON CONFLICT DO UPDATE` richiede un'autorizzazione `update` su ogni riga senza preset. `COPY` richiede un'autorizzazione `insert` senza preset.

GraphQL offre a ogni ruolo un campo di mutazione per esattamente le operazioni per cui detiene autorizzazioni, con le colonne dell'autorizzazione. Un ruolo figlio eredita le autorizzazioni del padre, le proprie per prime. Un meta-ruolo detiene il ripiegamento delle autorizzazioni dei suoi membri: l'unione delle colonne, l'OR di `rows` e `check`, e solo i preset condivisi da tutti i membri. Nell'interfaccia di amministrazione, la sezione **Scritture** di una tabella elenca e modifica le sue autorizzazioni.

### Esempio {: #example }

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

In questo esempio:

- `email`: admin vede `alice@example.com` e può aggiornarla su qualsiasi riga; analyst e viewer vedono `a***@example.com` e non possono scrivere
- `salary`: admin e hr vedono il valore reale; hr può aggiornarlo sulle righe che hr può leggere, perché l'autorizzazione non indica `rows`; ogni altro ruolo visibile vede `0`
- `created_at`: tutti possono leggerlo, nessun ruolo lo fornisce, e un insert di hr lo imposta al momento della scrittura

## Autorizzazione delle mutazioni {: #mutation-authorization }

Le mutazioni registrate (GraphQL remoto, OpenAPI, gRPC, Hasura) sono protette da due controlli indipendenti. (REQ-868) Un ruolo può invocare una mutazione solo se detiene la capability globale `write` E il comando gli è assegnato tramite `visible_to` (un elenco vuoto lo assegna a ogni ruolo). (REQ-868) Un comando non ha un elenco `writable_by`; la configurazione ne rifiuta uno per nome. [tool-verified: `provisa/security/mutation_authz.py` `command_assigned`, `admit_command`; `provisa/core/models.py` `_one_role_list`]

Le mutazioni sono classificate come scritture per contratto, non in base a ciò che dichiara il chiamante. (REQ-869) Una `SELECT` che fa riferimento a una funzione di tipo mutazione viene promossa a scrittura e sottoposta allo stesso controllo a due porte, così un chiamante non può invocare una mutazione travestendola da lettura. (REQ-869) Riclassificare una mutazione come sicura in lettura richiede la capacità `access_config` e viene registrato come decisione di governance; non esiste un'esclusione per richiesta. (REQ-870)

## Visibilità dello schema {: #schema-visibility }

Gli schemi GraphQL per ruolo nascondono i contenuti non autorizzati: (REQ-039)

- **Accesso ai domini**: un ruolo vede le tabelle solo nei suoi domini `domain_access` (`"*"` = tutti) (REQ-039)
- **Visibilità delle colonne**: le colonne non presenti in `visible_to` per un ruolo vengono omesse dall'SDL (REQ-039)
- Tabelle/colonne non autorizzate non compaiono nello schema (REQ-039)

## Sicurezza a livello di riga (RLS) {: #row-level-security-rls }

Iniezione di clausole SQL WHERE per tabella e per ruolo. Applicata dopo la compilazione, prima dell'esecuzione. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```
Il filtro viene combinato in AND con la clausola WHERE della query. Funziona sia per le query sia per le mutazioni (UPDATE/DELETE). (REQ-035, REQ-041)

Un termine `current_setting('provisa.<name>')` si risolve rispetto alle variabili di sessione che la richiesta associa: `user_id` dall'identità autenticata, `role` dal ruolo agente, e ogni claim scalare del token con il suo nome in minuscolo, trattini come trattini bassi. Una variabile di sessione Hasura mantiene il proprio nome: `X-Hasura-User-Id` è `hasura_user_id`, letta dal claim `x-hasura-user-id` del token (al livello superiore o dentro l'oggetto di claim Hasura, come `https://hasura.io/jwt/claims`) o registrata per il membro come attributo `hasura_user_id`. Un filtro Hasura importato su `X-Hasura-User-Id` legge quindi `provisa.hasura_user_id`. `provisa.user_id` è sempre l'identità autenticata; nessun claim la sostituisce. Le `session_vars` configurate di un ruolo sono le costanti di base; le associazioni della richiesta le sovrascrivono. Un ruolo che eredita da un altro prende le costanti dei suoi antenati sotto le proprie, con il valore del ruolo più vicino per ogni nome. `user_id`, `role`, `region` e `org` sono associati dalla richiesta o dal nodo e non si possono configurare come costanti. In modalità non protetta, un'intestazione `x-provisa-session-<name>` associa `<name>`. Una variabile non associata da nessuna parte si risolve in NULL e non corrisponde ad alcuna riga. (REQ-1682)

Una costante di ruolo può essere un riferimento `${env:NAME}` o `${secret:NAME}`, risolto con lo stesso risolutore di ogni altro riferimento quando una query lo usa; il modello conserva il riferimento, mai il valore risolto, e un riferimento che non si risolve in nulla fa fallire la query nominando la costante. Una costante risolta fa parte della query. Le registrazioni che Provisa fa della query sono coperte dall'oscuramento centrale; i log propri di una sorgente federata sono fuori dalla portata di Provisa. (REQ-2086)

## Mascheramento a livello di colonna {: #column-level-masking }

Il mascheramento è definito come una lista ordinata su ogni colonna. La prima voce che il lettore detiene decide cosa vede. (REQ-2059, REQ-263)

| Tipo di maschera | Tipi di colonna supportati | Espressione SQL |
| ----------- | ----------------------- | --------------- |
| `regex` | Stringa (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Qualsiasi | Valore letterale (`NULL`, `0`, una stringa personalizzata) |
| `truncate` | Data/Timestamp | `DATE_TRUNC(precision, col)` |

Ogni voce ha una lista `to` che nomina i lettori a cui si rivolge, e una `mask` facoltativa. Una voce senza `mask` significa non mascherato per quei lettori. `*` in `to` corrisponde a qualsiasi lettore.

L'espressione di maschera viene spinta nella proiezione SQL SELECT. Il database restituisce il valore mascherato; i dati non mascherati non attraversano mai la rete per i ruoli mascherati. (REQ-263) Le colonne mascherate sono inoltre bloccate nelle clausole `WHERE` e `HAVING` (protezione dei predicati del livello 5) per impedire di dedurre valori tramite filtri. (REQ-263, REQ-531)

**Una maschera copre ogni nome della colonna, non una colonna correlata.** Una colonna letta con il suo alias o con il suo nome fisico è mascherata allo stesso modo. Una maschera non nasconde una chiave che una relazione approvata unisce a una colonna che il ruolo può vedere: il join confronta i valori reali, quindi la colonna correlata mostra ciò che contiene la chiave mascherata. Mascherate anche la colonna correlata, oppure non approvate la relazione per quel ruolo. (REQ-263, REQ-019)

**Nessuna voce detenuta significa non mascherato.** Un lettore che non detiene alcuna voce legge la colonna non mascherata. Terminate una lista con una voce `*` quando a ogni lettore deve essere applicata una maschera. L'editor di colonne avvisa quando a una lista manca una voce `*` (`unlisted_unmasked`).

**Negare con l'ordine.** Una voce precedente vince anche quando il lettore ne detiene anche una successiva più permissiva. Perché i collaboratori esterni vedano sempre uno stipendio mascherato, qualunque altro ruolo abbiano, mettete la voce dei collaboratori prima della voce non mascherata. Un ruolo concesso non perde mai ciò che il padre concede per ereditarietà, ma una voce di maschera posta prima nella lista si applica per prima — così funziona qui la restrizione.

**Meta-ruoli.** La catena di un meta-ruolo include i ruoli di ogni membro, i loro antenati e i ruoli derivati nominati. Una voce concessa a qualsiasi membro raggiunge il meta-ruolo. Una voce precedente, detenuta da un altro membro, si applica comunque per prima.

**Verificare l'accesso.** `POST /admin/masks/check` accetta una lista di maschere e un insieme facoltativo di ruoli, e restituisce quale voce si applica per quei ruoli più le voci che nessun lettore può raggiungere. Protetto da `masking_config`.

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
## Governance delle risposte delle azioni {: #governing-action-responses }

La risposta di una funzione tracciata o di un webhook viene governata come le righe di una tabella. (REQ-1679) Le righe che un'azione restituisce vengono associate come una relazione le cui colonne sono il contratto di output dichiarato dell'azione — `output_columns` per una funzione, `inline_return_type` per un webhook, o le proprietà dell'oggetto del `return_schema` di tipo array di una funzione — e quella relazione attraversa la stessa fase di governance di ogni lettura di tabella, su una CTE VALUES che contiene le righe. (REQ-1679) Nulla di RLS, mascheramento o visibilità viene reimplementato per le azioni.

Si applicano tre cose, nello stesso ordine che per una tabella. (REQ-1679)

- Un filtro di riga: la regola RLS del ruolo salvata sull'azione per nome (`upsertRlsRule` con `actionName`), oppure, quando l'azione non ne ha una propria, la regola di dominio del dominio dell'azione. Il predicato viene convalidato rispetto al contratto al salvataggio, come una regola di tabella. (REQ-1676)
- Visibilità delle colonne: una colonna del contratto che dichiara `visible_to` viene rimossa per un ruolo che la lista non nomina; una colonna che non ne dichiara nessuno fa parte della forma pubblica dell'azione e resta.
- Maschere: una colonna del contratto porta `masks`, la stessa lista ordinata di maschere di una colonna di tabella. (REQ-2059)

L'ereditarietà dei ruoli risolve la catena per tutte e tre. (REQ-1677) Una risposta che restituisce una colonna fuori dal contratto dichiarato viene rifiutata con un 502 anziché passare senza governance. Un'azione che non dichiara alcun contratto di colonne restituisce uno scalare e non ha nulla da associare; viene restituita così com'è. L'invocazione di test di amministrazione riporta i filtri, le esclusioni e le maschere applicati, e l'istruzione governata viene scritta nel log di audit delle query.

## Campionamento {: #sampling }

Tutti i ruoli vedono risultati campionati (predefinito: 100 righe) a meno che non abbiano la capacità `full_results`. (REQ-554) Controllato dalla variabile d'ambiente `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Registrazione di audit {: #audit-logging }

Ogni query che tocca un asset di dominio viene registrata in `query_audit_log`. (REQ-596, REQ-613) Il log di audit appartiene all'organizzazione, non alla piattaforma: il registro proprio della piattaforma con organizzazioni, utenti e inviti non contiene record di audit. (REQ-1922) Il `query_audit_log` di ciascuna organizzazione è una tabella nello schema proprio dell'organizzazione, nello store che l'organizzazione indica per il proprio record in ciascuna regione che seleziona. Un'organizzazione che non dichiara regioni mantiene quello schema nel database del control plane del deployment, accanto al suo modello.

Ogni riga registra `tenant_id` (l'organizzazione), `user_id`, `role_id`, un hash SHA-256 del testo della query, il testo della query stesso cifrato (REQ-689), `table_ids` e `table_names`, `source`, `status_code`, `duration_ms`, la route e il numero di righe, ciò che è stato applicato all'istruzione, e `logged_at`. Il testo della query è cifrato con la chiave dell'organizzazione quando l'organizzazione ne ha impostata una, altrimenti con la chiave del deployment (REQ-1574). Un deployment che non configura alcun provider di cifratura lo memorizza non cifrato. Il testo non è mai esposto nelle tabelle ops interrogabili e viene decifrato solo in letture di amministrazione autorizzate.

Il log è di sola aggiunta a livello di database: regole PostgreSQL trasformano `DELETE` e `UPDATE` su `query_audit_log` in operazioni nulle. (REQ-596, REQ-613) Due indici, `(tenant_id, logged_at)` e `(user_id, logged_at)`, supportano query di conformità per organizzazione e per utente su intervalli di tempo. (REQ-596, REQ-613)

Conservazione: le righe sono conservate a tempo indeterminato. Provisa non ha alcuna impostazione di conservazione e non elimina mai il log di audit; poiché `DELETE` è un'operazione nulla, rimuovere le righe vecchie significa che un operatore elimina o archivia i dati della tabella al di fuori di Provisa.

## Limitazione della frequenza {: #rate-limiting }

I limiti di frequenza per ruolo si configurano in `provisa.yaml`: massimo di richieste al secondo, massimo di sottoscrizioni SSE simultanee e massimo di flussi Arrow Flight simultanei. (REQ-369) I limiti vengono applicati nel livello API prima della compilazione o dell'esecuzione; le richieste oltre il limite vengono rifiutate con HTTP 429 e un'intestazione `Retry-After`. (REQ-369)

Il servizio di query NL (`POST /query/nl`) ha un limite indipendente tramite `nl.rate_limit` (richieste al minuto per ruolo). Le richieste oltre il limite vengono rifiutate prima di qualsiasi chiamata a un LLM. (REQ-370)

Lo stato dei limiti di frequenza risiede in Redis (`cache.redis_url`) come contatore a finestra scorrevole — nessuno stato per istanza — così i limiti valgono su tutte le istanze orizzontali di Provisa. (REQ-371)

## Limite di complessità delle query {: #query-complexity-limit }

Un limite di frequenza limita quante richieste invia un ruolo. Il limite di complessità limita ciò che un'istruzione può chiedere. (REQ-1174)

Ogni istruzione ha un punteggio di complessità: le relazioni che legge, i suoi join, le colonne che seleziona (un `*` conta come le colonne che rappresenta) e le query annidate in essa. Una relazione letta da una sorgente API remota (OpenAPI, GraphQL remoto, gRPC remoto) conta 10, perché ogni lettura consuma un budget che il sistema remoto fissa per tutti gli utenti di quella sorgente.

Il limite è `max_query_complexity` nel `rate_limit` di un ruolo. L'impostazione a livello di organizzazione `limits.max_query_complexity` è un tetto per ogni ruolo: un ruolo può fissare un limite più basso, e uno più alto non ha effetto. Nessuno dei due è impostato per impostazione predefinita.

```yaml
roles:
  - id: analyst
    rate_limit:
      max_query_complexity: 200
```
Il punteggio viene misurato dopo l'analisi di una richiesta e prima della sua governance o esecuzione, così un unico limite vale su ogni interfaccia da cui può arrivare un'istruzione: GraphQL, SQL su HTTP, pgwire, Arrow Flight, gRPC, Cypher, JSON:API e MCP. Un'istruzione oltre il limite viene rifiutata con il suo punteggio, il limite e ciò che chiedeva. Le interfacce HTTP rispondono 413. Il rifiuto viene registrato come diniego di policy.

## Autenticazione {: #authentication }

Provider di autenticazione collegabili: (REQ-120)

| Provider | Credenziale | Caso d'uso |
| ---------- | ----------- | ---------- |
| `none` | Intestazione X-Provisa-Role | Solo sviluppo |
| `basic` | Account locali (bcrypt) + JWT di sessione | Deployment autonomi |
| `firebase` | Token ID Firebase | Produzione |
| `keycloak` | JWT Keycloak | Impresa |
| `oauth` / `oidc` | JWT OIDC | PingFed, Okta, Azure AD, Auth0 |
| `ldap` | Nome utente e password della directory (LDAP o Active Directory) | Directory aziendali (REQ-1265) |
| `saml` | Asserzione firmata SAML 2.0, poi un JWT di sessione | Single sign-on aziendale (REQ-1265) |
| `simple` | bcrypt + JWT | Test |

`ldap` autentica un utente con search-then-bind: Provisa si collega come account di servizio, trova l'unica voce indicata dal nome utente, poi si collega come quella voce con la password presentata, così che sia la directory a verificarla. I gruppi dell'utente diventano i suoi ruoli. L'account e i suoi gruppi vengono riletti ogni volta che un token di sessione ritorna, quindi un utente rimosso dalla directory, o da un gruppo, perde quell'accesso prima della scadenza del token. Sono supportati sia `ldaps://` sia StartTLS.

`saml` è il Web Browser SSO avviato dal SP. La firma XML della risposta viene verificata rispetto ai metadati del provider di identità e i claim sono letti solo dall'elemento firmato. Issuer, Audience, l'URL ACS, la scadenza e `InResponseTo` sono tutti controllati. Risposte non sollecitate (avviate dall'IdP), asserzioni cifrate, documenti con DOCTYPE, asserzioni multiple e firme SHA-1 sono rifiutati. Una sessione SAML non viene ricontrollata con il provider di identità: un utente rimosso lì mantiene l'accesso fino alla scadenza del token di sessione.

Mappatura dei ruoli: claim di identità → ruolo Provisa tramite regole configurabili. (REQ-120) Il campo `assignments_source` controlla da dove provengono le assegnazioni dei ruoli: `claims` le legge dai claim del token JWT (predefinito), `provisa` le legge dall'archivio interno delle assegnazioni di Provisa. (REQ-551) Un deployment multi-tenant (`multitenancy: true`) concede i ruoli da sé, per organizzazione, quindi deve usare `assignments_source: provisa`: con `claims` non si avvia, e una modifica delle impostazioni che produrrebbe quella combinazione viene rifiutata. Un deployment single-tenant può usare l'uno o l'altro.

Un superutente configurato in `provisa.yaml` (nome utente più una password da un segreto d'ambiente) riceve sempre il ruolo admin e tutte le capacità, qualunque sia il provider configurato — un percorso di bootstrap per la configurazione iniziale. (REQ-125)

### Superfici e credenziali {: #surfaces-and-credentials }

Ogni superficie si autentica tramite lo stesso contratto di provider, così una credenziale che funziona su una funziona su tutte, ovunque il protocollo possa trasportarla. (REQ-124, REQ-1263) Questa tabella è l'unico riferimento; la documentazione di ogni superficie non la ripete.

| Superficie | Password | Token del provider | Token di accesso personale | Certificato client (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | tramite un proxy di terminazione |
| pgwire | campo password (in chiaro o SCRAM) | campo password, deployment OIDC | campo password | sì |
| Bolt | schema `basic` | schema `bearer` | schema `bearer` | sì |
| Arrow Flight | — | `token` nell'handshake o nel payload del ticket; intestazione di chiamata `authorization: Bearer` su `list_flights`, `get_flight_info` e `get_schema` | uguale | sì |
| gRPC | — | metadati `authorization` | metadati `authorization` | sì |
| Airport (estensione DuckDB `airport`, opzionale) | — | `authorization: Bearer` (l'`auth_token` del segreto) | uguale | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | tramite un proxy di terminazione |

L'unica opzione di autenticazione dell'estensione DuckDB `airport` è l'`auth_token` bearer del suo segreto, quindi un client DuckDB presenta lì un token del provider o un token di accesso personale e agisce come il ruolo a cui si risolve la sua identità. Un client che può impostare intestazioni di chiamata può richiedere un ruolo detenuto, o un insieme separato da virgole, in `x-provisa-role`. (REQ-1120, REQ-1263)

Un rifiuto indica di che tipo è su ognuna di queste superfici. Una credenziale mancante, rifiutata o inutilizzabile è un rifiuto di autenticazione: HTTP 401, gRPC `UNAUTHENTICATED`, Arrow Flight e Airport `Unauthenticated`. Una credenziale valida che chiede di agire come un ruolo che la sua identità non detiene è un rifiuto di autorizzazione, con il ruolo nominato: HTTP 403, gRPC `PERMISSION_DENIED`, Arrow Flight e Airport `Unauthorized`. (REQ-273, REQ-1263) Un'eccezione è nella libreria client di Arrow, non nel server: un rifiuto della chiamata `get_schema` di Flight raggiunge un client Python come errore Arrow generico con il motivo del server, senza il suo tipo; `list_flights`, `get_flight_info` e `do_get` consegnano il tipo.

Dove una cella riporta `—`, il protocollo non porta un campo nome utente a cui abbinare una password; le forme a token lo coprono. pgwire è il caso speculare: il pacchetto di avvio ha un campo segreto e nessuno schema, quindi ciò che il segreto *è* sceglie il metodo — un PAT viene riconosciuto dal prefisso, il segreto viene letto come token bearer quando il provider configurato è un provider di token, e qualsiasi altra cosa è una password. La scelta si fa una volta — una credenziale che il validatore scelto rifiuta non viene ritentata con un altro.

La matrice è imposta da `tests/unit/test_auth_surface_conformance.py`, che esercita il vero punto di ingresso di convalida di ogni superficie e fallisce quando si aggiunge una nuova superficie senza riga.

### Token di accesso personali {: #personal-access-tokens }

Un PAT è un segreto bearer di lunga durata che un utente genera per un client che non può completare un accesso interattivo — uno script, uno strumento BI, un driver. (REQ-1263) Porta la propria organizzazione e il proprio ruolo, e ogni superficie lo risolve tramite lo stesso validatore, quindi nessuna superficie deve sapere cos'è un PAT.

La forma wire è `provisa_pat_` seguito da 43 caratteri base64 sicuri per URL. Il prefisso è ciò che indirizza un segreto presentato all'archivio dei token anziché al provider di identità, e rende un token trapelato ricercabile in log e repository.

- **Memorizzazione** — viene conservato solo lo SHA-256 del segreto. Il segreto stesso viene mostrato una sola volta, alla creazione, e non può essere recuperato. L'elenco riporta il prefisso di visualizzazione e i timestamp del ciclo di vita, mai una credenziale funzionante.
- **Emissione e revoca** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}`, più la sezione self-service nel profilo dell'utente nell'interfaccia di amministrazione. Generare e revocare una credenziale è un atto del titolare del token.
- **Attribuzione** — un PAT convalidato si risolve nell'account del suo proprietario: id utente, email e nome visualizzato. Una riga di audit o un rapporto di utilizzo scritto con un PAT nomina quindi la persona, non la credenziale. Quale dei token di quella persona abbia agito viene portato separatamente, in `raw_claims["token_name"]`.
- **Scadenza** — un token può avere una scadenza; un token scaduto viene rifiutato alla convalida. Eliminare l'appartenenza di un utente revoca con essa i suoi token.

### SCRAM-SHA-256 su pgwire {: #scram-sha-256-on-pgwire }

Con il provider `basic`, impostare `auth.scram: true` fa annunciare a pgwire SASL (codice di autenticazione 10) con il meccanismo `SCRAM-SHA-256`, così una password viene dimostrata anziché inviata. (REQ-1394) Il channel binding (`SCRAM-SHA-256-PLUS`) non è offerto.

SCRAM richiede un verificatore RFC 5802, che non può essere derivato da un hash bcrypt. Un verificatore viene scritto ogni volta che una password passa in chiaro — registrazione, accesso, cambio password, reimpostazione da parte di un amministratore — così un deployment che attiva SCRAM raccoglie verificatori man mano che i suoi utenti si autenticano, e la prima connessione SCRAM di ogni utente segue il suo successivo inserimento della password. A un utente ancora senza verificatore si risponde con uno scambio fittizio indistinguibile da uno reale, così la rete non rivela chi ha migrato.

### TLS reciproco {: #mutual-tls }

La verifica del certificato client sposta il primo controllo nell'handshake TLS: un chiamante senza un certificato firmato dalla CA del deployment non raggiunge mai il livello delle credenziali. (REQ-1228) È disponibile su pgwire, Bolt, gRPC e Arrow Flight — i quattro trasporti che terminano il proprio TLS.

| Variabile | Significato |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | Bundle PEM delle CA autorizzate a firmare certificati client |
| `PROVISA_MTLS_MODE` | `required` (il predefinito una volta impostata una CA) o `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Se vero, il common name del certificato deve essere uguale al nome utente con cui la connessione poi si autentica |

Le sostituzioni per protocollo seguono la stessa denominazione delle impostazioni TLS. Nulla viene dedotto: una modalità impostata senza CA rifiuta di avviarsi, e una modalità non riconosciuta rifiuta di avviarsi anziché essere letta come il vicino più sicuro — un deployment che crede di richiedere certificati client e non lo fa è messo peggio di uno che non si avvia.

### Versioni TLS {: #tls-versions }

Ogni listener che Provisa termina (l'API HTTP e la UI, pgwire, Bolt, MCP, gRPC e Arrow Flight) accetta TLS 1.2 e TLS 1.3 e rifiuta TLS 1.0 e 1.1. Provisa non imposta alcuna versione di protocollo né elenco di cifrari propri. I listener HTTP, pgwire e Bolt usano i valori predefiniti server del modulo `ssl` di Python (minimo TLS 1.2). gRPC e Arrow Flight usano i valori predefiniti di gRPC (minimo TLS 1.2). Le versioni non sono configurabili.

### Limitazione degli accessi {: #login-throttling }

Indovinare le password non dipende dal protocollo: lo stesso account può essere attaccato via HTTP, pgwire e Bolt. Il contatore risiede quindi nel livello di convalida delle credenziali, non su una singola superficie, così un blocco ottenuto ovunque viene applicato ovunque. (REQ-1393)

È attiva per impostazione predefinita — cinque errori in cinque minuti bloccano il soggetto per quindici minuti — e si regola in `auth.login_throttle`. Un soggetto bloccato viene rifiutato prima ancora che la credenziale venga esaminata, e un'autenticazione riuscita cancella la cronologia di quel soggetto.

La chiave è il principal che il protocollo porta. Una superficie solo bearer non porta un principal, quindi la chiave è un digest della credenziale stessa; ciò che impedisce è che un token errato venga riprodotto senza limiti. L'archivio è per processo, quindi un deployment con più worker API consente fino a `max_attempts` per worker — la limitazione frena i tentativi, non è una quota distribuita.

### Sondaggio dei ruoli {: #role-probing }

Nominare un ruolo che il chiamante non detiene è trattato come un evento di sicurezza rilevato di gravità alta, su ogni superficie: l'API HTTP, MCP, pgwire, Bolt, Arrow Flight, il servizio Flight airport e gRPC. Un client legittimo nomina i ruoli che gli sono stati dati; uno che ne nomina un altro è guasto oppure sta sondando. (REQ-2076)

La richiesta viene rifiutata. Al chiamante viene detto solo «not permitted», niente sull'esistenza del ruolo, e la connessione o la sessione termina dove ce n'è una: la connessione pgwire, la sessione Bolt, la chiamata gRPC, la sessione MCP. L'eccezione è l'API HTTP con qualsiasi credenziale diversa da un token di accesso personale: lì il browser presenta la propria sessione o il token del suo provider di identità, quindi il rifiuto continua a nominare il ruolo.

Il resto dipende dalla credenziale e dal trasporto:

| Credenziale | Trasporto | Sospeso |
|---|---|---|
| Token di accesso personale | qualsiasi | il token |
| Token firmato da un provider di identità esterno | MCP, pgwire, Bolt, Flight, gRPC | l'accesso tramite token del principal all'organizzazione |
| Token firmato da un provider di identità esterno | l'API HTTP | niente |
| Una sessione o una password che questo deployment verifica | qualsiasi | niente |
| Una sessione MCP locale (senza bearer) | MCP | niente; l'evento va nel log dell'operatore |

L'API HTTP fa eccezione perché il browser presenta lo stesso token bearer di un client API, quindi il server non può distinguerli, e una scheda che nomina ancora un ruolo che l'utente ha perso (dopo che un amministratore ne ha cambiato i ruoli, o dopo un cambio di organizzazione) è normale. MCP e gli altri trasporti non sono mai una sessione del browser. Lì l'evento viene rifiutato, registrato e notificato come qualsiasi altro; si omette solo la sospensione.

Un token sospeso viene rifiutato come uno revocato. Un principal il cui accesso tramite token è sospeso viene rifiutato su MCP, pgwire, Bolt, Flight e gRPC in quell'organizzazione, e può ancora accedere all'interfaccia. Solo un amministratore revoca una sospensione; niente la revoca allo scadere di un tempo. Team → Accesso sospeso elenca ogni sospensione con l'evento che l'ha causata e la revoca (`GET /admin/orgs/{org}/suspensions`, `POST /admin/orgs/{org}/suspensions/lift`).

Ogni evento viene scritto nel registro amministrativo dell'organizzazione (azione `security.role_probe`) con il principal, l'identificativo della credenziale, l'indirizzo del client, il trasporto, l'endpoint o lo strumento, il ruolo nominato, i ruoli detenuti, l'ora e gli identificativi delle istruzioni sottoposte ad audit più recenti del principal. Il loro testo resta nel log di audit delle query e si legge con il diritto `view_governance`.

Gli amministratori dell'organizzazione ricevono un'e-mail dove il deployment invia posta. L'evento viene anche inviato come JSON al webhook di sicurezza dell'operatore quando `security.webhook_url` è impostato. Ciò che lascia l'organizzazione nomina il principal, l'identificativo della credenziale, il trasporto, l'endpoint, il ruolo nominato, l'ora e la gravità, con un link alla vista di amministrazione; non contiene alcuna istruzione né alcun ruolo detenuto. Un avviso che non può essere consegnato viene registrato e non annulla mai il rifiuto né la sospensione.

### Nominare l'organizzazione {: #naming-the-org }

In modalità multi-tenant ogni richiesta nomina l'organizzazione a cui è destinata. Appartenere esattamente a un'organizzazione non la nomina: una richiesta che non ne nomina nessuna viene rifiutata, e il rifiuto dice cosa inviare. (REQ-1235) Ogni superficie nomina un'organizzazione così:

| Superficie | Come un client nomina l'organizzazione |
| ------- | -------------------------- |
| HTTP (interfaccia, REST, GraphQL) | Il nome host proprio dell'organizzazione, `acme.provisa.dev`, o l'intestazione `X-Org-Provisa` sull'host del control plane |
| pgwire | Il nome del database (`psql -d acme`, il campo database di uno strumento BI), o il nome host proprio dell'organizzazione raggiunto via TLS |
| Bolt | Il nome del database, `<org>.provisa_<role>` (o `<org>.provisa_ops_<role>`), o il nome host proprio dell'organizzazione raggiunto via TLS |
| Arrow Flight | `"org"` nel ticket |
| gRPC | L'intestazione di metadati `x-provisa-org` |
| MCP (trasporto HTTP) | Il nome host proprio dell'organizzazione, o l'intestazione `X-Org-Provisa` |
| Client Python | `org=` su `ProvisaClient`, `connect()` e `adbc_connect()`; `?org=` in un URL SQLAlchemy |

Un client pgwire che raggiunge un nome host via TLS lo invia nel ClientHello TLS, e Provisa ne legge l'organizzazione (REQ-1234). Quando il nome host e il nome del database nominano entrambi un'organizzazione e differiscono, la connessione viene rifiutata; nessuno dei due è preferito. Il nome del database `provisa`, il predefinito, non nomina alcuna organizzazione, e uno strumento BI connesso a un'organizzazione mostra
quell'organizzazione come suo database. Su Bolt, `SHOW DATABASES` elenca un `<org>.provisa_<role>` per ogni organizzazione a cui
l'utente appartiene; un semplice `provisa_<role>` viene rifiutato in modalità multi-tenant, e una sessione resta nell'
organizzazione che ha nominato per prima.

Un nome è una richiesta, non una concessione. Raggiunge ovunque lo stesso resolver, che rifiuta un'organizzazione di cui il principal autenticato non è membro né detiene il diritto tra organizzazioni.

Un token di accesso personale viene emesso per un'organizzazione e la nomina da sé. Apre solo quell'organizzazione: una richiesta che ne nomina un'altra viene rifiutata, anche quando il proprietario del token appartiene a entrambe.

Un deployment single-tenant non ha un'organizzazione da nominare. Nessuna superficie vi legge un nome host, un nome di database o un'intestazione a tale scopo.

## Segreti tra organizzazioni {: #secrets-across-organizations }

I segreti di un'organizzazione vengono letti solo da quell'organizzazione. Nell'archivio proprio di Provisa ogni segreto è conservato sotto la sua organizzazione. In un servizio centrale (Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) di un deployment con molte organizzazioni, ogni lettura avviene all'interno del prefisso dell'organizzazione che legge, e un riferimento che ne uscirebbe viene rifiutato. (REQ-1557) I prefissi e i passaggi per spostarvi i segreti esistenti sono in [Segreti](secrets.md#one-organizations-secrets-in-a-central-service).

## Telemetria tra organizzazioni {: #telemetry-across-organizations }

Su un deployment condiviso da molte organizzazioni, i segnali operativi (`"ops"."traces"` e, sul motore Trino, le metriche e i log accanto) sono un unico archivio in cui arriva il lavoro di ogni organizzazione. Ogni span registra l'organizzazione per cui è stato avviato, conservata come `tenant_id`, e ogni lettura dei segnali restituisce solo le righe dell'organizzazione che legge, qualunque sia il ruolo. Uno span avviato fuori dal lavoro di qualsiasi organizzazione (l'avvio del server, ad esempio) non appartiene a nessuna e nessuna lo legge. L'amministratore della piattaforma non ha accesso ai dati di alcuna organizzazione e non ne legge nessuna tramite Provisa. (REQ-1349, REQ-1327)

I segnali operativi sono conservati a tempo indeterminato su entrambi i motori, come il log di audit: la compattazione riscrive i file in cui sono archiviati e non rimuove nessuna delle loro righe. Su Trino sono le tabelle Iceberg del catalogo `otel`. Su un motore nativo (DuckDB) il server deposita i propri record di trace come file Parquet nella sua cartella di telemetria e il motore li legge con lo stesso nome, `"ops"."traces"`, con la vista di lineage dei refresh `"ops"."mv_refresh_lineage"` sopra; entrambi sono letti con la stessa regola per organizzazione. (REQ-862)

I segnali sono conservati in chiaro nel loro archivio. Un carico di lavoro che richiede più di un confine per organizzazione all'interno di un deployment gira su un deployment dedicato; uno molto sensibile o regolamentato è self-hosted.

## Hardening del deployment {: #deployment-hardening }

### Credenziali degli store e indirizzo su cui ascoltano {: #store-credentials-and-where-the-stores-listen }

Gli store inclusi non hanno credenziali predefinite. Il control plane PostgreSQL, Redis, MinIO, l'account admin di Grafana e il segreto condiviso del cluster Trino ricevono ciascuno un valore generato per il deployment:

- I launcher (`provisa start`, gli installer desktop e server) li generano una volta e li conservano in `~/.provisa/store-credentials.env` (modalità 600). Un valore già presente nell'ambiente viene usato così com'è, e un nodo secondario, che usa gli store del primary, rifiuta di avviarsi senza quelli del primary.
- I moduli Terraform li generano con `random_password` e danno gli stessi valori a ogni nodo. Sono contenuti nello stato di Terraform, quindi proteggi lo stato come proteggeresti le password.
- Il chart Helm non ne imposta alcuno. Richiede un Secret che crei tu, nominato da `credentials.existingSecret`, contenente `pg-password`, `redis-password`, `minio-access-key`, `minio-secret-key`, `grafana-admin-password` e `trino-shared-secret`.

Redis richiede la propria password, e la password del ruolo PostgreSQL viene reimpostata a ogni avvio, così un volume creato in precedenza segue quella attuale del deployment. Gli store, il motore di federazione e i servizi di telemetria pubblicano le loro porte su un solo indirizzo, `PROVISA_STORE_BIND_IP`: `127.0.0.1` su un nodo a sé stante, o l'indirizzo privato del primary quando i secondary vi si connettono. L'account admin di Grafana usa la password generata; lo stack compose consente ai visitatori anonimi di vedere le dashboard, mentre il chart Helm non ammette alcun visitatore anonimo.

### Cifratura del trasporto {: #transport-encryption }

Ogni listener che Provisa termina serve TLS (vedi [Versioni TLS](#tls-versions)). Un nodo server usa il certificato in `PROVISA_TLS_CERT`/`PROVISA_TLS_KEY`, oppure ne genera uno autofirmato. Il chart Helm serve TLS per impostazione predefinita (`tls.enabled: true`) dal Secret `kubernetes.io/tls` nominato da `tls.secretName`.

### Il PostgreSQL embedded {: #the-embedded-postgresql }

I livelli desktop e nativo eseguono PostgreSQL embedded. Su macOS e Linux ascolta su un unix socket dentro la sua directory dei dati (modalità 700), raggiungibile solo dall'account proprietario della directory. Su TCP (Windows, dove ascolta su 127.0.0.1), ogni connessione deve fornire una password generata per la directory dei dati e conservata al suo interno (`provisa.password`, modalità 600). Non viene mai avviato come root.

### Il motore di federazione (Trino) {: #the-federation-engine-trino }

- **Tra i nodi.** Un coordinator e i suoi worker si autenticano a vicenda con il segreto condiviso generato del deployment (`internal-communication.shared-secret`). Cifrare il loro traffico è un'opzione dell'operatore, disattivata per impostazione predefinita: disattivata, il traffico resta dentro la rete privata o il cluster del deployment. Attivala dove un requisito di conformità o zero-trust chiede la cifratura all'interno del deployment. Trino documenta che rallenta le query ad alto shuffle (join distribuiti, aggregazioni, funzioni finestra) dal 10% a oltre il 100%. Su un deployment compose è l'impostazione `engine.trino_internal_tls` (Impostazioni del deployment → Engine), con accanto la rete del motore e l'indirizzo fissato del coordinator, e si applica al successivo avvio. Con il chart Helm è il valore `trino.internalTls`; il chart genera la CA e il certificato usati dai nodi, li conserva tra un aggiornamento e l'altro e indirizza ogni nodo con il nome DNS del suo pod. I motori della piattaforma ospitata sono coordinator a nodo singolo, senza traffico tra nodi, e la rifiutano. (REQ-2072)
- **Dati spooled.** L'esecuzione fault-tolerant cifra i dati di exchange che scrive in spool con una chiave generata per ogni query e scartata al suo termine (`fault-tolerant-execution.exchange-encryption-enabled=true`, impostato esplicitamente). Lo spill legacy su disco non è usato.
- **Organizzazioni su un solo motore.** I catalog di ogni organizzazione sono suoi, denominati con l'organizzazione, e le istruzioni di ogni organizzazione vengono eseguite nel suo resource group.
- **I cataloghi, e le credenziali che contengono, vivono solo nella memoria del motore.** Provisa crea ogni catalogo Trino a runtime, e ognuno contiene le proprietà di connessione della sua sorgente, password incluse. Ogni motore eseguito da Provisa li tiene in memoria (`catalog.store=memory`): nessun catalogo viene scritto sul disco del motore e nessuno viene montato da un file. Un coordinatore che si riavvia torna senza cataloghi; ogni processo Provisa se ne accorge alla query successiva, e il watcher del motore entro un minuto, e ricrea i cataloghi che serve. Una sorgente Kafka con credenziali SASL ha bisogno di un file: il connettore Kafka di Trino legge le impostazioni del client solo da un file, quindi Provisa le scrive, password inclusa, in `kafka-client/<catalog>.properties`. Il file è leggibile solo dall'utente di Trino (modalità 0600 in una directory 0700). Nello stack di container la directory è un volume in memoria condiviso da API e motore. Sui motori della piattaforma ospitata è il Secret Kubernetes del motore stesso, tenuto nella memoria del nodo e leggibile solo dal gruppo di Trino (modalità 0440). In entrambi i casi il file non raggiunge mai un disco. Sui motori ospitati una sorgente Kafka deve nominare uno schema registry: una sorgente le cui colonne dei topic sono dichiarate solo dal modello viene rifiutata per nome al salvataggio o all'applicazione, perché le sue descrizioni di tabella richiederebbero una directory che non si può dare al motore in esecuzione. L'avvio dello stack rimuove i file di catalogo e di client Kafka che le versioni precedenti hanno lasciato su disco.

### Sorgenti su un deployment condiviso {: #sources-on-a-shared-deployment }

Su un deployment che ospita molte organizzazioni, le sorgenti di un'organizzazione possono raggiungere solo indirizzi pubblici: mai la rete privata della piattaforma, dove si trovano il suo control plane, la cache e l'archivio a oggetti. Il controllo si applica quando una sorgente viene salvata, dalla UI, dall'API di amministrazione o da un'applicazione di configurazione, e di nuovo a ogni connessione e a ogni redirect HTTP. Le sorgenti di una sandbox demo sono inoltre vincolate all'indirizzo a cui sono state verificate. L'organizzazione del deployment stesso, le cui sorgenti sono configurate dall'operatore, non è vincolata.

## Cifratura a riposo e chiavi proprie dell'organizzazione {: #encryption-at-rest-and-the-organizations-own-keys }

La cifratura con una chiave propria dell'organizzazione è disponibile ed è disattivata finché l'organizzazione non la attiva. Nulla è cifrato con una chiave dell'organizzazione a meno che un amministratore dell'organizzazione ne crei una.

**La chiave di cifratura dell'organizzazione** (REQ-1574) avvolge i segreti dell'organizzazione, le sue colonne cifrate e il testo delle query di audit. Un amministratore dell'organizzazione la crea in **Admin → Org Secrets → Org Key**, oppure con `PUT /admin/org-encryption`: senza corpo Provisa genera la chiave, che non viene mai mostrata, e un corpo `key_b64` fornisce 32 byte propri. `GET /admin/org-encryption` riporta solo la sua impronta. Senza una chiave dell'organizzazione, i dati dell'organizzazione sono avvolti con la chiave del deployment (`encryption.provider` in `provisa.yaml`). Un deployment che non configura alcun provider di cifratura conserva in chiaro le colonne cifrate e il testo delle query di audit; i segreti non sono mai conservati in chiaro (vedi [Segreti](secrets.md#provisas-built-in-store)).

**La cartella dell'organizzazione** (REQ-1992) contiene i suoi file caricati e atterrati, i file di spill del suo motore e i suoi file temporanei. Un amministratore dell'organizzazione ne attiva la cifratura in **Admin → Org Secrets → File folder encryption**, oppure con `PUT /admin/org-files` (`{"encrypted": true, "key_reference": "${secret:NAME}"}`). `POST /admin/org-files/generate-key` risponde una sola volta con una nuova chiave a 256 bit, da conservare nella vault. La chiave resta nella vault propria dell'organizzazione (AWS Secrets Manager, GCP Secret Manager, Azure Key Vault o HashiCorp Vault): Provisa conserva solo il riferimento, e gocryptfs mantiene cifrati su disco i file della cartella con quella chiave. Serve FUSE, quindi un host Linux o un container con `/dev/fuse`; su macOS l'impostazione viene rifiutata.

Su un deployment self-hosted, che tipicamente esegue una sola organizzazione sui propri motori, attivare entrambe cifra i segreti, i file, i file di spill e i file temporanei di quell'organizzazione con le sue chiavi:

- Il motore DuckDB che un'organizzazione esegue scrive lo spill nella cartella dell'organizzazione.
- Le righe che un row filter recupera e il codice caricato vengono messi in staging nella cartella dell'organizzazione.
- L'esecuzione fault-tolerant di Trino cifra i dati di exchange in spool con una chiave generata per ogni query e scartata al suo termine (`fault-tolerant-execution.exchange-encryption-enabled=true`). Provisa non attiva lo spill legacy di Trino su disco.
- Trino tiene i cataloghi dell'organizzazione, con le loro credenziali, in memoria e non ne scrive nessuno su disco (`catalog.store=memory`); le impostazioni client SASL di una sorgente Kafka sono in un file che legge solo l'utente di Trino, in memoria nello stack di container (vedere [Il motore di federazione](#the-federation-engine-trino)).
- Un motore di federazione PostgreSQL scrive i propri file temporanei dove quel server PostgreSQL li conserva, sotto la cifratura dello storage del server stesso, non nella cartella dell'organizzazione.

### Piattaforme ospitate condivise e dedicate {: #shared-and-dedicated-hosted-platforms }

La piattaforma ospitata condivisa isola le organizzazioni in modo logico. Un solo deployment le serve tutte, e i dati di ogni organizzazione restano suoi grazie alla governance, ai suoi schemi, catalog e resource group sul motore, alle sue chiavi, alle chiavi per query per i dati di motore in spool e al rifiuto delle sue sorgenti sugli indirizzi privati della piattaforma. È consigliata per la sperimentazione e per le piccole imprese. Una piattaforma ospitata dedicata è separata fisicamente: macchine, rete e store propri, al servizio di un solo cliente. Per carichi di lavoro molto sensibili o regolamentati, ospitate Provisa in proprio all'interno del vostro perimetro, dove i vostri operatori detengono ogni chiave e ogni host. Le piattaforme ospitate oggi non dispongono di un report SOC 2. Stiamo valutando SOC 2 e daremo aggiornamenti. Se vi serve SOC 2, consigliamo il self-hosting.

### Python nel browser {: #python-in-the-browser }

Le schede Python e Notebook di Explore eseguono Python nel browser del visitatore. Ogni lettura che fanno va al server come l'utente autenticato, con il suo ruolo attivo, la sua organizzazione e il suo ambiente, ed è governata esattamente come la stessa istruzione da qualsiasi altro client.

## Modalità ad alta sicurezza {: #high-security-mode }

`security.mode: high` in `provisa.yaml` afferma una garanzia: il backend di Provisa non gestisce mai dati in chiaro. (REQ-693) Ogni colonna che conta è cifrata alla sorgente, e solo un client che detiene la chiave di decifratura può leggerla. Questa garanzia ha conseguenze che un deployment deve pianificare.

**Cosa fa la modalità:**

- **Gli endpoint dei dati richiedono la prova della decifratura lato client.** Tutto sotto `/data/` restituisce 403 a meno che il chiamante non presenti l'intestazione `X-Provisa-KMS-Key` — il segno di un client JDBC o Python configurato per decifrare localmente. Un browser o un consumatore REST in chiaro non porta una tale chiave e viene rifiutato. Il controllo è un diniego predefinito sull'intero albero: una route aggiunta domani è protetta il giorno del rilascio, e un'esenzione va motivata.
- **Gli endpoint dei metadati dello schema restano aperti.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` e `/data/compile` non restituiscono dati di riga, e un client deve leggere lo schema — compresi i campi `@encrypted` — prima di potersi connettere.
- **gRPC e Arrow Flight continuano a servire, con la stessa prova.** Sono i trasporti che i client con cifratura usano davvero; chiuderli lascerebbe un deployment ad alta sicurezza senza protocollo wire. Una chiamata di dati su uno dei due deve portare la stessa chiave KMS come metadati di chiamata.
- **pgwire, Bolt e MCP non si avviano.** Nessuno dei tre ha un handshake per connessione in grado di portare un contesto di decifratura: un insieme di righe pgwire e un risultato Cypher viaggiano in chiaro, e una chiamata a uno strumento MCP consegna i suoi risultati a un modello come testo. Una porta configurata per uno di essi viene rifiutata all'avvio anziché servita.
- **La protezione delle relazioni non può essere aggirata.** `ignore_relationships` e `relationship_guard: false` vengono entrambi ignorati; vedere [Governance delle relazioni](#relationship-governance-v002).

**Verificare che un deployment sia nella modalità:** il log di avvio la nomina, una richiesta `/data/sql` senza chiave KMS risponde 403 con un messaggio che nomina REQ-693, e le porte di pgwire, Bolt e MCP non sono in ascolto.

## Hook di approvazione ABAC {: #abac-approval-hook }

Un hook di policy esterno facoltativo che scatta prima dell'esecuzione della query. (REQ-203) Quando è configurato, Provisa chiama il vostro motore di policy con l'identità dell'utente, i ruoli, le tabelle, le colonne e l'operazione. La risposta determina se la query prosegue. (REQ-203) Un comando registrato con **Richiede approvazione** viene sottoposto allo stesso hook prima di ogni chiamata; vedere [Chiamate di comandi](#command-calls). (REQ-1924)

### Ambito {: #scoping }

L'hook scatta solo quando la query tocca una tabella o una sorgente incluse — nessun sovraccarico per tutto il resto. (REQ-204)

| Configurazione | Effetto |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Ogni query fa scattare l'hook |
| `sources[].approval_hook: true` | Tutte le tabelle di quella sorgente fanno scattare l'hook |
| `tables[].approval_hook: true` | Quella tabella fa scattare l'hook |

### Protocolli {: #protocols }

Sono supportati tre trasporti: (REQ-246)

| Tipo | Caso d'uso | Campo di configurazione |
| ------ | ---------- | ------------- |
| `webhook` | Qualsiasi servizio di policy con HTTP (OPA, personalizzato) | `url` |
| `unix_socket` | OPA o sidecar di policy sulla stessa macchina | `socket_path` + `url` |
| `grpc` | Servizio di policy co-locato ad alto throughput | `url` (host:port) |

Il trasporto gRPC usa il contratto `provisa.auth.ApprovalService` definito in `provisa/auth/approval.proto`. Implementate questo servizio nel vostro motore di policy: (REQ-246)

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
Il canale gRPC è persistente — un canale per istanza di Provisa, riutilizzato per tutte le chiamate a quell'endpoint dell'hook. (REQ-555)

### Richiesta / risposta {: #request-response }

Tutti e tre i trasporti portano lo stesso payload: (REQ-246)

| Campo | Tipo | Descrizione |
| ------- | ------ | ------------- |
| `user` | string | Identità dell'utente autenticato |
| `roles` | string[] | Ruoli Provisa dell'utente |
| `tables` | string[] | Id delle tabelle referenziate nella query |
| `columns` | string[] | Colonne selezionate nella query |
| `operation` | string | `"query"` o `"mutation"`; `"command"` per una chiamata di comando |
| `command` | string | Il nome del comando per una chiamata di comando; vuoto per una query |
| `arguments` | object | Gli argomenti con cui viene chiamato un comando; vuoto per una query |

I trasporti webhook e socket Unix scambiano JSON, con `command` e `arguments` come chiavi. Su gRPC gli argomenti viaggiano come testo JSON `arguments_json`. La risposta deve includere `approved` (bool) e facoltativamente `reason` (string). (REQ-246)

### Chiamate di comandi {: #command-calls }

Un comando con `requires_approval` impostato (l'interruttore **Richiede approvazione** nel modulo del comando) passa dall'hook prima di ogni chiamata, su ogni superficie. Viene eseguito solo se l'hook approva. La richiesta contiene `operation: "command"`, il nome del comando e i suoi argomenti; `tables` e `columns` sono vuoti. (REQ-1924) [tool-verified: `provisa/api/data/action_exec.py` `_require_approval` (`operation="command"`, `tables=[]`, `columns=[]`, `command=fn["name"]`, `arguments=args`)]

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

Due rifiuti, entrambi 403:

- `functions.approval_unavailable`: nessun hook è configurato. A differenza di una query, un comando che richiede approvazione non passa mai senza di esso.
- `functions.approval_denied`: l'hook ha risposto `approved: false`. Il motivo è nel messaggio.

### Timeout e fallback {: #timeout-and-fallback }

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```
In caso di timeout o errore di trasporto si applica la policy `fallback`. (REQ-247) Un circuit breaker (predefinito: aperto dopo 5 errori consecutivi, semi-aperto dopo 30 s) previene guasti a cascata dovuti a un endpoint dell'hook lento. (REQ-556)

### Esempio di configurazione {: #configuration-example }

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
## Segreti {: #secrets }

Le credenziali usano la sintassi `${env:VAR_NAME}`, risolta in fase di esecuzione. (REQ-557) Le password non vengono mai memorizzate nel database di configurazione. (REQ-557)

Per il servizio di segreti completo — vault, sintassi dei riferimenti e provider — vedere [Segreti](secrets.md).
