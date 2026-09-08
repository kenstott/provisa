# Principi del Modello di Dominio

---

## 1. Governance

### Principi fondamentali

1. **Ogni risorsa deve essere di proprietà di un dominio.** Tabelle, viste e relazioni sono tutti asset di dominio. Non esistono risorse fluttuanti non governate. Il dominio è l'unità di responsabilità.
2. **Ogni dominio deve avere uno steward.** Un dominio può esistere in uno stato in sospeso finché non viene assegnato uno steward, ma non può servire dati governati senza uno.
3. **L'admin possiede le origini dati.** Le origini dati sono infrastruttura, non risorse di dominio. L'admin registra e gestisce le connessioni ai sistemi dati esterni.
4. **Gli steward possono rivendicare tabelle per un dominio.** La rivendicazione è esclusiva — una tabella appartiene esattamente a un dominio. Questo è l'atto governato che collega l'infrastruttura al livello semantico.
5. **Gli steward possono creare viste intra-dominio a partire dagli asset di dominio.** Le viste esprimono logica di business — join, aggregazioni, metriche derivate — su asset di proprietà dello steward all'interno dello stesso dominio. Le viste creano nuovo significato semantico e richiedono l'approvazione dello steward.
6. **Gli analisti possono creare query cross-dominio a partire da relazioni approvate.** Le query sono viste interdominio espresse in qualsiasi linguaggio di query supportato. Non creano nuova semantica — attraversano percorsi di relazione approvati. Non è richiesta alcuna approvazione aggiuntiva: la governance è gestita a monte, ai livelli di Relazione e visibilità delle colonne. Il catalogo è il meccanismo di enforcement: il compilatore rifiuta gli attraversamenti non presenti nel catalogo delle relazioni approvate.
7. **Chiunque può richiedere l'accesso a una risorsa di dominio.** L'accesso è concesso a livello di risorsa, non a livello di query. Se si ha accesso a una risorsa, la si può interrogare. La governance è applicata a runtime attraverso la pipeline.

### Risorse: tabelle e viste come pari

La distinzione tra una tabella e una vista è solo di origine — una tabella è rivendicata da un'origine dati, una vista è definita da uno steward. Una volta che entrambe esistono come asset di dominio, il modello di governance le tratta in modo identico:

- Entrambe sono asset di dominio di prima classe visibili nel catalogo
- Entrambe possono essere il target di una relazione
- Entrambe possono essere concesse secondo il Principio 6
- Entrambe sono soggette alla stessa pipeline di governance

Uno steward può rivendicare tabelle privatamente ed esporre solo viste curate come prodotti dati rivolti al pubblico.

### Composizione delle viste

Una vista appartiene sempre a un singolo dominio — esiste un solo tipo di vista, sempre intra-dominio. Una vista esiste per uno dei due scopi seguenti:

- **Importazione cross-dominio** — l'origine è esterna al dominio. I dati cross-dominio possono entrare in un dominio solo tramite una vista, che funge da adattatore in sola lettura che denomina i dati esterni come un concetto di business del dominio.
- **Derivazione locale** — l'origine è nello stesso dominio. La vista deriva dati nuovi o calcolati da asset di dominio esistenti. I dati nuovi o derivati possono esistere solo come vista.

Una vista può fare riferimento a:

- Tabelle rivendicate all'interno dello stesso dominio
- Campi importati da un altro dominio tramite una concessione di accesso al campo
- Un'altra vista all'interno dello stesso dominio, dove la variazione è mirata: restrizione di campi, aggregazione o arricchimento tramite un join aggiuntivo

La profondità di composizione non è tecnicamente applicata — il giudizio dello steward durante la revisione HITL è il meccanismo di controllo qualità.

Ogni vista porta uno scopo di business dichiarato, indicato al momento della creazione:

- Fa parte dell'artefatto governato — gli steward approvano sapendo a cosa serve la vista
- Viene referenziato dalle richieste di accesso secondo il Principio 7, così lo steward può valutarne l'idoneità
- Accompagna la vista dalla sua creazione lungo l'intero flusso di governance

### Query

Una Query attraversa percorsi di relazione approvati sugli asset di dominio. A differenza delle Viste, le Query non creano nuovo significato semantico — attraversano la struttura approvata del modello. Le Query possono essere espresse in qualsiasi linguaggio di query supportato (SQL, GraphQL, Cypher).

**Enforcement strutturale:** il catalogo delle relazioni è il meccanismo di enforcement. Il compilatore valida ogni attraversamento rispetto alle voci approvate nel catalogo e rifiuta le query che fanno riferimento a percorsi non approvati. La governance è strutturale, non un controllo a runtime.

**Nessuna approvazione richiesta:** la governance avviene a monte — ai livelli di Relazione e visibilità delle colonne. Se un utente ha accesso alle colonne e il percorso di attraversamento è approvato, la Query è un uso valido. Nessun ulteriore gate.

**Distinzione dalle Viste:**

- Viste: intra-dominio, introducono nuovo significato semantico, curate dallo steward
- Query: attraversano relazioni approvate, nessuna nuova semantica, nessun gate di approvazione

**Espressione del dominio per linguaggio di query:**

Ogni linguaggio supportato espone il dominio come namespace strutturale nativo di quel linguaggio:

| Linguaggio | Espressione del dominio | Esempio |
| --- | --- | --- |
| GraphQL | Prefisso del nome di tipo e campo | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Nome dello schema | `SELECT * FROM sales.orders` |
| Cypher | Etichetta di nodo aggiuntiva (il dominio è richiesto solo quando il nome del tipo è ambiguo) | `MATCH (o:Sales:Order)` |

Il compilatore risolve l'appartenenza al dominio da queste posizioni strutturali — non è richiesta alcuna annotazione o suggerimento.

### Relazioni

Una relazione è un percorso di attraversamento approvato tra due asset. I confini di dominio sono irrilevanti rispetto a cosa sia una relazione — determinano solo chi la approva.

**Approvazione:**

- L'approvazione è richiesta da ogni steward distinto che possiede un asset coinvolto nella relazione
- Se uno steward possiede entrambi gli asset, è richiesta un'approvazione. Se sono coinvolti due steward, sono richieste due approvazioni
- Non esiste una classificazione intra-dominio/cross-dominio — la proprietà determina naturalmente l'onere di approvazione
- Approvare una relazione costruisce il grafo delle dipendenze di ciascuno steward, abilitando notifiche proattive sull'evoluzione dello schema

Le relazioni sono create su richiesta, non speculativamente. Il primo team con l'esigenza di business svolge il lavoro; i team successivi ereditano l'infrastruttura.

**Conseguenza sull'ottimizzazione:** una dichiarazione di relazione non è solo un artefatto di governance — è anche una descrizione strutturale della forma di un join. Le due tabelle, le due colonne e il tipo di join che definiscono una relazione sono esattamente ciò di cui l'ottimizzatore di query ha bisogno per pre-materializzare quel join. Le relazioni cross-source generano automaticamente tabelle di join pre-materializzate; le relazioni same-source possono aderire tramite `materialize: true`. Gli steward che pensano e approvano relazioni valide ottengono l'accelerazione delle query come sottoprodotto diretto — il lavoro di governance e il lavoro di ottimizzazione sono lo stesso atto.

### Concessioni di accesso al campo

Una concessione di accesso al campo è un permesso da dominio a dominio — il Dominio A può usare campi specifici del Dominio B nelle proprie viste.

**Ciclo di vita della concessione:**

- Sollecitata dalla creazione di una vista quando vengono identificati campi esterni necessari
- Approvata una volta dallo steward del dominio target
- Appartiene al dominio richiedente, non alla vista che l'ha sollecitata
- Qualsiasi vista successiva nel dominio richiedente può usare i campi concessi senza ulteriore coinvolgimento cross-dominio
- Campi aggiuntivi non concessi richiedono una nuova richiesta

**Notifica post-uso:** quando una vista viene creata usando campi concessi, lo steward di origine viene notificato — non gli viene chiesta l'approvazione. La notifica include il nome della vista, lo scopo di business dichiarato, i campi specifici usati e quale steward l'ha approvata. Questo offre allo steward di origine:

- **Visibilità** — consapevolezza di come vengono usati i propri dati
- **Supervisione** — motivi per sollevare una preoccupazione se l'uso appare inappropriato
- **Rimedio** — capacità di revocare la concessione, invalidando le viste dipendenti

Il compromesso: il dominio di origine approva l'accesso al campo senza conoscere ogni uso futuro. L'approvazione per singola vista è corretta in teoria e impraticabile nella realtà.

### Flusso di creazione delle query

Tre fasi, in ordine.

**Fase 1 — Shaping (esplorazione SQL, dalla pagina Relationships):**

- L'analista apre lo strumento di Shaping dalla pagina Relationships per esplorare potenziali percorsi di join in SQL grezzo
- L'SQL viene eseguito sui dati accessibili, soggetto a RLS e mascheramento delle colonne esistenti
- I JOIN nell'SQL vengono analizzati e presentati come proposte candidate di Relationship
- I candidati suggeriti dalla macchina (inferenza FK, inferenza semantica) sono mostrati insieme all'esplorazione SQL dell'analista nella stessa vista
- L'analista seleziona i candidati da promuovere a una richiesta formale di Relationship

**Fase 2 — Approvazione della relazione** (conseguente — strutturale e permanente):

- Sollevata a ogni steward distinto che possiede un asset coinvolto nella relazione
- Si tratta di un percorso di attraversamento legittimo? Il join è semanticamente valido?
- Tutti gli steward coinvolti devono approvare; la relazione diventa una voce permanente del catalogo

**Fase 3 — Creazione della query:**

- L'analista costruisce la Query in qualsiasi linguaggio supportato (SQL, GraphQL, Cypher), attraversando percorsi di relazione approvati
- Solo le relazioni approvate nel catalogo sono attraversabili — il compilatore lo applica strutturalmente
- Nessuna approvazione richiesta — la visibilità delle colonne e l'approvazione della relazione sono gli unici gate

### HITL come controllo primario

Le regole tecniche gestiscono ciò che è oggettivo — tracciamento della provenienza dei campi, enforcement dei confini di dominio, validazione del compilatore. Il giudizio contestuale rimane in capo allo steward. Vincoli come la profondità di composizione delle viste, i requisiti di scopo per query e le decisioni di approvazione delle relazioni sono questioni HITL, non regole applicate dal compilatore.

**Neutralità del dominio di origine:** lo steward del dominio di origine approva la relazione una volta e la concessione del campo una volta. Dopodiché, i domini a valle operano entro quei confini concessi:

- **Alta attenzione** al momento della decisione di attraversamento del confine
- **Consapevolezza leggera** in seguito, tramite notifiche e cronologia delle query

---

## 2. Rilevabilità

### Livelli di scoperta

La scoperta è strutturata su cinque livelli di governance crescente. Ogni livello è un prerequisito per il successivo.

| Livello | Descrizione | Stato di governance |
| --- | --- | --- |
| 1 — Schema dell'origine registrata | Ogni tabella, colonna e tipo di un'origine dati registrata. Visibilità a livello di admin. | Nessuno — inventario grezzo |
| 2 — Tabelle non rivendicate | Tabelle introspezionate da origini dati registrate senza un proprietario di dominio. Visibili agli steward con accesso all'origine. | Disponibili ma non governate |
| 3 — Asset di dominio | Tabelle rivendicate e viste definite dallo steward. Pienamente governate, di proprietà, visibili nel catalogo. | Pienamente governate |
| 4 — Relazioni | Percorsi di attraversamento approvati tra asset di Livello 3. Prerequisito per la creazione di viste cross-dominio. | Approvate da entrambi gli steward |
| 5 — Concessioni di campo | Permessi di accesso ai campi da dominio a dominio. L'accesso governato più specifico e deliberato. | Approvate dallo steward di origine |

Una tabella non rivendicata è un segnale di lacuna — se i dati necessari esistono solo al Livello 2, uno steward deve rivendicarla prima che la governance possa procedere. L'assenza di qualsiasi candidato in tutti i livelli richiede un'escalation all'admin.

### Vincoli FK

I vincoli FK sono un costrutto a livello di origine — non possono estendersi su più origini dati. I percorsi di join cross-source sono derivati interamente da relazioni approvate nel catalogo (Livello 4), che sono più solide, essendo state validate da entrambi gli steward.

All'interno di un'origine:

- I vincoli FK vengono presentati automaticamente come relazioni candidate alla registrazione dell'origine
- Rappresentano un intento di modellazione esplicito — non applicato nella maggior parte dei sistemi SQL analitici, ma dichiarato con intenzione
- La validazione dello steward è comunque richiesta prima che un candidato diventi una relazione approvata

### Gerarchia di confidenza delle relazioni

| Evidenza | Confidenza |
| --- | --- |
| Relazione approvata nel catalogo — cross-source, validata da entrambi gli steward | Massima |
| Vincolo FK intra-source — intento di modellazione esplicito, non applicato ma intenzionale | Alta |
| Inferenza semantica intra-source — somiglianza di nome/tipo di colonna all'interno di uno schema coerente | Media |
| Inferenza semantica cross-source — le convenzioni di denominazione divergono tra i sistemi; alto rischio di falsi positivi | Bassa |

I suggerimenti corroborati da più tipi di evidenza accumulano confidenza.

### Probing dei dati e correlazione

Per i candidati inferiti semanticamente, il probing dei dati fornisce un passaggio di validazione:

- **Sovrapposizione dei valori** — proporzione dei valori della colonna di origine che compaiono nella colonna target
- **Cardinalità** — se la distribuzione corrisponde al tipo di relazione atteso
- **Tasso di null** — proporzione della colonna di origine che è null, indicando opzionalità

Un'alta correlazione aumenta la confidenza; una bassa correlazione sopprime o retrocede il candidato. Il probing è evidenza corroborante, non prova — gli intervalli interi possono sovrapporsi per coincidenza e l'integrità referenziale parziale è comune nei sistemi analitici. Rimane un margine di errore significativo. Il giudizio semantico dello steward è l'unico controllo finale affidabile.

### Scoperta assistita da LLM

L'LLM opera su tutti e cinque i livelli simultaneamente, suggerendo relazioni, rivendicazioni candidate e percorsi di attraversamento classificati per confidenza.

**Cosa presenta l'LLM:**

- Relazioni candidate classificate per confidenza
- Tabelle non rivendicate che potrebbero soddisfare un'esigenza di dati, con un invito ad avviare la rivendicazione
- Assenza di qualsiasi candidato — segnale di escalation all'admin

**Progettazione della vista a partire da una descrizione di business:**

L'analista fornisce una descrizione in linguaggio naturale e vincoli opzionali. L'LLM produce una struttura di vista suggerita.

*Input:*

- Descrizione di business: entità, metriche, relazioni, intento
- Vincoli opzionali: filtri, finestre temporali, aggregazioni, campi esclusi, restrizioni di sensibilità

*Esempio:*
> "Volumi di scambio giornalieri per controparte negli ultimi 30 giorni, solo controparti attive, con ragione sociale della controparte e rating di credito. Nessun dato PII."

*Processo dell'LLM:*

1. Analisi — identifica entità, metriche, dimensioni, filtri, esclusioni
2. Ricerca — tutti i livelli del catalogo per asset corrispondenti
3. Suggerimento — asset di dominio, relazioni, campi, struttura di aggregazione
4. Punteggio — confidenza per componente basata sull'evidenza del livello
5. Prerequisiti — elenco ordinato di rivendicazioni, relazioni e concessioni di campo richieste
6. Lacune — entità o campi senza candidato in alcun livello, segnalati per escalation all'admin

*Output:*

- Bozza di query per la revisione e il perfezionamento da parte dell'analista
- Punteggi di confidenza per componente
- Elenco ordinato dei prerequisiti
- Elenco delle lacune

La descrizione di business diventa lo scopo di business dichiarato della vista una volta che la vista viene formalmente creata.

**Scoperta di relazioni SQL-first (strumento Modeling):**

Accessibile come modale dalla pagina Relationships. L'intento è costruire il modello semantico — identificando i percorsi di join strutturali prima di formalizzarli come relazioni governate.

1. L'analista scrive SQL libero sulle tabelle accessibili (RLS e mascheramento restano applicati)
2. L'AST SQL viene analizzato — ogni condizione JOIN diventa una proposta candidata di Relationship
3. L'elenco dei candidati viene mostrato insieme ai candidati suggeriti dalla macchina (inferenza FK, inferenza semantica) per una revisione unificata
4. L'analista promuove i candidati selezionati a richieste formali di Relationship
5. Le Relationship approvate vengono aggiunte al catalogo e diventano attraversabili nelle Query

Lo strumento di Modeling può mostrare tutte le tabelle registrate per l'esplorazione strutturale, anche dove l'analista non può vedere i dati sottostanti — l'approvazione dello steward governa l'accesso effettivo ai dati, non la visibilità dello schema.

---

## 3. Utilizzo

### Traccia di audit delle query

Ogni query che tocca un asset di dominio viene registrata in un `query_audit_log` di sola aggiunta. Ogni voce cattura:

- `tenant_id`, `user_id`, `role_id` — il contesto di identità
- Un hash SHA-256 della query — il testo letterale della query non viene mai memorizzato
- `table_ids` — gli asset di dominio toccati dalla query
- `source`, `status_code`, `duration_ms`
- `logged_at` — il timestamp

Il log è di sola aggiunta (DELETE e UPDATE bloccati a livello di database) e indicizzato per `(tenant_id, logged_at)` e `(user_id, logged_at)`.

Il report della cronologia query dello steward è una vista aggregata su questo log, filtrabile per asset, ruolo e finestra temporale. Il catalogo è uno strumento di governance vivo — gli steward mantengono consapevolezza di come i propri asset vengono usati mentre accade, non a posteriori.

**Due meccanismi di visibilità:**

- **Push** — notifiche post-uso per atti strutturali (è stata creata una nuova vista usando i tuoi campi)
- **Pull** — cronologia delle query per i pattern di utilizzo a runtime


