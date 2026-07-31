# Principi del modello di dominio

---

## 1. Governance

### Principi fondamentali

1. **Ogni risorsa deve appartenere a un dominio.** Tabelle, viste e relazioni sono tutte asset di dominio. Non esistono risorse fluttuanti non governate. Il dominio è l'unità di responsabilità.
2. **Ogni dominio deve avere un data steward.** Un dominio può esistere in stato di attesa finché non viene assegnato un data steward, ma non può servire dati governati senza di esso.
3. **L'amministratore possiede le origini.** Le origini sono infrastruttura, non risorse di dominio. L'amministratore registra e gestisce le connessioni ai sistemi di dati esterni.
4. **I data steward possono rivendicare tabelle per un dominio.** La rivendicazione è esclusiva: una tabella appartiene esattamente a un dominio. Questo è l'atto governato che collega l'infrastruttura al livello semantico.
5. **I data steward possono creare viste intra-dominio a partire dagli asset di dominio.** Le viste esprimono logica di business — join, aggregazioni, metriche derivate — su asset che il data steward possiede all'interno dello stesso dominio. Le viste creano nuovo significato semantico e richiedono l'approvazione del data steward.
6. **Gli analisti possono creare query cross-dominio a partire da relazioni approvate.** Le query sono viste inter-dominio espresse in qualsiasi linguaggio di query supportato. Non creano nuova semantica: attraversano percorsi di relazione approvati. Non è richiesta alcuna approvazione aggiuntiva: la governance viene gestita a monte, ai livelli di Relazione e visibilità delle colonne. Il catalogo è il meccanismo di applicazione: il compilatore rifiuta gli attraversamenti non presenti nel catalogo delle relazioni approvate.
7. **Chiunque può richiedere l'accesso a una risorsa di dominio.** L'accesso viene concesso a livello di risorsa, non a livello di query. Se si ha accesso a una risorsa, la si può interrogare. La governance viene applicata in fase di esecuzione tramite la pipeline.

### Risorse: tabelle e viste come pari

La distinzione tra una tabella e una vista è solo di origine — una tabella viene rivendicata da un'origine, una vista viene definita da un data steward. Una volta che una delle due esiste come asset di dominio, il modello di governance le tratta in modo identico:

- Entrambe sono asset di dominio di prima classe, visibili nel catalogo
- Entrambe possono essere target di una relazione
- Entrambe possono essere concesse ai sensi del Principio 6
- Entrambe sono soggette alla stessa pipeline di governance

Un data steward può rivendicare tabelle privatamente ed esporre solo viste curate come prodotti dati destinati al pubblico.

### Composizione delle viste

Una vista appartiene sempre a un unico dominio — esiste un solo tipo di vista, sempre intra-dominio. Una vista esiste per uno dei due scopi seguenti:

- **Importazione cross-dominio** — l'origine è esterna al dominio. I dati cross-dominio possono entrare in un dominio solo tramite una vista, che funge da adattatore in sola lettura che denomina i dati esterni come un concetto di business del dominio.
- **Derivazione locale** — l'origine appartiene allo stesso dominio. La vista deriva dati nuovi o calcolati a partire da asset di dominio esistenti. I dati nuovi o derivati possono esistere solo come vista.

Una vista può fare riferimento a:
- Tabelle rivendicate all'interno dello stesso dominio
- Campi importati da un altro dominio nell'ambito di una concessione di accesso ai campi
- Un'altra vista all'interno dello stesso dominio, laddove la variazione abbia uno scopo preciso: restrizione dei campi, aggregazione o arricchimento tramite un join aggiuntivo

La profondità di composizione non è applicata tecnicamente — il giudizio del data steward durante la revisione HITL è il meccanismo di controllo qualità.

Ogni vista porta uno scopo di business dichiarato, indicato al momento della creazione:
- Fa parte dell'artefatto governato — i data steward approvano sapendo a cosa serve la vista
- Viene referenziato nelle richieste di accesso ai sensi del Principio 7, così che il data steward possa valutarne l'idoneità
- Accompagna la vista dalla sua creazione lungo l'intero flusso di governance

### Query

Una query attraversa percorsi di relazione approvati sugli asset di dominio. A differenza delle viste, le query non creano nuovo significato semantico — attraversano la struttura approvata del modello. Le query possono essere espresse in qualsiasi linguaggio di query supportato (SQL, GraphQL, Cypher).

**Applicazione strutturale:** il catalogo delle relazioni è il meccanismo di applicazione. Il compilatore convalida ogni attraversamento rispetto alle voci approvate del catalogo e rifiuta le query che fanno riferimento a percorsi non approvati. La governance è strutturale, non un controllo a runtime.

**Nessuna approvazione richiesta:** la governance avviene a monte — ai livelli di Relazione e visibilità delle colonne. Se un utente ha accesso alle colonne e il percorso di attraversamento è approvato, la query rappresenta un uso valido. Nessun controllo aggiuntivo.

**Differenza rispetto alle viste:**
- Viste: intra-dominio, introducono nuovo significato semantico, curate dal data steward
- Query: attraversano relazioni approvate, nessuna nuova semantica, nessun controllo di approvazione

**Espressione del dominio per linguaggio di query:**

Ciascun linguaggio supportato esprime il dominio come spazio dei nomi strutturale nativo di quel linguaggio:

| Linguaggio | Espressione del dominio | Esempio |
|---|---|---|
| GraphQL | Prefisso del nome di tipo e campo | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Nome dello schema | `SELECT * FROM sales.orders` |
| Cypher | Etichetta di nodo aggiuntiva (il dominio è richiesto solo quando il nome di tipo è ambiguo) | `MATCH (o:Sales:Order)` |

Il compilatore risolve l'appartenenza al dominio a partire da queste posizioni strutturali — non è richiesta alcuna annotazione o indicazione.

### Relazioni

Una relazione è un percorso di attraversamento approvato tra due asset. I confini di dominio sono irrilevanti rispetto a cosa sia una relazione — determinano solo chi la approva.

**Approvazione:**
- L'approvazione è richiesta da ogni data steward distinto che possiede un asset coinvolto nella relazione
- Se un data steward possiede entrambi gli asset, è richiesta una sola approvazione. Se sono coinvolti due data steward, sono richieste due approvazioni
- Non esiste una classificazione intra-dominio/cross-dominio: la proprietà determina naturalmente l'onere di approvazione
- L'approvazione di una relazione costruisce il grafo delle dipendenze di ciascun data steward, abilitando notifiche proattive sull'evoluzione dello schema

Le relazioni vengono create su richiesta, non in via speculativa. Il primo team con l'esigenza di business svolge il lavoro; i team successivi ereditano l'infrastruttura.

**Conseguenza sull'ottimizzazione:** una dichiarazione di relazione non è solo un artefatto di governance — è anche una descrizione strutturale della forma di un join. Le due tabelle, le due colonne e il tipo di join che definiscono una relazione sono esattamente ciò di cui l'ottimizzatore di query ha bisogno per pre-materializzare quel join. Le relazioni cross-origine generano automaticamente tabelle di join pre-materializzate; le relazioni sulla stessa origine possono aderire tramite `materialize: true`. I data steward che riflettono e approvano relazioni valide ottengono l'accelerazione delle query come sottoprodotto diretto — il lavoro di governance e il lavoro di ottimizzazione sono lo stesso atto.

### Concessioni di accesso ai campi

Una concessione di accesso ai campi è un'autorizzazione dominio-a-dominio — il Dominio A può utilizzare campi specifici del Dominio B nelle proprie viste.

**Ciclo di vita della concessione:**
- Viene avviata dalla creazione di una vista quando vengono identificati campi esterni necessari
- Viene approvata una volta dal data steward del dominio destinazione
- Appartiene al dominio richiedente, non alla vista che l'ha originata
- Qualsiasi vista successiva nel dominio richiedente può utilizzare i campi concessi senza ulteriore coinvolgimento cross-dominio
- I campi aggiuntivi non concessi richiedono una nuova richiesta

**Notifica successiva all'uso:** quando viene creata una vista utilizzando campi concessi, il data steward di origine viene notificato — non gli viene chiesto di approvare. La notifica include il nome della vista, lo scopo di business dichiarato, i campi specifici utilizzati e quale data steward l'ha approvata. Questo offre al data steward di origine:
- **Visibilità** — consapevolezza di come vengono utilizzati i propri dati
- **Supervisione** — basi per sollevare una preoccupazione se l'uso appare inappropriato
- **Rimedio** — capacità di revocare la concessione, invalidando le viste dipendenti

Il compromesso: il dominio di origine approva l'accesso ai campi senza conoscere ogni uso futuro. L'approvazione per singola vista è corretta in teoria e impraticabile nella pratica.

### Flusso di creazione delle query

Tre fasi, in ordine.

**Fase 1 — Shaping (esplorazione SQL, dalla pagina Relazioni):**
- L'analista apre lo strumento di shaping dalla pagina Relazioni per esplorare potenziali percorsi di join in SQL grezzo
- L'SQL viene eseguito sui dati accessibili, soggetto alla RLS e al mascheramento delle colonne esistenti
- I JOIN nell'SQL vengono analizzati e presentati come proposte di Relazione candidate
- I candidati suggeriti automaticamente (inferenza di chiave esterna, inferenza semantica) vengono mostrati insieme all'esplorazione SQL dell'analista nella stessa vista
- L'analista seleziona i candidati da promuovere a una richiesta formale di Relazione

**Fase 2 — Approvazione della relazione** (rilevante — strutturale e permanente):
- Sollevata a ogni data steward distinto che possiede un asset coinvolto nella relazione
- Si tratta di un percorso di attraversamento legittimo? Il join è semanticamente valido?
- Tutti i data steward coinvolti devono approvare; la relazione diventa una voce permanente del catalogo

**Fase 3 — Creazione della query:**
- L'analista costruisce la query in qualsiasi linguaggio supportato (SQL, GraphQL, Cypher), attraversando percorsi di relazione approvati
- Sono attraversabili solo le relazioni approvate del catalogo — il compilatore lo applica in modo strutturale
- Nessuna approvazione richiesta — la visibilità delle colonne e l'approvazione della relazione sono gli unici controlli

### HITL come controllo primario

Le regole tecniche gestiscono ciò che è oggettivo — il tracciamento della provenienza dei campi, l'applicazione dei confini di dominio, la convalida del compilatore. Il giudizio contestuale resta al data steward. Vincoli come la profondità di composizione delle viste, i requisiti di scopo per query e le decisioni di approvazione delle relazioni sono questioni HITL, non regole applicate dal compilatore.

**Neutralità del dominio di origine:** il data steward del dominio di origine approva la relazione una volta e la concessione dei campi una volta. Successivamente, i domini a valle operano entro tali confini concessi:
- **Alta considerazione** al momento della decisione di attraversamento del confine
- **Consapevolezza leggera** in seguito, tramite notifiche e cronologia delle query

---

## 2. Individuabilità

### Livelli di scoperta

La scoperta è strutturata su cinque livelli di governance crescente. Ogni livello è un prerequisito per il successivo.

| Livello | Descrizione | Stato di governance |
|---|---|---|
| 1 — Schema dell'origine registrata | Ogni tabella, colonna e tipo di un'origine registrata. Visibilità a livello amministratore. | Nessuno — inventario grezzo |
| 2 — Tabelle non rivendicate | Tabelle introspezionate da origini registrate senza proprietario di dominio. Visibili ai data steward con accesso all'origine. | Disponibile ma non governato |
| 3 — Asset di dominio | Tabelle rivendicate e viste definite dal data steward. Completamente governate, di proprietà, visibili nel catalogo. | Completamente governato |
| 4 — Relazioni | Percorsi di attraversamento approvati tra asset di Livello 3. Prerequisito per la creazione di viste cross-dominio. | Approvato da entrambi i data steward |
| 5 — Concessioni di campi | Autorizzazioni di accesso ai campi dominio-a-dominio. L'accesso governato più specifico e deliberato. | Approvato dal data steward di origine |

Una tabella non rivendicata è un segnale di lacuna — se i dati necessari esistono solo al Livello 2, un data steward deve rivendicarla prima che la governance possa procedere. L'assenza di qualsiasi candidato in tutti i livelli richiede l'escalation all'amministratore.

### Vincoli di chiave esterna

I vincoli di chiave esterna sono una costruzione a livello di origine — non possono estendersi su più origini dati. I percorsi di join cross-origine sono derivati interamente da relazioni di catalogo approvate (Livello 4), che sono più solide, essendo state convalidate da entrambi i data steward.

All'interno di un'origine:
- I vincoli di chiave esterna vengono presentati automaticamente come relazioni candidate alla registrazione dell'origine
- Rappresentano un'intenzione di modellazione esplicita — non applicata nella maggior parte dei sistemi SQL analitici, ma dichiarata deliberatamente
- È comunque richiesta la convalida del data steward prima che un candidato diventi una relazione approvata

### Gerarchia di affidabilità delle relazioni

| Evidenza | Affidabilità |
|---|---|
| Relazione di catalogo approvata — cross-origine, convalidata da entrambi i data steward | Massima |
| Vincolo di chiave esterna intra-origine — intenzione di modellazione esplicita, non applicata ma deliberata | Alta |
| Inferenza semantica intra-origine — somiglianza di nome/tipo di colonna all'interno di uno schema coerente | Media |
| Inferenza semantica cross-origine — le convenzioni di denominazione divergono tra i sistemi; alto rischio di falsi positivi | Bassa |

I suggerimenti corroborati da più tipi di evidenza accumulano affidabilità.

### Analisi e correlazione dei dati

Per i candidati inferiti semanticamente, l'analisi dei dati fornisce un passaggio di convalida:
- **Sovrapposizione dei valori** — proporzione dei valori della colonna di origine che compaiono nella colonna di destinazione
- **Cardinalità** — se la distribuzione corrisponde al tipo di relazione atteso
- **Tasso di valori nulli** — proporzione della colonna di origine che è nulla, indicando opzionalità

Un'elevata correlazione aumenta l'affidabilità; una bassa correlazione sopprime o declassa il candidato. L'analisi è evidenza a supporto, non prova — gli intervalli di interi possono sovrapporsi per coincidenza e l'integrità referenziale parziale è comune nei sistemi analitici. Rimane un margine di errore significativo. Il giudizio semantico del data steward è l'unica verifica finale affidabile.

### Scoperta assistita da LLM

Il LLM opera su tutti e cinque i livelli simultaneamente, suggerendo relazioni, rivendicazioni candidate e percorsi di attraversamento classificati per affidabilità.

**Cosa presenta il LLM:**
- Relazioni candidate classificate per affidabilità
- Tabelle non rivendicate che potrebbero soddisfare un'esigenza di dati, con un suggerimento per avviare la rivendicazione
- Assenza di qualsiasi candidato — segnale di escalation all'amministratore

**Progettazione di viste a partire da una descrizione di business:**

L'analista fornisce una descrizione in linguaggio naturale e vincoli opzionali. Il LLM produce una struttura di vista suggerita.

*Input:*
- Descrizione di business: entità, metriche, relazioni, intento
- Vincoli opzionali: filtri, finestre temporali, aggregazioni, campi esclusi, restrizioni di sensibilità

*Esempio:*
> "Volumi di scambio giornalieri per controparte negli ultimi 30 giorni, solo controparti attive, con ragione sociale della controparte e rating di credito. Nessun dato personale."

*Processo del LLM:*
1. Analisi — identificare entità, metriche, dimensioni, filtri, esclusioni
2. Ricerca — in tutti i livelli del catalogo, gli asset corrispondenti
3. Suggerimento — asset di dominio, relazioni, campi, struttura di aggregazione
4. Punteggio — affidabilità per componente in base all'evidenza di livello
5. Prerequisiti — elenco ordinato di rivendicazioni, relazioni e concessioni di campi richieste
6. Lacune — entità o campi senza candidato a nessun livello, segnalati per l'escalation all'amministratore

*Output:*
- Bozza di query per la revisione e il perfezionamento da parte dell'analista
- Punteggi di affidabilità per componente
- Elenco ordinato dei prerequisiti
- Elenco delle lacune

La descrizione di business diventa lo scopo di business dichiarato della vista una volta che questa viene formalmente creata.

**Scoperta delle relazioni SQL-first (strumento di Modeling):**

Accessibile come finestra modale dalla pagina Relazioni. L'intento è costruire il modello semantico — identificando i percorsi di join strutturali prima di formalizzarli come relazioni governate.

1. L'analista scrive SQL libero contro le tabelle accessibili (RLS e mascheramento ancora applicati)
2. L'AST dell'SQL viene analizzato — ogni condizione JOIN diventa una proposta di Relazione candidata
3. L'elenco dei candidati viene mostrato insieme ai candidati suggeriti automaticamente (inferenza di chiave esterna, inferenza semantica) per una revisione unificata
4. L'analista promuove i candidati selezionati a richieste formali di Relazione
5. Le relazioni approvate vengono aggiunte al catalogo e diventano attraversabili nelle query

Lo strumento di Modeling può mostrare tutte le tabelle registrate per l'esplorazione strutturale, anche laddove l'analista non possa vedere i dati sottostanti — l'approvazione del data steward governa l'accesso effettivo ai dati, non la visibilità dello schema.

---

## 3. Utilizzo

### Audit trail delle query

Ogni query che tocca un asset di dominio viene registrata in un `query_audit_log` di sola aggiunta. Ogni voce cattura:

- `tenant_id`, `user_id`, `role_id` — il contesto di identità
- Un hash SHA-256 della query — il testo letterale della query non viene mai memorizzato
- `table_ids` — gli asset di dominio toccati dalla query
- `source`, `status_code`, `duration_ms`
- `logged_at` — il timestamp

Il log è di sola aggiunta (DELETE e UPDATE sono bloccati a livello di database) e indicizzato per `(tenant_id, logged_at)` e `(user_id, logged_at)`.

Il report della cronologia delle query del data steward è una vista aggregata su questo log, filtrabile per asset, ruolo e finestra temporale. Il catalogo è uno strumento di governance dal vivo — i data steward mantengono consapevolezza di come i propri asset vengono utilizzati in tempo reale, non a posteriori.

**Due meccanismi di visibilità:**
- **Push** — notifiche successive all'uso per atti strutturali (è stata creata una nuova vista utilizzando i tuoi campi)
- **Pull** — cronologia delle query per i pattern di utilizzo a runtime
