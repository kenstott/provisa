# Principi del Modello di Dominio {: #domain-model-principles }

---

## 1. Governance

### Principi fondamentali {: #core-principles }

1. **Ogni risorsa deve essere di proprietà di un dominio.** Tabelle, viste e relazioni sono tutti asset di dominio. Non esistono risorse fluttuanti non governate. Il dominio è l'unità di responsabilità.
2. **Ogni dominio deve avere uno steward.** Un dominio può esistere in uno stato in sospeso finché non viene assegnato uno steward, ma non può servire dati governati senza uno.
3. **L'admin possiede le origini dati.** Le origini dati sono infrastruttura, non risorse di dominio. L'admin registra e gestisce le connessioni ai sistemi dati esterni.
4. **Gli steward possono rivendicare tabelle per un dominio.** Una rivendicazione è esclusiva: una tabella appartiene a esattamente un dominio. Rivendicare è l'atto governato che collega l'infrastruttura al modello.
5. **Gli steward possono creare viste intra-dominio dagli asset del dominio.** Le viste esprimono logica di business, come join, aggregazioni e metriche derivate, su asset che lo steward possiede nello stesso dominio. Una vista crea un significato semantico nuovo e richiede l'approvazione dello steward.
6. **Gli analisti possono creare query tra domini a partire da relazioni approvate.** Le query sono viste interdominio scritte in qualsiasi linguaggio di query supportato. Non creano semantica nuova; percorrono cammini di relazione approvati. Non serve un'ulteriore approvazione, perché la governance agisce prima, ai livelli di relazione e di visibilità delle colonne. È il catalogo a imporre: il compilatore rifiuta qualsiasi attraversamento che non sia nel catalogo delle relazioni approvate.
7. **Chiunque può richiedere l'accesso a una risorsa di dominio.** L'accesso si concede per risorsa, mai per query: se avete accesso a una risorsa, potete interrogarla. La governance viene applicata al momento dell'esecuzione, nella pipeline.

### Risorse: tabelle e viste come pari {: #resources-tables-and-views-as-peers }

Una tabella e una vista differiscono solo per l'origine: una tabella viene rivendicata da un'origine, una vista viene definita da uno steward. Appena una delle due è un asset di dominio, il modello di governance le tratta allo stesso modo:

- Entrambe sono asset di dominio a pieno titolo, visibili nel catalogo
- Entrambe possono essere il target di una relazione
- Entrambe possono essere concesse secondo il Principio 6
- Entrambe sono soggette alla stessa pipeline di governance

Uno steward può rivendicare tabelle privatamente ed esporre solo viste curate come prodotti dati rivolti al pubblico.

### Composizione delle viste {: #view-composition }

Una vista appartiene sempre a un solo dominio. Esiste un solo tipo di vista, sempre intra-dominio. Una vista esiste per uno di due scopi:

- **Importazione cross-dominio** — l'origine è esterna al dominio. I dati cross-dominio possono entrare in un dominio solo tramite una vista, che funge da adattatore in sola lettura che denomina i dati esterni come un concetto di business del dominio.
- **Derivazione locale** — l'origine è nello stesso dominio. La vista deriva dati nuovi o calcolati da asset di dominio esistenti. I dati nuovi o derivati possono esistere solo come vista.

Una vista può fare riferimento a:

- Tabelle rivendicate all'interno dello stesso dominio
- Campi importati da un altro dominio tramite una concessione di accesso al campo
- Un'altra vista all'interno dello stesso dominio, dove la variazione è mirata: restrizione di campi, aggregazione o arricchimento tramite un join aggiuntivo

Niente di tecnico limita la profondità di composizione delle viste. Il controllo di qualità è il giudizio dello steward durante la revisione HITL.

Ogni vista porta uno scopo di business dichiarato, indicato al momento della creazione:

- Fa parte dell'artefatto governato, così gli steward approvano sapendo a cosa serve la vista
- Viene referenziato dalle richieste di accesso secondo il Principio 7, così lo steward può valutarne l'idoneità
- Accompagna la vista dalla sua creazione lungo l'intero flusso di governance

### Query {: #queries }

Una query percorre cammini di relazione approvati sugli asset di dominio. A differenza di una vista, non crea nuovo significato semantico; segue la struttura approvata del modello. Una query si può scrivere in qualsiasi linguaggio di query supportato (SQL, GraphQL, Cypher).

**Imposizione strutturale:** È il catalogo delle relazioni a imporre. Il compilatore verifica ogni attraversamento rispetto alle voci approvate del catalogo e rifiuta una query che nomina un cammino non approvato. La governance è nella struttura, quindi non c'è un controllo separato a runtime.

**Nessuna approvazione:** La governance agisce prima, ai livelli di relazione e di visibilità delle colonne. Se un utente può vedere le colonne e il cammino è approvato, la query è un uso valido, senza altri cancelli.

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

Il compilatore ricava l'appartenenza al dominio da queste posizioni strutturali, quindi non servono annotazioni né suggerimenti.

### Relazioni {: #relationships }

Una relazione è un cammino di attraversamento approvato tra due asset. I confini di dominio non cambiano cosa sia una relazione; decidono solo chi la approva.

**Approvazione:**

- Una richiesta di relazione la decidono i domini che tocca: il dominio della sua tabella di origine e il dominio della sua tabella di destinazione
- Un'approvazione conta solo se viene da un utente il cui diritto di creare relazioni raggiunge uno di quei domini; qualsiasi utente di questo tipo può rifiutarla
- La richiesta viene eseguita quando due utenti diversi, nessuno dei quali il richiedente, l'hanno approvata e ogni dominio coinvolto ha un approvatore: una relazione tra due domini ha bisogno di un sì da ciascun lato, e una relazione all'interno di un dominio ha bisogno di due approvatori di quel dominio
- Approvare una relazione costruisce il grafo delle dipendenze di ciascuno steward, così gli steward vengono a sapere delle modifiche allo schema prima che entrino in vigore

Le relazioni si creano quando qualcuno ne ha bisogno, mai per ipotesi. Il primo team con l'esigenza di business fa il lavoro, e i team successivi lo ereditano.

**Conseguenza per l'ottimizzazione:** Una dichiarazione di relazione è un artefatto di governance e anche una descrizione strutturale della forma di un join. Le due tabelle, le due colonne e il tipo di join che definiscono una relazione sono esattamente ciò che serve all'ottimizzatore per prematerializzare quel join. Le relazioni tra origini diverse ottengono automaticamente tabelle di join prematerializzate, e quelle nella stessa origine possono attivarle con `materialize: true`. Gli steward che ragionano su relazioni valide e le approvano ottengono query più veloci come conseguenza diretta: il lavoro di governance e quello di ottimizzazione sono lo stesso atto.

### Concessioni di accesso al campo {: #field-access-grants }

Una concessione di accesso ai campi è un permesso da un dominio a un altro: il dominio A può usare campi specifici del dominio B nelle sue viste.

**Ciclo di vita della concessione:**

- Sollecitata dalla creazione di una vista quando vengono identificati campi esterni necessari
- Approvata una volta dallo steward del dominio target
- Appartiene al dominio richiedente, non alla vista che l'ha sollecitata
- Qualsiasi vista successiva nel dominio richiedente può usare i campi concessi senza ulteriore coinvolgimento cross-dominio
- Campi aggiuntivi non concessi richiedono una nuova richiesta

**Notifica dopo l'uso:** Quando si crea una vista con campi concessi, lo steward di origine viene informato, senza che gli si chieda di approvare. La notifica indica il nome della vista, lo scopo di business dichiarato, i campi usati e lo steward che l'ha approvata. Questo dà allo steward di origine:

- **Visibilità** — consapevolezza di come vengono usati i propri dati
- **Supervisione** — motivi per sollevare una preoccupazione se l'uso appare inappropriato
- **Rimedio** — capacità di revocare la concessione, invalidando le viste dipendenti

Il compromesso: il dominio di origine approva l'accesso al campo senza conoscere ogni uso futuro. L'approvazione per singola vista è corretta in teoria e impraticabile nella realtà.

### Flusso di creazione delle query {: #query-creation-workflow }

Tre fasi, in ordine.

**Fase 1 — Shaping (esplorazione SQL, dalla pagina Relationships):**

- L'analista apre lo strumento di Shaping dalla pagina Relationships per provare percorsi di join in SQL grezzo
- L'SQL viene eseguito sui dati accessibili, soggetto a RLS e mascheramento delle colonne esistenti
- I JOIN nell'SQL vengono analizzati e presentati come proposte candidate di Relationship
- I candidati suggeriti dalla macchina (inferenza FK, inferenza semantica) sono mostrati insieme all'SQL dell'analista nella stessa vista
- L'analista seleziona i candidati da promuovere a una richiesta formale di Relationship

**Fase 2 — Approvazione della relazione** (conseguente — strutturale e permanente):

- Decisa dai domini che la relazione tocca, come descritto in [Relazioni](#relationships)
- È un cammino di attraversamento legittimo? Il join è semanticamente valido?
- Devono approvare due utenti diversi, nessuno dei quali sia il richiedente, e ogni dominio coinvolto deve avere un approvatore; dopo, la relazione diventa una voce permanente del catalogo

**Fase 3 — Creazione della query:**

- L'analista costruisce la Query in qualsiasi linguaggio supportato (SQL, GraphQL, Cypher), attraversando percorsi di relazione approvati
- Solo le relazioni approvate nel catalogo sono attraversabili, e il compilatore lo applica strutturalmente
- Nessuna approvazione richiesta: la visibilità delle colonne e l'approvazione della relazione sono gli unici gate

### HITL come controllo primario {: #hitl-as-the-primary-control }

Le regole tecniche si occupano di ciò che è oggettivo: il tracciamento della provenienza dei campi, l'imposizione dei confini di dominio e la validazione del compilatore. Il giudizio che dipende dal contesto resta allo steward. Limiti come la profondità di composizione delle viste, uno scopo dichiarato per ogni query e le decisioni di approvazione delle relazioni sono questioni HITL, che il compilatore non impone.

**Neutralità del dominio di origine:** lo steward del dominio di origine approva la relazione una volta e la concessione del campo una volta. Dopodiché, i domini a valle operano entro quei confini concessi:

- **Alta attenzione** al momento della decisione di attraversamento del confine
- **Consapevolezza leggera** in seguito, tramite notifiche e cronologia delle query

---

## 2. Rilevabilità {: #2-discoverability }

### Livelli di scoperta {: #discovery-tiers }

La scoperta è strutturata su cinque livelli di governance crescente. Ogni livello è un prerequisito per il successivo.

| Livello | Descrizione | Stato di governance |
| --- | --- | --- |
| 1 — Schema dell'origine registrata | Ogni tabella, colonna e tipo di un'origine dati registrata. Visibilità a livello di admin. | Nessuno — inventario grezzo |
| 2 — Tabelle non rivendicate | Tabelle introspezionate da origini dati registrate senza un proprietario di dominio. Visibili agli steward con accesso all'origine. | Disponibili ma non governate |
| 3 — Asset di dominio | Tabelle rivendicate e viste definite dallo steward. Pienamente governate, di proprietà, visibili nel catalogo. | Pienamente governate |
| 4 — Relazioni | Percorsi di attraversamento approvati tra asset di Livello 3. Prerequisito per la creazione di viste cross-dominio. | Approvate da entrambi gli steward |
| 5 — Concessioni di campo | Permessi di accesso ai campi da dominio a dominio. L'accesso governato più specifico e deliberato. | Approvate dallo steward di origine |

Una tabella non rivendicata segnala una lacuna. Se i dati che vi servono esistono solo al livello 2, uno steward deve rivendicarli prima che la governance possa procedere. Quando nessun livello ha candidati, rivolgetevi a un amministratore.

### Vincoli FK {: #fk-constraints }

I vincoli FK appartengono a una sola origine, quindi non possono attraversare più origini. I cammini di join tra origini derivano interamente dalle relazioni approvate del catalogo (livello 4), una prova più solida perché entrambi gli steward le hanno convalidate.

All'interno di un'origine:

- I vincoli FK vengono presentati automaticamente come relazioni candidate alla registrazione dell'origine
- Rappresentano un intento di modellazione esplicito: non applicato nella maggior parte dei sistemi SQL analitici, ma dichiarato con intenzione
- La validazione dello steward è comunque richiesta prima che un candidato diventi una relazione approvata

### Gerarchia di confidenza delle relazioni {: #relationship-confidence-hierarchy }

| Evidenza | Confidenza |
| --- | --- |
| Relazione approvata nel catalogo — cross-source, validata da entrambi gli steward | Massima |
| Vincolo FK intra-source — intento di modellazione esplicito, non applicato ma intenzionale | Alta |
| Inferenza semantica intra-source — somiglianza di nome/tipo di colonna all'interno di uno schema coerente | Media |
| Inferenza semantica cross-source — le convenzioni di denominazione divergono tra i sistemi; alto rischio di falsi positivi | Bassa |

I suggerimenti corroborati da più tipi di evidenza accumulano confidenza.

### Probing dei dati e correlazione {: #data-probing-and-correlation }

Per i candidati inferiti semanticamente, il probing dei dati fornisce un passaggio di validazione:

- **Sovrapposizione dei valori** — proporzione dei valori della colonna di origine che compaiono nella colonna target
- **Cardinalità** — se la distribuzione corrisponde al tipo di relazione atteso
- **Tasso di null** — proporzione della colonna di origine che è null, indicando opzionalità

Una correlazione alta aumenta la fiducia, una bassa sopprime o declassa il candidato. Il sondaggio corrobora, non dimostra. Gli intervalli di interi possono sovrapporsi per caso e l'integrità referenziale parziale è comune nei sistemi analitici, quindi il margine d'errore resta ampio. Il giudizio semantico dello steward è l'unico controllo finale affidabile.

### Scoperta assistita da LLM {: #llm-assisted-discovery }

L'LLM opera su tutti e cinque i livelli simultaneamente, suggerendo relazioni, rivendicazioni candidate e percorsi di attraversamento classificati per confidenza.

**Cosa presenta l'LLM:**

- Relazioni candidate classificate per confidenza
- Tabelle non rivendicate che potrebbero soddisfare un'esigenza di dati, con un invito ad avviare la rivendicazione
- L'assenza di qualsiasi candidato è il segnale di escalation all'admin

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

Si apre come finestra modale dalla pagina Relazioni. Serve a costruire il modello semantico: trovare cammini di join strutturali prima di formalizzarli come relazioni governate.

1. L'analista scrive SQL libero sulle tabelle accessibili (RLS e mascheramento restano applicati)
2. L'AST SQL viene analizzato, e ogni condizione JOIN diventa una proposta candidata di Relationship
3. L'elenco dei candidati viene mostrato insieme ai candidati suggeriti dalla macchina (inferenza FK, inferenza semantica) per una revisione unificata
4. L'analista promuove i candidati selezionati a richieste formali di Relationship
5. Le Relationship approvate vengono aggiunte al catalogo e diventano attraversabili nelle Query

Mentre definite la struttura, lo strumento di modellazione può mostrare ogni tabella registrata, anche quelle di cui l'analista non vede i dati. La visibilità dello schema è un'altra questione; è l'approvazione dello steward a governare l'accesso ai dati stessi.

---

## 3. Utilizzo {: #3-usage }

### Traccia di audit delle query {: #query-audit-trail }

Ogni query che tocca un asset di dominio viene registrata in un `query_audit_log` di sola aggiunta. Ogni voce cattura:

- `tenant_id`, `user_id`, `role_id` — il contesto di identità
- Un hash SHA-256 della query; il testo letterale della query non viene mai memorizzato
- `table_ids` — gli asset di dominio toccati dalla query
- `source`, `status_code`, `duration_ms`
- `logged_at` — il timestamp

Il log è di sola aggiunta (DELETE e UPDATE bloccati a livello di database) e indicizzato per `(tenant_id, logged_at)` e `(user_id, logged_at)`.

Il report della cronologia delle query dello steward è una vista aggregata su questo log, filtrabile per asset, ruolo e finestra temporale. Così il catalogo diventa uno strumento di governance vivo: gli steward vedono come vengono usati i loro asset mentre accade, invece di scoprirlo dopo.

**Due meccanismi di visibilità:**

- **Push** — notifiche post-uso per atti strutturali (è stata creata una nuova vista usando i tuoi campi)
- **Pull** — cronologia delle query per i pattern di utilizzo a runtime
