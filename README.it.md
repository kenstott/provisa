# Provisa

**Collegate i vostri database. Interrogateli in 5 minuti con GraphQL, gRPC, SQL o MCP, su qualsiasi API o protocollo.**

Provisa mette una sola API sopra tutti i vostri dati. Puntatelo sui vostri database e sulle applicazioni aziendali, descrivete come si collegano, e interrogate il risultato unito via REST, GraphQL, SQL, gRPC, MCP, JSON:API, Arrow Flight, pgwire o Bolt.

Funziona perché tutto viene da un unico modello: i vostri domini, le relazioni tra le tabelle e le politiche su di esse, in tutte le sorgenti. Provisa gira a partire da quel modello e governa a partire da esso, quindi non c'è nulla da tenere sincronizzato. Le relazioni registrate sono gli unici join che una query può fare, e le politiche di accesso sono compilate in ogni piano di query. Dall'unico modello derivano quattro cose:

- **Federazione.** 73 connettori raggiungono più di 85 tipi di sorgente, tra cui Salesforce, ServiceNow e Stripe. I sistemi SaaS su cui gira la vostra attività si leggono come tabelle accanto ai database, dove si trovano, senza prima una pipeline verso un data warehouse. Interrogateli in SQL, GraphQL o Cypher, su nove protocolli.
- **Metriche.** Una metrica è un aggregato con un nome e governato. La si definisce una volta, e ogni strumento e linguaggio che la chiede riceve la stessa definizione.
- **Ontologia.** Il glossario aziendale è un grafo di concetti, delle loro definizioni e di come si collegano. Provisa lo associa alle tabelle e alle colonne che contengono i valori, ed è questo che gli permette di rispondere a domande sui vostri dati. Un concetto resta una bozza finché non raggiunge i dati, direttamente o attraverso altri concetti. Il glossario dice che cos'è un concetto; una metrica dice come si calcola un numero.
- **Governance.** Sei livelli di controlli sono compilati in ogni piano di query, in ogni linguaggio e su ogni protocollo, e ogni richiesta viene registrata allo stesso modo: chi ha interrogato cosa, con quale ruolo, sotto quale politica. Tracce, metriche e log sono registrati come tabelle interrogabili accanto ai vostri dati aziendali.

Si può interrogare in **GraphQL, Cypher o SQL** e connettersi tramite **pgwire, Bolt, gRPC, REST, Arrow Flight o JDBC**. Ogni linguaggio di query viene ridotto a un'unica rappresentazione intermedia, e lì si applica la governance, una sola volta, così una politica non può divergere tra linguaggi. In uscita quella forma viene tradotta nel dialetto di ogni sorgente. Un nuovo linguaggio di query è un nuovo front end sullo stesso nucleo.

Letture e scritture ci passano entrambe. Una query su più sorgenti si distribuisce tramite il livello di federazione. Le scritture, e le letture da una sola sorgente, vanno dirette al driver di quella sorgente, governate allo stesso modo e di solito di ritorno in meno di 100 ms. Lo streaming colonnare di Arrow Flight è integrato.

Il modello ha un vocabolario ridotto: domini, relazioni, ruoli e policy. Questo lo rende facile da leggere e da verificare, e potete leggere l'insieme delle policy e sapere cosa fa. Provisa in sé è un compilatore di query leggero. Non sta mai nel vostro percorso dei dati come runtime. Trasforma una richiesta in query native, le instrada e si fa da parte, ed è per questo che il patrimonio di dati resta veloce.

Lo si può usare in due modi, e si combinano:

- **Per modernizzare.** Modellate il vostro patrimonio, lasciate che Provisa generi l'SQL nativo per ogni sorgente, poi prendete quell'SQL e adottatelo nel sistema di destinazione. Provisa fa da ponte per la transizione, e dopo se ne può fare a meno.
- **Per governare in modo stabile.** Tenetelo come il percorso di ogni query, così definizione, applicazione e audit restano insieme finché esiste il patrimonio.

## Provare una demo {: #try-a-demo }

Il modo più rapido è una demo dal vivo nel browser, senza installare nulla: [provisa.dev/demos](https://provisa.dev/demos). Ognuna apre una sandbox privata con un tour guidato.

Da una copia di questo repository puoi eseguire qualsiasi demo sulla tua macchina:

```bash
./start-ui-install.sh --list-demos     # the demos you can run, and which are published
./start-ui-install.sh --demo weblogs   # start one, with its tour
```

Una demo pubblicata viene scaricata come bundle pronto; le altre vengono eseguite da `demo/named/<name>/`. Vedi [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) per tutte le opzioni.

## Il modello di federazione {: #the-federation-model }

Quattro idee reggono tutto il progetto. Ogni sorgente si riduce a tabelle. Ogni query si riduce a una forma simile a SQL. La raggiungibilità decide cosa si interroga in diretta e cosa si copia prima. E una strategia di aggiornamento tiene ogni copia, e ogni dataset derivato, aggiornata quanto serve.

Si parte dalle sorgenti. Ognuna si esprime come un insieme di tabelle bidimensionali su un unico sistema di tipi condiviso, ed è l'unico contratto che una sorgente deve rispettare per entrare nel patrimonio. Una tabella MySQL o PostgreSQL lo è già. Il risultato di una query GraphQL lo diventa una volta appiattito. Triplestore SPARQL e Neo4j non somigliano per nulla a tabelle, ma voi fornite una query con risultato tabellare, e la query diventa l'adattatore. Qualunque sia la sorgente, il patrimonio vede righe, colonne e tipi. Portare dentro un nuovo tipo di sorgente vuol dire rispettare quell'unico contratto, a volte con un po' di aiuto umano. Nessuno scrive un'integrazione su misura.

Le query vengono trattate allo stesso modo. SQL, con tutti i suoi dialetti, è il linguaggio dell'analisi sulle tabelle, e questo fa di una forma simile a SQL il bersaglio comune naturale. Quindi ogni richiesta, in qualunque linguaggio arrivi, viene prima ridotta a quella forma. SQL e GraphQL si riducono facilmente. I percorsi e la semantica a grafo di Cypher richiedono vero lavoro, ma ci arrivano. Poiché tutto passa per una sola forma prima di ogni altra cosa, la governance si applica in un unico punto, qualunque sia il linguaggio della richiesta.

Con sorgenti tabellari e una sola forma di query, la federazione può coprire sia le interrogazioni in diretta sia il data warehousing: ciò che fa un motore di query in diretta come Trino, più la materializzazione su cui questi motori si appoggiano. La **raggiungibilità** tiene insieme le due cose. Il motore può interrogare una sorgente dove si trova, o i suoi dati vanno prima copiati in un posto interrogabile? La risposta divide il patrimonio fra ciò che si legge in diretta e ciò che si copia.

La maggior parte dei database ha già una qualche forma di collegamento diretto: `ATTACH` di DuckDB, `postgres_fdw` di PostgreSQL, i collegamenti esterni di Databricks. Quindi quasi tutti possono fare da motore di federazione fino a un certo punto. Nessuno copre tutto. Ciascuno raggiunge un certo insieme di sorgenti e copia il resto, e nessuno dice chiaramente cosa sia cosa. Il modello rende esplicita la raggiungibilità, come un insieme definito di metodi per sorgente che dicono cosa il motore raggiunge in diretta e, per esclusione, cosa va copiato.

Resta l'aggiornamento. Per ogni sorgente che il motore non raggiunge, quanto deve essere aggiornata la sua copia? In pratica le risposte sono poche: su richiesta, secondo un calendario, a un segnale di modifica (CDC, un watermark, uno snapshot) oppure congelata. Sceglierne una per sorgente è tutta la politica di aggiornamento.

I dataset derivati rientrano nella stessa forma: tabelle derivate, aggregati, l'output di una trasformazione. Si scrivono nella stessa forma, quindi la lineage viene da sé. Il percorso da ogni sistema di origine a un output finale è la definizione che lo ha prodotto, leggibile da capo a fondo. Costruirli porta la domanda sull'aggiornamento un passo più in là: il dataset si aggiorna secondo un calendario, solo quando le sue precondizioni sono soddisfatte, in modo continuo, oppure come snapshot storico congelato? Le scelte sono lo stesso piccolo insieme della copia di una sorgente, quindi la politica di costruzione di un dataset derivato usa lo stesso vocabolario.

I modelli dimensionali ne discendono direttamente. Fatti e dimensioni di uno schema a stella sono dataset derivati come gli altri. Una dimensione è una proiezione conforme e deduplicata, una tabella dei fatti è un join con aggregato ridotto a una granularità, ciascuna con la propria politica di costruzione e di aggiornamento. Le dimensioni a variazione lenta non hanno bisogno di nulla di speciale: uno snapshot congelato è storia di tipo 2, una ricostruzione programmata è di tipo 1. Poiché lo schema è definito nel modello e non è legato alle tabelle di un warehouse, gli stessi fatti e dimensioni si possono costruire in Oracle, in Databricks, o lasciare virtuali sopra un motore MPP, senza rimodellare.

Data Vault si inserisce allo stesso modo, uno strato prima. Gli hub sono dataset deduplicati di chiavi di business, i link sono le relazioni registrate fra loro, e i satelliti sono dataset di attributi in sola inserzione, con marca temporale, che conservano la storia. Un satellite è semplicemente un dataset derivato con la strategia del segnale di modifica: data di caricamento più hashdiff è la cattura delle modifiche applicata agli attributi descrittivi, e la storia in sola inserzione è la strategia dello snapshot congelato. Le tabelle point-in-time e bridge sono altri dataset derivati, costruiti per la velocità delle query. Un raw vault è quindi un insieme di dataset derivati nel modello, e uno schema a stella ne è una proiezione, entrambi generati ed entrambi portabili fra motori. La metodologia resta una vostra decisione: cosa diventa un hub, la granularità di un satellite, come suddividere. Una volta deciso, la decisione vive nel modello. Nessun ETL la salda a un warehouse.

Entrambi gli schemi si dichiarano con **due scorciatoie** invece di viste scritte a mano. Sono i mattoni di ogni schema a stella e di ogni Data Vault, e non vi legano a nessuno dei due metodi:

- **`entity`** è una proiezione di una sorgente, con chiave e deduplicata, che può conservare la storia. Dichiarate la chiave, gli attributi e una modalità di storia, e Provisa costruisce una vista materializzata. Se chiedete la storia, costruisce invece una **MV bitemporale** (`scd2` → delta, `snapshot` → snapshot). Un solo costrutto vi dà una **dimensione** di Kimball (SCD1 o SCD2) o un **hub con satellite** di Data Vault.
- **`fact`** si unisce a chiavi di entità, si riduce a una granularità dichiarata e aggrega misure. Provisa costruisce una MV di aggregato più le relazioni registrate verso le entità. Un solo costrutto vi dà una **tabella dei fatti** a stella o un **link** di Data Vault; un fatto senza misure è un link di puro insieme di chiavi.

Ogni `entity` o `fact` diventa esattamente le viste materializzate, le impostazioni bitemporali e le relazioni che un modellatore scriverebbe altrimenti a mano, così il warehouse resta nel modello e passa da un motore all'altro senza rimodellare. Dichiaratene uno nell'interfaccia di amministrazione (il modulo **Model**) o tramite l'API di amministrazione (`registerEntity` / `registerFact`). Il modello vi genera una stella di Kimball o un Data Vault; non impone nessuno dei due.

### Time travel

Viaggiare nel tempo significa conservare ogni versione di una riga invece di sovrascriverla, così si può chiedere com'erano i dati in qualsiasi momento passato. I motori differiscono molto nell'efficienza con cui lo fanno, perciò Provisa lo rende parte della **definizione** della vista materializzata, qualunque sia il motore di archiviazione. Si dichiara una volta e funziona su qualsiasi archivio in grado di ospitare una vista materializzata.

Ciò che lo rende portabile è che è **solo in aggiunta**. Una versione, una volta scritta, non viene mai aggiornata né cancellata. Il solito trucco bitemporale di chiudere una riga riscrivendo una data «valido fino a» richiede un UPDATE, che molti motori non sanno fare in modo economico, o non sanno fare affatto, su un archivio federato. Così ogni aggiornamento **aggiunge**, e quale versione era in vigore in un dato momento si ricava in lettura dal log. Ci sono due modi di aggiungere:

- **Snapshot** aggiunge l'intero dataset fresco, con la marca dell'ora di sistema dell'aggiornamento. Non c'è confronto delle differenze ed è corretto su ogni motore, ma l'archivio cresce di una copia completa per aggiornamento.
- **Delta** aggiunge solo ciò che è cambiato, più lapidi per le chiavi rimosse. È il motore stesso a calcolare il delta, con anti-join dentro un `INSERT … SELECT`. Provisa non lo compone mai riga per riga. È più piccolo e richiede una chiave di entità.

Provisa gestisce così il tempo di sistema, cioè quando ha registrato una versione. Il tempo di validità, quando un fatto è vero per il business, viene dalla SELECT della vista e si conserva. I motori che sanno fare di meglio, come gli snapshot nativi di Iceberg o un MERGE che tocca meno righe, si possono usare dietro la stessa dichiarazione; il percorso in sola aggiunta è la base corretta ovunque.

La lettura non richiede una sintassi particolare. Una query semplice su una MV bitemporale restituisce lo stato **attuale**, ricostruito dal log. Per tornare indietro nel tempo, inviate un header `X-Provisa-As-Of: <timestamp>` e l'intera query riceve risposta com'era il patrimonio in quel momento, allo stesso modo su ogni archivio. Aggiungete `X-Provisa-As-Recorded` per leggere i dati come Provisa li aveva registrati in un momento, così una correzione registrata dopo non cambia la riproduzione del passato. Attivatelo per qualsiasi vista materializzata nell'interfaccia di amministrazione (il controllo **Time Travel**: disattivato, snapshot o delta, più una chiave di entità) o tramite l'API di amministrazione.

Raggiungibilità e aggiornamento insieme formano un modello generale di federazione: una definizione di cosa è in diretta, cosa si copia e quanto resta aggiornata ogni copia, indipendente da ciò che un singolo motore raggiunge. Il modello è portabile, quindi il vostro patrimonio non è legato al fornitore la cui federazione oggi raggiunge più sorgenti.

## Funzionalità {: #features }

### Interfacce di query {: #query-interfaces }

Questi sono i linguaggi e le API strutturate con cui scrivi le query. Ognuno ha la propria sintassi e semantica; la governance (RLS, mascheramento, visibilità delle colonne, applicazione delle relazioni) si applica in modo uniforme su tutti loro indipendentemente da quale protocollo di trasporto li veicola.

- **GraphQL** — Schemi per ruolo con visibilità per campo, filtri, paginazione a cursore e query di aggregazione (`count`, `sum`, `avg`, `min`, `max`). Lo schema offre solo relazioni registrate, quindi ogni query è strutturalmente valida per costruzione, e GraphQL è la strada più rapida verso una query semplice corretta. Apollo APQ è incluso: le query vengono hashate e registrate sul server, le chiamate successive inviano solo l'hash via HTTP GET, e le risposte diventano memorizzabili in una CDN senza modifiche al client. Le tabelle di lookup sotto una soglia di righe configurabile compaiono come tipi enum.
- **SQL** — SQL completo sui dati federati, senza vincoli e più espressivo di GraphQL. Scrivete SQL standard, sottoquery correlate comprese, e gira tra le sorgenti senza modifiche. Una query su una sola sorgente salta del tutto il livello di federazione (meno di 100 ms).
- **Cypher** — Linguaggio di query per grafi sullo stesso schema federato. Attraversa le relazioni come archi di un grafo; unisce origini; percorsi a lunghezza variabile. La governance si applica in modo identico a GraphQL e SQL.
- **API modello gRPC** — Un `.proto` generato dallo schema registrato, con RPC tipizzate di query e di inserimento per ogni tabella alla radice dello schema di un ruolo, e risposte in streaming. Come GraphQL è guidata dallo schema: il modello di registrazione è il contratto, e protobuf è la codifica di rete. È un'interfaccia di query completa per tabella, mentre Arrow Flight è un trasporto di streaming colonnare.
- **JSON:API** — Un'API di query strutturata su `/data/jsonapi/{table}`, solo HTTP per progetto. Supporta JSON:API 1.1: fieldset sparsi (`fields[table]=col1,col2`), espressioni di filtro (`filter[field][op]=value`), documenti composti (`include=relation`) e ordinamento. Interroga una tabella alla volta con una sintassi di filtro standard, quindi usate SQL o GraphQL quando vi serve un linguaggio di query generico.
- **Query Language Explorer** — Scrivi una query GraphQL e vedi traduzioni **Semantic SQL** e **Cypher** dal vivo in pannelli laterali; copia entrambe o passa direttamente all'editor SQL o Graph. Un workflow pratico è abbozzare frammenti di query in GraphQL, poi cucire l'SQL risultante in viste o report complessi.

L'Explorer mostra una query GraphQL insieme alle sue traduzioni SQL e Cypher dal vivo:

![Query Language Explorer](docs/images/query-explorer.png)

Lo stesso schema federato si mostra anche come grafo vivo, con etichette di dominio e di nodo, tipi di relazione e attraversamenti a lunghezza variabile:

![Graph Visualization](docs/images/graph-view.png)

### Strumenti di composizione query {: #query-composition-tools }

Questi strumenti vi aiutano a scrivere query nei linguaggi sopra. Non sono linguaggi di query in sé.

- **Query in linguaggio naturale** — Una pipeline NL→SQL/Cypher/GraphQL che gira su Claude. Descrivete ciò che volete in linguaggio semplice, e la pipeline scrive una query nel linguaggio scelto, con un ciclo di validazione interattivo prima che qualcosa venga eseguito.

![Natural Language Query](docs/images/natural-language.png)

### Protocolli di trasporto {: #wire-protocols }

Questi sono i protocolli di connessione. SQL, GraphQL e Cypher viaggiano su di essi, e il protocollo scelto non cambia né l'interfaccia di query né il comportamento della governance.

- **pgwire** — Qualsiasi client PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, pandas `read_sql`) si connette sulla porta 5439 come se fosse un server Postgres. Accetta solo SQL. Si applica la pipeline di governance completa. `pg_catalog` e `information_schema` vengono risposti da un catalogo in memoria così i browser di schema funzionano senza un round-trip di federazione. TLS opzionale.
- **Bolt (Neo4j)** — Qualsiasi client Neo4j (Neo4j Browser, Bloom, driver ufficiali) si connette tramite il protocollo Bolt ed esegue Cypher contro il grafo federato. Ogni ruolo che l'utente possiede appare come un database `provisa_<role>`. Stessa governance di ogni altro trasporto. TLS opzionale.
- **Arrow Flight** — Streaming colonnare ad alto throughput su gRPC; accetta GraphQL o SQL come input di query. Result set illimitati, nessuna materializzazione lato server, nessuna infrastruttura separata richiesta.
- **JDBC** — Integrazione con strumenti BI (Tableau, Power BI, DBeaver).
- **WebSocket / SSE** — Subscription: eventi di cambiamento quasi in tempo reale; backend: PG nativo, MongoDB nativo, CDC, polling. Esposto anche su Kafka.

### Origini dati {: #data-sources }

- **73 connettori, oltre 85 tipi di sorgente** — Il numero più alto conta uno per uno i servizi PostgreSQL ospitati (RDS, Aurora, Cloud SQL, AlloyDB, Azure, Supabase, Neon) e i formati di file del File Crawler. PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, triplestore SPARQL, Kafka, Google Sheets, Kaggle, Salesforce, ServiceNow, Stripe e gli altri stanno dietro un'unica API. Le sorgenti a grafo e RDF stanno alla pari con quelle relazionali.
- **Routing intelligente** — Una query su una sola sorgente salta la federazione (meno di 100 ms). Una query su più sorgenti passa per il livello di federazione, sul vostro cluster o sui worker embedded.
- **Origini API** — Registrate endpoint REST, GraphQL, gRPC, WebSocket o RSS come tabelle interrogabili, con helper SPARQL inclusi. I join federati tra origini API e sorgenti relazionali non richiedono configurazioni aggiuntive.
- **Introspezione degli schemi remoti** — Punta a qualsiasi endpoint GraphQL, OpenAPI o gRPC; le operazioni documentate vengono automaticamente esposte come tabelle interrogabili, nodi e archi del grafo con governance completa applicata sopra
- **Origini file** — File CSV, Parquet e SQLite come tabelle interrogabili; supporta percorsi locali e object storage remoto (`s3://`, `ftp://`, `sftp://`)
- **Dataset Kaggle** — cerca nel catalogo pubblico di Kaggle e registra un dataset tramite ricerca live autenticata da token, senza download manuale; si registra come un'unica origine file le cui tabelle vengono scoperte automaticamente, solo bundle CSV/Parquet
- **Integrazione Kafka** — I topic come tabelle in sola lettura; i risultati delle query come sink Kafka
- **Trigger pianificati** — Trigger cron e a intervallo (APScheduler) che scatenano webhook, mutation o pubblicazioni sink Kafka
- **Hint di prestazione della federazione** — Hint di routing tramite commento SQL che sovrascrivono le decisioni di routing automatico

![Data Sources](docs/images/data-sources.png)

Origini, file ed endpoint remoti vengono registrati come tabelle governate dalla UI:

![Table Registration](docs/images/table-registration.png)

### Sicurezza e governance {: #security-governance }

- **Sicurezza a livello di riga** — Iniezione di clausole WHERE per tabella e per ruolo
- **Mascheramento delle colonne** — Mascheramento per colonna (regex, costante, troncamento) con bypass basato su ruolo
- **Preset di colonna** — Valori statici lato server o basati su variabile di sessione iniettati in insert/update; non esposti nei tipi di input delle mutation
- **Permessi di scrittura** — Controllo di accesso alle mutation per colonna (`writable_by`)
- **Ruoli ereditati** — I ruoli ereditano RLS, visibilità e mascheramento da un ruolo padre in modo ricorsivo
- **Funzioni e webhook tracciati** — Funzioni del DB e webhook in uscita esposti come mutation GraphQL con shape di ritorno tipizzate
- **Hook di approvazione ABAC** — Hook di autorizzazione pre-esecuzione; trasporto webhook, gRPC o unix_socket; scope per tabella, per origine o globale; policy di fallback configurabile
- **Autenticazione pluggable** — Firebase, Keycloak, OAuth 2.0, simple (test)

![Security Roles](docs/images/security-roles.png)

### Delivery e prestazioni {: #delivery-performance }

- **Viste materializzate come trasformazioni registrate** — Una MV registra la trasformazione che l'ha prodotta: la sua forma di join o il suo SQL, i segnali di input per sorgente da cui è stata costruita (snapshot Iceberg, watermark RDB) e un controllo di determinismo alla registrazione. Poiché la trasformazione è registrata, Provisa può riscrivere una query, o una sua parte, su una MV fresca senza che modifichiate la query. Il confronto lavora sui pattern di join e accetta corrispondenze parziali, quindi una MV che copre parte dei join si applica comunque, e i join restanti vengono mantenuti.
- **Inlining delle tabelle hot** — Le piccole tabelle di lookup unite frequentemente vengono inlineate come CTE VALUES direttamente nel piano di query, eliminando i round trip cross-source per i dati dimensionali
- **Cache delle query** — Cache dei risultati Redis partizionata per ruolo+RLS; cache dell'hash APQ inclusa
- **Osservabilità come dati** — Trace distribuite, metriche e log vengono raccolti tramite OpenTelemetry, compattati in Iceberg su S3 e registrati come tabelle interrogabili (`traces`, `metrics`, `logs`, `queries`) nello schema federato. Interrogateli con SQL, GraphQL o Cypher accanto ai dati di business. Unite per esempio una tabella `customers` a `queries` per vedere chi ha eseguito cosa e quanto ci ha messo.

### Amministrazione e integrazione {: #administration-integration }

- **Admin API** — GraphQL su `/admin/graphql`; upload/download della configurazione, editing delle relazioni, approvazione delle query
- **Visualizzatore di report** — `/admin/reports` elenca le viste di gestione integrate del dominio ops e qualsiasi report personalizzato registrato; richiede la capability `observability`
- **Anteprima tabella** — ogni tabella registrata dispone di un visualizzatore di dati governato con paginazione lato server, filtri spinti alla sorgente, raggruppamento multilivello ed esportazione CSV
- **GraphQL Voyager** — Visualizzazione interattiva dello schema per ruolo come diagramma entità-relazione
- **Scoperta delle relazioni con LLM** — Claude suggerisce candidati a chiave esterna.
- **Client Python** — `pip install provisa-client`; GraphQL/SQL → DataFrame, Arrow Flight → tabelle pyarrow, dialetto SQLAlchemy, supporto ADBC
- **Ingestione dati** — Endpoint HTTP per inviare dati evento JSON nella piattaforma
- **Import Hasura v2 / DDN** — Converte i metadati Hasura v2 o lo YAML del supergraph DDN in configurazione Provisa
- **Apollo Federation** — Espone Provisa come subgraph Apollo Federation v2

Schema per ruolo visualizzato come diagramma entità-relazione (GraphQL Voyager):

![Schema Voyager](docs/images/schema-voyager.png)

Le relazioni vengono registrate, approvate e applicate come gli unici percorsi JOIN legali:

![Relationships](docs/images/relationships.png)

## Modello di sicurezza {: #security-model }

È qui che "sul percorso che ogni query già fa" deve reggere. Provisa applica un unico modello di sicurezza a livelli a ogni linguaggio di query (GraphQL, SQL, Cypher) e a ogni trasporto (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket), e nessun percorso di query lo aggira. La copertura viene dal design, quindi nessuno deve ricordarsi nulla: aggiungete un'origine, una colonna o una relazione, e ogni livello vi si applica subito, senza altro da registrare.

I livelli si applicano in ordine, e una richiesta deve superarli uno per uno prima che venga controllato il successivo.

### Livello 0 — Filtraggio dell'introspezione {: #layer-0-introspection-filtering }

Lo schema e il catalogo che un ruolo vede contengono solo le tabelle della sua lista `domain_access` e le colonne che superano le loro regole `visible_to`. Tutto ciò che è fuori dall'accesso del ruolo è invisibile fin dall'inizio: non si può interrogare, completare automaticamente, né dedurne l'esistenza. Questo vale per lo schema GraphQL, il catalogo SQL e il browser dello schema dell'editor di query.

### Livello 1 — Accesso pubblico {: #layer-1-public-access }

Le tabelle dei domini senza restrizione `domain_access` sono visibili a ogni identità autenticata, senza altro da configurare. I dati davvero pubblici non richiedono sforzo.

### Livello 2 — Accesso al dominio {: #layer-2-domain-access }

Ogni ruolo porta una lista `domain_access` di ID di dominio, e una query che tocca una tabella fuori da quei domini viene rifiutata prima dell'esecuzione. È il confine ampio di proprietà: un ruolo HR non raggiunge le tabelle finanziarie, comunque sia scritto l'SQL.

### Livello 3 — Sicurezza a livello di riga {: #layer-3-row-level-security }

Confermato l'accesso al dominio, predicati `WHERE` per tabella e per ruolo vengono aggiunti a ogni `SELECT` al momento dell'esecuzione e valutati sui dati grezzi. Una responsabile regionale che interroga una tabella ordini condivisa vede solo le righe della sua regione, anche con `SELECT *`.

### Livello 4 — Visibilità e mascheramento delle colonne {: #layer-4-column-visibility-and-masking }

Le colonne la cui lista `visible_to` esclude il ruolo richiedente vengono tolte dall'output. Le colonne con una regola di mascheramento hanno i valori sostituiti da oscuramento regex, una costante o un troncamento prima che i risultati lascino il server. Il mascheramento vale in ogni linguaggio di query e ogni formato di output.

### Livello 5 — Guardia dei predicati {: #layer-5-predicate-guard }

Le colonne mascherate vengono rifiutate nelle clausole `WHERE` e `HAVING`. Altrimenti chi interroga potrebbe ricavare il valore reale cercandolo per bisezione in un filtro, anche con l'output mascherato. Il rifiuto avviene all'analisi della query, prima che giri qualsiasi cosa.

### Governance delle relazioni {: #relationship-governance }

In SQL una condizione JOIN deve corrispondere a una relazione registrata e approvata tra le tabelle, e un join non approvato viene rifiutato. Ogni relazione porta un motivo e una descrizione leggibili, che spiegano a persone e agenti autonomi perché esiste quel percorso. È una policy di governance e non un confine di sicurezza rigido. I livelli da 2 a 5 reggono qualunque forma abbia il join, quindi un aggiramento deliberato non espone nulla che il ruolo non potrebbe raggiungere con due query separate, e ogni tentativo viene registrato per l'audit.

Nessun join non valido gira mai sui vostri dati. I ruoli sono governati per impostazione predefinita, e uscirne significa concedere di proposito il diritto `ignore_relationships` a un ruolo di cui vi fidate. Il token OAuth di un agente corrisponde a un ruolo, quindi un agente riceve gli stessi livelli di una persona. I cataloghi di dati con un livello di contesto dicono a un agente cosa dovrebbe fare; Provisa impone cosa può fare.

---

Questi livelli si compongono. Un ruolo con accesso ai domini, RLS e colonne mascherate ha tutti i livelli attivi insieme, perché ogni livello si configura da sé e si applica a ogni query che tocca un oggetto governato.

## Avvio rapido {: #quick-start }

### macOS

1. Scarica [Provisa-macOS.dmg](https://provisa.dev/dl/macos) (sempre l'ultima release)
2. Trascina **Provisa.app** in `/Applications` e fai doppio clic per avviarlo
3. Il primo avvio completa una configurazione una tantum (~2 min, nessuna connessione internet richiesta)
4. Apri Terminal:

```bash
provisa start   # start all services
provisa open    # open the UI in your browser
```

### Linux

1. Scarica [Provisa-linux-x86_64.AppImage](https://provisa.dev/dl/linux) (sempre l'ultima release)
2. Rendilo eseguibile ed eseguilo. Il primo avvio completa una configurazione una tantum (nessuna connessione internet richiesta):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Scarica [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (sempre l'ultima release)
2. Esegui l'installer. Nessun diritto di amministratore richiesto
3. Apri **Provisa First Launch** dal menu Start. Completa una configurazione una tantum (~5 min, nessuna connessione internet richiesta)
4. Apri un nuovo terminale:

```bash
provisa start
```

### Prima query {: #first-query }

Nello sviluppo locale (`PROVISA_MODE=test`) non servono credenziali. In produzione vi autenticate con un token Bearer, e Provisa ne legge il ruolo.

```bash
# Local dev — no auth required, role defaults to admin
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'

# Ad-hoc SQL works the same way
curl -X POST http://localhost:8001/data/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT id, amount, region FROM orders"}'

# Production — authenticate with a Bearer token; role is derived from the token
curl -X POST https://provisa.example.com/data/graphql \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ orders { id amount region } }"}'
```

### JDBC (Tableau, DBeaver, Power BI)

Scarica [provisa-jdbc.jar](https://provisa.dev/dl/jdbc) (sempre l'ultima release) e aggiungilo al percorso driver del tuo strumento BI.

```text
jdbc:provisa://localhost:8815
```

Autenticatevi con nome utente e password di Provisa, e il server vi assegna il ruolo.

Vedi [docs/integrations.md](docs/integrations.md) per i passaggi di configurazione di Tableau e Power BI.

### Protocollo di trasporto PostgreSQL (pgwire) {: #postgresql-wire-protocol-pgwire }

Provisa parla il protocollo di rete di PostgreSQL sulla porta 5439. Qualsiasi client in grado di collegarsi a Postgres si collega a Provisa, senza driver, senza adattatore e senza modificare i vostri strumenti esistenti.

**Lo username PostgreSQL seleziona il ruolo Provisa.** Con `provider: none` (modalità trust), la password viene ignorata e qualsiasi nome di ruolo configurato viene accettato come username — connettiti come `analyst`, `admin`, o qualsiasi ruolo per vedere la vista governata di quel ruolo sui dati. Con `provider: simple`, la password viene validata con bcrypt. Altri provider (`firebase`, `keycloak`, `oauth`) non sono supportati su pgwire.

```bash
# psql — connect as analyst role
psql -h localhost -p 5439 -U analyst

# psql — connect as admin role
psql -h localhost -p 5439 -U admin

# asyncpg (Python) — role = username, password ignored in trust mode
conn = await asyncpg.connect(host="localhost", port=5439, user="analyst", password="x")
rows = await conn.fetch("SELECT id, amount FROM orders WHERE region = 'west'")

# SQLAlchemy
engine = create_engine("postgresql+psycopg2://analyst:x@localhost:5439/provisa")

# pandas
df = pd.read_sql("SELECT * FROM orders", engine)
```

Ogni query passa per l'intera pipeline di governance, quindi accesso ai domini, RLS, mascheramento e guardia sui predicati si applicano esattamente come per GraphQL e REST. I browser di schema (DBeaver, DataGrip, pgAdmin) funzionano senza configurazione. Provisa risponde alle loro query su `pg_catalog` e `information_schema` da un catalogo in memoria limitato all'accesso ai domini del ruolo, così gli utenti vedono solo le tabelle e le colonne che possono interrogare.

DataGrip esplora lo schema governato e il suo diagramma delle chiavi esterne via pgwire, senza driver né adattatore:

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

TLS viene abilitato impostando `PROVISA_PGWIRE_CERT` e `PROVISA_PGWIRE_KEY`. La porta è configurabile tramite `PROVISA_PGWIRE_PORT` (default `5439`).

### Bolt (protocollo di trasporto Neo4j) {: #bolt-neo4j-wire-protocol }

Provisa parla anche il protocollo **Bolt** di Neo4j, quindi gli strumenti nativi per i grafi si collegano direttamente ed eseguono Cypher sul grafo federato. Non si esporta nulla, e non serve un database a grafo separato. Puntate **Neo4j Browser** o **Bloom** su Provisa e attraversate le relazioni tra sorgenti con la stessa governance (accesso ai domini, RLS, mascheramento).

Neo4j Browser che esegue Cypher contro Provisa. Etichette dei nodi, tipi di relazione e chiavi delle proprietà provengono direttamente dallo schema registrato:

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

Attivatelo impostando `PROVISA_BOLT_PORT` (il default di Neo4j è `7687`), e attivate TLS con `PROVISA_BOLT_CERT` e `PROVISA_BOLT_KEY`. Ogni ruolo Provisa dell'utente autenticato compare come database selezionabile `provisa_<role>` (il selettore `provisa_admin` qui sopra). Sceglierne uno restringe la sessione ai diritti di dominio di quel ruolo, e l'utente non può mai andare oltre i ruoli che possiede.

### Client Python {: #python-client }

```bash
pip install provisa-client                       # core
pip install "provisa-client[pandas]"             # + DataFrame support
pip install "provisa-client[sqlalchemy]"         # + SQLAlchemy dialect
pip install "provisa-client[adbc]"               # + ADBC over Arrow Flight
```

```python
from provisa_client import ProvisaClient, connect

# GraphQL → DataFrame
client = ProvisaClient("http://localhost:8001", username="alice", password="secret")
df = client.query_df("{ orders { id amount region } }")

# SQL → DataFrame
df = client.query_df("SELECT id, amount, region FROM orders WHERE region = 'west'")

# Arrow Flight → pyarrow Table (high-throughput columnar)
table = client.flight("{ orders { id amount region } }")

# DB-API 2.0 (PEP 249) — GraphQL or SQL, detected automatically
with connect("http://localhost:8001", username="alice", password="secret") as conn:
    cur = conn.cursor()

    # GraphQL
    cur.execute("{ orders { id amount region } }")
    rows = cur.fetchall()

    # SQL (routed through governance engine — RLS and masking applied)
    cur.execute("SELECT id, amount FROM orders WHERE region = %s", ("west",))
    rows = cur.fetchall()

# SQLAlchemy dialect — provisa+http:// or provisa+https://
from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("provisa+http://alice:secret@localhost:8001")

# pandas read_sql — GraphQL or SQL
df = pd.read_sql("{ orders { id amount region } }", engine)
df = pd.read_sql("SELECT id, amount, region FROM orders WHERE region = 'west'", engine)

# raw execute
with engine.connect() as conn:
    rows = conn.execute(text("SELECT id, amount FROM orders")).fetchall()

# role URL parameter
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

Vedi [docs/python-client.md](docs/python-client.md) per il riferimento completo.

## Documentazione {: #documentation }

| Argomento | Documento |
| --- | --- |
| Guida rapida per sviluppatori (esecuzione dal sorgente) | [docs/quickstart.md](docs/quickstart.md) |
| Riferimento completo alla configurazione YAML | [docs/configuration.md](docs/configuration.md) |
| Riferimento degli endpoint (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Design di sistema e mappa dei componenti | [docs/architecture.md](docs/architecture.md) |
| Modello di sicurezza (RLS, mascheramento, autenticazione) | [docs/security.md](docs/security.md) |
| Archiviazione dei segreti e riferimenti `${secret:NAME}` | [docs/secrets.md](docs/secrets.md) |
| Glossario aziendale e curatela dei termini | [docs/glossary.md](docs/glossary.md) |
| Ambienti (dev / staging / prod) | [docs/environments.md](docs/environments.md) |
| Creare ambienti di test (profilazione, dati fittizi, dati sintetici) | [docs/test-data.md](docs/test-data.md) |
| Riferimento dei metodi dei dati fittizi | [docs/fake-methods.md](docs/fake-methods.md) |
| Eseguire e creare demo su un portatile (sales engineer) | [docs/sales-engineer-demos.md](docs/sales-engineer-demos.md) |
| Tipi di origine supportati | [docs/sources.md](docs/sources.md) |
| Subscription SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, strumenti BI, client Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Client Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| Admin API | [docs/admin.md](docs/admin.md) |
| Deployment (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Import Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Workflow di release (tag alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionamento {: #sizing }

Provisa include un motore di federazione integrato per le query multi-origine. Al primo avvio scegli un budget di RAM; Provisa deriva automaticamente il numero di worker di federazione locali.

| RAM host | Worker | Carico di lavoro tipico |
| --- | --- | --- |
| < 24 GB | 0 | Sviluppo, query su singola origine, team piccoli |
| 24–47 GB | 1 | Team piccolo, query cross-source moderate |
| 48–95 GB | 2 | Deployment dipartimentale, uso misto BI + notebook |
| 96 GB+ | 4 | Grande dipartimento, federazione concorrente pesante |

Il numero di worker può essere modificato in qualsiasi momento modificando `~/.provisa/config.yaml` (`federation_workers: N`) ed eseguendo `provisa restart`. Imposta a `0` per eseguire in modalità solo coordinamento (single-node).

### Scalare oltre una singola macchina {: #scaling-beyond-a-single-box }

**Scale-out orizzontale** — Eseguite più istanze di Provisa dietro un load balancer. Ogni istanza è un sistema completo. Tutte devono puntare allo stesso database di configurazione (impostate `CONFIG_DB_HOST` sulle macchine secondarie), e possono condividere un'istanza Redis (`REDIS_URL`) per un'unica cache. La maggior parte delle query si distribuisce senza modifiche. Un join tra sorgenti molto grande può superare un'istanza, e allora serve una macchina più grande o un cluster di federazione esterno.

**Redis condiviso** — Imposta `REDIS_URL` su ogni istanza per puntare a un Redis esterno. Redis condiviso significa che le voci di cache di un'istanza sono disponibili per tutte, migliorando gli hit rate sull'intero cluster.

**Porta il tuo cluster di federazione** — Punta Provisa verso un cluster di federazione esterno esistente invece dei worker integrati. Consigliato per deployment su larga scala o cloud; vedi [docs/deployment.md](docs/deployment.md) per la configurazione.

## Licenza {: #license }

Business Source License 1.1 (non modificata, secondo i covenant del Licenziante di MariaDB). Ogni
versione rilasciata si converte nella Change License (GPL v2.0 o successiva) al 4°
anniversario della sua release pubblica; il codice attuale e recente resta sotto BSL.
L'uso in produzione al di sopra delle soglie dell'Additional Use Grant (meno di 100
dipendenti/collaboratori e sotto 1M$ di fatturato dell'anno precedente) richiede una licenza
commerciale. Vedi [LICENSE](LICENSE).

Il Licenziante non acconsente all'uso di quest'opera per l'addestramento AI/ML. Vedi
[NOTICE](NOTICE), [ai.txt](ai.txt) e [robots.txt](robots.txt). Per licenze commerciali
o di addestramento AI: <kennethstott@gmail.com>
