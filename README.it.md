# Provisa

**Connetti i tuoi database. Interroga con GraphQL, gRPC, SQL o MCP — su qualsiasi API o protocollo — in 5 minuti.**

Provisa serve ogni superficie API (REST, GraphQL, SQL, gRPC, MCP e altre) sul risultato congiunto di tutte le tue origini. Può farlo perché è un **layer semantico attivo**: una definizione unica del tuo patrimonio dati — ogni dominio, relazione e policy tra le tue origini, escludendo solo i sistemi di origine stessi — che opera il patrimonio e allo stesso tempo lo governa. La definizione non è documentazione che un motore può consultare; *è* il motore. I domini e le relazioni registrati sono gli unici percorsi di join legali, e le policy di accesso sono compilate in ogni piano di query. Un modello, tre compiti:

- **Definire** — Domini, colonne e relazioni sono dichiarati una sola volta. Quella dichiarazione è lo schema che ogni consumer vede e l'unico insieme di percorsi di join che qualsiasi query può percorrere.
- **Applicare** — Sicurezza a livello di riga, mascheramento delle colonne, visibilità delle colonne e approvazione delle query sono applicati inline sul percorso di esecuzione. Nessuna query raggiunge i dati senza passare da questi controlli, quindi la copertura è totale per costruzione e non per diligenza.
- **Auditing** — Poiché ogni richiesta percorre lo stesso percorso governato, chi ha interrogato cosa, con quale ruolo e contro quale policy viene registrato in modo uniforme. Trace distribuite, metriche e log sono essi stessi registrati come tabelle interrogabili accanto ai tuoi dati di business.

Un unico core governato serve ogni linguaggio e trasporto. Interroga con **GraphQL, Cypher o SQL**; consuma su **pgwire, Bolt, gRPC, REST, Arrow Flight o JDBC**. Ogni linguaggio di query viene ridotto a un'unica rappresentazione intermedia dove la governance viene iniettata una sola volta — così una policy non può divergere tra linguaggi — e quella IR viene ritradotta verso il dialetto nativo di ciascuna origine in uscita. Aggiungere un linguaggio è un nuovo front-end sul core condiviso, non un nuovo motore.

Il patrimonio dati è sia analitico che transazionale. Le letture cross-source passano attraverso il layer di federazione; le scritture e le letture su singola origine vengono instradate direttamente al driver dell'origine — governate in modo identico, ma transazionali e sub-100ms. Lo streaming colonnare Arrow Flight è integrato.

L'intero modello è costruito a partire da una manciata di primitive — domini, relazioni, ruoli e policy. Vocabolario ridotto, quindi la definizione è facile da comprendere e semplice da valutare e sottoporre ad audit: puoi leggere l'insieme di policy e sapere cosa fa. Provisa è un compilatore di query leggero, non un runtime che si colloca nel percorso dei dati. Converte una richiesta in query native, le instrada, e si fa da parte — motivo per cui il patrimonio dati mantiene le prestazioni.

Questo design supporta due modalità d'uso, e non sono esclusive:

- **Come impalcatura per la modernizzazione** — Modella il tuo patrimonio dati, lascia che Provisa generi l'SQL nativo per ciascuna origine, quindi cattura quell'SQL e adottalo direttamente nel sistema di destinazione. Provisa è il layer di transizione, non una dipendenza permanente.
- **Come infrastruttura permanente di applicazione delle policy** — Mantienilo in essere come percorso governato che ogni query percorre, così che definizione, applicazione e audit restano unificati per tutta la vita del patrimonio dati.

## Il modello di federazione

L'intero modello si riduce a due contratti e due policy: le origini si riducono a tabelle 2-D su un unico sistema di tipi, le query si riducono a un'unica IR simile a SQL, la reachability decide cosa viene interrogato dal vivo rispetto a cosa viene materializzato, e una strategia di freshness governa ogni copia materializzata e dataset derivato. Forma dei dati in ingresso, forma della query in ingresso, governance al join, query native in uscita. Il resto di questa sezione percorre ogni elemento.

Il modello si basa su una riduzione: ogni origine è espressa come una raccolta di tabelle bidimensionali su un unico sistema di tipi generalizzato. Questo è il contratto che un'origine deve soddisfare per unirsi al patrimonio dati, ed è lo stesso contratto per tutte. Alcune origini si adattano già — una tabella MySQL o PostgreSQL *è* una relazione 2-D tipizzata. Alcune si adattano con una proiezione: un risultato di query GraphQL, una volta appiattito, è una tabella. Alcune sono estranee alla forma — triplestore SPARQL, Neo4j — ma restano lavorabili, perché l'utente fornisce una query il cui result set è tabulare; la query è l'adattatore. Qualunque sia l'origine, il patrimonio dati vede righe, colonne e tipi generalizzati, e nient'altro. Integrare un nuovo tipo di origine significa soddisfare quell'unico contratto, a volte con un passaggio di intervento umano, non scrivere un'integrazione su misura.

Quella riduzione ha un gemello sul lato query. SQL — attraverso tutti i suoi dialetti e le sue peculiarità — è essenzialmente il linguaggio per l'analisi su dataset 2-D, il che rende una forma simile a SQL il naturale target universale per le query. Quindi ogni richiesta, in qualunque linguaggio arrivi, viene ridotta a quella rappresentazione intermedia come primo passo. Alcune si riducono in modo pulito — SQL stesso, e persino GraphQL; altre sono difficili — la semantica di percorsi e grafi di Cypher richiede lavoro reale — ma tutte sono fattibili. Convogliare ogni richiesta in un'unica IR prima che accada qualsiasi altra cosa è ciò che permette alla governance di applicarsi in esattamente un punto, su una forma, a prescindere dal linguaggio con cui è arrivata.

Sopra queste due forme uniformi — origini tabulari e un'unica forma di query — la federazione qui significa sia query dal vivo che warehousing — lo stesso ambito che copre un motore di query dal vivo come Trino, più la materializzazione su cui questi motori fanno affidamento. Il concetto che li unifica è la **reachability**: per una data origine, il motore può interrogarla sul posto, o i suoi dati devono prima essere materializzati da qualche parte di interrogabile? La reachability partiziona il patrimonio dati in ciò che viene interrogato dal vivo e ciò che viene copiato prima.

La maggior parte dei database porta già con sé una qualche nozione di collegamento dal vivo — `ATTACH` di DuckDB, `postgres_fdw` di PostgreSQL, i link esterni di Databricks. Quindi la maggior parte dei database può fungere da motore di federazione fino a un certo punto. Nessuno è esaustivo: ciascuno raggiunge un particolare insieme di origini e materializza il resto, senza un resoconto unico di quale sia quale. Il modello colma quel divario rendendo la reachability esplicita — un insieme definito di metodi, per origine, che stabiliscono cosa il motore può raggiungere dal vivo e, per eliminazione, cosa deve essere materializzato.

Ciò che resta è la freshness: per ogni origine non raggiungibile, quanto deve essere aggiornata la sua copia materializzata? In pratica questo si riduce a un piccolo insieme di strategie — su richiesta, su pianificazione, su segnale di cambiamento (CDC, watermark, snapshot), o fissata. Sceglierne una per origine è l'intera policy di freshness.

I dataset analitici — tabelle derivate, aggregati, gli output di una trasformazione — rientrano nella stessa forma. Anche loro devono essere espressi nella IR, e proprio perché lo sono, la derivazione dei dati non è un sistema separato da mantenere: il percorso da ciascun sistema di origine fino a un output finale *è* la IR che lo ha prodotto, leggibile dall'inizio alla fine. Costruirli solleva la questione della freshness un passo più in là — il dataset si aggiorna su pianificazione, solo quando le sue precondizioni sono soddisfatte, in modo continuo come quasi-tempo-reale, o come snapshot storico fissato? I modi per esprimere come e quando costruire un dataset sono lo stesso piccolo insieme enumerabile, quindi un dataset derivato porta una build policy nello stesso identico vocabolario di una copia di origine.

I modelli dimensionali sono un'applicazione diretta. Le tabelle fact e dimension di uno star schema sono dataset analitici come ogni altro — una dimension è una proiezione conforme e deduplicata; una fact table è un join e un'aggregazione ridotti a un grain — ciascuna con la propria build policy e freshness policy. Le slowly changing dimension non richiedono alcun meccanismo speciale: uno snapshot fissato è la storia di Tipo 2, una ricostruzione pianificata è il Tipo 1. E poiché lo schema è definito nella IR anziché legato fisicamente alle tabelle di un solo warehouse, le stesse definizioni di fact e dimension si ritraducono — materializzate in Oracle, in Databricks, o lasciate virtuali sopra un motore MPP — senza rimodellare. Il modello genera lo star schema; non lo blocca a un motore.

Data Vault si adatta nello stesso modo, un livello più a monte. I suoi hub sono dataset di chiavi di business deduplicate, i suoi link sono le relazioni registrate tra di essi, e i suoi satellite sono dataset di attributi insert-only con timestamp — il record storico. Un satellite è semplicemente un dataset derivato sulla strategia di freshness a segnale di cambiamento: load-date più hashdiff è il CDC applicato agli attributi descrittivi, e la storia insert-only è la strategia dello snapshot fissato. Le tabelle point-in-time e bridge sono ulteriori dataset derivati costruiti per le prestazioni delle query. Quindi un raw vault è un insieme di dataset analitici nella IR, e uno star schema è una proiezione a partire da esso — entrambi generati, entrambi portabili tra motori. Ciò che il modello non fa è decidere la metodologia: cosa diventa un hub, il grain di un satellite, la strategia di suddivisione. Quelle restano scelte di modellazione; una volta fatte, vivono come IR portabile anziché come ETL saldato a un solo warehouse.

Entrambi i pattern sono dichiarati attraverso **due scorciatoie di prima classe** anziché viste scritte a mano — le primitive da cui sono costruiti ogni star schema e Data Vault, mantenute neutrali rispetto alla metodologia:

- **`entity`** — una proiezione con chiave, deduplicata, opzionalmente storicizzata di un'origine. Dichiara una chiave di entity, gli attributi e una modalità di storia; Provisa la riduce a una vista materializzata, e quando viene richiesta la storia a una **MV bitemporale** (`scd2` → delta, `snapshot` → snapshot). Un unico costrutto serve sia una **dimension** Kimball (SCD1/SCD2) sia un **hub + satellite** Data Vault.
- **`fact`** — un join a chiavi di entity, ridotto a un grain dichiarato, con misure aggregate. Provisa la riduce a una MV di aggregazione più relazioni registrate verso le entity. Un unico costrutto serve sia una **fact table** dello star schema sia un **link** Data Vault (una fact senza misure è un puro link a insieme di chiavi).

Poiché la riduzione è pura — una specifica `entity`/`fact` diventa esattamente le definizioni di MV, bitemporali e di relazione che un modellista scriverebbe altrimenti a mano — il warehouse è IR fino in fondo e si ritraduce tra motori senza rimodellare. Dichiara un warehouse nella UI di amministrazione (un form **Model** per entity e fact) o tramite la admin API (`registerEntity` / `registerFact`); il modello *genera* lo star Kimball o il Data Vault, non ne impone uno.

### Time travel

Il time travel è un'idea semplice — mantenere ogni versione di una riga invece di sovrascriverla, così puoi chiedere quale fosse il dato in un qualsiasi momento passato. Ciò che differisce è quanto efficientemente ogni motore possa farlo, ed è esattamente per questo che Provisa lo rende una proprietà della **definizione** della vista materializzata anziché del motore di storage (REQ-1162). Dichiaralo una volta; funziona su qualsiasi backend che materializza.

La regola che lo mantiene portabile è **append-only**: una versione, una volta scritta, non viene mai aggiornata né eliminata. Ritirare una riga scrivendo una data "valid-to" — il consueto trucco bitemporale — richiede un UPDATE, che molti motori non possono fare a basso costo (o non possono fare affatto) su uno store federato, quindi Provisa non lo fa. Invece ogni refresh **aggiunge**, e "quale versione era in vigore al tempo T" viene derivato al momento della lettura dal log immutabile. Ci sono esattamente due modi di aggiungere:

- **Snapshot** — aggiunge l'intero dataset aggiornato, marcato con il tempo di sistema di questo refresh. Nessun diffing; corretto su ogni motore; lo storage cresce di una copia intera per refresh.
- **Delta** — aggiunge solo ciò che è cambiato, più tombstone per le chiavi rimosse. Il delta viene **calcolato dal motore** (anti-join dentro un `INSERT … SELECT`), mai ripiegato riga per riga in Provisa. Più piccolo, e richiede una chiave di entity.

Il tempo di sistema (quando Provisa ha registrato una versione) è gestito in questo modo; il tempo di validità (quando un fatto è vero nel business) viene fornito dalla SELECT della vista stessa e preservato. I motori che offrono di più — snapshot Iceberg nativi, un MERGE che mantiene meno righe — possono essere mirati per efficienza dietro la stessa dichiarazione; il percorso append-only è il pavimento corretto ovunque.

La lettura è trasparente. Una query semplice contro una MV bitemporale ricostruisce lo stato **corrente** dal log di append per default; per viaggiare nel tempo, invia un header `X-Provisa-As-Of: <timestamp>` e l'intera query viene risposta come il patrimonio dati era in quel momento — semantica identica su ogni substrato. Attivalo per qualsiasi vista materializzata nella UI di amministrazione (un controllo **Time Travel**: off / snapshot / delta più una chiave di entity) o tramite la admin API.

Reachability più freshness è un modello generale per la federazione dei dati: una definizione che dice cosa è dal vivo, cosa è materializzato, e quanto resta aggiornata ogni copia — indipendente dalla portata di un singolo motore. Il risultato è libertà dal lock-in proprietario. Il modello è portabile; il patrimonio dati non è prigioniero di qualunque vendor la cui federazione oggi raggiunga il maggior numero di origini.

## Funzionalità

### Interfacce di query

Questi sono i linguaggi e le API strutturate con cui scrivi le query. Ognuno ha la propria sintassi e semantica; la governance (RLS, mascheramento, visibilità delle colonne, applicazione delle relazioni) si applica in modo uniforme su tutti loro indipendentemente da quale protocollo di trasporto li veicola.

- **GraphQL** — Schemi per ruolo con visibilità a livello di campo, filtri, paginazione basata su cursore e query di aggregazione (`count`, `sum`, `avg`, `min`, `max`). Vincolato dallo schema alle relazioni registrate — strutturalmente valido per costruzione, il percorso più rapido verso una query semplice corretta. Apollo APQ incluso: le query vengono hashate e registrate lato server; le chiamate successive inviano solo l'hash via HTTP GET, rendendo le risposte cacheable da CDN senza alcuna modifica lato client. Le tabelle di lookup sotto una soglia di righe configurabile vengono esposte come tipi enum.
- **SQL** — SQL completo su dati federati; non vincolato e più espressivo di GraphQL. Scrivi SQL standard — sottoquery correlate comprese — e viene eseguito su tutte le origini senza modifiche. Le query su singola origine bypassano completamente il layer di federazione (sub-100ms).
- **Cypher** — Linguaggio di query per grafi sullo stesso schema federato. Attraversa le relazioni come archi di un grafo; unisce origini; percorsi a lunghezza variabile. La governance si applica in modo identico a GraphQL e SQL.
- **API modello gRPC** — `.proto` generato automaticamente dallo schema registrato; RPC di query e insert tipizzate per tabella, risposte in streaming. Guidato dallo schema nello stesso senso di GraphQL — il modello di registrazione è il contratto, protobuf è la codifica sul wire. A differenza di Arrow Flight (che è un trasporto di streaming colonnare), questa è un'interfaccia di query completa per tabella.
- **JSON:API** — API di query strutturata su `/data/jsonapi/{table}`, HTTP-only per design. Supporta JSON:API 1.1: sparse fieldset (`fields[table]=col1,col2`), espressioni di filtro (`filter[field][op]=value`), documenti composti (`include=relation`) e ordinamento. Non è un linguaggio di query generico — interroga una tabella alla volta con sintassi di filtro standardizzata anziché una stringa di query ad-hoc.
- **Query Language Explorer** — Scrivi una query GraphQL e vedi traduzioni **Semantic SQL** e **Cypher** dal vivo in pannelli laterali; copia entrambe o passa direttamente all'editor SQL o Graph. Un workflow pratico è abbozzare frammenti di query in GraphQL, poi cucire l'SQL risultante in viste o report complessi.

L'Explorer mostra una query GraphQL insieme alle sue traduzioni SQL e Cypher dal vivo:

![Query Language Explorer](docs/images/query-explorer.png)

Lo stesso schema federato è esplorabile come un grafo dal vivo — etichette di dominio e nodo, tipi di relazione e attraversamenti a lunghezza variabile:

![Graph Visualization](docs/images/graph-view.png)

### Strumenti di composizione query

Questi strumenti aiutano a scrivere query nei linguaggi sopra elencati — non sono essi stessi linguaggi di query.

- **Query in linguaggio naturale** — Pipeline NL→SQL/Cypher/GraphQL potenziata da Claude. Descrivi cosa vuoi in inglese semplice; la pipeline produce una query nel linguaggio scelto con un ciclo di validazione interattivo prima dell'esecuzione.

![Natural Language Query](docs/images/natural-language.png)

### Protocolli di trasporto

Questi sono i protocolli di connessione. SQL, GraphQL e Cypher vi transitano sopra — la scelta del protocollo di trasporto non cambia l'interfaccia di query né il comportamento della governance.

- **pgwire** — Qualsiasi client PostgreSQL (psql, DBeaver, DataGrip, asyncpg, SQLAlchemy, pandas `read_sql`) si connette sulla porta 5439 come se fosse un server Postgres. Accetta solo SQL. Si applica la pipeline di governance completa. `pg_catalog` e `information_schema` vengono risposti da un catalogo in memoria così i browser di schema funzionano senza un round-trip di federazione. TLS opzionale.
- **Bolt (Neo4j)** — Qualsiasi client Neo4j (Neo4j Browser, Bloom, driver ufficiali) si connette tramite il protocollo Bolt ed esegue Cypher contro il grafo federato. Ogni ruolo che l'utente possiede appare come un database `provisa_<role>`. Stessa governance di ogni altro trasporto. TLS opzionale.
- **Arrow Flight** — Streaming colonnare ad alto throughput su gRPC; accetta GraphQL o SQL come input di query. Result set illimitati, nessuna materializzazione lato server, nessuna infrastruttura separata richiesta.
- **JDBC** — Integrazione con strumenti BI (Tableau, Power BI, DBeaver) in modalità `approved` o `catalog`.
- **WebSocket / SSE** — Subscription: eventi di cambiamento quasi in tempo reale; backend: PG nativo, MongoDB nativo, CDC, polling. Esposto anche su Kafka.

### Origini dati

- **53 tipi di origine** — PostgreSQL, MySQL, MongoDB, Cassandra, Elasticsearch, Neo4j, triplestore SPARQL, Kafka, Google Sheets e altre tramite un'unica API; le origini grafo e RDF sono di prima classe, non adattatori
- **Routing intelligente** — Le query su singola origine bypassano la federazione (sub-100ms); le query multi-origine vengono instradate attraverso il layer di federazione — porta il tuo cluster o usa i worker integrati
- **Origini API** — Registra endpoint REST, GraphQL, gRPC, WebSocket o RSS come tabelle interrogabili; helper SPARQL inclusi; i join federati tra origini API e origini relazionali funzionano in modo trasparente
- **Introspezione degli schemi remoti** — Punta a qualsiasi endpoint GraphQL, OpenAPI o gRPC; le operazioni documentate vengono automaticamente esposte come tabelle interrogabili, nodi e archi del grafo con governance completa applicata sopra
- **Origini file** — File CSV, Parquet e SQLite come tabelle interrogabili; supporta percorsi locali e object storage remoto (`s3://`, `ftp://`, `sftp://`)
- **Integrazione Kafka** — I topic come tabelle in sola lettura; i risultati delle query come sink Kafka
- **Trigger pianificati** — Trigger cron e a intervallo (APScheduler) che scatenano webhook, mutation o pubblicazioni sink Kafka
- **Hint di prestazione della federazione** — Hint di routing tramite commento SQL che sovrascrivono le decisioni di routing automatico

![Data Sources](docs/images/data-sources.png)

Origini, file ed endpoint remoti vengono registrati come tabelle governate dalla UI:

![Table Registration](docs/images/table-registration.png)

### Sicurezza e governance

- **Sicurezza a livello di riga** — Iniezione di clausole WHERE per tabella e per ruolo
- **Mascheramento delle colonne** — Mascheramento per colonna (regex, costante, troncamento) con bypass basato su ruolo
- **Preset di colonna** — Valori statici lato server o basati su variabile di sessione iniettati in insert/update; non esposti nei tipi di input delle mutation
- **Permessi di scrittura** — Controllo di accesso alle mutation per colonna (`writable_by`)
- **Ruoli ereditati** — I ruoli ereditano RLS, visibilità e mascheramento da un ruolo padre in modo ricorsivo
- **Funzioni e webhook tracciati** — Funzioni del DB e webhook in uscita esposti come mutation GraphQL con shape di ritorno tipizzate
- **Hook di approvazione ABAC** — Hook di autorizzazione pre-esecuzione; trasporto webhook, gRPC o unix_socket; scope per tabella, per origine o globale; policy di fallback configurabile
- **Autenticazione pluggable** — Firebase, Keycloak, OAuth 2.0, simple (test)

![Security Roles](docs/images/security-roles.png)

### Delivery e prestazioni

- **Viste materializzate come trasformazioni registrate** — Una MV cattura la trasformazione che l'ha prodotta: la sua shape di join o SQL, i segnali di input per origine (snapshot Iceberg, watermark RDB) da cui è stata costruita, e un controllo di determinismo alla registrazione. Poiché la trasformazione è registrata, le query (o le sottoespressioni) vengono riscritte in modo trasparente su una MV aggiornata — matching strutturale del pattern di join con supporto per corrispondenza parziale, così una MV che copre un sottoinsieme dei join si applica comunque, con i join rimanenti preservati
- **Inlining delle tabelle hot** — Le piccole tabelle di lookup unite frequentemente vengono inlineate come CTE VALUES direttamente nel piano di query, eliminando i round trip cross-source per i dati dimensionali
- **Cache delle query** — Cache dei risultati Redis partizionata per ruolo+RLS; cache dell'hash APQ inclusa
- **Osservabilità come dati** — Trace distribuite, metriche e log vengono raccolti tramite OpenTelemetry, compattati in Iceberg su S3, e registrati automaticamente come tabelle interrogabili (`traces`, `metrics`, `logs`, `queries`) nello schema federato; interrogali con SQL, GraphQL o Cypher accanto ai tuoi dati di business — unisci una tabella `customers` alla tabella `queries` per vedere chi ha eseguito cosa e quanto tempo ha impiegato

### Amministrazione e integrazione

- **Admin API** — GraphQL su `/admin/graphql`; upload/download della configurazione, editing delle relazioni, approvazione delle query
- **Visualizzatore di report** — `/admin/reports` elenca le viste di gestione integrate del dominio ops e qualsiasi report personalizzato registrato; richiede la capability `observability`
- **Anteprima tabella** — ogni tabella registrata dispone di un visualizzatore di dati governato con paginazione lato server, filtri spinti alla sorgente, raggruppamento multilivello ed esportazione CSV
- **GraphQL Voyager** — Visualizzazione interattiva dello schema per ruolo come diagramma entità-relazione
- **Scoperta delle relazioni con LLM** — Suggerimenti di chiave esterna candidata potenziati da Claude
- **Client Python** — `pip install provisa-client`; GraphQL/SQL → DataFrame, Arrow Flight → tabelle pyarrow, dialetto SQLAlchemy, supporto ADBC
- **Ingestione dati** — Endpoint HTTP per inviare dati evento JSON nella piattaforma
- **Import Hasura v2 / DDN** — Converte i metadati Hasura v2 o lo YAML del supergraph DDN in configurazione Provisa
- **Apollo Federation** — Espone Provisa come subgraph Apollo Federation v2

Schema per ruolo visualizzato come diagramma entità-relazione (GraphQL Voyager):

![Schema Voyager](docs/images/schema-voyager.png)

Le relazioni vengono registrate, approvate e applicate come gli unici percorsi JOIN legali:

![Relationships](docs/images/relationships.png)

## Modello di sicurezza

È qui che "sul percorso che ogni query già percorre" smette di essere uno slogan. Provisa applica un modello di sicurezza multilivello su ogni linguaggio di query (GraphQL, SQL, Cypher) e ogni trasporto (REST, gRPC, Arrow Flight, JDBC, pgwire, Bolt, WebSocket). La governance è applicata in modo uniforme — non esiste un percorso di query che la bypassi. La copertura è totale per costruzione, non per diligenza: aggiungi un'origine, una colonna o una relazione e ogni livello si applica automaticamente, senza nulla da ricordare di registrare.

I livelli si applicano in ordine. Una richiesta deve superare ogni livello prima che il successivo venga valutato.

### Livello 0 — Filtraggio dell'introspezione

Lo schema e il catalogo presentati a un ruolo contengono solo le tabelle nella sua lista `domain_access` e le colonne che superano le regole `visible_to` per colonna. Gli oggetti fuori dall'accesso di un ruolo sono invisibili al momento della scoperta — non possono essere interrogati, autocompletati, o dedotti come esistenti. Questo si applica allo schema GraphQL, al catalogo SQL e al browser di schema dell'editor di query.

### Livello 1 — Accesso pubblico

Le tabelle in domini senza restrizione `domain_access` sono visibili a tutte le identità autenticate senza configurazione aggiuntiva. Zero attrito per i dati genuinamente pubblici.

### Livello 2 — Accesso al dominio

Ogni ruolo porta una lista `domain_access` di ID di dominio. Una query che tocca una tabella fuori da quei domini viene rifiutata prima dell'esecuzione. Questo è il confine di ownership grezzo — un ruolo HR non può raggiungere le tabelle finance indipendentemente da come è scritto l'SQL.

### Livello 3 — Sicurezza a livello di riga

Dopo che l'accesso al dominio è confermato, i predicati `WHERE` per tabella e per ruolo vengono iniettati in ogni `SELECT` al momento dell'esecuzione. I predicati vengono valutati contro i dati grezzi. Un regional manager che interroga una tabella orders condivisa vede solo le righe della propria regione anche su una `SELECT *`.

### Livello 4 — Visibilità e mascheramento delle colonne

Le colonne con una lista `visible_to` che esclude il ruolo richiedente vengono rimosse dall'output della query. Le colonne con una regola di mascheramento hanno i propri valori sostituiti — redazione regex, sostituzione con costante o troncamento — prima che i risultati lascino il server. Il mascheramento si applica in tutti i linguaggi di query e formati di output.

### Livello 5 — Guardia dei predicati

Le colonne mascherate vengono rifiutate dalle clausole `WHERE` e `HAVING`. Senza questo, un chiamante potrebbe inferire il valore non mascherato facendo una ricerca binaria in un filtro anche se l'output è mascherato. Il rifiuto viene applicato al momento del parsing della query, prima dell'esecuzione.

### Governance delle relazioni

Le condizioni JOIN in SQL devono corrispondere a una relazione registrata e approvata tra le tabelle. I join non approvati vengono rifiutati. Ogni relazione porta un motivo e una descrizione leggibili dall'uomo — una guida sia per gli utenti che per gli agenti autonomi sul perché esiste un percorso di attraversamento. Questa è policy di governance, non un confine di sicurezza rigido: i Livelli 2-5 restano validi indipendentemente dalla struttura del join, quindi un'elusione deliberata non espone dati che il ruolo non potrebbe raggiungere con due query separate. I tentativi di elusione vengono registrati e sottoposti ad audit.

---

Questi livelli si compongono. Un ruolo con accesso al dominio, RLS e colonne mascherate ha tutti e cinque i vincoli attivi simultaneamente. Aggiungere una nuova origine dati, colonna o relazione non richiede di aggiornare ogni regola — ogni livello è configurato in modo indipendente e si applica automaticamente a qualsiasi query che tocchi oggetti governati.

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
2. Rendilo eseguibile ed eseguilo — il primo avvio completa una configurazione una tantum (nessuna connessione internet richiesta):

```bash
chmod +x Provisa-*-linux-x86_64.AppImage
./Provisa-*-linux-x86_64.AppImage
provisa start && provisa open
```

### Windows

1. Scarica [Provisa-windows-x64.exe](https://provisa.dev/dl/windows) (sempre l'ultima release)
2. Esegui l'installer — nessun diritto di amministratore richiesto
3. Apri **Provisa First Launch** dal menu Start — completa una configurazione una tantum (~5 min, nessuna connessione internet richiesta)
4. Apri un nuovo terminale:

```bash
provisa start
```

### Prima query

In sviluppo locale (`PROVISA_MODE=test`), non sono richieste credenziali. In produzione, autenticati con un token Bearer — il ruolo viene estratto automaticamente da esso.

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

Autenticati con il tuo username e password Provisa — il server assegna il tuo ruolo.

- **modalità `catalog`** — schema completo visibile; da usare con strumenti di catalogo (Collibra, Atlan, DBeaver)

Vedi [docs/integrations.md](docs/integrations.md) per i passaggi di configurazione di Tableau e Power BI.

### Protocollo di trasporto PostgreSQL (pgwire)

Provisa parla il protocollo di trasporto PostgreSQL sulla porta 5439. Qualsiasi client in grado di connettersi a Postgres si connette a Provisa — nessun driver, nessun adattatore, nessuna modifica agli strumenti esistenti.

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

Tutte le query passano attraverso la pipeline di governance completa — accesso al dominio, RLS, mascheramento e guardia dei predicati si applicano esattamente come fanno per GraphQL e REST. I browser di schema (DBeaver, DataGrip, pgAdmin) funzionano senza configurazione: le query `pg_catalog` e `information_schema` vengono risposte da un catalogo in memoria delimitato dall'accesso al dominio del ruolo, così gli utenti vedono solo le tabelle e le colonne che sono autorizzati a interrogare.

DataGrip mentre esplora lo schema governato e il suo diagramma delle chiavi esterne su pgwire — nessun driver, nessun adattatore:

![Provisa in DataGrip over pgwire](docs/images/pgwire-datagrip.png)

TLS viene abilitato impostando `PROVISA_PGWIRE_CERT` e `PROVISA_PGWIRE_KEY`. La porta è configurabile tramite `PROVISA_PGWIRE_PORT` (default `5439`).

### Bolt (protocollo di trasporto Neo4j)

Provisa parla anche il protocollo **Bolt** di Neo4j, così gli strumenti nativi per grafi si connettono direttamente ed eseguono Cypher contro il grafo federato — nessuna esportazione, nessun database a grafo separato. Punta **Neo4j Browser** o **Bloom** verso Provisa e attraversa le relazioni tra le origini con la stessa governance (accesso al dominio, RLS, mascheramento) applicata.

Neo4j Browser che esegue Cypher contro Provisa — etichette dei nodi, tipi di relazione e chiavi delle proprietà provengono direttamente dallo schema registrato:

![Provisa in Neo4j Browser over Bolt](docs/images/bolt-neo4j-browser.png)

Abilitalo impostando `PROVISA_BOLT_PORT` (il default di Neo4j è `7687`). TLS viene abilitato con `PROVISA_BOLT_CERT` e `PROVISA_BOLT_KEY`. Ogni ruolo Provisa che l'utente autenticato possiede appare come un database selezionabile `provisa_<role>` (il selettore `provisa_admin` sopra) — sceglierne uno restringe la sessione ai diritti di dominio di quel ruolo; l'utente non può mai eccedere i ruoli che possiede.

### Client Python

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

# role + mode URL parameters (mode=catalog for arbitrary SQL)
engine = create_engine(
    "provisa+http://alice:secret@localhost:8001?role=analyst&mode=catalog"
)

# ADBC — Arrow-native streaming via Flight
from provisa_client.adbc import adbc_connect
with adbc_connect("http://localhost:8001", user="alice", password="secret") as conn:
    with conn.cursor() as cur:
        cur.execute("{ orders { id amount } }")
        table = cur.fetch_arrow_table()
```

Vedi [docs/python-client.md](docs/python-client.md) per il riferimento completo.

## Documentazione

| Argomento | Documento |
| --- | --- |
| Guida rapida per sviluppatori (esecuzione dal sorgente) | [docs/quickstart.md](docs/quickstart.md) |
| Riferimento completo alla configurazione YAML | [docs/configuration.md](docs/configuration.md) |
| Riferimento degli endpoint (GraphQL, REST, Flight, gRPC) | [docs/api-reference.md](docs/api-reference.md) |
| Design di sistema e mappa dei componenti | [docs/architecture.md](docs/architecture.md) |
| Modello di sicurezza (RLS, mascheramento, autenticazione) | [docs/security.md](docs/security.md) |
| Tipi di origine supportati | [docs/sources.md](docs/sources.md) |
| Subscription SSE | [docs/subscriptions.md](docs/subscriptions.md) |
| JDBC, strumenti BI, client Arrow Flight, Apollo Federation | [docs/integrations.md](docs/integrations.md) |
| Client Python (`provisa-client`) | [docs/python-client.md](docs/python-client.md) |
| Admin API | [docs/admin.md](docs/admin.md) |
| Deployment (Docker Compose, Kubernetes, macOS) | [docs/deployment.md](docs/deployment.md) |
| Import Hasura v2 / DDN | [docs/import.md](docs/import.md) |
| Workflow di release (tag alpha/beta/stable) | [docs/releasing.md](docs/releasing.md) |

## Dimensionamento

Provisa include un motore di federazione integrato per le query multi-origine. Al primo avvio scegli un budget di RAM; Provisa deriva automaticamente il numero di worker di federazione locali.

| RAM host | Worker | Carico di lavoro tipico |
| --- | --- | --- |
| < 24 GB | 0 | Sviluppo, query su singola origine, team piccoli |
| 24–47 GB | 1 | Team piccolo, query cross-source moderate |
| 48–95 GB | 2 | Deployment dipartimentale, uso misto BI + notebook |
| 96 GB+ | 4 | Grande dipartimento, federazione concorrente pesante |

Il numero di worker può essere modificato in qualsiasi momento modificando `~/.provisa/config.yaml` (`federation_workers: N`) ed eseguendo `provisa restart`. Imposta a `0` per eseguire in modalità solo coordinamento (single-node).

### Scalare oltre una singola macchina

**Scale-out orizzontale** — Esegui più istanze Provisa dietro un load balancer. Ogni istanza è un sistema pienamente funzionante. Tutte le istanze devono puntare allo stesso config DB (imposta `CONFIG_DB_HOST` sulle macchine secondarie) e opzionalmente a un'istanza Redis condivisa (`REDIS_URL`) per una cache unificata. La maggior parte delle query si distribuisce in modo trasparente; join cross-source molto grandi possono eccedere le risorse di una singola istanza e richiedere una macchina più grande o un cluster di federazione esterno.

**Redis condiviso** — Imposta `REDIS_URL` su ogni istanza per puntare a un Redis esterno. Redis condiviso significa che le voci di cache di un'istanza sono disponibili per tutte, migliorando gli hit rate sull'intero cluster.

**Porta il tuo cluster di federazione** — Punta Provisa verso un cluster di federazione esterno esistente invece dei worker integrati. Consigliato per deployment su larga scala o cloud; vedi [docs/deployment.md](docs/deployment.md) per la configurazione.

## Licenza

Business Source License 1.1 (non modificata, secondo i covenant del Licenziante di MariaDB). Ogni
versione rilasciata si converte nella Change License (GPL v2.0 o successiva) al 4°
anniversario della sua release pubblica; il codice attuale e recente resta sotto BSL.
L'uso in produzione al di sopra delle soglie dell'Additional Use Grant (meno di 100
dipendenti/collaboratori e sotto 1M$ di fatturato dell'anno precedente) richiede una licenza
commerciale. Vedi [LICENSE](LICENSE).

Il Licenziante non acconsente all'uso di quest'opera per l'addestramento AI/ML. Vedi
[NOTICE](NOTICE), [ai.txt](ai.txt) e [robots.txt](robots.txt). Per licenze commerciali
o di addestramento AI: <kennethstott@gmail.com>
