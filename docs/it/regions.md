# Regioni {: #regions }

Provisa vi offre due primitive per collocare i dati: una **regione** e l'**assegnazione a una regione**, cioè creare una regione e assegnarle una tabella. Nessuna delle due è di per sé una policy. L'uso che ne fate rientra in tre gruppi:

- **Prestazioni.** La latenza verso chi legge, quanto è recente la sua copia, e l'adeguatezza del motore: dove si trova il suo data center, che tipo di storage legge, quanta CPU e memoria ha.
- **Costi.** Dove gira il lavoro e su cosa: hardware di proprietà o cloud, un motore grande o uno piccolo, e quanto costa spostare dati tra luoghi (egress).
- **Sovranità.** Chi controlla i dati e dove possono andare: le regole di residenza della legge (dove i dati personali possono essere elaborati e dove possono essere conservati) e le linee di controllo proprie di un'organizzazione, come una business unit o una controllata che tiene dati e infrastruttura sotto il proprio controllo.

Il meccanismo è lo stesso per tutti e tre. Cambiano solo le regole che scrivete sopra.

## Una regione è un motore con i suoi store in un luogo {: #a-region-is-an-engine-and-its-stores-in-a-place }

Una regione è un luogo in cui Provisa lavora e conserva ciò che il lavoro lascia. Ogni regione ha il proprio motore e i propri store. (REQ-1922) [tool-verified: `provisa/core/regions.py` `OrgRegion`, `STORE_ROLES`]

| Store | Cosa contiene |
| --- | --- |
| `engine` | Il motore di federazione che esegue le istruzioni in questa regione |
| `replicas` | Le repliche delle tabelle assegnate qui e delle tabelle senza regione che questa regione copia |
| `views` | Le copie delle viste materializzate di questa regione |
| `cache` | Le risposte in cache |
| `state` | Lo stato di build e di aggiornamento |
| `record` | Il registro di ogni richiesta servita qui |

Un unico modello governa ogni regione. Sorgenti, tabelle, relazioni, ruoli, regole di riga e di colonna, viste e impostazioni di freschezza si dichiarano una volta, in un unico model store, e ogni regione serve lo stesso modello, così le definizioni non possono divergere tra regioni. Una modifica raggiunge ogni regione con lo stesso stamp, e ogni regione poi vi converge. (REQ-1914, REQ-1922)

La piattaforma dichiara le regioni fisiche in cui girano i suoi nodi, con l'indirizzo a cui risponde ciascuna. Il modello di ogni organizzazione sceglie quali di queste regioni usa e nomina il proprio store per ogni ruolo in ogni regione. Un nodo parte in una regione con `provisa run --region <id>` e serve quella regione per ogni organizzazione che la sceglie. Con le regioni dichiarate, un nodo avviato senza `--region` rifiuta di partire ed elenca le regioni che potrebbe servire. (REQ-1922) [tool-verified: `provisa/cli.py` `--region`; `provisa/core/regions.py` `validate_regions`]

```yaml
platform:
  regions:
    - { id: eu, address: "https://eu.provisa.example" }
    - { id: us, address: "https://us.provisa.example" }

stores:
  - { id: eu_engine, url: "${secret:eu-trino-url}", kind: trino }
  - { id: us_engine, url: "${secret:us-trino-url}", kind: trino }
  - { id: eu_data,   url: "${secret:eu-pg-url}" }
  - { id: us_data,   url: "${secret:us-pg-url}" }
  - { id: eu_cache,  url: "${secret:eu-redis-url}" }
  - { id: us_cache,  url: "${secret:us-redis-url}" }

regions:
  - { id: eu, engine: eu_engine, replicas: eu_data, views: eu_data, cache: eu_cache, state: eu_data, record: eu_data }
  - { id: us, engine: us_engine, replicas: us_data, views: us_data, cache: us_cache, state: us_data, record: us_data }
```
[inferred: the URLs and secret names are placeholders; the shape follows `PlatformRegion`, `StoreConfig` and `OrgRegion` in `provisa/core/regions.py`]

Al caricamento, la configurazione viene rifiutata, indicandone il motivo, se le regioni non sono coerenti. Una regione deve nominare uno store dichiarato per ogni ruolo. Le sue `replicas` e `views` devono essere lo stesso store, e il suo store del motore deve nominare un `kind` di motore. Con più di una regione di piattaforma, un file di motore incorporato viene rifiutato, perché le altre regioni non possono raggiungerlo. [tool-verified: `require_one_materialize_store`, `require_engine_kind`, `require_reachable_engine`]

Più regioni possono condividere un'istanza di database, un Redis o un bucket di object storage. Tutto ciò che una regione conserva porta la regione nel nome, così i loro store non entrano in collisione. (REQ-1922)

Un deployment che non dichiara regioni ha una regione implicita e non mostra la regione da nessuna parte. Non c'è il campo regione né l'opzione `--region`, e una configurazione che nomina una regione viene rifiutata. (REQ-1922)

## Assegnazione a una regione: una tabella nomina la sua regione di casa {: #region-homing-a-table-names-its-home }

Una tabella può nominare una delle regioni della sua organizzazione come regione di casa. È l'unica regione che costruisce e aggiorna la sua replica, e l'unica che può tenerla hot. Ogni altra regione legge la tabella dalla replica della regione di casa, sul posto, all'indirizzo indicato dal modello. Le regole di riga e di colonna di chi legge entrano in quell'istruzione, così il filtraggio avviene nella regione di casa e attraversa il confine solo ciò che chi legge può vedere. (REQ-1921) [tool-verified: `provisa/core/regions.py` module docstring]

```yaml
tables:
  - source_id: eu-shop
    schema_name: public
    table_name: customers
    region: eu
```
Alcune conseguenze:

- Nessun'altra regione legge la sorgente della tabella; leggono solo la replica della regione di casa. Una sorgente che replicate per proteggerla dal carico resta protetta.
- Se la replica non è stata costruita o la regione di casa non è raggiungibile, la richiesta viene rifiutata nominando la regione di casa. La sorgente non viene letta live al suo posto.
- Una tabella assegnata che non viene replicata, con `replicate` Never o con un'impostazione che non ha ancora richiesto una copia, viene letta live dalla sua sorgente da ogni regione, come qualsiasi tabella non replicata.
- Il conteggio hot di una tabella è solo il traffico della sua regione di casa. (REQ-239)
- Anche una vista materializzata può nominare una regione. Viene allora costruita solo lì, e ogni altra regione legge quella copia, esattamente come per una tabella assegnata.

Una regione che legge la replica di un'altra ha bisogno di un motore in grado di leggerla: `duckdb`, `motherduck`, `pg`, `trino` o `trino-byo`. Lo store `replicas` della regione di casa deve essere un server PostgreSQL. La configurazione nomina entrambi quando non sono adatti. [tool-verified: `require_readable_elsewhere`; `REGION_READERS` in `provisa/core/engine_kinds.py`]

Anche una sorgente può nominare una regione, ma quella regione si limita a precompilare il modulo quando registrate una nuova tabella della sorgente. Non decide nulla su dove vivono i dati, e cambiarla in seguito non sposta alcuna tabella. (REQ-1921)

## Le tabelle senza regione vengono copiate dove vengono lette {: #unhomed-tables-are-copied-where-they-are-read }

Una tabella che non nomina una regione può essere copiata in ogni regione. Quando la sua impostazione `replicate` richiede una copia, la regione a cui si è collegato chi legge ne costruisce una nel proprio store. Una vista materializzata che non nomina una regione viene costruita in ogni regione, ogni copia nello storage delle viste di quella regione. (REQ-1921)

Aggiungere una regione significa quindi più copie e più lavoro di aggiornamento. Mai più governance. Un solo modello governa ogni copia, e in ogni regione valgono le stesse regole di riga e di colonna. La build di una vista in una regione è governata come l'amministratore dell'organizzazione che agisce in quella regione, così la copia di ogni regione contiene ciò che lì può essere conservato. (REQ-1921, REQ-1922)

## Tre motivi per usare le regioni {: #three-reasons-to-use-regions }

### Prestazioni {: #performance }

Le regioni usate solo per le prestazioni non dichiarano regole tra regioni. Tabelle, ruoli e regole sono quelli che scrivereste per una sola regione, e l'assegnazione decide dove avviene il lavoro:

- **Latenza.** Chi legge si collega alla regione vicina, il cui motore, cache e copie sono locali.
- **Freschezza.** Una tabella assegnata dove cambia viene aggiornata lì, vicino alla sua sorgente.
- **Adeguatezza del motore.** Ogni regione esegue il proprio motore, quindi potete adattare un motore a un luogo: il data center in cui si trova, lo storage che legge, la CPU e la memoria che ha.

**Esempio: due regioni identiche.** Dichiarate `east` e `west` in due data center, ciascuna con lo stesso tipo di motore e gli stessi tipi di store, e non assegnate alcuna tabella. Ogni regione serve chi legge più vicino a lei: costruisce le proprie copie di ciò che leggono e mette in cache le loro risposte. Le due possono stare dietro un load balancer e dividersi il carico. C'è un solo calendario di aggiornamento, quello del modello, ed entrambe le regioni iniziano ad aggiornare le proprie copie secondo quello, ma i loro motori potrebbero non finire nello stesso tempo. In un dato momento non è quindi garantito che siano nello stesso stato, e chi legge e passa da una regione all'altra può vedere dati di freschezza diversa. Provisa non sposta un client nell'altra regione quando una si ferma; dove va un client lo decide il load balancer. [inferred: no failover or cross-region redirect exists in `provisa/`; clients connect to a region by its address, and each region builds and refreshes only its own copies, on the model's one schedule (REQ-1922)]

### Costi {: #cost }

Nemmeno le regioni di costo dichiarano regole tra regioni. Portano il lavoro dove costa meno eseguirlo:

- **Hardware di proprietà e cloud.** Tenete una regione on-premise, su hardware che già possedete, per le tabelle e chi legge lì, ed eseguite un'altra regione nel cloud per il resto.
- **Dimensione del motore.** Eseguite un motore grande in una regione per il lavoro pesante occasionale e uno piccolo in un'altra per le letture di tutti i giorni. Chi legge sceglie la regione dal suo indirizzo.
- **Egress.** Chi legge in altre regioni riceve solo le righe filtrate della replica di una tabella assegnata, e una tabella grande assegnata a una regione non viene copiata in ogni regione.

### Sovranità {: #sovereignty }

Le regioni usate per la sovranità dichiarano invece regole tra regioni, che il motivo sia la legge o l'organizzazione: una controllata che assegna le sue tabelle alla propria regione le fa elaborare e conservare sulla propria infrastruttura, e decide con queste regole cosa legge il resto del gruppo. Sono normali regole di riga e di colonna, scritte su attributi che dicono dove si trova chi legge, o il nodo che fa il lavoro:

- `current_setting('provisa.region')` è la regione del nodo che risponde alla richiesta, quando la piattaforma dichiara regioni. Un filtro di riga scritto su di esso tiene le righe di una regione in quella regione. (REQ-1922) [tool-verified: `provisa/core/request_context.py` `session_vars_for`]
- L'attributo `region` di un membro gli fa detenere i pseudo-ruoli `region` e `region__<value>`, così una voce di maschera o di concessione può nominare `analyst+region__eu`. Il lavoro in background di un nodo porta la sua regione allo stesso modo. (REQ-2057) Vedere [Ruoli: concessi, pseudo e meta](security.md#roles-granted-pseudo-and-meta).

Insieme all'assegnazione, queste regole decidono cosa attraversa un confine. I dati personali di una tabella assegnata vengono conservati ed elaborati nella sua regione di casa, e chi legge altrove riceve solo ciò che le sue regole consentono, filtrato a casa. Una vista senza regione costruita in ogni regione contiene ciò che un amministratore di quella regione può vedere, così la copia in una regione può includere righe che la copia in un'altra regione omette. (REQ-1921)

## Chi può assegnare una tabella {: #who-may-home-a-table }

Impostare, cambiare o rimuovere la regione di una tabella, di una sorgente o di una vista materializzata sposta dati attraverso un confine. Richiede il diritto `data_residency` oltre alla proprietà dell'oggetto tramite il suo dominio. La concessione nomina i valori che copre: le regioni dell'organizzazione e "nessuna regione". Spostare un oggetto tra due regioni richiede una concessione che le copra entrambe. Un salvataggio fuori dalla concessione viene rifiutato, nominando il valore non coperto. (REQ-1921)

Il diritto esiste solo quando la piattaforma dichiara regioni. La proprietà del dominio continua a decidere chi può modificare tutto il resto dell'oggetto. Una regione non conferisce alcun diritto proprio; un'organizzazione che vuole che le tabelle di una regione siano gestite dalle persone di quella regione dà loro domini propri. (REQ-1921)

Una tabella o una vista può anche essere messa in **bozza**: fuori servizio, senza copia in alcuna regione. Una tabella registrata dall'amministrazione parte in bozza. Uno spostamento tra regioni può passare dalla bozza, così nel frattempo nessun dato viene conservato o letto da nessuna parte: il lato della prima regione imposta la bozza, e il titolare della seconda regione rivendica l'oggetto impostando la propria regione. (REQ-1921)

Vedere anche la [demo a due regioni](regions-demo.md).
