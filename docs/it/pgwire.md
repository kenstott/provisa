# Server pgwire di Provisa

Provisa espone un endpoint del protocollo di rete di PostgreSQL (pgwire). Qualsiasi strumento che parla il protocollo client di PostgreSQL — psycopg2, asyncpg, DBeaver, Tableau, JDBC — può connettersi ed eseguire query sui dati di Provisa attraverso la stessa pipeline di governance che governa l'API HTTP. (REQ-266)

Le query attraversano l'intero stack di governance: applicazione della sicurezza a livello di riga, regole di mascheramento, protezioni sulle relazioni, controlli di accesso al dominio. (REQ-001, REQ-002, REQ-263) L'interfaccia pgwire non è un modo per aggirare i controlli. (REQ-002, REQ-266)

---

## Dettagli di connessione

Il server si avvia quando `PROVISA_PGWIRE_PORT` è impostato su un intero diverso da zero. È disabilitato per impostazione predefinita. (REQ-527) [tool-verified: `app.py:1739`]

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Impostare `PROVISA_PGWIRE_CERT` e `PROVISA_PGWIRE_KEY` sui percorsi di un certificato e di una chiave PEM. Quando entrambi sono presenti, il server incapsula le connessioni in entrata in TLS. Quando sono assenti, TLS è disattivato e il server risponde `N` alle richieste di negoziazione SSL. (REQ-530) [tool-verified: `server.py:1746-1750`]

**Versione del server segnalata.** I client vedono `14.0.provisa`. Gli strumenti che attivano funzionalità in base al numero di versione possono comportarsi come se fossero connessi a PostgreSQL 14. (REQ-579) [tool-verified: `server.py:208`]

---

## Autenticazione

Il pacchetto di startup porta un nome utente e un solo campo segreto, senza alcuno schema che dica cosa sia quel segreto. Provisa decide dal segreto stesso, così un client non ha bisogno di configurazione oltre a `user` e `password`:

| Il segreto è | Riconosciuto da | Si risolve in |
| --------------- | --------------- | ------------- |
| Un token di accesso personale | il suo prefisso `provisa_pat_` | il proprietario e il ruolo del token (REQ-1263) |
| Un token bearer OIDC / del provider | il fatto che il provider configurato sia un provider di token | l'identità che il token afferma (REQ-890) |
| Una password | qualsiasi altra cosa | l'account nel provider configurato (`basic` o `simple`) |

La decisione viene presa una sola volta. Una credenziale rifiutata dal validatore scelto non viene ritentata con un altro, quindi un rifiuto non diventa un secondo tentativo.

La modalità trust (`provider: none`, o middleware di autenticazione inattivo) è l'eccezione: il nome utente viene usato direttamente come `role_id` e il segreto viene ignorato. Non usarla su una connessione non cifrata.

**SCRAM-SHA-256.** Con `provider: basic` e `auth.scram: true`, il server annuncia SASL (codice di autenticazione 10) con `SCRAM-SHA-256` e la password viene dimostrata anziché inviata. (REQ-1394) `SCRAM-SHA-256-PLUS` non viene offerto. A un utente il cui verifier non è ancora stato scritto — i verifier non possono essere derivati dagli hash bcrypt — viene risposto con uno scambio fittizio, così la rete non rivela chi ha migrato; quell'utente si autentica con password in chiaro su TLS finché l'immissione successiva della password non ne scrive uno. Con `auth.scram` disattivato, il server usa il tipo di autenticazione PG 3 (password in chiaro). MD5 non è supportato in nessuno dei due casi.

**Certificati client.** Imposta `PROVISA_MTLS_CLIENT_CA` e il server verifica un certificato client durante l'handshake, prima di esaminare qualsiasi credenziale. (REQ-1228) Con `PROVISA_MTLS_BIND_PRINCIPAL` il common name del certificato deve coincidere con lo `user` con cui la connessione si autentica subito dopo. Vedi [Configurazione](configuration.md#mutual-tls).

**I tentativi falliti vengono contati.** Cinque fallimenti in cinque minuti bloccano l'account per quindici minuti, e il contatore è condiviso con HTTP e Bolt: un blocco ottenuto su una superficie vale su tutte. (REQ-1393)

**Scegliere un'organizzazione.** In un deployment multi-organizzazione, connettiti a `<org>.<il-tuo-dominio>` e pgwire legge l'organizzazione dall'hostname nel ClientHello TLS, allo stesso modo in cui HTTP la legge dall'header `Host`. (REQ-1234) L'hostname richiede un'organizzazione; non la concede, e un principal che non ne è membro viene rifiutato. Connettersi tramite indirizzo IP non richiede alcuna organizzazione.

---

## Cosa funziona

### SELECT

Tutte le istruzioni SELECT attraversano la pipeline di governance (`_pipeline.py`). (REQ-001, REQ-262, REQ-266) La pipeline:

1. Riscrive l'SQL semantico in SQL fisico (`rewrite_semantic_to_physical`)
2. Applica la governance (sicurezza a livello di riga, mascheramento, accesso al dominio) (REQ-263)
3. Valida rispetto allo schema registrato (REQ-011)
4. Instrada verso Trino o direttamente verso il pool dell'origine (REQ-027, REQ-028)

Sono supportate query semplici multi-istruzione. Le istruzioni separate da punto e virgola vengono suddivise ed eseguite in ordine. (REQ-580) [tool-verified: `server.py:318-381`]

Le query parametrizzate (`$1`, `$2`, ...) sono supportate sia in modalità query semplice sia in modalità query estesa (Bind/Execute). I parametri vengono sostituiti come letterali prima dell'esecuzione. (REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` e `SELECT fn(args)` — dove `fn` indica una funzione registrata e tracciata — vengono intercettati prima della pipeline di governance e instradati attraverso l'unico esecutore governato (`invoke_tracked_function`). Il risultato è un insieme di righe tipizzato identico a quello restituito da ogni altra superficie per quel comando. `writable_by` e le regole di governance vengono applicate all'interno dell'esecutore. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### DDL

Le istruzioni DDL vengono rilevate tramite l'espressione regolare in `server.py` e inoltrate a `DdlHandler`. Il ruolo deve possedere la capability `"ddl"`. (REQ-042) Senza di essa, l'istruzione viene rifiutata con SQLSTATE 42501. [tool-verified: `ddl_handler.py:82-83`]

Le forme di DDL riconosciute sono:

```sql
CREATE TABLE / VIEW / INDEX / UNIQUE INDEX / SEQUENCE / SCHEMA
ALTER TABLE / INDEX / SEQUENCE / VIEW
DROP TABLE / VIEW / INDEX / SEQUENCE / SCHEMA
```

[tool-verified: `server.py:56-61`]

Esistono due percorsi di esecuzione a seconda di `ddl_catalog`: (REQ-582)

**Percorso Trino** — usato quando `ddl_catalog` è un catalogo Trino Iceberg, Hive o un altro catalogo non registrato (ad es. `iceberg`, `hive`, `otel`, `results`). Su questo percorso sono supportati solo `CREATE TABLE` e `CREATE VIEW`. Tentare `ALTER`, `DROP` o `CREATE INDEX` genera un errore. Il nome della tabella è pienamente qualificato come `catalog.schema.table`. [tool-verified: `ddl_handler.py:92-100`]

**Percorso diretto** — usato quando `ddl_catalog` corrisponde a un ID di origine registrato. È supportato il DDL completo: CREATE, ALTER, DROP, indici, sequenze. `CREATE TABLE` e `CREATE VIEW` sono qualificati per schema come `schema.table`. Tutto il resto del DDL (ALTER, DROP, CREATE INDEX) viene passato così com'è dopo aver impostato il contesto dello schema. Per le origini PostgreSQL e SQLite, il contesto viene impostato con `SET search_path TO schema`. Per MySQL e MariaDB, con `USE schema`. [tool-verified: `ddl_handler.py:139-170`, `ddl_handler.py:207-213`]

Dopo il DDL su entrambi i percorsi, la nuova tabella viene registrata nel contesto di compilazione del ruolo in modo da essere immediatamente interrogabile. (REQ-583) [tool-verified: `ddl_handler.py:216-250`]

**Risoluzione della destinazione di scrittura.** Il catalogo e lo schema DDL provengono dai campi `ddl_catalog` e `ddl_schema` del dominio. Se `ddl_catalog` non è impostato, il sistema utilizza per impostazione predefinita il catalogo Iceberg. Se `ddl_schema` non è impostato, utilizza per impostazione predefinita l'ID del dominio. Il dominio viene risolto tramite l'elenco `domain_access` del ruolo. (REQ-584) [tool-verified: `app.py:804-811`, `ddl_handler.py:104-115`]

### COPY

`COPY ... TO STDOUT` e `COPY ... FROM STDIN` sono entrambi supportati. (REQ-585) [tool-verified: `copy_handler.py:231-257`]

**COPY TO STDOUT** — esporta i risultati della query nel formato di rete COPY di PG. Funzionano due forme:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

Formati supportati: `text` (delimitato da tabulazioni, predefinito) e `csv`. Il formato binario non è supportato in output da COPY. [tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** — inserisce righe in una tabella di destinazione. Limitato a origini di tipo `postgresql`, `mysql`, `sqlite` o `mariadb`. (REQ-586) Tentare COPY FROM su un'origine esclusivamente Trino (ad es. Iceberg) genera un errore di autorizzazione. [tool-verified: `copy_handler.py:65`, `copy_handler.py:351-356`]

```sql
COPY my_table (col1, col2) FROM STDIN WITH (FORMAT text)
```

Se non viene fornito un elenco di colonne, le colonne vengono dedotte dallo schema registrato. [tool-verified: `copy_handler.py:357`]

### Transazioni e comandi di sessione

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET e DEALLOCATE vengono intercettati e restituiscono una risposta di successo vuota. (REQ-587) Il server è stateless rispetto alle transazioni — non esiste isolamento delle transazioni né supporto per il rollback. (REQ-587) [tool-verified: `catalog.py:27-31`, `catalog.py:1129-1132`]

---

## Intercettazione del catalogo

Le query su `information_schema` e `pg_catalog` vengono risolte localmente senza un round-trip verso Trino. (REQ-532) Il livello di intercettazione costruisce un database DuckDB in memoria per ogni richiesta, popolato a partire dal contesto di compilazione del ruolo. (REQ-532) [tool-verified: `catalog.py:210-213`]

Tabelle intercettate:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`, `key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`, `pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`, `pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`, `pg_extension`, `pg_enum`, `pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` viene popolata con dati reali di chiave primaria e chiave esterna derivati dai campi `pk_columns` e `joins` del modello di dominio. (REQ-392, REQ-399) Gli strumenti di BI che ispezionano le relazioni di chiave esterna (Tableau, DBeaver, ecc.) vedranno il grafo di join che Provisa conosce. [tool-verified: `catalog.py:551-632`] I join a colonna singola tra la stessa coppia origine/destinazione le cui colonne di destinazione formano insieme la chiave primaria composita della destinazione vengono raggruppati in un'unica riga FK con array `conkey`/`confkey` a più elementi. (REQ-1094) [tool-verified: `catalog_constraints.py`]

`pg_index` viene popolata con una riga per ogni vincolo di chiave primaria e UNIQUE (`indrelid` = oid della tabella, `indkey` = attnum delle chiavi in ordine, `indisprimary`/`indisunique` impostati). I client che risolvono le colonne chiave tramite `pg_index.indkey` anziché tramite `pg_constraint` — DataGrip, ad esempio — individuano le colonne corrette tramite il join standard `pg_index` → `pg_attribute`. (REQ-1095) [tool-verified: `catalog_constraints.py:340-384`]

Vengono intercettate anche le seguenti espressioni scalari: (REQ-588)

- `current_user`, `session_user` → il `role_id` autenticato
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → restituisce un valore da una tabella di impostazioni fissa
- `SHOW <setting>` → restituisce un valore dalla stessa tabella di impostazioni

[tool-verified: `catalog.py:168-207`, `catalog.py:1076-1120`]

---

## Codifica binaria dei parametri

Il protocollo di query estesa (Bind/Execute) supporta parametri codificati in binario. (REQ-589) I seguenti OID di tipo vengono decodificati dal binario: [tool-verified: `postgres.py:69-97`]

| OID | Tipo PG | Tipo Python |
| ----- | --------- | ------------- |
| 16 | bool | bool |
| 17 | bytea | bytes |
| 20 | int8 | int |
| 21 | int2 | int |
| 23 | int4 | int |
| 25 | text | str |
| 700 | float4 | float |
| 701 | float8 | float |
| 1043 | varchar | str |
| 1082 | date | datetime.date |
| 1114 | timestamp | datetime.datetime |
| 1184 | timestamptz | datetime.datetime (UTC) |
| 1700 | numeric | decimal.Decimal |
| 2950 | uuid | str |

Qualsiasi OID non presente in questa tabella genera `"Unsupported binary parameter type: <oid>"`. (REQ-589) [tool-verified: `postgres.py:579`]

Anche le colonne dei risultati vengono inviate in binario quando il client lo richiede, per lo stesso insieme di tipi più ARRAY, JSON, INTERVAL e BIGINT. (REQ-589) [tool-verified: `postgres.py:191-244`]

---

## Raccomandazioni sui driver

**Driver Python nativi (psycopg2, asyncpg).** Questi negoziano il protocollo di query estesa per impostazione predefinita e utilizzano la codifica binaria per la maggior parte dei tipi. La fedeltà dei tipi qui è massima — le colonne `NUMERIC` arrivano come `Decimal`, `TIMESTAMP` come `datetime`, e così via. Usarli per ETL basati su Python, script o integrazione diretta.

**JDBC (driver JDBC di PostgreSQL).** Usarlo per gli strumenti dell'ecosistema Java: DBeaver, Tableau, Power BI, Metabase, operatori JDBC di Airflow. JDBC utilizza per impostazione predefinita il protocollo di query semplice, che evita complicazioni legate alla codifica binaria. Stringa di connessione:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Alcuni strumenti di BI basati su JDBC inviano, alla connessione, una raffica di query verso `information_schema` e `pg_catalog` per popolare il proprio browser di schema. Tutte vengono risolte dal livello di intercettazione del catalogo — non viene generato traffico verso Trino durante l'ispezione dello schema. (REQ-532)

**Quando preferire l'uno o l'altro.** Se il client è in Python, usare psycopg2 o asyncpg per una migliore gestione dei tipi. Se il client è uno strumento di BI o qualsiasi applicazione JVM, usare JDBC. Evitare di mescolare aspettative di protocollo binario e testuale sulla stessa connessione se si osservano anomalie nella conversione dei tipi — il comportamento in modalità testo di JDBC è più semplice da interpretare.

---

## Avvertenze e vincoli

**Solo SQL; nessuna mutazione DML.** Il listener pgwire analizza ed esegue esclusivamente SQL — le stringhe GraphQL e Cypher non sono accettate. (REQ-614) `INSERT`, `UPDATE` e `DELETE` semplici non vengono instradati verso un percorso di scrittura. (REQ-615) Scrivere dati tramite `COPY FROM STDIN` (origini scrivibili) o `CREATE TABLE AS`; le mutazioni a livello di riga devono invece passare attraverso i percorsi di scrittura GraphQL, Cypher o Trino.

**COPY e DDL richiedono la capability `ddl`.** Sia `COPY` (in entrambe le direzioni) sia il DDL sono subordinati alla capability `ddl` del ruolo; i ruoli che non la possiedono ricevono SQLSTATE 42501. (REQ-616)

**Nessun supporto reale alle transazioni.** BEGIN/COMMIT/ROLLBACK vengono accettati e ignorati silenziosamente. Ogni istruzione viene eseguita in modo indipendente. (REQ-587) [tool-verified: `server.py:146-158` — `in_transaction()` restituisce sempre `False`]

**Timeout di 60 secondi per il DDL, 120 secondi per le query.** Questi valori sono codificati in modo fisso nei thread del gestore. (REQ-590) Un DDL di lunga durata contro origini remote (modifiche di schema su tabelle di grandi dimensioni) può andare in timeout. [tool-verified: `ddl_handler.py:136`, `server.py:186`]

**COPY FROM funziona solo con origini scrivibili.** Iceberg, Hive, origini esclusivamente Trino e tipi di origine di sola lettura non accettano COPY FROM. L'errore è SQLSTATE 42501. (REQ-586) [tool-verified: `copy_handler.py:65`]

**Il formato di output di COPY è text o csv.** Il formato binario COPY di PG (`FORMAT binary`) non è implementato. [inferred: in `_rows_to_copy_text` / `_rows_to_copy_csv` esistono solo i rami `text` e `csv`]

**Il DDL sul percorso Trino è limitato a CREATE.** ALTER, DROP e CREATE INDEX contro cataloghi Iceberg o Hive non sono supportati. Usare un'origine SQL registrata come `ddl_catalog` se è necessario il DDL completo. (REQ-582) [tool-verified: `ddl_handler.py:92-100`]

**La sostituzione dei parametri è letterale.** I parametri `$1`, `$2`, ... vengono sostituiti come letterali SQL prima dell'esecuzione, non inviati come parametri di bind al motore sottostante. Ciò significa che il motore sottostante non vede mai un'istruzione preparata. Per Trino ciò non ha alcun impatto pratico; per le origini a pool diretto, elude la cache delle istruzioni preparate. (REQ-581) [tool-verified: `server.py:78-85`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.** Queste tabelle esistono nel livello di catalogo ma sono stub vuoti. Gli strumenti di monitoraggio che le interrogano riceveranno zero righe anziché errori. (REQ-532) [tool-verified: `catalog.py:519-535`, `catalog.py:639-934`] (`pg_index` è popolata — vedere Intercettazione del catalogo.)
