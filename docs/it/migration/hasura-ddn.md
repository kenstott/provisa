# Migrazione da Hasura DDN (v3) a Provisa

## Prerequisiti

1. Un progetto Hasura DDN con file HML (estensione `.hml`).
   I progetti DDN hanno tipicamente una struttura di directory come:
   ```
   my-ddn-project/
     app/
       subgraph1/
         models/
           MyModel.hml
         commands/
           MyCommand.hml
       subgraph2/
         ...
     globals/
       ...
   ```
2. Python 3.11+ con il pacchetto `provisa` installato.

## Utilizzo della CLI

```bash
python -m provisa.ddn <hml-dir> -o provisa.yaml
```

### Argomenti

| Argomento | Obbligatorio | Descrizione |
|----------|----------|-------------|
| `hml_dir` | Sì | Percorso della directory del progetto DDN HML (analizzata ricorsivamente alla ricerca di file `.hml`) |

### Opzioni

| Opzione | Predefinito | Descrizione |
|--------|---------|-------------|
| `-o, --output FILE` | stdout | Percorso del file YAML di output |
| `--source-overrides FILE` | Nessuno | File YAML con override di connessione per singola origine |
| `--domain-map KEY=VAL ...` | Nessuno | Mappature da subgraph a domain (ad es. `app=core analytics=reporting`) |
| `--dry-run` | disattivato | Analizza e valida senza scrivere l'output |

### File di override dell'origine

Un file YAML indicizzato per nome del connettore (dopo la sanificazione dell'ID: spazi, punti e
barre diventano trattini bassi) con le proprietà di connessione:

```yaml
my_pg_connector:
  host: prod-db.example.com
  port: 5432
  database: chinook
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

## Matrice di equivalenza delle funzionalità

| Tipo DDN | Equivalente Provisa | Note |
|---|---|---|
| **DataConnectorLink** | `sources[]` | Il tipo di origine viene dedotto dall'URL del connettore (postgres, mysql, mssql, mongo, clickhouse, snowflake, bigquery). I dettagli di connessione utilizzano segnaposto per impostazione predefinita; usare `--source-overrides` per impostare i valori effettivi. |
| **ObjectType** | Definizioni di colonna su `tables[]` | I campi diventano colonne. `dataConnectorTypeMapping.fieldMapping` risolve i nomi dei campi GraphQL nei nomi delle colonne fisiche. |
| **Model** | `tables[]` | Ogni Model produce una tabella. `source_id` deriva dal connettore, `table_name` dalla collection. `graphql_type_name` diventa `alias`. Il subgraph (e quindi `domain_id`) viene derivato dalla directory del file: il primo componente di directory sotto la root del progetto. |
| **Relationship** | `relationships[]` | Tipo Object -> `many-to-one`, tipo Array -> `one-to-many`. La mappatura dei campi viene risolta tramite ricerca della colonna fisica. |
| **TypePermissions** | `columns[].visible_to[]` | `allowedFields` determina quali ruoli possono vedere ciascuna colonna. |
| **ModelPermissions** | `rls_rules[]` | I predicati di filtro vengono convertiti in clausole SQL WHERE. Supporta `_eq`, `_neq`, `_gt`, `_lt`, `_gte`, `_lte`, `_in`, `_nin`, `_like`, `_is_null`, `_and`, `_or`, `_not`. I riferimenti alle variabili di sessione vengono mantenuti come `${x-hasura-...}`. |
| **Command** | `functions[]` | Vengono mappate sia le funzioni sia le procedure. Argomenti, tipo di ritorno e nome del campo radice GraphQL vengono mantenuti. `domain_id` viene impostato in base al subgraph. |
| **AggregateExpression** | File collaterale `provisa-aggregates.yaml` | Count, count_distinct e le funzioni di aggregazione per campo vengono mantenute in un file collaterale e convertite nella configurazione di aggregazione di Provisa. |
| **BooleanExpressionType** | Ignorato (silenziosamente) | Utilizzato internamente da DDN per il filtraggio; non è necessario un equivalente diretto in Provisa. |
| **AuthConfig** | Ignorato (silenziosamente) | La configurazione di autenticazione DDN non viene mappata; configurare l'autenticazione di Provisa separatamente. |
| **ScalarType** | Ignorato | Viene emesso un avviso con il conteggio. |
| **GraphqlConfig** | Ignorato | Viene emesso un avviso con il conteggio. |
| **CompatibilityConfig** | Ignorato | Viene emesso un avviso con il conteggio. |
| **Altri tipi non riconosciuti** | Ignorato | Viene emesso un avviso con il conteggio per ciascun tipo. |

## Concetto chiave: risoluzione dal campo GraphQL alla colonna fisica

DDN separa lo schema GraphQL (nomi dei campi) dallo schema fisico del database
(nomi delle colonne) tramite `dataConnectorTypeMapping` sugli ObjectType. Il convertitore:

1. Legge le voci `fieldMapping` dalle mappature di tipo di ciascun ObjectType.
2. Costruisce una tabella di ricerca: `{graphql_field_name -> physical_column_name}`.
3. Per i campi senza una mappatura esplicita, presume che il nome del campo coincida con quello della colonna.
4. Utilizza questa tabella di ricerca durante la costruzione di colonne, relazioni ed espressioni di filtro RLS.

Questo significa che il file `provisa.yaml` di output utilizza **nomi di colonna fisici** per `columns[].name`
e imposta `columns[].alias` sul nome del campo GraphQL quando differiscono.

## Passaggi successivi alla conversione

1. **Rivedere il file YAML di output.** Verificare origini, tabelle e mappature delle colonne.
2. **Configurare le connessioni alle origini.** I connettori forniscono solo un suggerimento nell'URL
   per il rilevamento del tipo. Host, porta, database e credenziali effettivi devono essere forniti tramite
   `--source-overrides` oppure modificando l'output.
3. **Verificare le assegnazioni di domain.** I nomi dei subgraph vengono derivati dalla struttura di
   directory (il primo componente di directory sotto la root del progetto). Senza `--domain-map`, ogni
   nome di subgraph diventa direttamente un ID di domain. Usare `--domain-map` per rinominarli.
4. **Controllare le regole RLS.** I predicati di filtro DDN vengono convertiti in approssimazioni SQL.
   La logica booleana annidata (`_and`/`_or`/`_not`) è supportata, ma i filtri complessi che
   attraversano le relazioni potrebbero richiedere una revisione manuale.
5. **Rivedere la configurazione di aggregazione.** Le espressioni di aggregazione vengono scritte in un
   file collaterale `provisa-aggregates.yaml` e convertite nella configurazione di aggregazione di Provisa.
6. **Rivedere gli avvisi.** Il convertitore stampa su stderr un riepilogo che elenca i tipi DDN ignorati
   e qualsiasi model che fa riferimento a ObjectType sconosciuti.
7. **Eseguire i test.** Avviare il server Provisa e verificare le query rispetto alle proprie origini dati.

## Problemi comuni e risoluzione dei problemi

### Il rilevamento del tipo di origine non riesce

L'URL del connettore viene utilizzato in modo euristico (verificando parole chiave come "postgres",
"mysql", "mongo"). Se l'URL non contiene una parola chiave riconoscibile, l'origine utilizza per
impostazione predefinita `postgresql`. Sovrascrivere con `--source-overrides`.

### ObjectType mancante per un Model

Se un Model fa riferimento a un nome di ObjectType non trovato in alcun file `.hml`,
la tabella viene ignorata e viene emesso un avviso. Assicurarsi che tutti i file HML siano
inclusi nella directory analizzata.

### Individuazione dei subgraph

I subgraph vengono derivati dalla struttura di directory: il primo componente di directory sotto
la root del progetto viene considerato come nome del subgraph. Il campo `subgraph` all'interno dei
documenti HML non viene utilizzato. I file all'interno di una directory `globals/` vengono assegnati
al subgraph `globals` ed esclusi dall'individuazione dei domain.

### Risoluzione dell'origine della relazione

Le relazioni fanno riferimento a un `source_type` (nome ObjectType) e a un `target_model` (nome
Model). Se nessun Model utilizza l'ObjectType indicato, la relazione viene ignorata silenziosamente.

### Alias di colonna ovunque

Se il proprio progetto DDN utilizza `fieldMapping` in modo estensivo, ci si può aspettare che la
maggior parte delle colonne abbia un `alias` nell'output. Questo è un comportamento corretto -- `name`
è la colonna fisica, `alias` è il nome GraphQL utilizzato dall'applicazione.

### Espressioni di aggregazione

Le espressioni di aggregazione vengono mantenute in un file collaterale `provisa-aggregates.yaml`
scritto accanto all'output e convertite nella configurazione di aggregazione di Provisa. Non vengono
memorizzate nella `description` della tabella.

## Esempio: conversione di un progetto DDN Chinook

```bash
# Convert the DDN project
python -m provisa.ddn ./chinook-ddn/ \
  -o provisa.yaml \
  --domain-map app=music \
  --source-overrides overrides.yaml

# Dry run to check warnings first
python -m provisa.ddn ./chinook-ddn/ --dry-run
```

Struttura dell'output:

```yaml
sources:
  - id: chinook_pg
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: chinook
    ...
domains:
  - id: music
tables:
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Album
    columns:
      - name: AlbumId
        visible_to: [admin, user]
      - name: Title
        visible_to: [admin, user]
      - name: ArtistId
        visible_to: [admin, user]
    alias: Albums
  - source_id: chinook_pg
    domain_id: music
    schema_name: public
    table_name: Artist
    columns:
      - name: artist_id
        visible_to: [admin, user]
        alias: ArtistId
      - name: artist_name
        visible_to: [admin, user]
        alias: Name
    alias: Artists
roles:
  - id: admin
    capabilities: [read]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
relationships:
  - id: chinook_pg.public.Album.Artist
    source_table_id: chinook_pg.public.Album
    target_table_id: chinook_pg.public.Artist
    source_column: ArtistId
    target_column: artist_id
    cardinality: many-to-one
functions:
  - name: GetTopTracks
    source_id: chinook_pg
    schema_name: public
    function_name: get_top_tracks
    returns: Track
    domain_id: music
    description: "DDN function"
```
