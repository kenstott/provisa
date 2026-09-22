# Importare da Hasura

Provisa può convertire i metadati Hasura esistenti in un `config.yaml` Provisa, preservando tabelle tracciate, relazioni, permessi, e schemi remoti.

## Importazione interattiva (Admin → Importa configurazione Hasura)

La superficie di amministrazione esegue gli stessi convertitori, quindi un'importazione non richiede accesso alla shell né un giro di andata e ritorno del file di config. Richiede la capability `org_settings`; l'importazione viene applicata all'organizzazione in cui opera la sessione.

1. **Caricamento.** Scegli una directory di metadati Hasura v2 compressa in zip, un progetto DDN compresso in zip, un export di metadati consolidato (`.yaml`/`.json`, incluso l'involucro `{resource_version, metadata}` restituito dall'API dei metadati), o un singolo `.hml`. Lascia il formato su *Rileva automaticamente* a meno che il caricamento non sia ambiguo.
2. **Mappa i domini** (opzionale). Ogni coppia mappa uno schema v2 o un subgraph DDN a un dominio Provisa; ciò che non viene mappato mantiene il nome originale.
3. **Converti e anteprima.** Il server converte e restituisce i conteggi, gli avvisi del convertitore e la configurazione generata. In questo passaggio non viene scritto nulla.
4. **Rivedi e modifica.** La configurazione è modificabile sul posto — dettagli di connessione, nomi dei domini, nomi dei ruoli. Ciò che applichi è ciò che viene mostrato.
5. **Applica.** *Sostituisci il livello semantico esistente* elimina ogni origine, tabella, ruolo e regola assente dalla configurazione; se disattivato, l'importazione viene unita a ciò che l'organizzazione già possiede. L'applicazione carica la configurazione e ricostruisce gli schemi dell'organizzazione.

Endpoint: `POST /admin/import/hasura/preview` e `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Esportazione dei metadati

Dalla tua console o CLI Hasura:

```bash
hasura metadata export --output metadata.yaml
```

Oppure usa l'API Hasura:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### Conversione

Il convertitore v2 legge una **directory** di metadati Hasura (il layout prodotto da `hasura metadata export`, o il layout piatto `tables.yaml` / `actions.yaml`) e scrive una config Provisa:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Ometti `-o` per scrivere la config su stdout.

Flag:

| Flag | Scopo |
| ------ | --------- |
| `-o`, `--output` | Percorso YAML di output (default: stdout) |
| `--source-overrides` | File YAML con override di connessione per origine (host, porta, credenziali) |
| `--domain-map` | Mappature schema-a-dominio come coppie `SCHEMA=DOMAIN` |
| `--auth-env-file` | File `.env` con config di autenticazione; converte JWT/JWK, admin secret, e claims map |
| `--dry-run` | Analizza e valida senza scrivere output |

### Cosa viene convertito

| Concetto Hasura | Equivalente Provisa |
| --------------- | ------------------- |
| Tabella tracciata | `tables[]` con `publish: true` |
| Relazione object | `relationships[]` con `cardinality: many-to-one`. Una dichiarata solo tramite colonna FK (`foreign_key_constraint_on: artist_id`) non nomina alcun target nell'export; il convertitore la risolve tramite la relazione array inversa, e la scarta con un avviso `[relationships]` quando non ne esiste una. (REQ-1680) |
| Relazione array | `relationships[]` con `cardinality: one-to-many` |
| Permesso select | Visibilità ruolo + filtro RLS. Un termine da variabile di sessione (`X-Hasura-User-Id`) diventa `current_setting('provisa.user_id')`, che la richiesta collega all'id utente e alle claim dell'identità al momento della query. (REQ-1682) |
| Permesso colonna | `visible_to` / `writable_by` |
| Permesso insert/update/delete | `writable_by` mutation + RLS |
| Schema remoto | Registrazione origine `graphql_remote` più una tabella atterrata per ogni campo root Query che l'SDL del ruolo espone; una colonna è visibile a ogni ruolo il cui SDL la espone, un argomento root non-null diventa una colonna native-filter `_nf_`, i campi annidati vengono citati in un avviso. (REQ-1681) |
| Campo calcolato | Voce `functions[]` con `kind: query` |

### Connessioni e domini nella scheda di importazione

L'export identifica i propri database tramite variabile d'ambiente, quindi dopo la prima conversione la scheda elenca ogni origine SQL con la connessione ipotizzata dalla conversione. Compila host, porta, database, nome utente e password, e converti di nuovo; solo i campi modificati vengono inviati, come override dell'origine. Le righe dei domini coprono ogni schema, subgraph e schema remoto presenti nel caricamento; ciascuna è un selettore sui domini esistenti dell'organizzazione che accetta anche un nome digitato, contrassegnato come "nuovo dominio" quando non corrisponde a nessuno. Applica esegue l'unione con ciò che l'organizzazione già possiede a meno che la casella di sostituzione non sia attiva. (REQ-1687)

### I tipi provengono dall'origine in anteprima

Un export Hasura nomina le colonne senza tipi, e una tabella tracciata senza permessi non nomina colonne. L'anteprima viene eseguita con le connessioni origine fornite, quindi legge lo `information_schema.columns` di ogni origine SQL raggiungibile: ogni colonna non tipizzata riceve il tipo dell'origine mappato al vocabolario IR, e una tabella senza colonne prende tutte le colonne dell'origine, visibili solo a `org_admin`, poiché Hasura non le esponeva ad alcun altro ruolo. Un'origine che l'anteprima non riesce a raggiungere viene segnalata con un avviso `[sources]` e le sue colonne restano non tipizzate, da completare prima dell'applicazione. (REQ-1691, REQ-1684)

### Limitazioni

- **Le Action** vengono convertite automaticamente: le action con handler HTTP diventano mutation `webhooks[]`; le action con handler non-HTTP (database) diventano un placeholder `functions[]` ed emettono un avviso per revisionare l'handler
- **Gli Event trigger** vengono convertiti in config `event_triggers` per tabella (operazioni, URL webhook, policy di retry) ed emettono un avviso sulla fedeltà limitata
- **Gli Schema remoti** vengono convertiti in voci origine `graphql_remote` e atterrati come tabelle a partire dagli SDL dei permessi di ruolo; uno schema remoto senza permessi non atterra nulla, poiché l'export non porta alcuna altra dichiarazione della sua forma (REQ-1681)
- **Le funzioni SQL custom** richiedono revisione — i casi semplici vengono convertiti in voci `functions[]`, quelli complessi necessitano di lavoro manuale
- **I Cron trigger** vengono convertiti in voci config `scheduler`, preservando l'espressione cron e il flag enabled

---

## Hasura DDN (v3)

### Localizzare il progetto HML

Il convertitore DDN legge direttamente la **directory** di progetto DDN dei file `.hml` — nessun passo di build del supergraph richiesto. Il primo componente di directory sotto la root del progetto viene preso come nome del subgraph; i file sotto `globals/` vengono assegnati al subgraph `globals`.

### Conversione

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Ometti `-o` per scrivere la config su stdout.

Flag:

| Flag | Scopo |
| ------ | --------- |
| `-o`, `--output` | Percorso YAML di output (default: stdout) |
| `--source-overrides` | File YAML con override di connessione per origine |
| `--domain-map` | Mappature subgraph-a-dominio come coppie `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | Percorso di output per il sidecar aggregate-expressions (default: `<output>-aggregates.yaml`) |
| `--dry-run` | Analizza e valida senza scrivere output |

I metadati `AggregateExpression` vengono preservati in un file sidecar `*-aggregates.yaml`.

### Cosa viene convertito

| Concetto DDN | Equivalente Provisa |
| ------------ | ------------------- |
| Modello subgraph | `tables[]` sotto un'origine |
| Relazione | `relationships[]` |
| Regola di permesso | Filtro RLS |
| Command | Mutation webhook o vista |
| Connettore | Voce origine con dettagli di connessione |

### Limitazioni

- I **connettori Lambda** (funzioni TypeScript/Python) richiedono setup manuale del webhook
- I **plugin di lifecycle** non hanno un equivalente diretto
- Le **modalità di autenticazione DDN** mappano ai provider di autenticazione Provisa ma i percorsi delle claim JWT potrebbero richiedere aggiustamenti

---

## Dopo l'import

1. Rivedi il `config.yaml` generato — presta attenzione ai `warnings` del convertitore
2. Verifica le credenziali di connessione (il convertitore usa valori placeholder)
3. Avvia Provisa e conferma che le tabelle appaiono nell'Explorer
4. Esegui le tue query GraphQL esistenti — lo schema è compatibile per i pattern comuni
5. Invia le query per l'approvazione tramite l'API Admin o la UI prima di abilitare la governance di produzione
