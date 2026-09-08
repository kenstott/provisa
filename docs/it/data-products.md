# Prodotti Dati (REQ-1634)

Un prodotto dati è un pacchetto di tabelle con nome e proprietà, pubblicato insieme per il consumo. È l'unità che il catalogo espone ai consumatori — non le singole tabelle, ma una superficie curata che un dominio dichiara esplicitamente pronta. I campi seguono il vocabolario ODPS (Open Data Product Standard) laddove Provisa possiede già la fonte di verità. [tool-verified: `provisa/core/models.py:318-342`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json`]

## Regola di proprietà del dominio

Ogni prodotto dati è di proprietà di esattamente un dominio (`domain_id` è un campo obbligatorio). Una tabella può unirsi a un prodotto dati solo quando entrambi condividono lo stesso `domain_id`. La UI limita il selettore di tabelle al dominio del prodotto; il backend rifiuta un'assegnazione `product_id` il cui dominio non corrisponde a quello del prodotto al momento del salvataggio. [tool-verified: `provisa/core/models.py:320`, `docs/arch/requirements.yaml:54573-54574`]

Un prodotto che necessita di dati da un altro dominio deve prima far entrare quei dati come vista di dominio, poi includere la vista come membro.

## Output port

Le tabelle e i comandi assegnati a un prodotto dati sono i suoi **output port** — la superficie interrogabile che i consumatori vedono. Assegnare una tabella imposta `Table.product_id`; rimuoverla elimina l'appartenenza. Una tabella appartiene al massimo a un prodotto. Anche i comandi nello stesso dominio possono essere assegnati come membri. [tool-verified: `provisa/core/models.py:941`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json:tablesLabel,commandsLabel`]

## Sezioni del pannello dei dettagli

Aprendo un prodotto dati nella UI di amministrazione vengono mostrati questi pannelli:

| Pannello | Cosa mostra |
| --- | --- |
| Output Ports | Tabelle membro e le loro colonne; comandi membro; query di esempio (GraphQL, SQL, Cypher, gRPC, JSON:API, REST) |
| Related Terms | Termini del glossario collegati alle tabelle membro del prodotto |
| Related Tables | Tabelle raggiungibili dalle tabelle membro tramite relazioni approvate ma non ancora parte del prodotto |
| Relationships | Relazioni approvate tra le tabelle membro di questo prodotto |
| Lineage | Grafo di derivazione delle colonne che mostra le tabelle membro come endpoint pubblicato più ogni tabella a monte. Richiede la capability `view_governance` |
| Input Ports | Input a un hop → trasformazione → output derivati dal lineage. Richiede `view_governance` |
| Data Quality | Tabelle di verifica i cui contratti scansionano gli output port di questo prodotto; una riga per ogni controllo per esecuzione. Include un modale delle regole e la visualizzazione dei tag PII |

[tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:detail`]

## Esportazione dei metadati {: #metadata-export }

Solo le tabelle assegnate a un prodotto vengono pubblicate nei cataloghi esterni per impostazione predefinita. `build_snapshot` applica un filtro `data_products_only`: le tabelle non assegnate vengono trattenute, insieme ai loro archi di relazione, archi di lineage e tag di governance. Le origini dati e i domini vengono sempre pubblicati a prescindere. [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

Un prodotto senza membri esportati non viene pubblicato — un elenco vuoto rivendicherebbe l'esistenza di un prodotto senza nulla dietro. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

Solo i cataloghi con un concetto nativo di prodotto dati lo pubblicano come entità di prima classe; gli altri pubblicano le tabelle membro (già filtrate) senza un raggruppamento di prodotto:

| Catalogo | Pubblicato come |
| --- | --- |
| Snowflake Horizon | SHARE + organization listing (Data Product nativo); `publish=false` mantiene lo stato DRAFT, `publish=true` lo rende attivo |
| BigQuery Analytics Hub | Listing di Analytics Hub (nativo) |
| OpenMetadata | Entità `DataProduct` (nativa) |
| DataHub | Entità URN `dataProduct` nativa con propri aspetti di proprietà/proprietà |
| Collibra | Asset di un community type `Data Product`, correlato alle tabelle membro |
| Apache Atlas | Typedef personalizzato `provisa_data_product` best-effort — Atlas non ha un tipo nativo di prodotto dati |
| Atlan | Ipotesi di typedef personalizzato `DataProduct` best-effort — Atlan non ha un tipo stabile documentato per questo |
| OpenLineage | Non è un listing — le tabelle membro portano un facet personalizzato `provisa_data_product` che nomina il prodotto |

[tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:389-418`, `provisa/api/metadata_export/bigquery_dataplex.py:112-136`, `provisa/api/metadata_export/openmetadata.py:326-344`, `provisa/api/metadata_export/datahub.py:133-136,443-483`, `provisa/api/metadata_export/collibra.py:129-133,371-388`, `provisa/api/metadata_export/atlas.py:134-147`, `provisa/api/metadata_export/atlan.py:60`, `provisa/api/metadata_export/openlineage.py:243,348`]

## Campi

| Campo | Obbligatorio | Note |
| --- | --- | --- |
| `id` | Sì | Identificatore stabile leggibile dalla macchina, es. `customer_360` |
| `domain_id` | Sì | Dominio proprietario; la regola di appartenenza è applicata rispetto a questo |
| `name` | Sì | Nome visualizzato |
| `owner_role` | No | Ruolo responsabile di questo prodotto; distinto dallo steward di dominio |
| `team_role` | No | Ruolo i cui titolari formano il team operativo quotidiano; si risolve in individui |
| `purpose` | No | Cosa pubblica questo prodotto e perché |
| `limitations` | No | Vincoli, avvertenze o esclusioni noti |
| `usage` | No | Come consumare questo prodotto |
| `version` | No | es. `1.2.0` |
| `status` | No | es. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | No | Impegni a livello di servizio; solo prosa — un prodotto abbraccia più tabelle membro e uno SLA strutturato non può nominare senza ambiguità quale membro descrive |
| `support` | No | Indicazioni di supporto in testo libero |
| `support_contact` | No | Email o URL; richiesto dai manifest di organization listing di Snowflake Horizon Catalog (REQ-1635) |
| `publish` | No | `true` per pubblicare immediatamente i listing di Horizon Catalog; i nuovi listing sono DRAFT per impostazione predefinita (REQ-1635) |
| `custom_properties` | No | Metadati chiave-valore arbitrari non coperti dai campi standard |

[tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/types.py:104-118,538-551`]
