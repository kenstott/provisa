# Produtos de dados (REQ-1634)

Um produto de dados é um conjunto nomeado e possuído de tabelas publicadas juntas para consumo. É a unidade que o catálogo expõe aos consumidores — não tabelas individuais, mas uma superfície curada que um domínio declara explicitamente como pronta. Os campos seguem o vocabulário do ODPS (Open Data Product Standard) onde o Provisa já detém a fonte da verdade. [tool-verified: `provisa/core/models.py:318-342`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json`]

## Regra de propriedade de domínio

Todo produto de dados é possuído por exatamente um domínio (`domain_id` é um campo obrigatório). Uma tabela pode integrar um produto de dados somente quando ambos compartilham o mesmo `domain_id`. A interface do usuário delimita o seletor de tabelas ao domínio do produto; o backend rejeita uma atribuição de `product_id` cujo domínio não corresponda ao do produto no momento de salvamento. [tool-verified: `provisa/core/models.py:320`, `docs/arch/requirements.yaml:54573-54574`]

Um produto que necessite de dados de outro domínio deve importá-los como uma view de domínio primeiro e só então incluir a view como membro.

## Portas de saída

As tabelas e comandos atribuídos a um produto de dados são suas **portas de saída** — a superfície consultável que os consumidores veem. Atribuir uma tabela define `Table.product_id`; removê-la cancela a associação. Uma tabela pertence a no máximo um produto. Comandos do mesmo domínio também podem ser atribuídos como membros. [tool-verified: `provisa/core/models.py:941`, `provisa-ui/src/i18n/locales/en/dataProductsTab.json:tablesLabel,commandsLabel`]

## Seções do painel de detalhes

Ao abrir um produto de dados na interface de administração, são exibidos os seguintes painéis:

| Painel | O que exibe |
| --- | --- |
| Output Ports | Tabelas membro e suas colunas; comandos membros; consultas de exemplo (GraphQL, SQL, Cypher, gRPC, JSON:API, REST) |
| Related Terms | Termos do glossário vinculados às tabelas membro do produto |
| Related Tables | Tabelas alcançáveis a partir das tabelas membro por meio de relacionamentos aprovados, mas ainda não integrantes do produto |
| Relationships | Relacionamentos aprovados entre as tabelas membro deste produto |
| Lineage | Grafo de linhagem de coluna mostrando as tabelas membro como o endpoint publicado mais toda tabela upstream. Exige a capability `view_governance` |
| Input Ports | Entradas a um salto → transformação → saídas derivadas da linhagem. Exige `view_governance` |
| Data Quality | Tabelas de verificação cujos contratos escaneiam as portas de saída deste produto; uma linha por verificação por execução. Inclui um modal de regras e exibição de tag PII |

[tool-verified: `provisa-ui/src/i18n/locales/en/dataProductsTab.json:detail`]

## Exportação de metadados {: #metadata-export }

Por padrão, somente tabelas atribuídas a um produto são publicadas em catálogos externos. `build_snapshot` aplica um filtro `data_products_only`: tabelas não atribuídas são retidas, juntamente com suas arestas de relacionamento, arestas de linhagem e tags de governança. Fontes e domínios sempre são publicados independentemente. [tool-verified: `provisa/api/metadata_export/builder.py:594,609,641`]

Um produto sem membros exportados não é publicado — uma listagem vazia afirmaria que um produto existe sem nada por trás dele. [tool-verified: `provisa/api/metadata_export/model.py:106-113`]

Somente catálogos com um conceito nativo de produto de dados o publicam como entidade de primeira classe; os demais publicam as tabelas membro (já filtradas) sem agrupamento por produto:

| Catálogo | Publicado como |
| --- | --- |
| Snowflake Horizon | SHARE + listagem da organização (Data Product nativo); `publish=false` mantém como DRAFT, `publish=true` torna ativo |
| BigQuery Analytics Hub | Listagem do Analytics Hub (nativo) |
| OpenMetadata | Entidade `DataProduct` (nativo) |
| DataHub | Entidade URN nativa `dataProduct` com suas próprias propriedades/aspectos de propriedade |
| Collibra | Ativo do tipo de comunidade `Data Product`, relacionado às tabelas membro |
| Apache Atlas | Melhor esforço com typedef personalizado `provisa_data_product` — o Atlas não tem tipo nativo de produto de dados |
| Atlan | Melhor esforço com suposição de typedef personalizado `DataProduct` — o Atlan não tem tipo documentado e estável para este conceito |
| OpenLineage | Não é uma listagem — tabelas membro carregam uma faceta personalizada `provisa_data_product` nomeando o produto |

[tool-verified: `provisa/api/metadata_export/snowflake_horizon.py:389-418`, `provisa/api/metadata_export/bigquery_dataplex.py:112-136`, `provisa/api/metadata_export/openmetadata.py:326-344`, `provisa/api/metadata_export/datahub.py:133-136,443-483`, `provisa/api/metadata_export/collibra.py:129-133,371-388`, `provisa/api/metadata_export/atlas.py:134-147`, `provisa/api/metadata_export/atlan.py:60`, `provisa/api/metadata_export/openlineage.py:243,348`]

## Campos

| Campo | Obrigatório | Notas |
| --- | --- | --- |
| `id` | Sim | Identificador estável legível por máquina, ex. `customer_360` |
| `domain_id` | Sim | Domínio proprietário; regra de associação aplicada contra este valor |
| `name` | Sim | Nome de exibição |
| `owner_role` | Não | Função responsável por este produto; distinta do steward do domínio |
| `team_role` | Não | Função cujos membros formam a equipe de trabalho do dia a dia; resolve para indivíduos |
| `purpose` | Não | O que este produto publica e por quê |
| `limitations` | Não | Restrições, ressalvas ou exclusões conhecidas |
| `usage` | Não | Como consumir este produto |
| `version` | Não | ex. `1.2.0` |
| `status` | Não | ex. `proposed`, `active`, `deprecated`, `retired` |
| `sla` | Não | Compromissos de nível de serviço; somente texto — um produto abrange múltiplas tabelas membro e um SLA estruturado não consegue nomear sem ambiguidade qual membro descreve |
| `support` | Não | Orientação de suporte em texto livre |
| `support_contact` | Não | Email ou URL; exigido pelos manifestos de listagem da organização do Snowflake Horizon Catalog (REQ-1635) |
| `publish` | Não | `true` para publicar listagens do Horizon Catalog imediatamente; novas listagens padrão como DRAFT (REQ-1635) |
| `custom_properties` | Não | Metadados arbitrários de chave-valor não cobertos pelos campos padrão |

[tool-verified: `provisa/core/models.py:318-342`, `provisa/api/admin/types.py:104-118,538-551`]
