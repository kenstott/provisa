# Migrando do Hasura v2 para o Provisa {: #migrating-from-hasura-v2-to-provisa }

## Pré-requisitos {: #prerequisites }

1. Uma instância Hasura v2 (v2.x) em execução com os metadados exportados.
2. Exporte os metadados em qualquer um dos dois layouts:

   **O diretório de metadados da CLI:**

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   Isso cria um diretório `metadata/`. `databases/databases.yaml` lista cada fonte com seu tipo
   e sua conexão e aponta para suas tabelas e funções por meio de índices `!include`;
   `actions.yaml`, `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml` e os
   demais arquivos ficam ao lado. O layout plano mais antigo (`tables.yaml` na raiz)
   também é convertido.

   **O documento único da API de metadados:**

   ```bash
   curl -X POST http://localhost:8080/v1/metadata \
     -H "X-Hasura-Admin-Secret: <secret>" \
     -d '{"type":"export_metadata","args":{}}' > metadata.json
   ```

   Tanto o objeto simples quanto o envelope `{resource_version, metadata}` funcionam. A importação
   de administração (Admin, Import Hasura Config) aceita esta forma; o conversor de linha de
   comando abaixo lê diretórios.
3. Python 3.11+ com o pacote `provisa` instalado.

## Uso da CLI {: #cli-usage }

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Argumentos {: #arguments }

| Argumento | Obrigatório | Descrição |
| ---------- | ---------- | ------------- |
| `metadata_dir` | Sim | Caminho para o diretório de metadados Hasura v2 exportado |

### Opções {: #options }

| Opção | Padrão | Descrição |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Caminho do arquivo YAML de saída |
| `--source-overrides FILE` | Nenhum | Arquivo YAML com overrides de conexão por fonte |
| `--domain-map KEY=VAL ...` | Nenhum | Mapeamentos de esquema-para-domínio (ex.: `public=core hr=people`) |
| `--auth-env-file FILE` | Nenhum | Caminho para um arquivo `.env` com config de auth JWT/admin-secret |
| `--dry-run` | desligado | Analisa e valida sem escrever saída |

### Arquivo de Overrides de Fonte {: #source-overrides-file }

Um arquivo YAML chaveado pelo nome da fonte com propriedades de conexão a sobrepor:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### Arquivo de Ambiente de Auth {: #auth-env-file }

Um arquivo estilo `.env` contendo a config de auth do Hasura a converter. O conversor mapeia:

- JWT com `jwk_url` -> `provider: oauth` do Provisa.
- `claims_map` JWT -> `role_mapping[]` do Provisa.
- Admin secret -> `superuser` do Provisa.
- Auth via webhook -> aviso emitido (sem equivalente Provisa).

## O relatório {: #the-report }

A conversão termina com um relatório em três partes:

1. **Levado como está.** O objeto tem uma contrapartida no Provisa e é convertido sem mudar de significado.
2. **Mapeado para um recurso melhor do Provisa.** Cada item diz por que o recurso do Provisa faz o trabalho melhor.
3. **Refazer à mão.** Cada item diz o que fazer. Estes são os avisos.

O conversor de linha de comando imprime o relatório em stderr como `Carried as`, `Mapped to` e `Redo by hand`, cada uma com uma contagem, quando algo fica para refazer. A importação de administração retorna as três listas com a pré-visualização.

## Matriz de paridade de recursos {: #feature-parity-matrix }

| Recurso do Hasura v2 | Parte | Equivalente no Provisa | Notas |
| --- | --- | --- | --- |
| **Fontes** (postgres, mysql, mssql, bigquery, citus) | levado | `sources[]` | Tipo mapeado: pg/postgres -> postgresql, mssql -> sqlserver. Uma URL de conexão escrita por extenso é decomposta em host/porta/banco/usuário/senha. Tamanho do pool preservado. Uma URL lida de uma variável de ambiente não é adivinhada (veja abaixo). |
| **Tabelas** (tabelas rastreadas) | levado | `tables[]` | Esquema e nome da tabela preservados. `source_id` liga à fonte. |
| **Nomes de tabela personalizados** (`custom_name`, `custom_root_fields.select`) | levado | `tables[].alias` | O primeiro valor não nulo entre `select`, `select_by_pk`, `custom_name`. |
| **Nomes de coluna personalizados** (`column_config`, antigo `custom_column_names`) | levado | `columns[].alias` | O nome personalizado de cada coluna vira o seu alias. |
| **Outros nomes de campo raiz** (`select_by_pk`, `select_aggregate`, `insert`, ...) | mapeado | O campo único da tabela | Informados pelo nome. Uma leitura por chave é um filtro sobre a tabela, um agregado o seu campo `_aggregate`, uma escrita a sua mutação. |
| **Permissões de select** (colunas, filtro) | levado | `columns[].visible_to[]`, `rls_rules[]` | As listas de colunas viram `visible_to`; `*` é suportado. Os filtros viram SQL; as variáveis de sessão viram `current_setting('provisa.<name>')`. |
| **Filtros através de relationships** | levado | Subconsultas `EXISTS` em `rls_rules[]` | A regra segue a relationship até a sua tabela. |
| **Permissões de insert, update e delete** | levado | `tables[].write_grants[]` | Uma permissão por função e operação: colunas de `columns`, `rows` de `filter`, `check` de `check`, presets de `set`. As funções recebem o direito `write`. |
| **Função admin** | levado | `org_admin` | Mantém todas as permissões sobre todas as colunas e linhas, e é nomeada em cada coluna importada. |
| **Relationships de objeto** | levado | `relationships[]` com `cardinality: many-to-one` | Aprovadas. O campo GraphQL é o nome da relationship do Hasura. |
| **Relationships de array** | levado | `relationships[]` com `cardinality: one-to-many` | Aprovadas. O campo GraphQL é o nome da relationship do Hasura. |
| **Funções herdadas** (uma função) | levado | `roles[].parent_role_id` | A função vira o pai. |
| **Funções herdadas** (várias funções) | mapeado | Uma função que mantém a união de suas permissões | As funções do Provisa têm um único pai, então a união é escrita por extenso na função. |
| **Computed fields** | refazer | Uma coluna de view | A exportação nomeia a função, não o seu corpo. O relatório dá o `SELECT` a adicionar. |
| **Funções rastreadas** | levado, senão refazer | Comandos `functions[]` | A pré-visualização lê a assinatura do banco de dados. Sem ela, adicione a função na página Commands. |
| **Actions** | mapeado | Comandos que chamam o handler | `{{VAR}}` na URL do handler é escrito `${env:VAR}` e resolvido quando o comando é chamado. |
| **Cron triggers** | levado | `scheduled_triggers[]` | Expressão cron, URL, payload e flag enabled. As novas tentativas não são levadas (aviso). |
| **Event triggers** | mapeado | `scheduled_triggers[]` sobre a alteração da tabela | Dispara por lote de alterações a qualquer alteração. As novas tentativas não são levadas (aviso). |
| **Remote schemas** | mapeado | `sources[]` (`graphql_remote`) aterrissado como tabelas | Nome, URL, cabeçalhos e autenticação preservados; os campos raiz viram tabelas governadas. |
| **Tabelas enum** | levado | Tabela criada | O flag `is_enum` não é levado (sem equivalente no Provisa). |
| **Allow lists, query collections, endpoints REST** | refazer | Nenhum | Informados quando presentes em uma exportação consolidada; não convertidos. |

### Relationships {: #relationships }

- O campo GraphQL de cada relationship é o nome do Hasura. Quando a tabela já tem um campo com esse nome, a relationship assume o nome que o Provisa lhe dá e o relatório avisa.
- Duas relationships que declaram o mesmo join são mescladas em uma. O relatório diz qual foi mesclada em qual.
- Uma relationship que une por vários pares de colunas é avisada e não é criada, pois um relacionamento do Provisa une por um par. As regras de linhas através dela são convertidas mesmo assim.
- Uma relationship de objeto declarada apenas pela coluna de chave estrangeira é resolvida pela relationship de array inversa; sem uma, é avisada.

### Permissões de escrita {: #write-grants }

Uma permissão indica o que uma função pode escrever em uma tabela por uma operação. Um `filter` ou `check` vazio significa todas as linhas (`TRUE`). Um valor de `set` `X-Hasura-User-Id` vira um preset `session` de `user_id`; qualquer outro valor vira um preset `literal`. Uma coluna com preset não está também nas colunas da permissão.

### Actions, funções rastreadas e triggers {: #actions-tracked-functions-and-triggers }

- Uma **action** vira um comando que chama o seu handler, visível às funções que suas permissões nomeiam (apenas a `org_admin` quando não nomeiam nenhuma). Uma action assíncrona é avisada: o comando espera a resposta do handler.
- Uma **função rastreada** vira um comando quando a pré-visualização consegue ler seus argumentos e sua tabela de retorno de uma fonte PostgreSQL. Offline, e para qualquer função que não exista, esteja sobrecarregada ou retorne uma tabela que a importação não registra, ela vai para refazer.
- Um **event trigger** dispara a cada alteração da sua tabela, uma vez por lote, e envia a tabela e as linhas alteradas em vez de um evento por linha com valores antigos e novos. Um trigger que escutava menos que as três operações é avisado.

## Etapas após a conversão {: #post-conversion-steps }

1. **Revise o relatório.** Comece por `Redo by hand`.
2. **Configure as conexões das fontes.** Uma conexão que a exportação escreve por extenso é convertida como está. Uma lida de uma variável de ambiente chega vazia. Use `--source-overrides` ou edite a saída diretamente.
3. **Verifique as atribuições de domínio.** Sem `--domain-map`, todas as tabelas vão para `default`.
   Atribua esquemas a domínios com `--domain-map public=core analytics=reporting`.
4. **Confira as regras de linhas.** Os filtros são convertidos em SQL. Revise as expressões booleanas
   complexas (`_and`/`_or`/`_exists` aninhados) e os filtros através de relationships.
5. **Refaça o que está listado.** Computed fields, funções rastreadas que a pré-visualização não conseguiu ler,
   novas tentativas de triggers e autenticação por webhook não têm conversão automática.
6. **Configure a autenticação.** Se a sua instância Hasura usa autenticação JWT/webhook, crie um arquivo
   de ambiente de autenticação e execute novamente com `--auth-env-file`.
7. **Teste.** Inicie o servidor Provisa e verifique as consultas contra suas fontes de dados.

## Problemas Comuns e Solução de Problemas {: #common-issues-and-troubleshooting }

### A conexão vem de uma variável de ambiente {: #connection-comes-from-an-environment-variable }

Se o `database_url` da fonte for uma referência a uma variável de ambiente (`{"from_env": "PG_URL"}`),
seu valor não está na exportação. O conversor não adivinha nenhum: a fonte fica com host, banco de dados,
usuário e senha vazios, e o relatório a lista em `Redo by hand`.
Informe a conexão nas configurações de fontes da importação (ou com `--source-overrides`) e converta
novamente.

### Colunas curinga {: #wildcard-columns }

Quando uma permissão concede `columns: "*"`, o conversor cria uma única entrada de coluna curinga.
Após a conversão, você pode querer substituí-la por listas de coluna explícitas inspecionando o
esquema real do banco de dados.

### Fidelidade dos event triggers {: #event-trigger-fidelity }

Um event trigger vira um trigger sobre as alterações da sua tabela, chamando o mesmo webhook. Ele dispara
por lote de alterações a qualquer alteração, tenha ela vindo pelo Provisa ou diretamente para o banco de dados, e envia a tabela e as
linhas alteradas. Os valores antigos e novos por linha, a filtragem por operação e as novas tentativas do Hasura não são levados;
o relatório avisa de cada um que se aplicar.
Faça o handler agir sobre as linhas a que se destina, e tente novamente dentro dele se uma chamada perdida importar.

### Funções ausentes {: #missing-roles }

As funções são coletadas das permissões de tabela, das permissões de action e das funções herdadas. Se uma função existe no Hasura
mas não aparece em nenhuma delas, não aparecerá na saída.

### Campos raiz personalizados {: #custom-root-fields }

Os nomes de campo raiz `select` e `select_by_pk` dão o alias da tabela. Os demais campos raiz personalizados
(`select_aggregate`, `insert`, `update`, `delete`) são informados pelo nome: o Provisa serve
uma tabela como um único campo, com agregados e escritas como o seu campo `_aggregate` e as suas mutações.

## Exemplo {: #example }

Converta um projeto Hasura v2 típico com dois esquemas mapeados para domínios:

```bash
# Export metadata from Hasura
hasura metadata export --endpoint http://localhost:8080

# Convert with domain mapping and source overrides
python -m provisa.hasura_v2 metadata/ \
  -o provisa.yaml \
  --domain-map public=core hr=people \
  --source-overrides overrides.yaml \
  --auth-env-file auth.env

# Dry run first to check for warnings
python -m provisa.hasura_v2 metadata/ --dry-run
```

Estrutura da saída (resumida):

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
domains:
  - id: core
  - id: people
tables:
  - source_id: default
    domain_id: core
    schema_name: public
    table_name: users
    columns:
      - name: id
        visible_to: [org_admin, user]
      - name: email
        visible_to: [org_admin, user]
    write_grants:
      - role: user
        operation: update
        columns: [email]
        rows: "id = current_setting('provisa.user_id')"
        check: "TRUE"
      - role: org_admin
        operation: insert
        columns: [id, email]
        check: "TRUE"
      - role: org_admin
        operation: update
        columns: [id, email]
        rows: "TRUE"
        check: "TRUE"
      - role: org_admin
        operation: delete
        rows: "TRUE"
roles:
  - id: user
    capabilities: [query_development, write]
    domain_access: ["*"]
rls_rules:
  - table_id: users
    role_id: user
    filter: "id = current_setting('provisa.user_id')"
relationships:
  - id: orders.user
    source_table_id: orders
    target_table_id: users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
    graphql_alias: user
```
