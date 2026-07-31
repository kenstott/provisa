# Migrando do Hasura v2 para o Provisa

## Pré-requisitos

1. Uma instância Hasura v2 em execução (v2.x) com metadados exportados.
2. Exporte os metadados usando a CLI do Hasura:

   ```bash
   hasura metadata export --endpoint http://localhost:8080
   ```

   Isso cria um diretório `metadata/` contendo `sources.yaml`, `actions.yaml`,
   `cron_triggers.yaml`, `inherited_roles.yaml`, `remote_schemas.yaml`, etc.
3. Python 3.11+ com o pacote `provisa` instalado.

## Uso da CLI

```bash
python -m provisa.hasura_v2 <metadata-dir> -o provisa.yaml
```

### Argumentos

| Argumento | Obrigatório | Descrição |
| ---------- | ---------- | ------------- |
| `metadata_dir` | Sim | Caminho para o diretório de metadados Hasura v2 exportado |

### Opções

| Opção | Padrão | Descrição |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Caminho do arquivo YAML de saída |
| `--source-overrides FILE` | Nenhum | Arquivo YAML com overrides de conexão por fonte |
| `--domain-map KEY=VAL ...` | Nenhum | Mapeamentos de esquema-para-domínio (ex.: `public=core hr=people`) |
| `--auth-env-file FILE` | Nenhum | Caminho para um arquivo `.env` com config de auth JWT/admin-secret |
| `--dry-run` | desligado | Analisa e valida sem escrever saída |

### Arquivo de Overrides de Fonte

Um arquivo YAML chaveado pelo nome da fonte com propriedades de conexão a sobrepor:

```yaml
default:
  host: prod-db.example.com
  port: 5432
  database: myapp
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

### Arquivo de Ambiente de Auth

Um arquivo estilo `.env` contendo a config de auth do Hasura a converter. O conversor mapeia:

- JWT com `jwk_url` -> `provider: oauth` do Provisa.
- `claims_map` JWT -> `role_mapping[]` do Provisa.
- Admin secret -> `superuser` do Provisa.
- Auth via webhook -> aviso emitido (sem equivalente Provisa).

## Matriz de Paridade de Recursos

| Recurso Hasura v2 | Equivalente Provisa | Notas |
| --- | --- | --- |
| **Sources** (postgres, mysql, mssql, bigquery, citus) | `sources[]` | Kind mapeado: pg/postgres -> postgresql, mssql -> sqlserver. URL de conexão analisada em host/porta/banco de dados/usuário/senha. Configurações de pool preservadas. |
| **Tables** (tabelas rastreadas) | `tables[]` | Esquema + nome de tabela preservados. `source_id` liga à fonte. |
| **Nomes de tabela customizados** (`custom_name`, `custom_root_fields.select`) | `tables[].alias` | Primeiro não-nulo de `select`, `select_by_pk`, `custom_name`. |
| **Nomes de coluna customizados** | `columns[].alias` | Mapeia o dict `custom_column_names` para aliases de coluna. |
| **Permissões de select** (colunas, filter) | `columns[].visible_to[]`, `rls_rules[]` | Listas de coluna se tornam `visible_to`. Colunas curinga (`*`) suportadas. Filtros convertidos para SQL via `bool_expr_to_sql`. |
| **Permissões de insert/update** (colunas) | `columns[].writable_by[]` | Listas de coluna se tornam `writable_by`. Funções elevadas à capacidade `write`. |
| **Permissões de delete** | Elevação de capacidade de função | A função recebe a capacidade `write`. Sem mapeamento de delete por tabela. |
| **Relacionamentos object** | `relationships[]` com `cardinality: many-to-one` | Mapeamento de coluna preservado. |
| **Relacionamentos array** | `relationships[]` com `cardinality: one-to-many` | Mapeamento de coluna preservado. |
| **Computed fields** | `functions[]` | Mapeado para Function com `returns` apontando para o ID da tabela pai. |
| **Tracked functions** | `functions[]` | `exposed_as` assume mutation por padrão. Esquema preservado. |
| **Actions** (handler de stored procedure) | `functions[]` | Convertido para uma config Function quando suportado por uma stored procedure. |
| **Actions** (handler de webhook) | Não convertido | Aviso emitido, incluindo a URL do handler. |
| **Cron triggers** | Não convertido | Aviso emitido. (Triggers agendados de runtime existem, mas o conversor não os mapeia.) |
| **Event triggers** | Não convertido | Aviso emitido. (Triggers de evento de runtime existem, mas o conversor não os mapeia.) |
| **Inherited roles** | `roles[].parent_role_id` | A primeira função em `role_set` se torna pai. Todas as funções filhas criadas. |
| **Remote schemas** | `sources[]` (`graphql_remote`) | Registrado como uma fonte `graphql_remote`. Nome, URL, headers, e config de autenticação preservados. |
| **Enum tables** | Tabela criada | Flag `is_enum` não carregada (sem equivalente Provisa). |
| **Allow lists** | Ignorado | Não presente no modelo de metadados. |

## Etapas Pós-Conversão

1. **Revise o YAML de saída.** Verifique se fontes, tabelas, e funções parecem corretas.
2. **Configure conexões de fonte.** O conversor analisa URLs de conexão mas assume `localhost` como
   padrão em caso de falha de análise. Use `--source-overrides` ou edite a saída diretamente.
3. **Verifique as atribuições de domínio.** Sem `--domain-map`, todas as tabelas caem em `default`.
   Atribua esquemas a domínios com `--domain-map public=core analytics=reporting`.
4. **Verifique as regras RLS.** Filtros são convertidos para aproximações SQL. Expressões booleanas
   complexas (`_and`/`_or`/`_exists` aninhados) devem ser revisadas manualmente.
5. **Revise os avisos.** O conversor imprime um resumo de avisos no stderr para recursos que o
   conversor não mapeia (event triggers, cron triggers, actions suportadas por webhook).
6. **Configure a auth.** Se sua instância Hasura usa auth JWT/webhook, crie um arquivo de ambiente
   de auth e reexecute com `--auth-env-file`.
7. **Teste.** Inicie o servidor Provisa e verifique consultas contra suas fontes de dados.

## Problemas Comuns e Solução de Problemas

### URL de conexão não analisada

Se o `database_url` da fonte é uma referência de variável de ambiente
(`{"from_env": "PG_URL"}`), o conversor não consegue resolvê-la no momento da conversão. A fonte
terá valores placeholder (`host: localhost`, `database: default`). Corrija com
`--source-overrides`.

### Colunas curinga

Quando uma permissão concede `columns: "*"`, o conversor cria uma única entrada de coluna curinga.
Após a conversão, você pode querer substituí-la por listas de coluna explícitas inspecionando o
esquema real do banco de dados.

### Fidelidade de event trigger

Event triggers são convertidos com `operations` e `webhook_url` mas garantias de entrega
específicas do Hasura (exactly-once, redelivery) não têm equivalentes Provisa diretos. Revise a
seção `event_triggers` e configure sua infraestrutura de webhook adequadamente.

### Funções ausentes

Funções são coletadas somente de entradas de permissão. Se uma função existe no Hasura mas não tem
permissões em nenhuma tabela ou action, ela não aparecerá na saída.

### Campos raiz customizados

Somente os campos raiz `select` e `select_by_pk` são usados para o alias da tabela. Outros campos
raiz customizados (`select_aggregate`, `insert`, `update`, `delete`) não são mapeados.

## Exemplo

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

Estrutura de saída:

```yaml
sources:
  - id: default
    type: postgresql
    host: prod-db.example.com
    port: 5432
    database: myapp
    ...
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
        visible_to: [user, admin]
      - name: email
        visible_to: [admin]
        writable_by: [admin]
    alias: Users
roles:
  - id: admin
    capabilities: [read, write]
    domain_access: ["*"]
  - id: user
    capabilities: [read]
    domain_access: ["*"]
rls_rules:
  - table_id: default.public.users
    role_id: user
    filter: "id = x-hasura-user-id"
relationships:
  - id: default.public.orders.user
    source_table_id: default.public.orders
    target_table_id: default.public.users
    source_column: user_id
    target_column: id
    cardinality: many-to-one
```
