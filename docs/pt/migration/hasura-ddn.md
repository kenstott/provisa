# Migrando do Hasura DDN (v3) para o Provisa

## Pré-requisitos

1. Um projeto Hasura DDN com arquivos HML (extensão `.hml`).
   Projetos DDN normalmente têm uma estrutura de diretório como:

   ```text
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

2. Python 3.11+ com o pacote `provisa` instalado.

## Uso da CLI

```bash
python -m provisa.ddn <hml-dir> -o provisa.yaml
```

### Argumentos

| Argumento | Obrigatório | Descrição |
| ---------- | ---------- | ------------- |
| `hml_dir` | Sim | Caminho para o diretório do projeto DDN HML (escaneado recursivamente por arquivos `.hml`) |

### Opções

| Opção | Padrão | Descrição |
| -------- | --------- | ------------- |
| `-o, --output FILE` | stdout | Caminho do arquivo YAML de saída |
| `--source-overrides FILE` | Nenhum | Arquivo YAML com overrides de conexão por fonte |
| `--domain-map KEY=VAL ...` | Nenhum | Mapeamentos de subgraph-para-domínio (ex.: `app=core analytics=reporting`) |
| `--dry-run` | desligado | Analisa e valida sem escrever saída |

### Arquivo de Overrides de Fonte

Um arquivo YAML chaveado pelo nome do connector (após sanitização de ID: espaços, pontos, barras
se tornam sublinhados) com propriedades de conexão:

```yaml
my_pg_connector:
  host: prod-db.example.com
  port: 5432
  database: chinook
  username: provisa_user
  password: "${env:PROD_DB_PASSWORD}"
```

## Matriz de Paridade de Recursos

| Kind DDN | Equivalente Provisa | Notas |
| --- | --- | --- |
| **DataConnectorLink** | `sources[]` | Tipo de fonte inferido da URL do connector (postgres, mysql, mssql, mongo, clickhouse, snowflake, bigquery). Detalhes de conexão assumem placeholders por padrão; use `--source-overrides` para definir valores reais. |
| **ObjectType** | Definições de coluna em `tables[]` | Campos se tornam colunas. `dataConnectorTypeMapping.fieldMapping` resolve nomes de campo GraphQL para nomes de coluna física. |
| **Model** | `tables[]` | Cada Model produz uma tabela. `source_id` do connector, `table_name` da collection. `graphql_type_name` se torna `alias`. O Subgraph (e portanto `domain_id`) é derivado do diretório do arquivo: o primeiro componente de diretório sob a raiz do projeto. |
| **Relationship** | `relationships[]` | Tipo Object -> `many-to-one`, tipo Array -> `one-to-many`. Mapeamento de campo resolvido através de lookup de coluna física. |
| **TypePermissions** | `columns[].visible_to[]` | `allowedFields` determina quais funções podem ver cada coluna. |
| **ModelPermissions** | `rls_rules[]` | Predicados de filtro convertidos para cláusulas SQL WHERE. Suporta `_eq`, `_neq`, `_gt`, `_lt`, `_gte`, `_lte`, `_in`, `_nin`, `_like`, `_is_null`, `_and`, `_or`, `_not`. Referências de variável de sessão preservadas como `${x-hasura-...}`. |
| **Command** | `functions[]` | Tanto functions quanto procedures mapeadas. Argumentos, tipo de retorno, e nome de campo raiz GraphQL preservados. `domain_id` definido a partir do subgraph. |
| **AggregateExpression** | Sidecar `provisa-aggregates.yaml` | Count, count_distinct, e funções de agregação por campo preservadas em um arquivo sidecar e convertidas para config de agregado Provisa. |
| **BooleanExpressionType** | Ignorado (silenciosamente) | Usado internamente pelo DDN para filtragem; nenhum equivalente Provisa direto necessário. |
| **AuthConfig** | Ignorado (silenciosamente) | Config de auth DDN não mapeada; configure a auth do Provisa separadamente. |
| **ScalarType** | Ignorado | Aviso emitido com contagem. |
| **GraphqlConfig** | Ignorado | Aviso emitido com contagem. |
| **CompatibilityConfig** | Ignorado | Aviso emitido com contagem. |
| **Outros Kinds não reconhecidos** | Ignorado | Aviso emitido com contagem por kind. |

## Conceito Chave: Resolução de Campo GraphQL para Coluna Física

O DDN separa o esquema GraphQL (nomes de campo) do esquema de banco de dados físico (nomes de
coluna) via `dataConnectorTypeMapping` em ObjectTypes. O conversor:

1. Lê entradas `fieldMapping` dos mapeamentos de tipo de cada ObjectType.
2. Constrói um lookup: `{graphql_field_name -> physical_column_name}`.
3. Para campos sem mapeamento explícito, assume que o nome do campo é igual ao nome da coluna.
4. Usa este lookup ao construir colunas, relacionamentos, e expressões de filtro RLS.

Isso significa que o `provisa.yaml` de saída usa **nomes de coluna física** para `columns[].name`
e define `columns[].alias` para o nome do campo GraphQL quando eles diferem.

## Etapas Pós-Conversão

1. **Revise o YAML de saída.** Verifique fontes, tabelas, e mapeamentos de coluna.
2. **Configure conexões de fonte.** Connectors fornecem somente uma dica de URL para detecção de
   tipo. Host/porta/banco de dados/credenciais reais devem ser fornecidos via
   `--source-overrides` ou editando a saída.
3. **Verifique as atribuições de domínio.** Nomes de subgraph são derivados da estrutura de
   diretório (o primeiro componente de diretório sob a raiz do projeto). Sem `--domain-map`, cada
   nome de subgraph se torna um ID de domínio diretamente. Use `--domain-map` para renomeá-los.
4. **Verifique as regras RLS.** Predicados de filtro DDN são convertidos para aproximações SQL.
   Lógica booleana aninhada (`_and`/`_or`/`_not`) é suportada mas filtros complexos que percorrem
   relacionamento podem precisar de revisão manual.
5. **Revise a config de agregado.** Expressões de agregado são escritas em um arquivo sidecar
   `provisa-aggregates.yaml` e convertidas para config de agregado Provisa.
6. **Revise os avisos.** O conversor imprime um resumo no stderr listando Kinds DDN ignorados e
   quaisquer models referenciando ObjectTypes desconhecidos.
7. **Teste.** Inicie o servidor Provisa e verifique consultas contra suas fontes de dados.

## Problemas Comuns e Solução de Problemas

### A detecção de tipo de fonte falha

A URL do connector é usada heuristicamente (verificando palavras-chave como "postgres", "mysql",
"mongo"). Se a URL não contiver uma palavra-chave reconhecível, a fonte assume `postgresql` como
padrão. Sobreponha com `--source-overrides`.

### ObjectType ausente para um Model

Se um Model referencia um nome de ObjectType que não foi encontrado em nenhum arquivo `.hml`, a
tabela é ignorada e um aviso é emitido. Garanta que todos os arquivos HML estejam incluídos no
diretório escaneado.

### Descoberta de subgraph

Subgraphs são derivados da estrutura de diretório: o primeiro componente de diretório sob a raiz do
projeto é tomado como o nome do subgraph. O campo `subgraph` dentro de documentos HML não é usado.
Arquivos sob um diretório `globals/` são atribuídos ao subgraph `globals` e excluídos da descoberta
de domínio.

### Resolução de fonte de relacionamento

Relacionamentos referenciam um `source_type` (nome de ObjectType) e `target_model` (nome de Model).
Se nenhum Model usa o ObjectType dado, o relacionamento é ignorado silenciosamente.

### Aliases de coluna em todo lugar

Se seu projeto DDN usa `fieldMapping` extensivamente, espere que a maioria das colunas tenha um
`alias` na saída. Este é o comportamento correto -- `name` é a coluna física, `alias` é o nome
GraphQL que sua aplicação usava.

### Expressões de agregado

Expressões de agregado são preservadas em um arquivo sidecar `provisa-aggregates.yaml` escrito ao
lado da saída e convertidas para config de agregado Provisa. Não são armazenadas na `description`
da tabela.

## Exemplo: Convertendo um Projeto DDN Chinook

```bash
# Convert the DDN project
python -m provisa.ddn ./chinook-ddn/ \
  -o provisa.yaml \
  --domain-map app=music \
  --source-overrides overrides.yaml

# Dry run to check warnings first
python -m provisa.ddn ./chinook-ddn/ --dry-run
```

Estrutura de saída:

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
