# Servidor pgwire do Provisa

O Provisa expõe um endpoint de protocolo de wire do PostgreSQL (pgwire). Qualquer ferramenta que
fale o protocolo de cliente PostgreSQL — psycopg2, asyncpg, DBeaver, Tableau, JDBC — consegue se
conectar e consultar dados do Provisa através do mesmo pipeline de governança que governa a API
HTTP. (REQ-266)

Consultas passam pela stack de governança completa: aplicação de RLS, regras de mascaramento,
guards de relacionamento, verificações de acesso a domínio. (REQ-001, REQ-002, REQ-263) A interface
pgwire não é um desvio (bypass). (REQ-002, REQ-266)

---

## Detalhes de Conexão

O servidor inicia quando `PROVISA_PGWIRE_PORT` é definido como um inteiro diferente de zero. É
desabilitado por padrão. (REQ-527) [tool-verified: `app.py:1739`]

```
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Defina `PROVISA_PGWIRE_CERT` e `PROVISA_PGWIRE_KEY` para os caminhos de um certificado PEM
e chave. Quando ambos estão presentes, o servidor envolve conexões de entrada em TLS. Quando
ausentes, o TLS fica desligado e o servidor responde `N` às requisições de negociação SSL.
(REQ-530) [tool-verified: `server.py:1746-1750`]

**Versão de servidor reportada.** Clientes veem `14.0.provisa`. Ferramentas que condicionam
recursos ao número de versão podem se comportar como se estivessem conectadas ao PostgreSQL 14.
(REQ-579) [tool-verified: `server.py:208`]

---

## Autenticação

Dois modos, controlados pela chave `provider` em `auth_config`:

| Modo | Valor de `provider` | Comportamento |
|------|-----------------|-----------|
| Trust | `none` (ou middleware de auth inativo) | O nome de usuário enviado pelo cliente é usado diretamente como o `role_id`. A senha é ignorada. |
| Simple | `simple` | A senha é verificada contra o provedor de auth `simple` (bcrypt). O nome de usuário se torna `role_id` em caso de sucesso. (REQ-124) |

Qualquer outro valor de provider retorna um erro FATAL no login. (REQ-529) O protocolo sempre usa o
tipo de auth PG 3 (senha em texto claro). (REQ-529) Não use o modo trust sobre uma conexão não
criptografada. [tool-verified: `server.py:282-311`]

---

## O Que Funciona

### SELECT

Todas as declarações SELECT passam pelo pipeline de governança (`_pipeline.py`). (REQ-001,
REQ-262, REQ-266) O pipeline:

1. Reescreve SQL semântico para SQL físico (`rewrite_semantic_to_physical`)
2. Aplica governança (RLS, mascaramento, acesso a domínio) (REQ-263)
3. Valida contra o esquema registrado (REQ-011)
4. Roteia para Trino ou pool de fonte direto (REQ-027, REQ-028)

Consultas simples multi-declaração são suportadas. Declarações separadas por ponto e vírgula são
divididas e executadas em ordem. (REQ-580) [tool-verified: `server.py:318-381`]

Consultas parametrizadas (`$1`, `$2`, ...) são suportadas tanto em modo de consulta simples quanto
de consulta estendida (Bind/Execute). Parâmetros são substituídos como literais antes da execução.
(REQ-581) [tool-verified: `server.py:78-85`]

`SELECT * FROM fn(args)` e `SELECT fn(args)` — onde `fn` nomeia uma função rastreada registrada —
são interceptados antes do pipeline de governança e roteados através do executor governado único
(`invoke_tracked_function`). O resultado é um conjunto de linhas tipado idêntico ao que toda outra
superfície retorna para esse command. `writable_by` e regras de governança são aplicadas dentro do
executor. (REQ-1156) [tool-verified: `provisa/pgwire/function_call.py:74-88`]

### DDL

Declarações DDL são detectadas pela regex em `server.py` e despachadas para `DdlHandler`. A função
deve ter a capacidade `"ddl"`. (REQ-042) Sem ela, a declaração é rejeitada com SQLSTATE 42501.
[tool-verified: `ddl_handler.py:82-83`]

As formas de DDL reconhecidas são:

```
CREATE TABLE / VIEW / INDEX / UNIQUE INDEX / SEQUENCE / SCHEMA
ALTER TABLE / INDEX / SEQUENCE / VIEW
DROP TABLE / VIEW / INDEX / SEQUENCE / SCHEMA
```

[tool-verified: `server.py:56-61`]

Dois caminhos de execução existem dependendo de `ddl_catalog`: (REQ-582)

**Caminho Trino** — usado quando `ddl_catalog` é um Iceberg, Hive, ou outro catálogo Trino não
registrado (ex.: `iceberg`, `hive`, `otel`, `results`). Somente `CREATE TABLE` e `CREATE VIEW` são
suportados neste caminho. Tentar `ALTER`, `DROP`, ou `CREATE INDEX` lança um erro. O nome da tabela
é totalmente qualificado como `catalog.schema.table`. [tool-verified: `ddl_handler.py:92-100`]

**Caminho direto** — usado quando `ddl_catalog` corresponde a um ID de fonte registrado. DDL
completo é suportado: CREATE, ALTER, DROP, índices, sequências. `CREATE TABLE` e `CREATE VIEW` são
qualificados por esquema como `schema.table`. Todo outro DDL (ALTER, DROP, CREATE INDEX) passa
como está após definir o contexto de esquema. Para fontes PostgreSQL e SQLite, o contexto é
definido com `SET search_path TO schema`. Para MySQL e MariaDB, o contexto é definido com
`USE schema`. [tool-verified: `ddl_handler.py:139-170`, `ddl_handler.py:207-213`]

Após DDL em qualquer caminho, a nova tabela é registrada no contexto de compilação da função para
que fique imediatamente consultável. (REQ-583) [tool-verified: `ddl_handler.py:216-250`]

**Resolução de alvo de escrita.** O catálogo e esquema de DDL vêm dos campos `ddl_catalog` e
`ddl_schema` do domínio. Se `ddl_catalog` não estiver definido, o sistema assume como padrão o
catálogo Iceberg. Se `ddl_schema` não estiver definido, assume como padrão o ID do domínio. O
domínio é resolvido através da lista `domain_access` da função. (REQ-584) [tool-verified:
`app.py:804-811`, `ddl_handler.py:104-115`]

### COPY

`COPY ... TO STDOUT` e `COPY ... FROM STDIN` são ambos suportados. (REQ-585) [tool-verified:
`copy_handler.py:231-257`]

**COPY TO STDOUT** — exporta resultados de consulta no formato de wire COPY do PG. Duas formas
funcionam:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```

Formatos suportados: `text` (delimitado por tab, padrão) e `csv`. O formato binário não é suportado
na saída de COPY. [tool-verified: `copy_handler.py:36-52`]

**COPY FROM STDIN** — insere linhas em uma tabela alvo. Restrito a fontes com tipos `postgresql`,
`mysql`, `sqlite`, ou `mariadb`. (REQ-586) Tentar COPY FROM contra uma fonte somente-Trino (ex.:
Iceberg) lança um erro de permissão. [tool-verified: `copy_handler.py:65`, `copy_handler.py:351-356`]

```sql
COPY my_table (col1, col2) FROM STDIN WITH (FORMAT text)
```

Se nenhuma lista de colunas for fornecida, colunas são inferidas do esquema registrado.
[tool-verified: `copy_handler.py:357`]

### Transações e Comandos de Sessão

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET, e DEALLOCATE são interceptados e
retornam uma resposta de sucesso vazia. (REQ-587) O servidor é stateless em relação a transações —
não há isolamento de transação ou suporte a rollback. (REQ-587) [tool-verified: `catalog.py:27-31`,
`catalog.py:1129-1132`]

---

## Interceptação de Catálogo

Consultas contra `information_schema` e `pg_catalog` são respondidas localmente sem uma ida e volta
ao Trino. (REQ-532) A camada de interceptação constrói um banco de dados DuckDB em memória por
requisição, populado a partir do contexto de compilação da função. (REQ-532) [tool-verified:
`catalog.py:210-213`]

Tabelas interceptadas:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`,
`key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`,
`pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`,
`pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`,
`pg_extension`, `pg_enum`, `pg_stat_activity`

[tool-verified: `catalog.py:39-67`]

`pg_constraint` é populada com dados reais de PK e FK derivados de `pk_columns` e `joins` do modelo
de domínio. (REQ-392, REQ-399) Ferramentas de BI que inspecionam relacionamentos de chave
estrangeira (Tableau, DBeaver, etc.) verão o grafo de join que o Provisa conhece. [tool-verified:
`catalog.py:551-632`] Joins de coluna única entre o mesmo par fonte/alvo cujas colunas alvo juntas
formam a chave primária composta do alvo são colapsados em uma única linha FK com arrays
`conkey`/`confkey` multi-elemento. (REQ-1094) [tool-verified: `catalog_constraints.py`]

`pg_index` é populada com uma linha por restrição de chave primária e UNIQUE (`indrelid` = oid da
tabela, `indkey` = attnums de chave ordenados, `indisprimary`/`indisunique` definidos). Clientes que
resolvem colunas de chave via `pg_index.indkey` em vez de `pg_constraint` — DataGrip, por exemplo —
descobrem as colunas corretas através do join padrão `pg_index` → `pg_attribute`. (REQ-1095)
[tool-verified: `catalog_constraints.py:340-384`]

As seguintes expressões escalares também são interceptadas: (REQ-588)
- `current_user`, `session_user` → o `role_id` autenticado
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → retorna de uma tabela de configurações fixa
- `SHOW <setting>` → retorna da mesma tabela de configurações

[tool-verified: `catalog.py:168-207`, `catalog.py:1076-1120`]

---

## Codificação Binária de Parâmetro

O protocolo de consulta estendida (Bind/Execute) suporta parâmetros codificados em binário.
(REQ-589) Os seguintes OIDs de tipo são decodificados de binário: [tool-verified:
`postgres.py:69-97`]

| OID | Tipo PG | Tipo Python |
|-----|---------|-------------|
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

Qualquer OID não presente nesta tabela lança `"Unsupported binary parameter type: <oid>"`.
(REQ-589) [tool-verified: `postgres.py:579`]

Colunas de resultado também são enviadas em binário quando o cliente solicita, para o mesmo
conjunto de tipos mais ARRAY, JSON, INTERVAL, e BIGINT. (REQ-589) [tool-verified:
`postgres.py:191-244`]

---

## Recomendações de Driver

**Drivers Python nativos (psycopg2, asyncpg).** Estes negociam o protocolo de consulta estendida
por padrão e usam codificação binária para a maioria dos tipos. A fidelidade de tipo é mais alta
aqui — colunas `NUMERIC` chegam como `Decimal`, `TIMESTAMP` como `datetime`, e assim por diante.
Use estes para ETL baseado em Python, scripts, ou integração direta.

**JDBC (driver JDBC PostgreSQL).** Use este para ferramentas do ecossistema Java: DBeaver, Tableau,
Power BI, Metabase, operadores JDBC do Airflow. JDBC assume por padrão o protocolo de consulta
simples, o que evita complicações de codificação binária. String de conexão:

```
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Algumas ferramentas de BI baseadas em JDBC enviam uma rajada de consultas `information_schema` e
`pg_catalog` na conexão para popular seu navegador de esquema. Todas são respondidas pela camada de
interceptação de catálogo — nenhum tráfego Trino é gerado durante a inspeção de esquema. (REQ-532)

**Quando preferir um sobre o outro.** Se o cliente é Python, use psycopg2 ou asyncpg para melhor
tratamento de tipo. Se o cliente é uma ferramenta de BI ou qualquer aplicação JVM, use JDBC. Evite
misturar expectativas de protocolo binário e texto na mesma conexão se você observar surpresas de
conversão de tipo — o comportamento de modo texto do JDBC é mais simples de raciocinar.

---

## Ressalvas e Restrições

**Somente SQL; sem mutações DML.** O listener pgwire analisa e executa somente SQL — strings
GraphQL e Cypher não são aceitas. (REQ-614) `INSERT`, `UPDATE`, e `DELETE` simples não são roteados
para um caminho de escrita. (REQ-615) Escreva dados através de `COPY FROM STDIN` (fontes graváveis)
ou `CREATE TABLE AS`; mutações em nível de linha passam pelos caminhos de escrita GraphQL, Cypher,
ou Trino.

**COPY e DDL exigem a capacidade `ddl`.** Tanto `COPY` (em qualquer direção) quanto DDL são
condicionados pela capacidade `ddl` da função; funções sem ela recebem SQLSTATE 42501. (REQ-616)

**Sem suporte real a transação.** BEGIN/COMMIT/ROLLBACK são aceitos e silenciosamente ignorados.
Cada declaração roda independentemente. (REQ-587) [tool-verified: `server.py:146-158` —
`in_transaction()` sempre retorna `False`]

**Timeout de 60 segundos para DDL, 120 segundos para consulta.** Estes são fixos no código nas
threads do handler. (REQ-590) DDL de longa duração contra fontes remotas (mudanças de esquema em
tabelas grandes) pode expirar. [tool-verified: `ddl_handler.py:136`, `server.py:186`]

**COPY FROM é somente para fonte gravável.** Iceberg, Hive, fontes somente-Trino, e tipos de fonte
somente-leitura não aceitam COPY FROM. O erro é SQLSTATE 42501. (REQ-586) [tool-verified:
`copy_handler.py:65`]

**O formato de saída COPY é text ou csv.** O formato binário COPY do PG (`FORMAT binary`) não está
implementado. [inferred: only `text` and `csv` branches exist in `_rows_to_copy_text` /
`_rows_to_copy_csv`]

**DDL no caminho Trino é somente CREATE.** ALTER, DROP, e CREATE INDEX contra catálogos Iceberg ou
Hive não são suportados. Use uma fonte SQL registrada como `ddl_catalog` se precisar de DDL
completo. (REQ-582) [tool-verified: `ddl_handler.py:92-100`]

**A substituição de parâmetro é literal.** Parâmetros `$1`, `$2`, ... são substituídos como
literais SQL antes da execução, não enviados como parâmetros de bind ao motor upstream. Isso
significa que o motor upstream nunca vê uma declaração preparada. Para o Trino isso não tem impacto
prático; para fontes de pool direto isso ignora o cache de declaração preparada. (REQ-581)
[tool-verified: `server.py:78-85`]

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.**
Estas tabelas existem na camada de catálogo mas são stubs vazios. Ferramentas de monitoramento que
as consultam receberão zero linhas em vez de erros. (REQ-532) [tool-verified: `catalog.py:519-535`,
`catalog.py:639-934`] (`pg_index` é populada — veja Interceptação de Catálogo.)
