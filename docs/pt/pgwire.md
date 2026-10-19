# Servidor pgwire do Provisa {: #provisa-pgwire-server }

O Provisa expõe um endpoint do protocolo de rede do PostgreSQL (pgwire). Qualquer ferramenta que fale o protocolo de cliente do PostgreSQL, como psycopg2, asyncpg, DBeaver, Tableau ou JDBC, pode se conectar e consultar dados do Provisa pelo mesmo pipeline de governança que governa a API HTTP.

Consultas passam pela stack de governança completa: aplicação de RLS, regras de mascaramento,
guards de relacionamento, verificações de acesso a domínio. A interface
pgwire não é um desvio (bypass).

---

## Detalhes de Conexão {: #connection-details }

O servidor inicia quando `PROVISA_PGWIRE_PORT` é definido como um inteiro diferente de zero. É
desabilitado por padrão.

```yaml
Host: 0.0.0.0  (all interfaces)
Port: $PROVISA_PGWIRE_PORT
```

**TLS.** Defina `PROVISA_PGWIRE_CERT` e `PROVISA_PGWIRE_KEY` para os caminhos de um certificado PEM
e chave. Quando ambos estão presentes, o servidor envolve conexões de entrada em TLS. Quando
ausentes, o TLS fica desligado e o servidor responde `N` às requisições de negociação SSL.

**Versão de servidor reportada.** Clientes veem `14.0.provisa`. Ferramentas que condicionam
recursos ao número de versão podem se comportar como se estivessem conectadas ao PostgreSQL 14.

---

## Autenticação {: #authentication }

O pacote de startup carrega um nome de usuário e um único campo de segredo, e nenhum esquema que
diga o que é esse segredo. O Provisa decide a partir do próprio segredo, de modo que um cliente não
precisa de configuração além de `user` e `password`:

| O segredo é | Reconhecido por | Resolve para |
| --------------- | --------------- | ------------- |
| Um token de acesso pessoal | seu prefixo `provisa_pat_` | o dono e o papel do token |
| Um token bearer de OIDC / do provedor | o provedor configurado ser um provedor de tokens | a identidade que o token afirma |
| Uma senha | qualquer outra coisa | a conta no provedor configurado (`basic` ou `simple`) |

A decisão é tomada uma única vez. Uma credencial que o validador escolhido recusa não é tentada
novamente contra outro, então uma recusa não vira uma segunda tentativa.

O modo trust (`provider: none`, ou middleware de auth inativo) é a exceção: o nome de usuário é
usado diretamente como o `role_id` e o segredo é ignorado. Não o use sobre uma conexão não
criptografada.

**SCRAM-SHA-256.** Sob `provider: basic` com `auth.scram: true`, o servidor anuncia SASL (código de
autenticação 10) com `SCRAM-SHA-256` e a senha é provada em vez de enviada.
`SCRAM-SHA-256-PLUS` não é oferecido. Um usuário cujo verificador ainda não foi escrito —
verificadores não podem ser derivados de hashes bcrypt — recebe uma troca simulada, para que a rede
não revele quem já migrou; esse usuário se autentica por senha em texto claro sobre TLS até que a
próxima digitação de senha escreva um. Com `auth.scram` desligado, o servidor usa o tipo de auth PG
3 (senha em texto claro). MD5 não é suportado em nenhum dos casos.

**Certificados de cliente.** Defina `PROVISA_MTLS_CLIENT_CA` e o servidor verifica um certificado de
cliente durante o handshake, antes de qualquer credencial ser examinada. Com
`PROVISA_MTLS_BIND_PRINCIPAL` o common name do certificado deve ser igual ao `user` com que a
conexão então se autentica. Veja [Configuração](configuration.md#mutual-tls).

**Tentativas que falham são contadas.** Cinco falhas em cinco minutos bloqueiam a conta por quinze minutos. O contador é compartilhado com HTTP e Bolt, então um bloqueio ganho em qualquer superfície vale em todas.

**Escolhendo uma organização.** Em um deployment multi-organização, conecte-se a
`<org>.<your-domain>` e o pgwire lê a organização do hostname no ClientHello do TLS, do mesmo modo
que o HTTP a lê do cabeçalho `Host`. O hostname solicita uma organização; ele não a
concede, e um principal sem participação nela é recusado. Conectar-se por endereço IP não solicita
organização alguma.

---

## O Que Funciona {: #what-works }

### SELECT

Todas as declarações SELECT passam pelo pipeline de governança (`_pipeline.py`). O pipeline:

1. Reescreve SQL semântico para SQL físico (`rewrite_semantic_to_physical`)
2. Aplica governança (RLS, mascaramento, acesso a domínio)
3. Valida contra o esquema registrado
4. Roteia para Trino ou pool de fonte direto

Consultas simples multi-declaração são suportadas. Declarações separadas por ponto e vírgula são
divididas e executadas em ordem.

Consultas parametrizadas (`$1`, `$2`, ...) são suportadas tanto em modo de consulta simples quanto
de consulta estendida (Bind/Execute). Parâmetros são substituídos como literais antes da execução.

`SELECT * FROM fn(args)` e `SELECT fn(args)` — onde `fn` nomeia uma função rastreada registrada —
são interceptados antes do pipeline de governança e roteados através do executor governado único
(`invoke_tracked_function`). O resultado é um conjunto de linhas tipado idêntico ao que toda outra
superfície retorna para esse command. A atribuição do comando (`visible_to`), o direito `write` para uma mutação e regras de governança são aplicadas dentro do
executor.

### Cursores (DECLARE / FETCH / MOVE / CLOSE) {: #cursors-declare-fetch-move-close }

O Provisa trata a sintaxe textual de cursores SQL pelo protocolo de consulta simples. Clientes que enviam `DECLARE`/`FETCH`/`MOVE`/`CLOSE` como instruções SQL comuns ganham streaming real no servidor, sem erro de protocolo e sem o resultado inteiro carregado na memória do cliente.

**Isso é diferente do streaming baseado em portais.** O protocolo de consulta estendida — Bind/Execute com um campo de limite de linhas — é um mecanismo diferente do protocolo de transmissão. O asyncpg e a maioria dos drivers PostgreSQL modernos o usam automaticamente, sem SQL de cursor. Se o seu driver já usa portais, você não precisa desta sintaxe. `DECLARE CURSOR` é para clientes que enviam SQL de cursor literalmente: os cursores nomeados/do lado do servidor do psycopg2 (`conn.cursor(name="...")`), certas ferramentas de BI e alguns drivers JDBC antigos.

**DECLARE**

```sql
DECLARE c1 CURSOR FOR SELECT id, amount FROM orders WHERE region = 'us-west';
DECLARE c2 SCROLL CURSOR FOR SELECT * FROM events ORDER BY ts;
DECLARE "CaseSensitive" NO SCROLL CURSOR FOR SELECT * FROM large_table;
```
O SELECT interno passa pelo pipeline governado normal do Provisa, com a mesma aplicação de RLS, o mesmo mascaramento e as mesmas verificações de acesso por domínio de qualquer SELECT isolado. Declarar um cursor não contorna a governança.

A palavra-chave `SCROLL` controla tanto a navegação permitida quanto a memória que o cursor usa:

| Declaração | Navegação para trás | Custo de memória |
|---|---|---|
| `DECLARE c CURSOR FOR ...` (padrão) | Não permitida | Limitado a um lote de FETCH |
| `DECLARE c NO SCROLL CURSOR FOR ...` | Não permitida | Limitado a um lote de FETCH |
| `DECLARE c SCROLL CURSOR FOR ...` | Permitida | Resultado completo em buffer na memória Python |

Quando nem `SCROLL` nem `NO SCROLL` é escrito, o padrão é `NO SCROLL` — igual ao padrão do PostgreSQL. Um cursor `NO SCROLL` remove do seu buffer interno as linhas consumidas a cada passo para frente, então uma varredura `FETCH FORWARD` por um resultado grande nunca mantém mais que aproximadamente um lote na memória Python de cada vez. Um cursor `SCROLL` guarda cada linha que já buscou — esse é o custo de suportar `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` e `LAST`. Antes de declarar um cursor `SCROLL` sobre um resultado grande, reserve o tamanho completo do resultado como heap Python.

Declarar de novo um nome de cursor já aberto fecha primeiro o cursor anterior. `WITH HOLD` e `WITHOUT HOLD` são aceitos na sintaxe; veja a duração do cursor abaixo.

**FETCH e MOVE**

```sql
FETCH 100 FROM c1            -- next 100 rows
FETCH FORWARD 100 FROM c1    -- same
FETCH ALL FROM c1            -- all remaining rows
FETCH BACKWARD 10 FROM c1    -- previous 10 rows (SCROLL cursors only)
FETCH ABSOLUTE 50 FROM c1    -- the row at absolute position 50 (SCROLL cursors only)
FETCH RELATIVE -5 FROM c1    -- 5 positions back (SCROLL cursors only)
FETCH FIRST FROM c1          -- row 1 (SCROLL cursors only)
FETCH LAST FROM c1           -- the final row (SCROLL cursors only)
MOVE 100 FROM c1             -- advance position without returning rows
```
`FETCH` retorna linhas e envia a tag de comando `FETCH <n>`, onde `n` é a quantidade realmente retornada — que pode ser menor que a pedida quando o cursor chega ao fim. `MOVE` avança a posição sem retornar linhas e envia `MOVE <n>`.

`FROM` e `IN` são intercambiáveis para introduzir o nome do cursor. Nomes de cursor sem aspas são convertidos para minúsculas; nomes entre aspas (`"CaseSensitive"`) preservam maiúsculas e minúsculas.

A navegação para trás (`BACKWARD`, `ABSOLUTE`, `FIRST`, `LAST`, `RELATIVE` negativo) em um cursor `NO SCROLL` retorna SQLSTATE 55000 (`object_not_in_prerequisite_state`), igual à recusa do próprio PostgreSQL. Um FETCH ou MOVE que nomeia um cursor nunca declarado retorna SQLSTATE 34000 (`invalid_cursor_name`).

**CLOSE**

```sql
CLOSE c1;    -- release one cursor and free its result stream
CLOSE ALL;   -- release every open cursor on this connection
```
`CLOSE` libera o fluxo de resultados subjacente e remove o cursor da sessão. Desconectar sem emitir `CLOSE` é seguro: `ProvisaSession.close()` libera automaticamente todos os cursores ainda abertos na desconexão.

**Vida de um cursor.** O servidor pgwire do Provisa não tem máquina de estados de transação (`ProvisaSession.in_transaction()` devolve sempre `False`). Por isso todo cursor se comporta como se tivesse sido declarado `WITH HOLD`: vive enquanto a conexão durar, e o `COMMIT` não o encerra. As palavras-chave `WITH HOLD` e `WITHOUT HOLD` são aceitas e ignoradas, já que não há ciclo de vida de cursor por transação para impor.

### Definições (CREATE, ALTER, DROP) {: #definitions-create-alter-drop }

Nada é definido pelo pgwire. Uma instrução que cria, altera ou remove um objeto (`CREATE TABLE`, `CREATE TABLE … AS SELECT`, `CREATE VIEW`, `ALTER`, `DROP`, um índice, uma sequência ou um esquema) é recusada com SQLSTATE 0A000, seja o que for que a função detenha; a exceção é a tabela temporária própria de uma sessão (veja [Tabelas temporárias](#temporary-tables)). Uma tabela ou visão é um objeto do modelo: crie-a no modelo (páginas de administração, API de administração ou configuração), ou crie-a na fonte de dados e admita-a no modelo. `TRUNCATE` não define nada: é uma gravação de dados (veja [Gravações de dados](#data-writes-insert-update-delete-merge-copy-from-stdin)).

A recusa é feita uma vez, no pipeline por onde passa toda superfície SQL, então SQL sobre HTTP, Flight e MCP recusam as mesmas instruções com a mesma mensagem.

### Tabelas temporárias {: #temporary-tables }

Uma sessão pode criar, gravar, ler e remover tabelas temporárias próprias:

```sql
CREATE TEMP TABLE busy AS SELECT customer_id, COUNT(*) AS n FROM sales.orders GROUP BY customer_id;
INSERT INTO busy VALUES (0, 0);
UPDATE busy SET n = n + 1 WHERE customer_id = 0;
DELETE FROM busy WHERE n < 5;
SELECT c.name, b.n FROM sales.customers c JOIN busy b ON b.customer_id = c.id;
DROP TABLE busy;
```
- **Criar** com `CREATE TEMP TABLE name (column type, ...)` ou `CREATE TEMP TABLE name AS SELECT ...`. O nome não leva esquema. Um `SELECT` a partir do qual ela é criada é uma leitura como qualquer outra: as regras de linha, as máscaras, os dados falsos e o limite de linhas da função se aplicam às linhas que ele pega.
- **Tipos de coluna**: `smallint`, `integer`, `bigint`, `real`, `double precision`, `boolean`, `date`, `timestamp`, `text`, `varchar` e `DECIMAL(precision, scale)`. Um decimal mantém sua precisão e escala. Qualquer outro tipo é recusado, nomeando a coluna e o tipo: um valor nunca é armazenado como um tipo diferente do seu. Uma coluna criada a partir de um `SELECT` assume o tipo dos seus valores; uma sem valor pelo qual deduzir um tipo é recusada, então aplique `CAST` no `SELECT`.
- **Limite de linhas.** As linhas que um `CREATE ... AS SELECT` ou um `INSERT ... SELECT` pega são lidas como a função lê qualquer coisa, então o limite de linhas da função se aplica. Uma tabela temporária preenchida a partir de uma leitura que o limite cortou traz o mesmo aviso de instrução que qualquer leitura cortada (`statement.rows_cut`), com o limite.
- **Gravar** com `INSERT ... VALUES`, `INSERT ... SELECT`, `UPDATE` e `DELETE`, sem `RETURNING` e sem `FROM`. Uma gravação em uma tabela temporária não é uma mutação: não chega a nenhuma fonte nem a nenhum dado do ambiente, então funciona qualquer que seja o tratamento de mutações do ambiente e não precisa de direito de escrita.
- **Ler** pelo seu nome, sozinha ou em join com tabelas do modelo. O motor calcula a leitura.
- **Duração.** Uma tabela temporária não é um objeto do modelo, nenhuma outra sessão a vê, e ela termina com a sessão. No pgwire uma sessão é a conexão. Em uma superfície sem conexão própria (SQL sobre HTTP, Flight, MCP) uma sessão é uma requisição, então uma tabela temporária vive para as instruções enviadas juntas nessa requisição.
- **Armazenamento.** As linhas ficam em um esquema próprio da sessão no store do motor e são removidas quando a sessão termina. Um `INSERT`, `UPDATE` ou `DELETE` reescreve a tabela inteira pelo motor, então uma tabela temporária serve para dados de trabalho, não para tabelas grandes gravadas linha a linha. Um processo que para sem encerrar suas sessões deixa os esquemas delas no store; eles são removidos quando o ambiente é aposentado.

As mesmas instruções funcionam em toda superfície SQL: elas são tratadas no pipeline que todas compartilham.

### Gravações de dados (INSERT, UPDATE, DELETE, MERGE, COPY FROM STDIN) {: #data-writes-insert-update-delete-merge-copy-from-stdin }

As linhas de uma tabela registrada existente são gravadas pelo pgwire como em qualquer outra superfície SQL, pela única admissão de gravações (`provisa/compiler/write_admission.py`):

1. **O direito.** A função tem `write`.
2. **A permissão.** A função tem uma permissão de escrita sobre a tabela para a operação: um `INSERT` exige uma permissão `insert`, um `UPDATE` uma `update`, um `DELETE` uma `delete`. Um `MERGE` exige uma permissão `insert` e uma `update` sobre todas as linhas, e um `INSERT ... ON CONFLICT DO UPDATE` uma `update` sobre todas as linhas.
3. **As colunas.** Cada coluna que a instrução grava é uma que a permissão nomeia. Uma coluna que a permissão predefine é da permissão: a instrução não pode fornecê-la, e o valor da permissão (uma variável de sessão, um literal ou o momento da escrita) é adicionado.
4. **As linhas que ela pode tocar.** O predicado `rows` da permissão é adicionado ao `WHERE` de um `UPDATE` ou `DELETE`. Uma permissão que não indica nenhum assume o filtro de linhas da função, o mesmo predicado que suas leituras carregam.
5. **As linhas que ela deixa.** As linhas de um `INSERT`, e os novos valores de um `UPDATE` que define uma coluna lida pelo `check` da permissão, são verificados contra o `check` antes de qualquer coisa ser enviada à fonte. Uma permissão que não indica `check` assume o filtro de linhas da função.

`TRUNCATE` também é uma gravação de dados, executada como tal e nunca reescrita como `DELETE`. Exige o direito `write`, uma permissão `delete` sobre todas as linhas da tabela e uma fonte que aceite exclusões. Esvazia uma tabela por vez, e uma função cuja permissão limita as linhas (um predicado `rows`, ou o filtro de linhas da função quando a permissão não indica nenhum) é recusada, pois um `TRUNCATE` não consegue aplicar o predicado: use `DELETE`, que o aplica.

O que não pode ser decidido é recusado pelo nome em vez de admitido na dúvida (um `INSERT … SELECT`, um valor que não é nem um literal nem um parâmetro vinculado, um filtro que lê uma coluna que a instrução não fornece).

### COPY

`COPY ... TO STDOUT` e `COPY ... FROM STDIN` são ambos suportados.

**COPY TO STDOUT** — exporta resultados de consulta no formato de transmissão COPY do PG. Duas formas funcionam:

```sql
-- Table reference
COPY my_table TO STDOUT WITH (FORMAT csv)

-- Arbitrary query
COPY (SELECT col1, col2 FROM my_table WHERE ...) TO STDOUT WITH (FORMAT text)
```
Formatos suportados: `text` (delimitado por tabulação, padrão), `csv` e `binary`, o formato COPY binário do PostgreSQL, com o qual a extensão postgres do DuckDB lê.

**COPY FROM STDIN** é uma gravação de dados em uma tabela registrada existente: é admitida pela mesma admissão de escrita de um `INSERT` das mesmas linhas, com a condição adicional de que a permissão `insert` não tenha presets (veja [Gravações de dados](#data-writes-insert-update-delete-merge-copy-from-stdin)). Lê `text` (padrão) e `csv`. Qualquer outro formato é recusado com SQLSTATE 0A000, nomeando-o, antes de qualquer dado ser enviado.

### Transações e Comandos de Sessão {: #transactions-and-session-commands }

SET, BEGIN, COMMIT, ROLLBACK, SAVEPOINT, RELEASE, DISCARD, RESET e DEALLOCATE são interceptados e respondidos com um sucesso vazio. O servidor não guarda estado de transação, então não há isolamento de transações nem rollback.

---

## Interceptação de Catálogo {: #catalog-intercept }

Consultas contra `information_schema` e `pg_catalog` são respondidas localmente sem uma ida e volta
ao Trino. A camada de interceptação constrói um banco de dados DuckDB em memória por
requisição, populado a partir do contexto de compilação da função.

Tabelas interceptadas:

**information_schema:** `schemata`, `tables`, `columns`, `views`, `table_constraints`,
`key_column_usage`, `referential_constraints`

**pg_catalog:** `pg_namespace`, `pg_class`, `pg_attribute`, `pg_type`, `pg_attrdef`,
`pg_description`, `pg_index`, `pg_constraint`, `pg_proc`, `pg_roles`, `pg_auth_members`,
`pg_database`, `pg_settings`, `pg_tables`, `pg_stat_user_tables`, `pg_statio_user_tables`, `pg_am`,
`pg_extension`, `pg_enum`, `pg_stat_activity`

`pg_constraint` é populada com dados reais de PK e FK derivados de `pk_columns` e `joins` do modelo
de domínio. Ferramentas de BI que inspecionam relacionamentos de chave
estrangeira (Tableau, DBeaver, etc.) verão o grafo de join que o Provisa conhece. Joins de coluna única entre o mesmo par fonte/alvo cujas colunas alvo juntas
formam a chave primária composta do alvo são colapsados em uma única linha FK com arrays
`conkey`/`confkey` multi-elemento.

Um relacionamento apoiado em tabela de junção não gera linha de FK. Ele é uma aresta por uma tabela associativa, e o `pg_constraint` não tem forma para dois saltos, então o modelo de domínio o deixa fora de `joins`, e a tabela de junção aparece como uma tabela comum com chaves estrangeiras próprias para cada ponta. Clientes SQL chegam a ele unindo essa tabela; clientes Cypher o percorrem como um único relacionamento.

`pg_index` é populada com uma linha por restrição de chave primária e UNIQUE (`indrelid` = oid da
tabela, `indkey` = attnums de chave ordenados, `indisprimary`/`indisunique` definidos). Clientes que
resolvem colunas de chave via `pg_index.indkey` em vez de `pg_constraint` — DataGrip, por exemplo —
descobrem as colunas corretas através do join padrão `pg_index` → `pg_attribute`.

As seguintes expressões escalares também são interceptadas:

- `current_user`, `session_user` → o `role_id` autenticado
- `current_database()` → `"provisa"`
- `current_schema()` → `"public"`
- `version()` → `"PostgreSQL 14.0 on Provisa"`
- `pg_backend_pid()` → `0`
- `current_setting(...)` → retorna de uma tabela de configurações fixa
- `SHOW <setting>` → retorna da mesma tabela de configurações

---

## Codificação Binária de Parâmetro {: #binary-parameter-encoding }

O protocolo de consulta estendida (Bind/Execute) suporta parâmetros codificados em binário.
 Os seguintes OIDs de tipo são decodificados de binário:

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

Qualquer OID não presente nesta tabela lança `"Unsupported binary parameter type: <oid>"`.

Colunas de resultado também são enviadas em binário quando o cliente solicita, para o mesmo
conjunto de tipos mais ARRAY, JSON, INTERVAL, e BIGINT.

---

## Recomendações de Driver {: #driver-recommendations }

**Drivers Python nativos (psycopg2, asyncpg).** Estes negociam o protocolo de consulta estendida
por padrão e usam codificação binária para a maioria dos tipos. A fidelidade de tipo é mais alta
aqui — colunas `NUMERIC` chegam como `Decimal`, `TIMESTAMP` como `datetime`, e assim por diante.
Use estes para ETL baseado em Python, scripts, ou integração direta.

**JDBC (driver JDBC PostgreSQL).** Use este para ferramentas do ecossistema Java: DBeaver, Tableau,
Power BI, Metabase, operadores JDBC do Airflow. JDBC assume por padrão o protocolo de consulta
simples, o que evita complicações de codificação binária. String de conexão:

```yaml
jdbc:postgresql://<host>:<PROVISA_PGWIRE_PORT>/provisa?user=<role_id>&password=<password>
```

Algumas ferramentas de BI baseadas em JDBC enviam uma rajada de consultas `information_schema` e
`pg_catalog` na conexão para popular seu navegador de esquema. Todas são respondidas pela camada de
interceptação de catálogo — nenhum tráfego Trino é gerado durante a inspeção de esquema.

**Quando preferir um sobre o outro.** Se o cliente é Python, use psycopg2 ou asyncpg para melhor
tratamento de tipo. Se o cliente é uma ferramenta de BI ou qualquer aplicação JVM, use JDBC. Evite
misturar expectativas de protocolo binário e texto na mesma conexão se você observar surpresas de
conversão de tipo — o comportamento de modo texto do JDBC é mais simples de raciocinar.

---

## Ressalvas e Restrições {: #caveats-and-constraints }

**Só SQL.** O listener do pgwire analisa e roda SQL; ele não aceita strings de GraphQL ou Cypher. Escritas de dados em tabelas existentes são admitidas pelos direitos do papel, suas concessões de colunas e seu filtro de linhas; veja [Gravações de dados](#data-writes-insert-update-delete-merge-copy-from-stdin).

**Sem definições.** `CREATE`, `ALTER` e `DROP` são recusados com SQLSTATE 0A000 seja o que for que a função detenha, exceto a tabela temporária própria de uma sessão; veja [Tabelas temporárias](#temporary-tables). `TRUNCATE` é uma gravação de dados.

**Sem suporte real a transação.** BEGIN/COMMIT/ROLLBACK são aceitos e silenciosamente ignorados.
Cada declaração roda independentemente.

**Timeout de consulta.** Uma instrução pode usar o timeout de requisição próprio do pgwire, `limits.request_timeouts.pgwire`, ou o compartilhado `limits.request_timeout` quando aquele não está definido; ambos são configurações do operador.

**A entrada de COPY é text ou csv.** `COPY ... TO STDOUT` escreve `text`, `csv` e `binary`; `COPY ... FROM STDIN` lê `text` e `csv`.

**Os parâmetros continuam vinculados.** Os valores de `$1`, `$2`, ... nunca são inseridos no texto da instrução: eles viajam vinculados até o motor ou a fonte, então todos os valores compartilham um único texto SQL (prepares no servidor e caches baseados no texto SQL continuam funcionando) e um valor nunca pode mudar a forma governada. Só a interceptação do catálogo, que responde ela mesma às consultas a `information_schema` e `pg_catalog`, lê os valores inline.

**DECLARE SCROLL guarda em buffer o resultado completo.** Um cursor `SCROLL` acumula no heap Python cada linha que busca do motor para poder servir `FETCH BACKWARD`, `ABSOLUTE`, `FIRST` e `LAST` sob demanda. Com um resultado grande, o resultado inteiro vive na memória enquanto o cursor estiver aberto. Use o padrão `NO SCROLL` para streaming só para frente.

**`pg_stat_activity`, `pg_stat_user_tables`, `pg_extension`, `pg_enum`, `pg_attrdef`, `pg_proc`.**
Estas tabelas existem na camada de catálogo mas são stubs vazios. Ferramentas de monitoramento que
as consultam receberão zero linhas em vez de erros. (`pg_index` é populada — veja Interceptação de Catálogo.)
