# Suporte a Consulta Cypher

O Provisa traduz um subconjunto do openCypher para SQL via o módulo `provisa/cypher/`. (REQ-345, REQ-347) Consultas são analisadas por um parser recursivo-descendente personalizado (nenhuma biblioteca Cypher externa) (REQ-571), resolvidas de esquema contra a camada semântica (REQ-351), e emitidas como SQL, depois roteadas para o motor de execução alvo. (REQ-066, REQ-067, REQ-347)

## Funcionalidades Implementadas

### Cláusulas

| Cláusula | Status | Notas |
| -------- | -------- | ------- |
| `MATCH (n:Label)` | ✓ | Padrões de nó com labels, variáveis, propriedades inline |
| `OPTIONAL MATCH` | ✓ | Emite LEFT JOIN |
| `WHERE` | ✓ | Suporte completo a expressão; aplicado após MATCH |
| `RETURN` | ✓ | Star, acesso a propriedade, expressões, aliases |
| `RETURN DISTINCT` | ✓ | Emite SELECT DISTINCT |
| `WITH` | ✓ | Emite um CTE nomeado (`_w0`, `_w1`, …); suporta `WITH … WHERE` |
| `ORDER BY` | ✓ | ASC / DESC |
| `SKIP` / `LIMIT` | ✓ | Mapeia para SQL OFFSET / LIMIT |
| `UNION` / `UNION ALL` | ✓ | União recursiva através de sub-ASTs |
| `CALL { … }` | ✓ | Decomposição de subconsulta de call de nível superior via `cypher_calls_to_sql_list` |
| `CALL { WITH x … }` | ✓ | Subconsulta correlacionada → `CROSS JOIN LATERAL`; veja §CALL Correlacionado |
| `CALL db.labels()` | ✓ | Retorna labels de nó da camada semântica; sem tradução SQL (REQ-572) |
| `CALL db.relationshipTypes()` | ✓ | Retorna tipos de relacionamento da camada semântica (REQ-572) |
| `CALL db.propertyKeys()` | ✓ | Retorna todos os nomes de chave de propriedade em todos os tipos de nó (REQ-572) |
| `UNWIND` | ✓ | Expansão de array para linhas; primeiro item se torna FROM, subsequentes se tornam CROSS JOIN UNNEST |

### Padrões de Match

| Padrão | Status | Notas |
| --------- | -------- | ------- |
| `(n)` — nó sem label | ✓ | UNION ALL sobre todos os tipos conhecidos |
| `(n:Label)` | ✓ | Mapeia para a tabela registrada para aquele tipo GraphQL |
| `(n:Label {prop: val})` | ✓ | Filtro de propriedade inline se torna WHERE |
| `(a)-[:TYPE]->(b)` | ✓ | Direcionado, hop único |
| `(a)<-[:TYPE]-(b)` | ✓ | Travessia reversa; colunas de join invertidas |
| `(a)-[]->(b)` | ✓ | Qualquer relacionamento direcionado a→b; UNION ALL se múltiplos tipos correspondem |
| `(a)-[]-(b)` | ✓ | Bidirecional; expande para UNION ALL de todos os relacionamentos frente e trás |
| `(a)-[:TYPE*..N]->(b)` | ✓ | Comprimento variável com limite superior; CTE recursivo para auto-referencial, JOIN plano caso contrário |
| `(a)-[]->(b)-[]->(c)` | ✓ | JOINs encadeados de múltiplos hops |
| `(n:DomainLabel)` | ✓ | Label de domínio → subconsulta UNION ALL sobre todos os tipos no domínio |
| `(n:A\|B)` | ✓ | Alternação de label → domínio ad-hoc injetado no mapa de label; UNION ALL sobre tipos correspondentes |
| `shortestPath(…)` | ✓ | JOIN plano para endpoints heterogêneos; CTE WITH RECURSIVE para mesmo-tipo/auto-referencial |
| `allShortestPaths(…)` | ✓ | Igual a shortestPath sem LIMIT 1 |

### Expressões e Predicados

| Funcionalidade | Status | Mapeamento SQL |
| --------- | -------- | ------------ |
| Acesso a propriedade `n.prop` | ✓ | `n."prop"` |
| Parâmetros `$name` | ✓ | Posicional `$N` |
| Parâmetros legados `{name}` | ✓ | Normalizado para `$name` no momento da análise |
| Comparação `=`, `<>`, `<`, `>`, `<=`, `>=` | ✓ | Direto |
| `AND`, `OR`, `NOT` | ✓ | Direto |
| `IS NULL` / `IS NOT NULL` | ✓ | Direto |
| `IN [list]` | ✓ | SQL IN; sintaxe de colchetes Cypher `[...]` reescrita para `(...)` |
| `STARTS WITH` | ✓ | `starts_with(col, val)` |
| `ENDS WITH` | ✓ | `col LIKE CONCAT('%', val)` |
| `CONTAINS` | ✓ | `strpos(col, val) > 0` |
| `=~` regex | ✓ | `regexp_like(col, pattern)` |
| `exists(n.prop)` | ✓ | `(n.prop) IS NOT NULL` |
| `EXISTS { MATCH … }` | ✓ | Subconsulta correlacionada `EXISTS (SELECT 1 FROM …)` |
| `COUNT { MATCH … }` | ✓ | Subconsulta correlacionada `(SELECT count(*) FROM …)` |
| `COLLECT { MATCH … RETURN x }` | ✓ | Subconsulta correlacionada `ARRAY(SELECT x FROM …)` |
| `id(n)` | ✓ | Resolvido para a coluna de ID configurada do nó |
| `labels(n)` | ✓ | `ARRAY['Label']` |
| `keys(n)` | ✓ | `ARRAY['prop1', 'prop2', …]` |
| `type(r)` | ✓ | Resolvido em tempo de compilação para literal string `'REL_TYPE'`; sem coluna em runtime |
| `length(p)` | ✓ | `_t.hops` para caminhos de CTE recursivo; `1` para caminhos de JOIN plano |
| `CASE WHEN … THEN … ELSE … END` | ✓ | Direto (formas buscadas e simples) |
| GROUP BY implícito | ✓ | Itens RETURN não agregados se tornam chaves GROUP BY quando qualquer item tem um agregado |

### Projeções de Mapa

| Sintaxe | Mapeamento SQL |
| -------- | ------------ |
| `n { .prop1, .prop2 }` | `MAP(ARRAY['prop1','prop2'], ARRAY[n."prop1",n."prop2"])` |
| `n { .* }` | `MAP(ARRAY[all props...], ARRAY[n."col",...])` — expandido do esquema |
| `n { .*, extra: expr }` | Todas as props do esquema mais chave nomeada; MAP combinado |
| `n { key: expr }` | `MAP(ARRAY['key'], ARRAY[expr])` |

### Funções de Agregação

| Cypher | SQL |
| -------- | ----- |
| `count(*)`, `count(x)` | direto |
| `count(DISTINCT x)` | `count(DISTINCT x)` |
| `collect(x)` | `array_agg(x)` |
| `avg`, `sum`, `min`, `max` | direto |
| `stDev(x)` | `stddev_samp(x)` |
| `stDevP(x)` | `stddev_pop(x)` |
| `percentileCont(x, p)` | `approx_percentile(x, p)` |
| `percentileDisc(x, p)` | `approx_percentile(x, p)` |

### Funções de String

| Cypher | SQL |
| -------- | ----- |
| `toLower(x)` | `lower(x)` |
| `toUpper(x)` | `upper(x)` |
| `ltrim(x)`, `rtrim(x)`, `trim(x)` | direto |
| `replace(x, a, b)` | direto |
| `reverse(x)` | direto |
| `split(x, d)` | direto |
| `left(x, n)` | `left(x, n)` |
| `right(x, n)` | `right(x, n)` |
| `substring(x, start, len)` | `substr(x, start+1, len)` (índice 0→1) |
| `size(string)` | `char_length(string)` |
| `size(list)` | `cardinality(list)` |

### Funções de Conversão de Tipo

| Cypher | SQL |
| -------- | ----- |
| `toString(x)` | `CAST(x AS VARCHAR)` |
| `toInteger(x)` | `TRY_CAST(x AS BIGINT)` |
| `toFloat(x)` | `TRY_CAST(x AS DOUBLE)` |
| `toBoolean(x)` | `TRY_CAST(x AS BOOLEAN)` |
| `toStringOrNull`, `toIntegerOrNull`, `toFloatOrNull`, `toBooleanOrNull` | variantes `TRY_CAST` |

### Funções Matemáticas

| Cypher | SQL |
| -------- | ----- |
| `log(x)` | `ln(x)` (log natural) |
| `log2(x)` | `log2(x)` |
| `range(start, end)` | `sequence(start, end)` |
| `abs`, `sqrt`, `ceil`, `floor`, `round`, `sign` | repassado |

### Funções de Lista

| Cypher | SQL |
| -------- | ----- |
| `head(list)` | `element_at(list, 1)` |
| `last(list)` | `element_at(list, -1)` |
| `tail(list)` | `slice(list, 2, cardinality(list))` |
| `isEmpty(list)` | `cardinality(list) = 0` |

### Compreensões de Lista

| Sintaxe | Mapeamento SQL |
| -------- | ------------ |
| `[x IN list \| f(x)]` | `transform(list, x -> f(x))` |
| `[x IN list WHERE p(x)]` | `filter(list, x -> p(x))` |
| `[x IN list WHERE p(x) \| f(x)]` | `transform(filter(list, x -> p(x)), x -> f(x))` |
| `any(x IN list WHERE p(x))` | `any_match(list, x -> p(x))` |
| `all(x IN list WHERE p(x))` | `all_match(list, x -> p(x))` |
| `none(x IN list WHERE p(x))` | `none_match(list, x -> p(x))` |
| `single(x IN list WHERE p(x))` | `cardinality(filter(list, x -> p(x))) = 1` |
| `reduce(acc = init, x IN list \| expr)` | `reduce(list, init, (acc, x) -> expr, acc -> acc)` |

### Compreensões de Padrão

| Sintaxe | Mapeamento SQL |
| -------- | ------------ |
| `[(a)-[:R]->(b) \| b.prop]` | `ARRAY(SELECT b."prop" FROM ... WHERE a.fk = b.pk)` |
| `[(a)-[]->(b:Label) \| b.prop]` | tipo inferido da camada semântica; mesma forma de subconsulta ARRAY |

### Subconsultas CALL Correlacionadas

`CALL { WITH x MATCH (x)-[:R]->(n) RETURN n.prop AS alias }` traduz para `CROSS JOIN LATERAL (SELECT n."prop" AS alias FROM ... WHERE x."pk" = n."fk")`. (REQ-573) Regras:

- A variável de escopo externo (`x`) deve aparecer em `WITH`
- Múltiplas vars importadas (`WITH a, b`) são suportadas
- O primeiro relacionamento no MATCH interno cuja fonte é uma var vinculada a lateral determina o `FROM` interno e a condição de join
- Blocos `CALL { ... }` de nível superior não correlacionados (sem `WITH`) são tratados por `cypher_calls_to_sql_list`

---

## Escritas

Cypher suporta três padrões de escrita através do endpoint `/data/cypher`, executados por `provisa/cypher/write_translator.py`. (REQ-818) [tool-verified: `provisa/api/rest/cypher_router.py:415-545`]

| Cypher | SQL | Req |
| -------- | ----- | ----- |
| `CREATE (n:Label {props})` | `INSERT INTO catalog.schema.table (cols) VALUES (vals)` | REQ-666 |
| `MATCH (n:Label) WHERE … DELETE n` | `DELETE FROM catalog.schema.table WHERE …` | REQ-667 |
| `MATCH (n:Label) WHERE … SET n.prop = val, …` | `UPDATE catalog.schema.table SET col = val, … WHERE …` | REQ-668 |

Nomes de propriedade mapeiam para colunas via remoção de prefixo de domínio e resolução de alias; valores escalares Cypher são coagidos ao tipo de coluna alvo. (REQ-666, REQ-668) O corpo da resposta carrega uma contagem `affected_rows`. (REQ-670)

Regras:

- O label deve resolver para exatamente uma tabela registrada. Labels ambíguos ou desconhecidos são erros rígidos; sem correspondência fuzzy. (REQ-661) Novos labels ou tipos não podem ser criados através do Cypher. (REQ-662)
- Toda escrita é bloqueada pela ACL `writable_by` da tabela alvo; uma função sem direitos de escrita é rejeitada no momento da compilação. (REQ-663)
- O conector de fonte subjacente deve suportar DML. Fontes somente-leitura (federadas via Trino, Iceberg sem um conector Delta) rejeitam escritas no momento da tradução. (REQ-664)
- Relacionamentos não podem ser escritos — eles são derivados de joins de chave estrangeira, não arestas armazenadas. Direcionar um relacionamento é um erro rígido. (REQ-665)
- Escritas rodam através do pipeline de escrita completo: injeção de RLS e hooks pós-mutação (invalidação de cache de resposta, marcação de obsolescência de view materializada, eventos de mudança Kafka, recarga de tabela quente). (REQ-798)
- `MERGE`, `DETACH DELETE`, e `REMOVE` não são suportados e são rejeitados no momento da análise. (REQ-671)

---

## Acesso por Protocolo

Cypher alcança o mesmo pipeline governado através de dois transportes:

- **HTTP** — `POST /data/cypher` com um corpo JSON (`{"query": "...", "params": {...}}`). Retorna linhas tipadas, ou `affected_rows` para escritas. Variáveis de grafo na cláusula `RETURN` serializam como JSON: nós carregam `id`, `label`, `tableLabel`, e `properties`; arestas carregam `identity`, `start`, `end`, `type`, `properties`, `startNode`, e `endNode`; caminhos carregam `nodes`, `edges`, e `length`/`hops`. (REQ-750) Commands registrados também são chamáveis aqui via `CALL fn(args) YIELD col1, col2` — args posicionais mapeiam para os nomes de argumento declarados do command em ordem. (REQ-1156) [tool-verified: `provisa/api/rest/registered_call.py:113-143`]
- **Bolt** — um servidor de protocolo binário compatível com Neo4j (codec PackStream, framing em chunks) que permite ao Neo4j Browser, Bloom, e drivers Bolt rodar Cypher sobre o grafo federado. (REQ-802) Ele inicia quando `PROVISA_BOLT_PORT` é definido para um valor não-zero e é desabilitado por padrão; defina `PROVISA_BOLT_CERT` / `PROVISA_BOLT_KEY` para TLS. [tool-verified: `provisa/api/app_startup.py:317-338`] A autenticação Bolt mapeia principal para usuário e banco de dados para função: `SHOW DATABASES` lista uma entrada por par (view × função), nomeada `provisa_<role>` (domínios de negócio) ou `provisa_ops_<role>` (com domínios system/meta/ops); `:use` seleciona a função e view ativas. (REQ-807) Relacionamentos recebem IDs inteiros duráveis via uma tabela `rel_ids`, espelhando o design de `node_ids`. (REQ-806) Commands registrados são chamáveis com `CALL command(args)` — args posicionais mapeiam para nomes de argumento declarados em ordem; procedimentos `CALL dbms.*` / `CALL db.*` têm precedência. (REQ-1156) [tool-verified: `provisa/bolt/session.py:722-749`]

### Análise de Grafo (Graph Analytics)

`POST /data/graph-analytics` roda uma consulta Cypher, constrói um grafo NetworkX em memória a partir dos nós e arestas resultantes, executa um algoritmo nomeado, e mescla um dict `_analytics` em cada nó e aresta antes de retorná-los como JSON com um campo `elapsed_ms`. (REQ-642) As chaves de `_analytics` variam por algoritmo: centralidade produz `score`; detecção de comunidade produz `cluster`; k-core produz `core_number`; centralidade de grau adiciona `in_degree` e `out_degree`. (REQ-643) O endpoint rejeita grafos acima de um tamanho configurável (padrão 10.000 nós / 50.000 arestas) com HTTP 413; Girvan-Newman é limitado a 500 nós a menos que o chamador passe `force=true`. (REQ-650, REQ-651)

---

## Limitações

### Restrições de design

1. **Escritas são limitadas a `CREATE`, `SET`, e `DELETE`.** Estas executam como escritas de tabela diretas através do mesmo pipeline que mutações GraphQL e SQL. (REQ-818, REQ-666, REQ-667, REQ-668) Veja §Escritas acima. `MERGE`, `DETACH DELETE`, e `REMOVE` são rejeitados no momento da análise. (REQ-671, REQ-818) Procedimentos APOC também são rejeitados.

2. **Sem propriedades de relacionamento.** Relacionamentos (`-[r:TYPE]->`) existem somente como metadados de join na camada semântica. (REQ-574) Eles não carregam atributos armazenados, então `WHERE r.since > 2020` ou `RETURN r.weight` não têm significado e não são suportados.

3. **Travessia bidirecional** `(a)-[]-(b)` reescreve para a UNION ALL frente+trás de todos os relacionamentos direcionados correspondentes da camada semântica. (REQ-575) Todo relacionamento na camada semântica é direcional; sintaxe bidirecional é açúcar sintático que se expande para ambas as direções. Branches extras são emitidos no nível de consulta mais externo — padrões MATCH subsequentes na mesma consulta não são duplicados entre branches (limitação para bidirecional de múltiplos MATCH).

4. **Caminhos recursivos exigem um limite.** Padrões de comprimento variável (`[*]`) devem incluir um limite superior (ex.: `[*..10]`). (REQ-348) Travessia sem limite é rejeitada no momento da análise para prevenir CTEs recursivos descontrolados.

### Notas de comportamento

5. **`shortestPath` em caminhos não auto-referenciais usa JOIN plano, não ordenação por hops.** Quando os tipos de início e fim diferem e nenhum relacionamento auto-referencial existe no esquema, o tradutor emite uma cadeia de JOIN plana (o caminho de esquema mais curto). (REQ-576) Ele não emite `ORDER BY hops` porque hops não são rastreados nesse caminho de código. O resultado é o caminho de esquema estruturalmente mais curto, não o caminho mais curto de dados através de múltiplas linhas.

6. **Múltiplos caminhos de esquema produzem `UNION ALL`.** Quando dois caminhos de esquema de igual contagem de hop conectam os mesmos tipos de início e fim (ex.: `Person -[WORKS_AT]-> Company` e `Person -[MANAGES]-> Company`), ambos são emitidos como branches `UNION ALL`. (REQ-577) Deduplicação de linhas que aparecem em ambos os branches não é realizada.

7. **Um `RelationshipMapping` por par fonte→alvo e combinação de rel\_type.** Se dois campos GraphQL no mesmo tipo de fonte produzem a mesma string `rel_type` (após maiusculização) para o mesmo tipo alvo, o segundo registro sobrescreve o primeiro em `CypherLabelMap.relationships`. A chave de relacionamento inclui nomes de tipo fonte e alvo, então pares fonte/alvo distintos com o mesmo nome de tipo cada um recebe sua própria entrada e não são afetados.

8. **CTEs de cláusula `WITH` são nomeados `_w0`, `_w1`, …** (REQ-578) Nomes são atribuídos posicionalmente dentro de uma única chamada de tradução. Compor múltiplas consultas traduzidas (ex.: em um batch) pode produzir nomes de CTE colidentes se forem concatenadas ingenuamente.

### Cobertura de expressão e padrão (REQ-913)

Expressões Cypher são analisadas em uma AST e reduzidas nó a nó para SQL (`provisa/cypher/expr_parser.py`, `provisa/cypher/expr_visitor.py`). A gramática segue a torre de precedência `oC_Expression` do openCypher. Suportado: literais, parâmetros, acesso a propriedade, `n.prop`, índice e slice, aritmética (`+ - * / % ^`), comparação, `IN`, `STARTS WITH` / `ENDS WITH` / `CONTAINS` / `=~`, `IS [NOT] NULL`, booleano `AND` / `OR` / `XOR` / `NOT`, `CASE`, literais de lista e mapa, compreensões de lista e padrão (incluindo a vinculação de caminho `p = (…)`), projeção de mapa, `reduce`, os quantificadores `all` / `any` / `none` / `single`, subconsultas existenciais, e chamadas de função.

9. **Labels são fixos; você não pode criar tipos de objeto através do Cypher.** Um label resolve para um domínio conhecido, um tipo de objeto conhecido, ou um `domain:object_type` qualificado — o conjunto fechado definido pelo esquema registrado. Cypher nunca introduz um novo label ou tipo. Criação de instância é possível somente para tipos já definidos dentro de uma fonte de dados gravável; `CREATE` escreve linhas em tal tabela (veja §Escritas) mas não pode definir um novo label ou tipo. (REQ-662) Ambas as formas de label são aceitas e significam o mesmo teste: o postfix `n:Label` e o verboso `n IS :Label` (e sua negação `n IS NOT :Label`). Um label qualificado é escrito `n:domain:object_type`.

10. **`shortestPath` e `allShortestPaths` são suportados somente dentro de `MATCH`, não como expressões.** Em um padrão (`MATCH p = shortestPath((a:Person)-[:KNOWS*..5]->(b:Person))`) eles traduzem para um CTE `WITH RECURSIVE` e exigem nós de origem e alvo com label. Usados em posição de expressão — por exemplo `RETURN shortestPath((a)-[*]->(b))` ou `WHERE length(shortestPath((a)-[*]->(b))) < 5` — eles não são suportados, porque a reescrita recursiva é orientada pela cláusula `MATCH` em vez de uma subconsulta correlacionada.

11. **Compreensões de lista, `REDUCE`, e quantificadores rodam contra valores de lista; compreensões de padrão percorrem.** `reduce(...)`, `all/any/none/single(...)`, e a compreensão de lista `[x IN list | …]` operam sobre uma expressão de lista e reduzem para as funções de lista de ordem superior do motor — elas mesmas não percorrem o grafo. A compreensão de **padrão** `[(a)-[:R]->(b) WHERE p | e]` percorre: seu padrão de grafo é endereçado como uma subconsulta correlacionada, então é uma compreensão cuja fonte é uma travessia. Alimente resultados de travessia nas formas de lista com `nodes(p)` / `relationships(p)` / `collect(...)`, ou use uma compreensão de padrão diretamente.
