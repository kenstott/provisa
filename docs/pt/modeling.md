<!-- markdownlint-disable MD046 -->
<!-- MD046 off: mkdocs-material `===` content-tab bodies are indented, which the linter
     misreads as indented code blocks; the fenced code blocks below are required for rendering. -->

# Modelagem de Dados (Entidades e Fatos)

O Provisa oferece duas primitivas declarativas — `entity` e `fact` — que cobrem os blocos de
construção com os quais todo star schema e Data Vault são montados. Declare a especificação; o
Provisa a reduz exatamente às definições de view materializada, bitemporais, e de relacionamento
que você teria que escrever manualmente (REQ-1164). [tool-verified: modeling.py module docstring lines 11-28]

## O que são entidades e fatos

Uma **entity** é uma projeção com chave, deduplicada, opcionalmente historizada de uma relação de
fonte. Você a nomeia, a aponta para uma fonte, declara a chave da entidade e os atributos que quer
carregar, e escolhe um modo de histórico. O Provisa escreve o SQL da view e registra a MV. Quando o
histórico é habilitado, a MV é bitemporal. [tool-verified: `Entity` dataclass, modeling.py lines 53-69;
`entity_registration` function, modeling.py lines 105-120]

Um **fact** é um join para chaves de entidade, reduzido a um grão declarado, com medidas agregadas.
O Provisa escreve uma consulta de MV agregada (`GROUP BY` grão + colunas FK) e registra um
relacionamento para cada link de dimensão declarado. Um fact sem medidas é um conjunto de chaves
puro — o padrão de link do Data Vault.
[tool-verified: `Fact` dataclass, modeling.py lines 91-102; `fact_registration` function, modeling.py
lines 123-141; comment at line 130 "a measureless fact is a pure key-set (DV link)"]

Ambas as construções são IR. As definições geradas se realvejam através de motores — materializadas
em Oracle, Databricks, ou deixadas virtuais sobre um motor MPP — sem remodelagem. [tool-verified: modeling.py
docstring lines 25-28]

## Modos de histórico

Três modos estão disponíveis em uma entity [tool-verified: `_HISTORY` constant at modeling.py line 38,
`_HISTORY_MODE` dict at modeling.py line 40]:

| Modo | Significado | Modo bitemporal |
| --- | --- | --- |
| `none` | Somente atual. Sem histórico. | — |
| `scd2` | Rastreia toda mudança. Anexa somente linhas alteradas (delta) chaveadas na chave da entidade. | `delta` |
| `snapshot` | Rastreia toda atualização. Anexa o conjunto de resultado completo a cada atualização, carimbado com tempo de sistema. | `snapshot` |

`scd2` precisa de uma chave de entidade para computar o delta. `snapshot` funciona em qualquer
motor mas o armazenamento cresce por uma cópia completa por atualização. Escolha `scd2` para fontes
grandes e de mudança lenta; escolha `snapshot` quando você precisa de histórico completo e a fonte
não consegue fornecer uma chave.

Facts não têm modo de histórico — a cobertura temporal vem do histórico da entity subjacente.

## Medidas e agregações

Medidas são declaradas como pares `column:agg`. Agregações suportadas [tool-verified: `_AGGS`
at modeling.py line 41]:

`sum` &nbsp;`avg` &nbsp;`min` &nbsp;`max` &nbsp;`count`

A agregação padrão é `sum` [tool-verified: `Measure.agg` default at modeling.py line 75].

## Exemplo trabalhado: Entity Customer + Fact Sales

### As tabelas de fonte

- `raw.customers` — id, name, region, tier
- `raw.orders` — order_id, customer_id, amount, quantity

### Registre a entity Customer

=== "UI de Administração"

    1. Abra **Tables** e clique em **+ Model**.
    2. Escolha **Entity (dimension)**.
    3. Preencha o formulário:
       - **Name:** `Customer`
       - **Source relation:** `raw.customers`
       - **Domain:** *(seu domínio)*
       - **Entity key:** `id`
       - **Attributes:** `name, region, tier`
       - **History:** `SCD2 (track changes — delta bitemporal)`
    4. Clique em **Create**.

=== "API GraphQL"

    ```graphql
    mutation {
      registerEntity(input: {
        name: "Customer"
        source: "raw.customers"
        domainId: "sales"
        key: ["id"]
        attributes: ["name", "region", "tier"]
        history: "scd2"
      }) {
        success
        message
      }
    }
    ```

O Provisa gera e registra esta MV bitemporal [tool-verified: `entity_registration` in
modeling.py lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- registered as a bitemporal delta MV, entity key: ["id"]
```

### Registre o fact Sales

=== "UI de Administração"

    1. Clique em **+ Model** novamente.
    2. Escolha **Fact**.
    3. Preencha o formulário:
       - **Name:** `Sales`
       - **Source relation:** `raw.orders`
       - **Domain:** *(seu domínio)*
       - **Grain:** `order_id`
       - **Measures:** `amount:sum, quantity:sum`
       - **Dimensions:** `Customer:customer_id`
    4. Clique em **Create**.

=== "API GraphQL"

    ```graphql
    mutation {
      registerFact(input: {
        name: "Sales"
        source: "raw.orders"
        domainId: "sales"
        grain: ["order_id"]
        measures: [
          { column: "amount", agg: "sum" }
          { column: "quantity", agg: "sum" }
        ]
        dimensions: [
          { entity: "Customer", via: "customer_id" }
        ]
      }) {
        success
        message
      }
    }
    ```

O Provisa gera e registra [tool-verified: `fact_registration` in modeling.py lines 123-141]:

```sql
SELECT "order_id", "customer_id",
       SUM("amount") AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

Mais um relacionamento registrado: `Sales.customer_id → Customer` (cardinalidade: many-to-one).
[tool-verified: `fact_table_input` in modeling_register.py lines 89-98, cardinality at line 95]

## O formulário Model (UI de administração)

O botão **+ Model** aparece na página **Tables** (tooltip: "Model an entity or fact (star
schema / Data Vault)"). [tool-verified: tablesPage.json line 13; TablesPage.tsx lines 441-450]

Um controle segmentado no topo do modal alterna entre **Entity (dimension)** e **Fact**.
[tool-verified: ModelingForm.tsx lines 102-110]

### Campos de Entity

[tool-verified: ModelingForm.tsx lines 141-171; modelingForm.json]

| Campo | Obrigatório | Notas |
| --- | --- | --- |
| Name | sim | O nome da MV no catálogo |
| Source relation | sim | Relação com ponto, ex.: `raw.customers` |
| Domain | sim | Domínio ao qual a MV pertence |
| Entity key | sim | Coluna(s) de chave separadas por vírgula, ex.: `id` |
| Attributes | não | Colunas de atributo separadas por vírgula, ex.: `name, region, tier` |
| History | não | `none` / `scd2` / `snapshot`; padrão é `none` |

### Campos de Fact

[tool-verified: ModelingForm.tsx lines 172-196; modelingForm.json]

| Campo | Obrigatório | Notas |
| --- | --- | --- |
| Name | sim | O nome da MV no catálogo |
| Source relation | sim | Relação com ponto, ex.: `raw.orders` |
| Domain | sim | Domínio ao qual a MV pertence |
| Grain | sim | Coluna(s) de grão separadas por vírgula, ex.: `order_id` |
| Measures | não | Pares `col:agg` separados por vírgula, ex.: `amount:sum, quantity:sum` |
| Dimensions | não | Pares `Entity:fk_column` separados por vírgula, ex.: `Customer:customer_id` |

Quando `agg` é omitido em uma measure (`amount` em vez de `amount:sum`), o padrão é `sum`.
[tool-verified: ModelingForm.tsx line 73 `agg: agg || "sum"`]

## A API GraphQL

Ambas as mutações vivem no esquema de administração. [tool-verified: schema_mutation.py lines 449-472]

### `registerEntity`

```graphql
mutation RegisterEntity($input: EntityInput!) {
  registerEntity(input: $input) {
    success
    message
  }
}
```

Campos de `EntityInput` [tool-verified: types.py lines 449-456]:

| Campo | Tipo | Padrão | Descrição |
| --- | --- | --- | --- |
| `name` | String | — | Nome de catálogo para a MV de entity |
| `source` | String | — | Relação de fonte (`schema.table` ou entre aspas) |
| `domainId` | String | — | Id do domínio |
| `key` | [String] | — | Coluna(s) de chave da entity |
| `attributes` | [String] | `[]` | Colunas de atributo a projetar |
| `history` | String | `"none"` | `"none"` \| `"scd2"` \| `"snapshot"` |
| `visibleTo` | [String] | `["public"]` | Lista de visibilidade de função |

### `registerFact`

```graphql
mutation RegisterFact($input: FactInput!) {
  registerFact(input: $input) {
    success
    message
  }
}
```

Campos de `FactInput` [tool-verified: types.py lines 472-479]:

| Campo | Tipo | Padrão | Descrição |
| --- | --- | --- | --- |
| `name` | String | — | Nome de catálogo para a MV de fact |
| `source` | String | — | Relação de fonte |
| `domainId` | String | — | Id do domínio |
| `grain` | [String] | — | Coluna(s) de grão para o GROUP BY |
| `measures` | [MeasureInput] | `[]` | Pares `{ column, agg }` |
| `dimensions` | [DimRefInput] | `[]` | Pares `{ entity, via }` |
| `visibleTo` | [String] | `["public"]` | Lista de visibilidade de função |

`MeasureInput`: `{ column: String, agg: String }` — agg tem `"sum"` como padrão.
[tool-verified: types.py lines 460-462]

`DimRefInput`: `{ entity: String, via: String }` — `entity` é o nome da entity referenciada;
`via` é a coluna FK na fonte do fact.
[tool-verified: types.py lines 465-468]

Em caso de sucesso, `registerFact` retorna uma mensagem no formato:
`Fact 'Sales' registered with 1 dimension link(s)`.
[tool-verified: schema_mutation.py line 471]

## Star schema Kimball e Data Vault

Nenhum padrão exige ferramentas separadas. As mesmas duas primitivas compõem ambos.

### Star schema Kimball

Este walkthrough constrói um star de três dimensões. Duas tabelas de fonte são novas:

- `raw.products` — `product_id`, `name`, `category`, `list_price` [inferred: introduced for this example]
- `raw.date_spine` — `date_key`, `year`, `quarter`, `month` [inferred: introduced for this example]

`raw.orders` também ganha colunas `product_id` e `order_date` aqui. [inferred]

#### Escolhendo o tipo SCD

O modo de histórico é o único controle entre SCD Type 1 e Type 2:

| Tipo SCD | Modo de histórico | Efeito |
| --- | --- | --- |
| Type 1 (somente atual) | `none` | MV reconstruída na atualização; sem histórico de linha |
| Type 2 (versionado) | `scd2` | MV delta bitemporal; cada mudança anexa uma nova linha chaveada na chave da entity |

[tool-verified: `_HISTORY_MODE` at modeling.py line 40; `entity_registration` history branch at
lines 115-119]

Use `scd2` quando consultas a jusante precisam unir uma dimensão como ela existia no momento da
transação — o tier de um cliente no momento da compra, não seu tier atual. Use `none` para lookups
estáveis. Um date spine nunca muda. Um catálogo de produtos onde você só precisa do preço atual pode
ser reconstruído a cada atualização.

#### Decisão de grão

O grão é o nível de detalhe mais baixo que o fact responde. `order_id` dá uma linha por pedido,
preservando a capacidade de contar pedidos distintos e unir a qualquer dimensão na granularidade do
pedido. Um grão mais grosso — digamos `["customer_id", "order_date"]` — pré-agrega através de
pedidos e descarta esse detalhe permanentemente. Declare o grão mais estreito que o negócio precisa;
rollups mais grossos são baratos de derivar depois.

#### Registre as dimensões

**Customer** (SCD Type 2 — mudanças de tier devem ser preservadas):

```graphql
mutation {
  registerEntity(input: {
    name: "Customer"
    source: "raw.customers"
    domainId: "sales"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

Gera uma MV delta bitemporal chaveada em `id` [tool-verified: entity_registration modeling.py
lines 105-120]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

**Product** (SCD Type 1 — catálogo atual, nenhum histórico de versão necessário):

```graphql
mutation {
  registerEntity(input: {
    name: "Product"
    source: "raw.products"
    domainId: "sales"
    key: ["product_id"]
    attributes: ["name", "category", "list_price"]
    history: "none"
  }) { success message }
}
```

Gera uma MV comum reconstruída na atualização [tool-verified: entity_registration modeling.py
lines 105-114; `mv_bitemporal_mode` is only added when `history != "none"`, line 115]:

```sql
SELECT "product_id", "name", "category", "list_price" FROM "raw"."products"
```

**DateDim** (sem histórico — uma data é imutável):

```graphql
mutation {
  registerEntity(input: {
    name: "DateDim"
    source: "raw.date_spine"
    domainId: "sales"
    key: ["date_key"]
    attributes: ["year", "quarter", "month"]
    history: "none"
  }) { success message }
}
```

Gera:

```sql
SELECT "date_key", "year", "quarter", "month" FROM "raw"."date_spine"
```

#### Registre o fact Sales através de três dimensões

Grão: `order_id`. Três referências de dimensão — uma coluna FK cada. Ambas as medidas são somas
aditivas.

```graphql
mutation {
  registerFact(input: {
    name: "Sales"
    source: "raw.orders"
    domainId: "sales"
    grain: ["order_id"]
    measures: [
      { column: "amount",   agg: "sum" }
      { column: "quantity", agg: "sum" }
    ]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
      { entity: "DateDim",  via: "order_date"  }
    ]
  }) { success message }
}
```

O Provisa computa `group_cols = dedup([grain] + [dim FKs])`
= `["order_id", "customer_id", "product_id", "order_date"]` e gera
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", "product_id", "order_date",
       SUM("amount")   AS "amount",
       SUM("quantity") AS "quantity"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id", "product_id", "order_date"
```

Três relacionamentos são registrados automaticamente [tool-verified: modeling_register.py lines 89-98,
cardinality `"many_to_one"` at line 95]:

| Relacionamento | Cardinalidade |
| --- | --- |
| `Sales.customer_id → Customer` | many-to-one |
| `Sales.product_id → Product` | many-to-one |
| `Sales.order_date → DateDim` | many-to-one |

#### Dimensões conformadas

Uma dimensão conformada é registrada uma vez e referenciada por nome por qualquer número de facts.
Suponha que `raw.returns` contenha `return_id`, `customer_id`, `product_id`, e `amount`. O fact
Returns reutiliza Customer e Product sem registrá-los novamente:

```graphql
mutation {
  registerFact(input: {
    name: "Returns"
    source: "raw.returns"
    domainId: "sales"
    grain: ["return_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "Customer", via: "customer_id" }
      { entity: "Product",  via: "product_id"  }
    ]
  }) { success message }
}
```

Tanto `Sales` quanto `Returns` apontam para as mesmas entities `Customer` e `Product`. Os caminhos
de join do Provisa garantem que consultas através de qualquer fact percorram a mesma definição de
dimensão
[tool-verified: fact_registration uses entity name as `target_table` at modeling.py lines 138-140;
fact_table_input wires `target_table_id` from that name at modeling_register.py lines 91-93].

---

### Data Vault

As mesmas primitivas mapeiam diretamente para o vocabulário do Data Vault:

| Artefato DV | Primitiva | Histórico |
| --- | --- | --- |
| Hub | `entity` | `none` — somente chaves de entity |
| Satellite | `entity` | `scd2` ou `snapshot` — histórico de atributo ao lado da chave do hub |
| Link | `fact` sem medidas | — |
| Bridge / aggregate link | `fact` com medidas | — |

O exemplo constrói um vault mínimo sobre `raw.customers` e `raw.orders`.

#### Hubs

Um hub contém a chave da entity e nada mais. `attributes: []` com `history: "none"` produz um
conjunto de chave atual deduplicado; o histórico de atributo vive inteiramente no satellite.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerHub"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

Gera [tool-verified: entity_registration modeling.py lines 107-108;
`cols = dedup([*key, *attributes])` = `["id"]` when `attributes=[]`]:

```sql
SELECT "id" FROM "raw"."customers"
```

```graphql
mutation {
  registerEntity(input: {
    name: "OrderHub"
    source: "raw.orders"
    domainId: "vault"
    key: ["order_id"]
    attributes: []
    history: "none"
  }) { success message }
}
```

Gera:

```sql
SELECT "order_id" FROM "raw"."orders"
```

#### Satellite

O satellite fica ao lado da chave do hub e carrega histórico de atributo completo. Use `scd2` para
anexar somente linhas alteradas; use `snapshot` para carimbar cada atualização completa.

```graphql
mutation {
  registerEntity(input: {
    name: "CustomerSat"
    source: "raw.customers"
    domainId: "vault"
    key: ["id"]
    attributes: ["name", "region", "tier"]
    history: "scd2"
  }) { success message }
}
```

Gera [tool-verified: entity_registration modeling.py lines 115-119;
`_HISTORY_MODE["scd2"]` = `"delta"` at modeling.py line 40]:

```sql
SELECT "id", "name", "region", "tier" FROM "raw"."customers"
-- bitemporal delta MV, entity key: ["id"]
```

`CustomerSat` e `CustomerHub` ambos chaveiam em `id`. O hub é o alvo de join estável; o satellite
fornece acesso de atributo point-in-time através da camada bitemporal.

#### Link (fact sem medidas)

Um link registra quais chaves de hub coocorreram — somente chaves, sem medidas. O Provisa omite o
`GROUP BY` quando `measures` está vazio [tool-verified: modeling.py lines 130-131:
`if f.measures: view_sql += " GROUP BY ..."`].

```graphql
mutation {
  registerFact(input: {
    name: "OrderCustomerLink"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: []
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`.
Sem medidas, então sem `GROUP BY`. Gera [tool-verified: fact_registration modeling.py lines
125-131]:

```sql
SELECT "order_id", "customer_id" FROM "raw"."orders"
```

Dois relacionamentos registrados: `OrderCustomerLink.customer_id → CustomerHub` e
`OrderCustomerLink.order_id → OrderHub`, ambos many-to-one
[tool-verified: modeling_register.py lines 89-98].

#### Bridge / aggregate link

Adicione medidas ao link e o Provisa emite o `GROUP BY`, produzindo um bridge pré-agregado. No grão
`order_id` com um cliente por pedido, o resultado é uma linha agregada por pedido:

```graphql
mutation {
  registerFact(input: {
    name: "OrderSummary"
    source: "raw.orders"
    domainId: "vault"
    grain: ["order_id"]
    measures: [{ column: "amount", agg: "sum" }]
    dimensions: [
      { entity: "CustomerHub", via: "customer_id" }
      { entity: "OrderHub",    via: "order_id"    }
    ]
  }) { success message }
}
```

`group_cols = dedup(["order_id"] + ["customer_id", "order_id"])` = `["order_id", "customer_id"]`
(o `order_id` duplicado da lista de dimensão é removido por `_dedup`). Gera
[tool-verified: fact_registration modeling.py lines 125-131]:

```sql
SELECT "order_id", "customer_id", SUM("amount") AS "amount"
FROM   "raw"."orders"
GROUP BY "order_id", "customer_id"
```

O modelo não decide a metodologia. Grão, conformidade, escolha de SCD, e a divisão hub/satellite
permanecem decisões do modelador. O Provisa as executa. [tool-verified: modeling.py
docstring lines 25-26]

## Metrics (REQ-1317, REQ-1318, REQ-1320)

Um **metric** é uma definição de agregado nomeada e governada sem grão próprio. O grão — as
dimensões pelas quais o agregado é discriminado — é vinculado no momento da consulta pelo chamador,
não no momento da definição. Isso é o que distingue um metric de uma view: uma view trava o grão na
criação; um metric permanece aberto até ser consultado. [tool-verified: `Metric` class comment, `provisa/core/models.py` lines
452–455: "A named, governed aggregate definition with no grain of its own... grain is bound at
query time by the requested dimension set"]

### O objeto Metric

[tool-verified: `Metric` class, `provisa/core/models.py` lines 451–476]

| Campo | Obrigatório | Notas |
| --- | --- | --- |
| `name` | sim | snake_case, ex.: `net_revenue`. Validado: `[a-z][a-z0-9_]*` |
| `expression` | sim | ANSI-SQL agregado; deve incluir pelo menos uma função de agregação |
| `datatype` | não | Dica de tipo de resultado, ex.: `number`, `integer` |
| `description` | não | Definição de negócio legível por humano |
| `ai_context` | não | Texto para consumidores de IA — projeta para ferramentas MCP, pg_description, docs GraphQL, e exportação Ossie |
| `visible_to` | não | Lista de função; padrão é `["*"]` (todas as funções) |
| `from_fact` | — | Definido automaticamente quando o metric foi gerado de uma measure de fact |

Referências de coluna dentro da expressão devem ser qualificadas por tabela (`orders.amount`, não
`amount`). Uma coluna não qualificada é um erro rígido no momento da expansão, não um aviso.
[tool-verified: `_expression_tables`, `provisa/compiler/metric_expand.py` lines 83–96]

O repositório de metric valida a expressão em toda escrita. Uma expressão que não analisa ou não
contém nenhuma função de agregação é rejeitada; nunca é armazenada.
[tool-verified: `validate_expression`, `provisa/core/repositories/metric.py` lines 34–43]

Exemplo de entrada de config:

```yaml
metrics:
  - name: net_revenue
    expression: "SUM(orders.amount) - SUM(orders.refunds)"
    datatype: number
    description: "Order revenue after refunds"
    ai_context: "Net revenue: total order amounts minus approved refunds. Use for P&L."
```

### Consultando um metric

O compilador reserva o esquema `metrics`. [tool-verified: `METRICS_SCHEMA = "metrics"`,
`provisa/compiler/metric_expand.py` line 43] Todo metric é endereçável como uma relação virtual
dentro desse esquema. Consulte-o como uma tabela — as colunas que você seleciona se tornam o
conjunto de dimensão e o GROUP BY:

```sql
-- Scalar total (no dimension)
SELECT value FROM metrics.net_revenue;

-- Broken out by region and month
SELECT region, month, value FROM metrics.net_revenue GROUP BY region, month;
```

O compilador reescreve isso em um agregado agrupado real sobre as tabelas semânticas subjacentes
antes de a governança rodar, para que RLS e mascaramento se apliquem às colunas reais.
[tool-verified: `expand_metric_query` docstring, `provisa/compiler/metric_expand.py` lines 263–276:
"BEFORE governance, so RLS/masking apply to the real columns (REQ-1317)"]

`SELECT *` contra uma relação de metric é rejeitado — nomeie as colunas de dimensão e `value`
explicitamente. [tool-verified: `expand_metric_query`, `provisa/compiler/metric_expand.py` lines 302–306]

Quando a expressão de um metric abrange múltiplas tabelas, o compilador as une através de
relacionamentos registrados. Uma dimensão que é uma coluna de uma tabela diretamente referenciada
resolve para aquela tabela. Uma dimensão a um hop de relacionamento de distância é unida
automaticamente. Dois hops ou uma dimensão ambígua é um erro rígido nomeando o infrator.
[tool-verified: `_JoinPlan.resolve_dimension`, `provisa/compiler/metric_expand.py` lines 190–228]

### Metrics de especificações de fact (REQ-1320)

Quando você registra um fact, cada measure declarada auto-registra um objeto Metric
correspondente. O campo `from_fact` do metric registra o nome da tabela de fact de fonte, e as
dimensões de agrupamento válidas são os atributos de entity alcançáveis sobre os relacionamentos
FK do fact.
[tool-verified: `Metric.from_fact` comment, `provisa/core/models.py` line 466–467:
"set when this metric was auto-registered from a fact spec's measure";
`from_fact` stored in `provisa/core/repositories/metric.py` line 57]

Metrics auto-registrados aparecem na página Metrics com um badge **fact**. Você pode editá-los
como qualquer outro metric. [tool-verified: `MetricsPage.tsx` lines 405–408:
`{m.fromFact && <Badge ... data-testid={`metrics-from-fact-${m.name}`}>...</Badge>}`]

### Views compostas por metric (view_metrics, REQ-1318)

Uma view `view_metrics` fecha o grão de um metric no momento da definição. Declare os nomes dos
metrics, colunas de dimensão, e filtros opcionais; o compilador gera o SELECT.

[tool-verified: `ViewMetricsSpec`, `provisa/core/models.py` lines 479–492]

```yaml
tables:
  - source_id: pg1
    domain_id: sales
    schema: public
    table: monthly_revenue
    view_metrics:
      metrics: [net_revenue]
      dimensions: [region, month]
      filters: ["orders.status = 'completed'"]
```

O compilador gera (para este exemplo):

```sql
SELECT orders.region AS region, orders.month AS month,
       SUM(orders.amount) - SUM(orders.refunds) AS net_revenue
FROM orders
WHERE orders.status = 'completed'
GROUP BY orders.region, orders.month
```

`view_metrics` e `view_sql` são mutuamente exclusivos na mesma tabela.
[tool-verified: `Table` model validator, `provisa/core/models.py` lines 614–617:
`if self.view_sql is not None and self.view_metrics is not None: raise ValueError(...)`]

**Auto-regeneração na mudança de metric.** Quando a expressão de um metric é atualizada, toda view
`view_metrics` que o referencia recompila e o novo SQL é persistido imediatamente. A view não pode
divergir da definição do metric por construção.
[tool-verified: `regenerate_metric_views`, `provisa/api/admin/_metric_views.py` lines 79–117:
"each dependent view_metrics spec recompiles against the UPDATED metric set and the fresh SQL
is persisted"]

**Chamadas `metric()` inline em SQL de view livre.** `view_sql` escrito manualmente também pode
referenciar metrics via `metric('name')`. O compilador substitui cada chamada pela expressão do
metric e registra uma aresta de lineage. Isso dá a views escritas manualmente a mesma propriedade
de recompilação na mudança quando elas referenciam um metric em vez de copiar sua fórmula.
[tool-verified: `expand_metric_calls_in_sql`, `provisa/compiler/metric_expand.py` lines 393–429]

Nota: views de caminho de config usando chamadas `metric()` inline regeneram no recarregamento de
config, não no upsert de metric. [tool-verified: `regenerate_metric_views` docstring, `_metric_views.py` lines 84–86:
"Free-hand view_sql born from inline metric() calls carries no stored provenance, so it is not
regenerated here (config-path views regenerate on config reload)"]

### A página de administração de Metrics (REQ-1323, REQ-1324)

Abra o item de navegação **Metrics** para gerenciar metrics governados. Clique em uma linha para
expandir um painel de detalhe somente leitura; clique em **Edit** dentro dele para alternar para
edição inline (sem modal). **New Metric** abre um card de criação inline acima da tabela. A
confirmação de exclusão é o único modal na página.
[tool-verified: `MetricsPage.tsx` lines 214–216 comment: "REQ-1317: registered-metrics management
page (list / create / edit / delete). REQ-1323: detail-then-edit"]

O formulário de criação/edição oferece um builder de três seletores para metrics originados de
fact: escolha a tabela de fact de fonte (filtrada para `modelingRole=fact`), uma coluna de measure,
e uma função de agregação (`SUM`, `AVG`, `COUNT`, `MIN`, `MAX`). O datatype é derivado
automaticamente: `COUNT → bigint`, `AVG → numeric`, `SUM/MIN/MAX → o tipo da coluna de measure`.
A área de texto de expressão permanece a válvula de escape para expressões arbitrárias.
[tool-verified: `deriveDatatype` function, `MetricsPage.tsx` lines 66–70;
`applyBuilder`, `MetricsPage.tsx` lines 273–285]

## O retorno do IR

Toda chamada de registro passa pelo mesmo caminho que uma MV escrita manualmente. A especificação
de entity/fact é uma representação intermediária — não um template, não uma macro. O warehouse que
ela visa é uma propriedade da implantação, não do modelo. Mude o motor alvo e as mesmas
declarações `entity` / `fact` materializam lá, porque o SQL gerado e os modos bitemporais são
neutros em relação ao motor por construção. [tool-verified: modeling.py docstring lines 25-28;
modeling_register.py lines 56-66, 80-88]
