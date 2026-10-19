# Dimensionamento e ajuste {: #sizing-and-tuning }

A ferramenta de dimensionamento mede a sua própria implantação do Provisa sob a sua própria carga de trabalho. A partir dessas medições, ela escreve um guia de dimensionamento e ajuste: de quantos núcleos físicos e processos worker a carga precisa e o que cada configuração proporciona. Todo número do guia vem de uma medição, portanto, após uma mudança na implantação ou na carga, você executa a ferramenta de novo e regenera o guia.

A ferramenta fica no repositório do Provisa, em `demo/named/perf/bench/`. Execute cada comando abaixo a partir desse diretório, com o ambiente Python do repositório (`../../../../.venv/bin/python3`).

## O que ela informa {: #what-it-reports }

Uma única execução de dimensionamento é composta por várias execuções de benchmark contra a sua implantação. Cada uma varia uma única coisa:

| Execução | O que muda | O que fornece |
|---|---|---|
| Linha de base | Nada: uma coluna, uma linha, sem filtro, sem junção, sem cache | O custo básico de uma requisição |
| Execuções por parâmetro | Um parâmetro de requisição por vez: campos, filtros, linhas, junções na mesma fonte, junções entre fontes | O custo que cada parâmetro acrescenta por unidade |
| Execuções de junção entre fontes | Os valores da junção entre fontes, uma vez com as duas pontas lidas ao vivo, uma com as duas a partir de réplicas, uma com uma de cada | O custo de uma junção entre fontes conforme a leitura de suas pontas e o ponto de equilíbrio entre pontas ao vivo e de réplica |
| Execuções de cache | Toda requisição aderindo ao cache de respostas, depois nenhuma | O custo de um acerto e de uma falha de cache |
| Execuções de replicação | Uma fonte lida ao vivo, depois a partir de uma réplica | A taxa acima da qual uma réplica custa menos |
| Execuções de workers | Processos worker e requisições em andamento por worker | Workers por núcleo físico, requisições em andamento por worker, o que um núcleo entrega |

As execuções por parâmetro, de cache e de replicação são feitas por transporte e por fonte, e uma vez para cada motor de federação indicado. As execuções de workers são feitas por transporte.

Custo aqui significa o tempo de CPU do Provisa por requisição, em milissegundos. As execuções por parâmetro não usam cache, de modo que o custo de um parâmetro nunca inclui o efeito do cache.

A partir das execuções, `bench_report.py` ajusta um modelo de custo por motor, transporte e fonte: um custo básico mais um custo por campo, filtro, linha e junção, com a junção entre fontes custeada conforme a leitura de suas duas pontas. Quando o motor roda como um servidor próprio e a CPU do seu contêiner é medida, o motor também recebe um modelo de custo. `sizing_guide.py` transforma o relatório no guia. Dado um contrato que descreve a sua carga de trabalho, ele também dimensiona essa carga.

## Escreva um contrato de configuração {: #write-a-setup-contract }

O contrato de configuração é um único arquivo YAML. Ele nomeia os endpoints da sua implantação, as tabelas que o benchmark pode ler, o formato das requisições e a carga. O benchmark confere o contrato com a implantação antes de executar. Ele recusa uma tabela, coluna ou junção que a implantação não expõe.

Tabelas e colunas são nomeadas como a sua implantação as registra. O nome que cada transporte lhes dá (nomes SQL, campos GraphQL, rótulos Cypher, caminhos REST, tipos gRPC) é consultado na implantação quando a execução começa.

```yaml
version: 1

deployment:
  name: acme-prod
  endpoints:
    http: {base_url: "https://provisa.acme.internal"}
    pgwire: {host: provisa.acme.internal, port: 5439}
    bolt: {host: provisa.acme.internal, port: 7687}
    flight: {host: provisa.acme.internal, port: 8815}
    grpc: {host: provisa.acme.internal, port: 50051}
  # The credential is read from an environment variable; the secret never appears here.
  # kind: password (the variable holds the user's password) or token (a ready bearer token).
  credentials: {mode: env, kind: password, user: bench-user, env: ACME_BENCH_PASSWORD}
  # The roles requests are sent as, with the share of requests each gets.
  roles:
    - {role: analyst, weight: 0.8}
    - {role: regional_manager, weight: 0.2}

sources:
  sales-pg:
    # What this source's reads are measured under: live, or a replica refreshed every ttl_seconds.
    replication: {setting: live, ttl_seconds: 0}
    # Optional: a Docker container whose CPU is sampled as the source's CPU.
    monitor: {container: sales-postgres}
    tables:
      - schema: public
        table: orders
        weight: 1.0              # share of this source's requests that start at this table
        base_column: order_id    # the one column a baseline request selects
        columns:
          # A column with a domain can be filtered on; the filter's value is drawn from it.
          - {name: order_id, domain: {kind: int_range, min: 1, max: 5000000}}
          - {name: region, domain: {kind: set, values: [NA, EMEA, APAC]}}
          - {name: amount}
      - schema: public
        table: order_lines
        weight: 0.0              # reached only through the join below
        base_column: line_id
        columns:
          - {name: line_id}
          - {name: order_id, domain: {kind: int_range, min: 1, max: 5000000}}
    # Joins within the source, by registered table and column. The relationship must be registered.
    joins:
      - {left: "orders.order_id", right: "order_lines.order_id"}

# Joins across sources: "source:table.column".
cross_source_joins: []

# The rows a request returns when the rows knob does not apply.
base: {rows: 1}

# Each knob is the probability it applies to a request and the distribution its value is drawn
# from when it does. Distributions: constant {value}, uniform {min, max}, zipf {exponent, max},
# geometric {p, max}.
knobs:
  fields: {probability: 1, distribution: {kind: uniform, min: 1, max: 3}}
  filters: {probability: 0.5, distribution: {kind: constant, value: 1}}
  rows: {probability: 1, distribution: {kind: zipf, exponent: 1.2, max: 100}}
  joins_same_source: {probability: 0.2, distribution: {kind: constant, value: 1}}
  joins_cross_source: {probability: 0, distribution: {kind: constant, value: 0}}
  # How often a request repeats one sent shortly before it: the value is how many requests back.
  cache_repetition: {probability: 0.3, distribution: {kind: uniform, min: 1, max: 50}}
  # Share of requests that opt into the response cache. REST and JSON:API have no opt-in.
  cache: {probability: 0.6}
  # How a request's cross-source joins are chosen by how their two ends are read: both_live,
  # both_replica, or mixed (one of each), as the sources' replication settings above make them.
  # Probability 0 chooses joins whatever their class.
  join_replication: {probability: 0, weights: {both_live: 1.0}}
  # Share of requests whose table is in each source.
  source_weights: {sales-pg: 1.0}
  # auto lets the deployment choose the route; leave every source on auto to size a workload.
  route:
    sales-pg: {mode: auto, federated_probability: 0}

# The transports the benchmark runs, one after another. A transport's entry overrides any of the
# knobs above for that transport only; {} means no override.
transports:
  graphql: {}
  pgwire:
    knobs:
      rows: {probability: 1, distribution: {kind: uniform, min: 100, max: 1000}}

load:
  mode: closed_loop
  steps: [1, 16, 64, 256]   # concurrent connections, one step after another
  window_s: 30              # seconds per step
  processes: half_cores     # load-generator processes: an integer, or half the client's cores
  profile_requests: 0
  idle_window_s: 0          # seconds of no load after the ramp; see the replication runs below

seed: 0

# The workload to size: its request rate and each transport's share of it. The knobs above
# describe its requests. A benchmark run does not read this section.
workload:
  rate_per_s: 1500
  transport_weights: {graphql: 0.7, pgwire: 0.3}
```

O formato do contrato é definido por `setup_contract.py` e `contract_parse.py`; um contrato inválido é recusado com uma mensagem que nomeia o campo. Os pesos de `workload` devem somar 1, e cada transporte que citam deve ser um dos que o contrato executa.

Descreva as requisições como elas realmente são. O guia dimensiona exatamente as requisições que este contrato gera: o dimensionamento as extrai do mesmo gerador com semente que uma execução usa.

## Execute {: #run-it }

### 1. Resolva os nomes uma vez {: #1-resolve-the-names-once }

Execute o contrato da linha de base uma vez. A execução pergunta à implantação como cada transporte chama as suas tabelas e grava `resolved-names.json` ao lado dos seus resultados.

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-pid "$PROVISA_PID" --output-dir results/first
```

`--engine` nomeia o motor de federação que a sua implantação executa (`duckdb`, `pg` ou `trino`). `--server-pid` é o id de processo do servidor; a execução lê o tempo de CPU desse processo e de seus processos filhos (os workers). Ela lê o processo diretamente, então só funciona quando o gerador de carga roda no host do servidor.

### Medindo a CPU do servidor a partir de outro host {: #measuring-the-servers-cpu-from-another-host }

Execute o gerador de carga em uma máquina própria sempre que puder: assim ele não toma núcleos do servidor. Para medir a CPU do servidor a partir de lá, inicie `server_cpu_agent.py` no host do servidor e passe à execução a URL dele em vez de `--server-pid`:

```bash
# on the server's host, from demo/named/perf/bench
../../../../.venv/bin/python3 server_cpu_agent.py --pid "$PROVISA_PID" --listen 10.0.0.5:9555

# on the load generator's host
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup acme.yaml --server-cpu-url http://10.0.0.5:9555/ --output-dir results/first
```

O agente responde um único número, o mesmo que `--server-pid` lê: `GET /` retorna `{"pid": ..., "cpu_seconds": ...}`, os segundos de CPU acumulados do processo do servidor e de seus workers. Todos os outros caminhos respondem 404, e todos os outros métodos são recusados. Dê a uma execução um de `--server-pid` ou `--server-cpu-url`, nunca os dois.

`--listen` é obrigatório: o agente não tem endereço padrão. Vincule-o a um endereço privado que apenas o gerador de carga alcance, nunca a uma interface pública, e interrompa-o (Ctrl+C) quando as execuções terminarem.

### 2. Planeje as execuções {: #2-plan-the-runs }

```bash
../../../../.venv/bin/python3 sweeps.py plan --setup acme.yaml --out results/sizing \
  --resolved-names results/first/optimistic/resolved-names.json \
  --physical-cores 16 --shared-host yes \
  --workers 8,16,32 --request-threads 1,2,4 \
  --replication live,replica --ttl 300 \
  --engines pg,duckdb --engine-container pg=perf-engine-postgresql-1
```

- `--physical-cores`: os núcleos físicos do servidor, não as suas threads de hardware. No Linux, `lscpu` mostra núcleos por soquete e soquetes; multiplique-os. A ferramenta nunca adivinha isso.
- `--shared-host`: `yes` quando as suas fontes ou o gerador de carga rodam no host do servidor. Eles então competem com o Provisa pelos núcleos dele, e o guia informa isso.
- `--workers` e `--request-threads`: os processos worker e as requisições em andamento por worker a testar. Informe os dois ou nenhum.
- `--replication` e `--ttl`: acrescentam as execuções ao vivo e de réplica para cada fonte, com a réplica atualizada a cada `--ttl` segundos. As execuções de junção entre fontes que leem de réplicas também precisam de `--ttl`.
- `--engines`: os motores de federação a medir, `pg` por padrão. Toda execução por parâmetro é planejada uma vez por motor; o motor é definido quando o servidor inicia, então as execuções de cada motor exigem reiniciar o servidor com ele.
- `--engine-container`: `ENGINE=CONTAINER` para um motor que roda como servidor em seu próprio contêiner, cuja CPU cada execução então informa. Omita um motor que roda dentro do Provisa (DuckDB).

O plano grava um contrato por execução e um `manifest.json`. Uma execução que a implantação não pode atender, como junções via gRPC ou uma tabela que um transporte não expõe, é listada no manifest com o motivo. Ela nunca é descartada em silêncio.

As execuções de réplica medem o custo de atualização da réplica durante uma janela sem carga. Defina `load.idle_window_s` no seu contrato (por exemplo, `120`) antes de planejá-las. Sem isso, o relatório recusa o ponto de equilíbrio com `no idle.json`.

### 3. Execute cada contrato planejado {: #3-run-every-planned-contract }

Cada entrada do manifest nomeia o seu arquivo de contrato. Execute cada um em `results/sizing/results/<run name>`:

```bash
../../../../.venv/bin/python3 run_benchmark.py --engine pg --optimistic \
  --setup results/sizing/graphql.sales-pg.filters.1.yaml \
  --resolved-names results/first/optimistic/resolved-names.json \
  --server-pid "$PROVISA_PID" --output-dir results/sizing/results/graphql.sales-pg.filters.1
```

A partir de outro host, use `--server-cpu-url` no lugar de `--server-pid`. Uma execução de workers reinicia o servidor, então o id do processo muda: reinicie o agente também, com o novo `--pid`.

Acrescente `--apply-replication` a toda execução cuja entrada no manifest tenha `"apply_replication": true` (as execuções de replicação e de junção entre fontes). Ele define a replicação que o contrato da execução declara e a restaura depois.

Toda execução traz um `server_env` no manifest: o servidor deve estar rodando com ele. O de uma execução por parâmetro é o seu motor, `{"PROVISA_ENGINE": "duckdb"}`; execute todas as execuções de um motor e depois reinicie o servidor com o seguinte. O de uma execução de workers acrescenta os seus workers e o limite de requisições em andamento, por exemplo `{"PROVISA_WORKERS": "16", "PROVISA_REQUEST_THREADS": "2", "PROVISA_ENGINE": "pg"}`: `PROVISA_WORKERS` é o número de processos worker e `PROVISA_REQUEST_THREADS` define `concurrency.request_threads`, as requisições em andamento que cada worker executa ao mesmo tempo. Aguarde até que todos os workers estejam atendendo (`/health` informa `workers.ready` de `workers.expected`) antes de iniciar a execução.

### 4. Gere o relatório {: #4-build-the-report }

```bash
../../../../.venv/bin/python3 bench_report.py --root results/sizing --out results/sizing/report.json
```

`--tolerance` (padrão `0.05`) define o quão perto da melhor vazão medida uma configuração de workers deve chegar para ser considerada suficiente.

### 5. Gere o guia {: #5-generate-the-guide }

```bash
../../../../.venv/bin/python3 sizing_guide.py --report results/sizing/report.json --out guide.md \
  --setup acme.yaml --resolved-names results/first/optimistic/resolved-names.json
```

Sem `--setup`, o guia tem apenas as seções de ajuste. Com ele, o guia dimensiona o `workload` do contrato com o motor que `--engine` nomeia (`pg` por padrão); um motor que as execuções não mediram é recusado.

## Leia o guia {: #read-the-guide }

### Dimensionando esta carga {: #sizing-this-workload }

O guia termina esta seção com uma linha, por exemplo: **12 núcleos físicos, 24 processos worker, 2 requisições em andamento por worker.** Acima dela, uma tabela mostra cada transporte e fonte: a parcela das requisições, a média de campos, filtros, linhas e junções, a parcela que são acertos de cache e a CPU que uma requisição custa.

Uma requisição custa o modelo de custo nas médias de seus parâmetros em caso de falha e o custo de acerto medido em caso de acerto. A CPU do Provisa por segundo de um transporte, dividida pelo que um núcleo físico entrega, dá os seus núcleos físicos. O guia soma esses valores entre os transportes e arredonda para cima. Os workers são os núcleos vezes o maior número de workers por núcleo que qualquer um dos transportes exige. As requisições em andamento por worker são, da mesma forma, o maior valor que qualquer um deles exige. Uma única configuração atende a todos.

A ferramenta não dimensiona nada que não tenha medido. Uma parte da carga para a qual as execuções não têm modelo interrompe o dimensionamento com uma mensagem que a nomeia:

- um motor que as execuções não mediram
- um transporte sem execuções de workers
- uma fonte sem modelo de custo nesse transporte
- um parâmetro que as execuções não variaram (varie-o e execute de novo)
- uma dica de rota (`direct` ou `federated`) em uma fonte
- uma configuração de replicação ou TTL diferente da medida
- acertos de cache quando as execuções não têm números de cache

### Processos worker e requisições em andamento {: #worker-processes-and-in-flight-requests }

Para cada transporte, o guia mostra o menor número de workers cuja vazão máxima fica dentro da tolerância da melhor. Nesse número de workers, ele escolhe então o menor número de requisições em andamento por worker que faz o mesmo. Mostra essa configuração de duas formas: workers por núcleo físico e requisições em andamento por worker. Mais de qualquer um deles não traz vazão. Menos deixa núcleos ociosos. A tabela também informa o pico de requisições por segundo e a latência p99 dele. Todo ponto medido é listado abaixo.

O fator de capacidade é o número de requisições por segundo que um núcleo físico entrega, vezes a CPU que uma requisição custa sozinha, sobre 1000. Acima de 1, o núcleo faz mais que o trabalho de uma thread, como ocorre quando as suas duas threads de hardware atendem.

### O que uma requisição custa {: #what-a-request-costs }

Para cada motor, transporte e fonte, lado a lado: o custo básico de uma requisição sem cache, o custo que cada campo, filtro, linha e junção acrescenta e o quanto o modelo se ajusta às medições (R², em que 1 é um ajuste perfeito). Uma junção entre fontes tem três custos, conforme a leitura de suas duas pontas: as duas ao vivo, as duas a partir de réplicas, ou uma de cada. Um traço significa que as execuções não variaram esse parâmetro. Onde o motor roda como um servidor próprio, uma segunda tabela dá da mesma forma a CPU do motor: uma requisição que o motor atende, como uma junção entre fontes, mostra ali o seu tempo de motor. Use esses números para ver quanto custaria uma mudança nas suas requisições antes de fazê-la: dois campos a mais em toda requisição acrescentam o dobro do custo por campo.

### A adesão ao cache de respostas {: #the-response-cache-opt-in }

Para cada transporte e fonte: a CPU de uma falha, de um acerto e o que um acerto economiza. Uma requisição só é atendida pelo cache quando adere e repete uma requisição anterior, portanto a economia vale para requisições repetidas. Faça as repetidas aderirem.

### Leituras ao vivo ou de réplica, e o TTL {: #live-or-replica-reads-and-the-ttl }

Para cada motor, transporte e fonte com execuções de replicação: o custo de uma leitura ao vivo, de uma leitura de réplica, a CPU de uma atualização, o TTL e a taxa de equilíbrio. Acima da taxa de equilíbrio, uma réplica custa menos CPU, a do Provisa, a da fonte e a do motor juntas, que a leitura ao vivo. Abaixo dela, leia ao vivo. Uma segunda tabela dá o mesmo para junções entre fontes, pelo número de junções na requisição: as duas pontas lidas ao vivo contra as duas a partir de réplicas, com a atualização contando cada fonte que a troca moveu para uma réplica.

O ponto de equilíbrio é a CPU da atualização por segundo sobre o que uma leitura economiza:

```text
break-even req/s = (refresh CPU s × 1000 / TTL s) / (live ms − replica ms)
```

Dobrar o TTL reduz à metade a taxa de equilíbrio, e os dados podem ter até o dobro da idade. Escolha o TTL conforme o quanto os seus usuários aceitam que os dados estejam desatualizados e depois compare a sua taxa de requisições nessa fonte com o ponto de equilíbrio nesse TTL.

O custo de uma leitura de réplica depende do armazenamento e dos seus índices, que são construídos pouco depois de cada pouso. Meça depois que os índices estiverem prontos e execute de novo após uma troca de armazenamento. [Replicas](replicas.md) trata da escolha do armazenamento e das estratégias de atualidade.
