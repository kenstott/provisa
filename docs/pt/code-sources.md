# Fontes Node e Python

Às vezes os dados de que você precisa estão atrás de um código que sua equipe já escreveu: um modelo de pontuação, um cliente para um serviço interno, um script que arruma uma exportação. Uma fonte Node ou Python permite servir esse código como tabelas. Você marca as funções que quer compartilhar, aponta o Provisa para o código, e cada função marcada vira uma tabela que todo papel lê sob as mesmas regras de qualquer outra tabela.

## No que uma função se transforma {: #what-a-function-becomes }

O Provisa lê as declarações do próprio código: tags JSDoc e tipos TypeScript no Node, docstrings e type hints no Python. Três tags contam.

| Tag | Vira |
|---|---|
| `@table` | Uma tabela. A função devolve uma lista ou um iterável, e cada elemento é uma linha |
| `@command` | Um command, executado sob demanda |
| `@stream` | Uma tabela ao vivo. A função é um gerador, e cada linha é enviada assim que é produzida |

Uma função sem tag é ignorada. Uma tag que o Provisa não conhece é recusada, assim como um tipo que ele não consegue mapear. A recusa nomeia a função e lista todos os problemas de uma vez, para você corrigir tudo numa passada só.

O tipo da linha dá as colunas. Texto, números, booleanos, datas e timestamps viram seus tipos de coluna, listas continuam listas, e objetos aninhados viram tipos nomeados. Quando o tipo de uma linha é frouxo (`any`, `unknown` ou um `Record` em TypeScript, `Any` ou um `dict` em Python), o Provisa chama a função uma vez, sem argumentos, e sugere colunas a partir das linhas que voltam. Se a função tem um parâmetro obrigatório, ela não pode ser chamada assim, e você tipa as colunas ao registrar a tabela.

Os parâmetros da função viram os parâmetros da tabela. Um parâmetro sem valor padrão é obrigatório, então uma leitura que o omite é recusada antes de o seu código rodar. Um parâmetro com valor padrão é opcional; se ficar de fora, vale o seu padrão. Um steward ainda pode tornar obrigatório um parâmetro opcional.

## De onde vem o código {: #where-the-code-comes-from }

O código é fixado, então uma fonte sempre executa exatamente o que foi registrado:

- **Um pacote**: um pacote npm para Node ou um pacote PyPI para Python, numa versão exata.
- **Um repositório Git**: um commit completo de 40 caracteres, com um subdiretório se o código não estiver na raiz.
- **Um upload**: um `.zip`, `.tgz` ou `.tar.gz` do código.

O código Node roda no Node.js, no Bun ou no Deno, o que você escolher. O código Python roda no Python que vem com o Provisa. Quando o código tem mais de um módulo Python de nível superior, informe em **Módulo de entrada** por qual se entra.

## Instalando dependências {: #installing-dependencies }

Código com dependências precisa de um lockfile, e toda instalação o segue à risca. Sem ele, um pacote que declara dependências é recusado, e a recusa diz para você adicionar um.

| Runtime | Lockfile |
|---|---|
| Node.js | `package-lock.json` ou `npm-shrinkwrap.json` |
| Bun | `bun.lock` ou `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (com seu `pyproject.toml`), `pylock.toml` ou um `requirements.txt` com `--hash` em cada linha |

Um lockfile é opcional, mas sem ele duas instalações do mesmo código podem diferir, e um teste que passa hoje pode falhar amanhã por causa de uma nova versão de uma dependência. Para uma instalação idêntica todas as vezes, o código precisa trazer um lockfile, ou você cria um e o informa aqui. Uma imagem construída já está fixada. Depois que você constrói a imagem da fonte ([Construir imagem](#running-on-a-remote-host)), as suas dependências ficam dentro dela, e se essa imagem é a que passa pelos seus testes e pelo seu CI/CD, a imagem testada é a que roda. Um lockfile importa para o modo dev e para cada nova construção da imagem. Código que traz o seu próprio lockfile é instalado a partir dele: um repositório Git ou um envio que o inclua, ou um pacote npm cujo autor publicou `npm-shrinkwrap.json` (o npm nunca publica `package-lock.json`). Código sem lockfile é instalado com dependências não fixadas: um repositório Git ou um envio sem lockfile, a maioria dos pacotes npm e os pacotes Python do PyPI, cujos wheels não têm lugar para um. Informe um lockfile ao lado desse código para que toda instalação seja idêntica. Há código que não precisa de nenhum, porque a forma como é distribuído já fixa cada dependência.

Nenhum pacote pode rodar os próprios scripts de instalação: o npm instala com `--ignore-scripts`, e o Python instala só wheels, nunca construindo a partir de uma distribuição de código-fonte.

Os pacotes vêm dos registros da sua organização, quando ela os tem. Um administrador cadastra uma vez seus registros npm, índices PyPI e hosts Git em **Admin → Registros de pacotes**, e toda fonte Node e Python da organização instala por eles. O registro npm público e o PyPI público não precisam de entrada. Veja [Registros de pacotes](package-registries.md).

Uma instalação fica em cache pelo hash de conteúdo do código, então o mesmo código é instalado uma única vez.

## O sandbox {: #the-sandbox }

Seu código roda num processo worker próprio, dentro de um sandbox. Ele pode ler o próprio código instalado e nada mais no disco. Só alcança os hosts da lista de saída permitida do deployment, por um proxy que o Provisa executa, e nunca as portas do próprio Provisa. Um host que não consegue oferecer o sandbox se recusa a iniciar o worker e diz o que falta, em vez de rodar o código sem restrição. No Windows, fontes de código são recusadas.

## Segredos {: #secrets }

O código que chama uma API paga precisa da chave dela, e a chave pertence ao cofre da sua organização, não ao código. Em **Secrets**, no formulário da fonte, escolha os segredos da organização que o código pode ler. Num arquivo de configuração, liste-os no mapping da fonte:

```yaml
mapping:
  secrets: [openrouter]
```

Um nome sozinho é o segredo da organização com esse nome. Para ler em vez disso uma variável de ambiente do deployment, informe a referência: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`. A lista é uma referência ao cofre, nunca uma cópia de um valor, então uma configuração que nomeia um segredo que ninguém guardou ainda carrega mesmo assim.

O seu código recebe uma variável de ambiente, `PROVISA_SERVICES`, e lê um segredo de `$PROVISA_SERVICES/secrets/<name>` quando precisa dele. Em Python:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

Em TypeScript:

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

A requisição passa pelo mesmo proxy de saída que o resto do tráfego do seu código, e o próprio proxy a responde. Nada mais do Provisa é alcançável por esse caminho. O endereço leva um token que só o worker desta fonte tem; ele lê apenas os nomes que a fonte lista, e para de funcionar quando o worker para. Cada leitura resolve o segredo no cofre naquele momento, então um segredo rotacionado vale a partir da próxima leitura.

Um nome que a fonte não lista é recusado, assim como um segredo que não pode ser resolvido. A recusa nomeia o segredo, nunca o seu valor. Cada leitura é registrada na trilha de auditoria da organização com a fonte, o nome e a hora, e nenhum valor é jamais escrito num log. O host da API ainda precisa estar na lista de saída permitida.

## Chamar um modelo {: #calling-a-model }

O seu código pode chamar um modelo de linguagem pelo Provisa em vez de guardar uma chave de fornecedor. `$PROVISA_SERVICES/llm/v1` é um endpoint compatível com OpenAI, então qualquer SDK da OpenAI funciona quando você aponta a URL base dele para lá. O endereço é a credencial, então a chave que o SDK pede pode ser qualquer texto. Em Python:

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

Em TypeScript:

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

Nomeie um modelo pelo seu papel, como `provisa:<role>`: você recebe o modelo que a sua organização atribui a esse papel em **Admin → AI Models**, e ele segue essa escolha quando um administrador a muda. Você também pode nomear um modelo diretamente quando ele está atribuído a um papel. Para embeddings, `$PROVISA_SERVICES/llm/v1/embeddings` aceita qualquer um dos modelos vetoriais da sua organização. Qualquer outro modelo é recusado pelo nome. A chamada sai pelos próprios fornecedores, endpoints e chaves da sua organização, e o que ela envia passa antes pelas regras de redação do Provisa. Streaming não é oferecido: envie `stream: false`, que é o padrão.

Cada chamada é registrada para a fonte, com o modelo, os seus tokens e o seu custo pela entrada do modelo em **Polly model prices**, e escrita na trilha de auditoria da organização. Para limitar o que uma fonte gasta, defina **LLM monthly cap** no formulário dela, em USD. Num arquivo de configuração, ele vai no mapping:

```yaml
mapping:
  llm: {cap_usd: 20}
```

Uma fonte com teto só pode chamar modelos que tenham preço ali. Quando o gasto dela no mês (UTC) atinge o teto, as chamadas são recusadas até o mês virar. Chamadas já em andamento quando o teto é atingido terminam mesmo assim, então o mês pode fechar um pouco acima do teto.

## Os serviços do Provisa {: #provisas-services }

O seu código pode importar um módulo que o Provisa sempre fornece onde quer que rode: `provisa_services` em Python e `@provisa/services` em Node.js, Bun e Deno. Não há nada para instalar, e ele não está em nenhum registro de pacotes. Fora do Provisa o módulo não existe, então os testes das suas funções o simulam.

```python
import provisa_services as ps

region = ps.settings.get("region")
rates = ps.cache.get("rates") or fetch_rates()
ps.cache.set("rates", rates, ttl=3600)
```

```typescript
import { settings, cache } from "@provisa/services";

const region = await settings.get("region");
const rates = (await cache.get("rates")) ?? (await fetchRates());
await cache.set("rates", rates, 3600);
```

| Chamada | O que faz |
|---|---|
| `secrets.get(name)` | Um dos segredos declarados da fonte, lido agora do cofre. Veja [Segredos](#secrets). |
| `settings.get(name, default)` | Uma das configurações da fonte, de **Settings** no formulário dela, que um steward pode mudar sem mudar o código. Ausente e sem padrão, ela gera um erro. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | O cache próprio da fonte, compartilhado por todos os workers dela. Um valor é qualquer coisa que JSON possa conter, até 1 MB, mantido por `ttl` segundos ou até ser substituído. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | A coleção vetorial própria da fonte, para os seus próprios dados de referência. Um item tem um `id` e um `text` ou um `vector`; um texto é vetorizado por um dos modelos vetoriais da sua organização. Ela não pode buscar nas suas tabelas governadas: os dados delas chegam ao código apenas como parâmetros. |
| `http.get(url)`, `http.post(url, ...)` | Uma requisição que o Provisa faz pelo código, pelo proxy de saída e pelas autoridades certificadoras do deployment. O host precisa estar na lista de saída permitida, e cada requisição é registrada na trilha de auditoria. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` em JavaScript) | O serviço de LLM do Provisa. Veja [Chamar um modelo](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | Uma linha de log e um ponto de métrica chamado `code_source.<name>`, ligados à requisição que o seu código está atendendo. |

Uma chamada que o serviço recusa gera um `ServiceError` que nomeia o serviço e diz o motivo.

Os serviços respondem ao código que roda no servidor do Provisa. Num host remoto eles ainda não são alcançáveis, e cada chamada gera um erro.

Cada chamada ao seu código é rastreada como parte da requisição que a disparou, com a função, o seu tipo, quantos argumentos recebeu (nunca os valores), as linhas e lotes que devolveu, a sua duração e a CPU do worker. Cada chamada de serviço é rastreada da mesma forma. No nível de detalhe normal, esses dados ficam no span da requisição; no nível de depuração, cada chamada é um span próprio. Se o seu código usa OpenTelemetry, os spans dele entram no mesmo trace.

## Rodar num host remoto {: #running-on-a-remote-host }

Uma fonte roda por padrão no servidor do Provisa; esse é o modo dev. Quando o código dela precisa de uma máquina que o servidor não é, como uma máquina com GPU ou um host dentro de outra rede, construa a imagem dela e rode-a lá. A fonte passa então ao modo built, e as suas tabelas, permissões e leitores continuam os mesmos.

**Construir a imagem.** Salve primeiro a fonte no modo dev, para que o código seja lido e fixado. Em **Remote host**, no formulário dela, escolha a arquitetura (Linux x86-64 ou ARM64) e para onde vai a imagem: um dos registros de contêineres da sua organização, cadastrados em **Admin → Container registries**, ou um download. O Provisa monta a imagem ele mesmo. Ela contém o código no seu hash fixado, as suas dependências instaladas para essa plataforma a partir do lockfile do código pelos seus registros de pacotes, o runtime fixado e o programa do host remoto, sobre uma imagem base mínima. Nada na imagem roda enquanto ela é construída, e nenhum daemon do Docker participa. A tag da imagem traz o hash do código e a versão do Provisa, e as labels nomeiam o runtime, o hash do código e as exportações.

**Rodá-la.** O formulário mostra um serviço de compose para a imagem. Uma imagem baixada é carregada antes com `docker load -i <file>`. O contêiner escuta na porta 7443 por dentro e guarda a sua identidade no volume. Você escolhe a porta do host com `PROVISA_HOST_PORT`; sem ela, o serviço não sobe:

```yaml
services:
  provisa-code-rates:
    image: ghcr.io/example/provisa-code-rates:3f9a1c2e7b40-0.1.0
    restart: unless-stopped
    ports:
      - "${PROVISA_HOST_PORT:?set PROVISA_HOST_PORT to a free port on this host}:7443"
    volumes:
      - provisa-code-rates-state:/var/lib/provisa
volumes:
  provisa-code-rates-state: {}
```

No primeiro início, o contêiner escreve no log uma linha que começa com `pairing code:`.

**Conectá-la.** De volta ao formulário, informe o host em que o Provisa alcança o contêiner, a porta que você publicou e o código de pareamento. Numa rede Docker compartilhada ou no Kubernetes, informe o nome do contêiner ou do Service e 7443, sem publicar nada. O Provisa se pareia com o contêiner por TLS mútuo. O código é usado uma vez, e daí em diante cada lado aceita só o outro. O Provisa verifica então que o contêiner roda o código da fonte e oferece as exportações dela, e passa a fonte para o modo built. Cada processo do Provisa mantém a sua própria conexão com o host e reconecta se o host parar de responder. Enquanto o host não responde, as leituras das tabelas da fonte falham e nomeiam o host.

**Se o código mudar.** Um contêiner que volta rodando outro código é recusado, nomeando o hash ao qual a fonte está fixada e o que o host informa, até que um steward revise o código e o fixe de novo pelo formulário. Para mudar o próprio código, volte a fonte para dev, salve o código novo e construa de novo.

No contêiner, a política de rede dele é o limite de saída do código; a lista de saída permitida do servidor vale para o modo dev.

## Lendo uma tabela {: #reading-a-table }

A tabela de uma fonte de código é pousada, nunca lida no lugar. O Provisa chama a função com os argumentos da leitura, confere cada linha com as colunas e guarda as linhas sob esses argumentos pelo TTL de cache da tabela. Visibilidade, máscaras e regras de linha então se aplicam às linhas pousadas, como em qualquer tabela.

Você passa argumentos como passa qualquer parâmetro de tabela: como condições `_nf_` em SQL e como argumentos de campo em GraphQL.

## Chaves em lote: o parâmetro `loader` {: #batched-keys }

Um join pode chegar a uma tabela de código: as linhas de outra tabela fornecem os seus parâmetros, como clientes unidos à sua pontuação de churn. Por padrão, o Provisa chama a função uma vez para cada conjunto distinto de argumentos que as linhas unidas fornecem, e envia todas essas chamadas ao worker numa única requisição.

Uma função que recebe um parâmetro chamado `loader` é chamada, em vez disso, uma vez por lote, o padrão que o DataLoader do GraphQL tornou conhecido. `loader` é um hash: um dict (um objeto em TypeScript) que vai de um hash estável de cada chave à própria chave, um objeto JSON com os valores das linhas unidas para os demais parâmetros da função. O Provisa elimina chaves duplicadas, então cada combinação distinta de valores aparece uma vez. Uma chave é esparsa: um campo sem valor fica de fora em vez de ser enviado como null, de modo que vale o valor padrão da própria função, como numa chamada única.

A função devolve a mesma forma: um hash que vai de cada chave recebida às linhas dessa chave, linhas comuns da tabela. O Provisa entrega as linhas de cada chave a todas as linhas unidas que compartilhavam a chave, e uma chave pode corresponder a nenhuma linha ou a muitas. Associar pelo hash, e não pelos valores das linhas, mantém associada uma chave esparsa: a função preenche os campos ausentes com valores padrão, então os valores que ela vê diferem dos da linha unida. O Provisa calcula o hash; a função apenas devolve as chaves que recebeu. A função continua declarando só o seu tipo de linha, uma lista de linhas, e as linhas de cada chave são desse tipo. Os seus parâmetros ao lado de `loader` precisam de valores padrão, porque uma chamada com loader passa só `loader`.

Uma função pode limitar o tamanho do seu lote, como faz o `maxBatchSize` do DataLoader, declarando `loader N` em qualquer um de três lugares: uma linha `@loader 100` ao lado de `@table` na docstring ou no JSDoc, um comentário `/* @loader 100 */` logo antes do parâmetro `loader` em TypeScript, ou uma string `Annotated` no parâmetro em Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. O Provisa então divide as chaves em lotes desse tamanho, chama a função uma vez por lote e junta as respostas pelo hash. Sem tamanho, um único lote contém todas as chaves. A descoberta recusa pelo nome um tamanho numa função sem parâmetro `loader`, um tamanho que não é um inteiro positivo e dois tamanhos numa mesma função.

Use `loader` quando uma chamada sobre muitas chaves funciona melhor do que muitas chamadas sobre uma:

- Pontuar um modelo sobre um lote inteiro numa única passada vetorizada.
- Agrupar requisições a uma API ou a um modelo de linguagem que aceita muitas entradas de uma vez.
- Preparar uma vez por lote, como carregar um modelo ou abrir uma conexão, em vez de uma vez por chave.

Esta função Python pontua cada cliente de um lote numa única passada:

```python
from dataclasses import dataclass
from typing import TypedDict


class CustomerKey(TypedDict, total=False):
    customer_id: str
    plan: str
    region: str


@dataclass
class Score:
    customer_id: str
    score: float


def churn_score(
    customer_id: str | None = None,
    plan: str = "basic",
    region: str = "US",
    loader: dict[str, CustomerKey] | None = None,
) -> list[Score]:
    """Churn score of each customer in the batch.

    @table
    """
    keys = loader or {"": {"customer_id": customer_id, "plan": plan, "region": region}}
    rows = {h: {"plan": "basic", "region": "US", **key} for h, key in keys.items()}
    scores = MODEL.predict(features(list(rows.values())))  # one vectorized call for the batch
    out = {h: [Score(r["customer_id"], s)] for (h, r), s in zip(rows.items(), scores)}
    return out if loader else out[""]  # a loader call returns the hash of key to rows
```

Unida a uma tabela de clientes, ela é chamada com um `loader` como `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. A segunda chave traz só `customer_id`, então o plano e a região recebem os valores padrão. A função devolve `{"9f2c...": [...], "41ab...": [...], ...}`, e o Provisa entrega cada lista de linhas a cada linha de cliente que compartilhava essa chave.

A mesma função em TypeScript:

```typescript
export interface CustomerKey {
  customer_id: string;
  plan?: string;
  region?: string;
}

export interface Score {
  customer_id: string;
  score: number;
}

/** Churn score of each customer in the batch.
 * @table
 */
export async function churnScore(
  customer_id?: string,
  plan = "basic",
  region = "US",
  loader?: Record<string, CustomerKey>,
): Promise<Score[]> {
  const keys = loader ?? { "": { customer_id, plan, region } };
  const rows = Object.entries(keys).map(([h, key]) => [h, { plan: "basic", region: "US", ...key }] as const);
  const scores = await model.predict(rows.map(([, row]) => row)); // one call for the whole batch
  const out = Object.fromEntries(
    rows.map(([h, row], i) => [h, [{ customer_id: row.customer_id, score: scores[i] }]]),
  );
  return loader ? out : out[""]; // a loader call returns the hash of key to rows
}
```

## Um exemplo {: #an-example }

Este módulo Python estima a chance de uma conta sair:

```python
from dataclasses import dataclass
from datetime import datetime


@dataclass
class RiskRow:
    account_id: str
    score: float
    computed_at: datetime
    reasons: list[str]


def churn_risk(account_id: str, as_of: datetime | None = None) -> list[RiskRow]:
    """Churn risk of one account.

    @table
    """
    ...
```

Registre-o como fonte `python` a partir do pacote PyPI, do commit Git ou de um upload. A descoberta então oferece `churn_risk` como tabela com quatro colunas (`account_id`, `score`, `computed_at`, `reasons`), um parâmetro obrigatório `account_id` e um opcional, `as_of`. Depois que um steward registra a tabela, leia-a em SQL:

```sql
SELECT score, reasons
FROM churn_risk
WHERE _nf_account_id = 'A-1042'
```

A mesma função em TypeScript fica assim:

```typescript
export interface RiskRow {
  account_id: string;
  score: number;
  computed_at: Date;
  reasons: string[];
}

/** Churn risk of one account.
 * @table
 */
export async function churnRisk(accountId: string, asOf?: Date): Promise<RiskRow[]> {
  // ...
}
```
