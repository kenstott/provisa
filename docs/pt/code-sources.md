# Resolvers de código

Lógica de negócio em Python ou Node.

Se você já escreveu um servidor GraphQL, conhece este padrão. Um resolver de código é uma função que responde por um tipo de objeto: no GraphQL, uma função marcada com `@table` *é* o resolver do tipo da sua tabela. O Provisa a chama com os argumentos da consulta e governa o que ela devolve.

O que muda em relação a um resolver escrito à mão:

- **Governado.** Visibilidade, máscaras e regras de linha se aplicam às linhas que sua função devolve, como em qualquer tabela.
- **Em lotes.** Uma função que recebe um parâmetro `loader` obtém todas as chaves de um join em uma única chamada, o padrão DataLoader.
- **Delegado.** Um filtro em uma coluna chega à sua função como parâmetro quando você pede.
- **Servido em todo lugar.** A mesma função responde a GraphQL, SQL (sobre HTTP, pgwire e Arrow Flight), Cypher (sobre HTTP e Bolt), gRPC, REST, JSON:API e MCP.


## Um primeiro resolver {: #an-example }

```python
from dataclasses import dataclass


@dataclass
class Region:
    code: str
    name: str


def regions() -> list[Region]:
    """Sales regions.

    @table
    """
    return [
        Region(code="emea", name="Europe, Middle East and Africa"),
        Region(code="amer", name="Americas"),
        Region(code="apac", name="Asia Pacific"),
    ]
```

Para servi-lo:

1. Adicione uma fonte `python` e aponte-a para o código: um pacote do PyPI, um commit do Git, um upload ou uma pasta (veja [De onde vem o código](#where-the-code-comes-from)).
2. Salve-a. O Provisa instala o código, lê e oferece `regions` como uma tabela com as colunas `code` e `name`.
3. Um steward registra a tabela em um domínio, aqui `sales`, e a concede como qualquer outra.

Depois leia-a no GraphQL:

```graphql
{
  regions {
    code
    name
  }
}
```

Ou no SQL:

```sql
SELECT code, name
FROM sales.regions
```

Node funciona do mesmo jeito, com tags JSDoc e tipos TypeScript: veja [Node](#node).

## Construir {: #build }

### No que uma função se torna {: #what-a-function-becomes }

O Provisa lê as próprias declarações do seu código: docstrings e anotações de tipo em Python, tags JSDoc e tipos TypeScript em Node.

| Tag | Torna-se |
|---|---|
| `@table` | Uma tabela. A função devolve uma lista ou um iterável, e cada elemento é uma linha. |
| `@command` | Um comando, executado sob demanda. |
| `@stream` | Uma tabela ao vivo. A função é um gerador, e cada linha é enviada assim que é produzida. |

- Uma função sem tag é ignorada.
- Uma tag que o Provisa não conhece é recusada, assim como um tipo que ele não consegue mapear. A recusa lista todos os problemas de uma vez, para você corrigi-los em uma só passada.

O tipo de linha dá as colunas:

- Texto, números, booleanos, datas e timestamps correspondem aos seus tipos de coluna. Listas continuam listas, e objetos aninhados viram tipos nomeados.
- Em Python um tipo de linha é uma dataclass, um `TypedDict`, um `NamedTuple`, um modelo Pydantic ou qualquer classe com campos anotados. As linhas de um modelo Pydantic são enviadas como `model_dump(by_alias=True)` as entrega, então um campo com alias vira uma coluna com o nome do alias.
- Em TypeScript um tipo de linha é uma interface, um alias de tipo ou uma classe.
- A documentação própria de um campo vira a descrição da sua coluna quando você registra a tabela: um `Field(description=...)` do Pydantic, ou um comentário de documentação em uma propriedade TypeScript.
- Um tipo de linha frouxo (`Any` ou um `dict` em Python, `any`, `unknown` ou um `Record` em TypeScript) não tem colunas para ler. O Provisa chama a função uma vez sem argumentos e sugere colunas a partir das linhas devolvidas. Se a função tem um parâmetro obrigatório, ela não pode ser chamada assim, e você mesmo informa os tipos das colunas ao registrar a tabela.

### Parâmetros {: #parameters }

Os parâmetros de uma função viram os parâmetros da sua tabela. Esta pontua uma conta:

```python
@dataclass
class Risk:
    score: float
    reasons: list[str]


def churn_risk(account_id: str) -> list[Risk]:
    """Churn risk of one account.

    @table
    """
    return [Risk(score=0.82, reasons=["no logins in 30 days"])]
```

No GraphQL o parâmetro é um argumento de campo:

```graphql
{
  churnRisk(account_id: "A-1042") {
    score
    reasons
  }
}
```

No SQL é uma condição `_nf_`:

```sql
SELECT score, reasons
FROM accounts.churn_risk
WHERE _nf_account_id = 'A-1042'
```

- Um parâmetro sem valor padrão é obrigatório. Uma leitura que o omite é recusada antes de o seu código rodar.
- Um parâmetro com valor padrão é opcional. Omita-o e seu valor padrão se aplica.
- Um steward ainda pode tornar obrigatório um opcional.

### O que chega à sua função {: #what-reaches-your-function }

Uma chamada recebe os argumentos da leitura e nada mais da requisição. O valor de cada parâmetro vem de um argumento de campo, de uma condição `_nf_`, de um filtro delegado a ele ou das chaves de um join. Sua função nunca vê quem pergunta: a identidade e os papéis de quem chama ficam com o Provisa, que governa as linhas que sua função retorna.

A delegação de filtros se apoia nisso. Um filtro na consulta vira o valor de um parâmetro, então sua função pode buscar só as linhas pedidas.

### Delegação de filtros {: #pushdown }

Um filtro em uma das colunas devolvidas pode chegar à sua função como argumento, para que ela busque só as linhas pedidas. Declare qual parâmetro o recebe de uma de três formas:

- **Por nome.** Um parâmetro com o nome exato de uma coluna devolvida recebe uma igualdade nessa coluna. Um chamado `<op>_<column>` recebe esse operador, como `gte_score` ou `in_region`.
- **Por tag.** Uma linha `@<op>_<column> <parameter>` (ou `@<column> <parameter>`) na docstring ou no JSDoc, ou um `/* @<op>_<column> */` em linha antes de um parâmetro TypeScript.
- **Por `Annotated`.** Em Python, a string `"<op>_<column>"` (ou `"<column>"`) na anotação de tipo do parâmetro.

Os operadores:

| `op` | Recebe |
|---|---|
| `ne`, `gt`, `gte`, `lt`, `lte` | Um valor do tipo da coluna |
| `in` | Uma lista do tipo da coluna |
| `like` | Texto, em uma coluna de texto |

- Um intervalo em uma coluna preenche os dois parâmetros dela: `score BETWEEN 0.5 AND 0.9` dá `gte_score` e `lte_score`.
- Os nomes de coluna são comparados exatamente como você os declarou no tipo de linha. Um nome que só corresponde quando o Provisa renomeia a coluna para um leitor não é uma correspondência.
- Uma declaração que os tipos da sua função não podem aceitar é recusada.
- O filtro ainda é aplicado às linhas que sua função devolve, então uma função que ignora ou afrouxa um valor recebido não consegue devolver linhas erradas.

### Chaves em lotes: o parâmetro `loader` {: #batched-keys }

Um join pode alcançar uma tabela de código, com as linhas de outra tabela fornecendo seus parâmetros: por exemplo, clientes unidos à sua pontuação de churn. Por padrão o Provisa chama a função uma vez para cada conjunto distinto de argumentos que as linhas do join dão, e envia todas essas chamadas ao worker em uma só requisição.

Uma função que recebe um parâmetro chamado `loader` é chamada uma vez por lote, o padrão que o DataLoader do GraphQL popularizou.

- **O que ela recebe.** `loader` é um hash: um dict (um objeto em TypeScript) de um hash estável de cada chave para a própria chave. Uma chave é um objeto JSON com os valores das linhas do join para os outros parâmetros da função.
- **Sem duplicatas.** Cada combinação distinta de valores aparece uma vez.
- **Esparsa.** Um campo sem valor fica de fora da chave, então o valor padrão da própria função se aplica, como em uma chamada única.
- **O que ela devolve.** Um hash de cada chave recebida para as linhas dessa chave: linhas comuns da tabela, nenhuma ou muitas. O Provisa entrega as linhas de cada chave a cada linha do join que compartilhava a chave.
- **Associado por hash.** A função preenche com valores padrão os campos ausentes de uma chave esparsa, então os valores que ela vê diferem dos da linha do join. O Provisa calcula o hash, e a função apenas devolve as chaves que recebeu.
- **Tipada como sempre.** A função continua declarando só seu tipo de linha, uma lista de linhas.
- **Valores padrão necessários.** Seus parâmetros ao lado de `loader` precisam de valores padrão, porque uma chamada com loader passa só `loader`.
- **Tamanho do lote.** Limite-o, como o `maxBatchSize` do DataLoader, com `loader N` em um de três lugares: uma linha `@loader 100` ao lado de `@table`, um comentário `/* @loader 100 */` antes do parâmetro `loader` em TypeScript, ou uma string `Annotated` em Python, `loader: Annotated[dict[str, CustomerKey] | None, "loader 100"] = None`. O Provisa então divide as chaves em lotes desse tamanho e junta as respostas pelo hash. Sem tamanho, um lote contém todas as chaves.
- **Recusado.** Um tamanho em uma função sem parâmetro `loader`, um tamanho que não é um inteiro positivo e dois tamanhos em uma função.

Use `loader` quando uma chamada sobre muitas chaves rende mais que muitas chamadas sobre uma:

- Pontuar um modelo sobre um lote inteiro em uma passada vetorizada.
- Agrupar requisições a uma API ou a um modelo de linguagem que aceita muitas entradas de uma vez.
- Preparar algo uma vez por lote, como carregar um modelo ou abrir uma conexão.

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

Unida a uma tabela de clientes, ela é chamada com um `loader` como `{"9f2c...": {"customer_id": "637", "plan": "pro", "region": "EU"}, "41ab...": {"customer_id": "638"}, ...}`. A segunda chave traz só `customer_id`, então seu plano e sua região recebem os valores padrão. A função devolve `{"9f2c...": [...], "41ab...": [...], ...}`.

### De onde vem o código {: #where-the-code-comes-from }

- **Um pacote.** Um pacote npm para Node ou um pacote do PyPI para Python, em uma versão exata.
- **Um repositório Git.** Um commit completo de 40 caracteres, com um subdiretório se o código não estiver na raiz.
- **Um upload.** Um `.zip`, `.tgz` ou `.tar.gz` do código.
- **Uma pasta na sua máquina.** **Escolher pasta** compacta a pasta no seu navegador, deixando de fora `node_modules`, `.venv`, `__pycache__` e `.git`, e a envia como arquivo da fonte. Em um navegador que consegue guardar a pasta (Chrome, Edge), **Salvar** a compacta e envia de novo; outro navegador pede a pasta novamente.
- **Um diretório no servidor.** Em uma instalação de uma única organização, um diretório sob os que o operador permite. Ele é lido onde está, então uma alteração é lida pela próxima consulta. Um servidor de muitas organizações o recusa.

Toda origem, exceto o diretório do servidor, é fixada, então a fonte executa exatamente o que foi salvo.

- **Módulo de entrada.** Quando o código Python tem mais de um módulo de nível superior, nomeie o de entrada.
- **Abrir no VS Code.** Um diretório do servidor abre no VS Code instalado; uma origem Git abre no seu commit no vscode.dev. Um pacote ou um upload não tem esse link.

### Instalar dependências {: #installing-dependencies }

Código com dependências precisa de um lockfile, e cada instalação o segue exatamente. Sem um, um pacote que declara dependências é recusado, e a recusa pede que você adicione um.

| Runtime | Lockfile |
|---|---|
| Node.js | `package-lock.json` ou `npm-shrinkwrap.json` |
| Bun | `bun.lock` ou `bun.lockb` |
| Deno | `deno.lock` |
| Python | `uv.lock` (com seu `pyproject.toml`), `pylock.toml`, ou um `requirements.txt` com `--hash` em cada linha |

- **Trazido ou fornecido.** Código que traz seu próprio lockfile instala a partir dele: um repositório Git, upload ou pasta com um, ou um pacote npm cujo autor publicou `npm-shrinkwrap.json` (o npm nunca publica `package-lock.json`). A maioria dos pacotes npm, e os pacotes Python do PyPI, não trazem nenhum, então forneça um ao lado deles.
- **Por que importa.** Sem um, duas instalações do mesmo código podem diferir, e um teste que passa hoje pode falhar amanhã por uma nova versão de dependência.
- **Uma imagem construída já é fixa.** Assim que você [constrói a imagem da fonte](#running-on-a-remote-host), as dependências estão dentro dela. Um lockfile importa para o modo dev e para cada novo build.
- **Sem scripts de instalação.** O npm instala com `--ignore-scripts`, e o Python instala só wheels, nunca construindo a partir de uma distribuição de código-fonte.
- **Seus registros.** Os pacotes vêm dos próprios registros npm, índices PyPI e hosts Git da sua organização, cadastrados uma vez em **Admin → Package registries**. O registro npm público e o PyPI não precisam de cadastro. Veja [Package Registries](package-registries.md).
- **Em cache.** Uma instalação fica em cache pelo hash do conteúdo do código, então o mesmo código instala uma vez.

### Node {: #node }

- O código Node roda em Node.js, Bun ou Deno, o que você escolher.
- As tags ficam no JSDoc, e os tipos vêm do TypeScript.

O primeiro resolver em TypeScript:

```typescript
export interface Risk {
  score: number;
  reasons: string[];
}

/** Churn risk of one account.
 * @table
 */
export async function churnRisk(account_id: string): Promise<Risk[]> {
  return [{ score: 0.82, reasons: ["no logins in 30 days"] }];
}
```

O `churnScore` em lotes em TypeScript:

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

## Implantação {: #deployment }

### Modo dev e workers {: #workers }

Por padrão uma fonte roda no servidor Provisa; este é o modo dev.

- Seu código roda em um processo worker próprio, um por fonte e versão do seu código em cada processo Provisa, iniciado na sua primeira chamada.
- Um worker que falha faz falhar as chamadas em andamento, nomeando a fonte e a função. A próxima chamada inicia um novo.
- Uma chamada é cancelada quando o prazo da sua requisição vence: o worker recebe a ordem de pará-la, e a requisição em espera é liberada na hora.

### Ler uma tabela {: #reading-a-table }

Como uma leitura chega à sua função depende do que mais ela lê:

- **Uma leitura de uma única tabela de código** vai direto à sua função. Nada é pousado. O Provisa a chama com os argumentos da leitura e seus filtros delegados, e aplica o resto da instrução às linhas que ela devolve.
- **Uma leitura que une outras tabelas** é do motor de federação. A tabela de uma fonte em modo dev é pousada para ela: as linhas são guardadas sob esses argumentos pelo TTL de cache da tabela.
- **A tabela de uma fonte construída** nesse join é lida ao vivo do seu host remoto, que o motor anexa como um catálogo Postgres. Ela é pousada em vez disso quando sua fonte é replicada ou com piso, quando a tabela é mantida para sempre, quando declara um TTL de pouso acima de 0, ou em um motor que não consegue anexar um host.

Em qualquer caso, visibilidade, máscaras e regras de linha se aplicam às linhas, como em qualquer tabela. Você passa argumentos como qualquer parâmetro de tabela: condições `_nf_` no SQL, argumentos de campo no GraphQL.

### Executar em um host remoto {: #running-on-a-remote-host }

Quando seu código precisa de uma máquina que não é o servidor, como uma máquina com GPU ou um host dentro de outra rede, construa sua imagem e execute-a lá. A fonte passa então ao modo construído, e suas tabelas, permissões e leitores continuam os mesmos.

**Construir a imagem.**

- Salve primeiro a fonte em modo dev, para que seu código seja lido e fixado.
- Em **Remote host** no formulário, escolha a arquitetura (Linux x86-64 ou ARM64) e para onde vai a imagem: um dos registros de contêiner da sua organização (**Admin → Container registries**), ou um download.
- O Provisa monta a imagem sozinho, sobre uma base mínima, sem daemon Docker: o código no seu hash fixado, suas dependências instaladas para aquela plataforma a partir do seu lockfile pelos seus registros de pacotes, o runtime fixado e o programa de host remoto. Nada na imagem roda enquanto ela é construída.
- A tag da imagem traz o hash do código e a versão do Provisa, e seus labels nomeiam o runtime, o hash do código e as exportações.

**Executá-la.** O formulário mostra um serviço compose para a imagem. Carregue primeiro uma imagem baixada com `docker load -i <file>`. O contêiner escuta em 7443 e guarda sua identidade no seu volume. Você escolhe a porta do host com `PROVISA_HOST_PORT`; o serviço não inicia sem ela.

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

Na primeira inicialização o contêiner imprime no log uma linha começando com `pairing code:`.

**Conectá-la.**

- No formulário, informe o host pelo qual o Provisa alcança o contêiner, a porta que você publicou e o código de pareamento. Em uma rede Docker compartilhada ou no Kubernetes, informe o nome do contêiner ou do Service e 7443, sem publicar nada.
- O Provisa se pareia com o contêiner por TLS mútuo. O código é usado uma vez; daí em diante cada lado aceita só o outro.
- O Provisa verifica que o contêiner executa o código da fonte e oferece suas exportações, e então passa a fonte ao modo construído.
- Cada processo Provisa mantém sua própria conexão com o host e liga de novo se o host deixar de responder. Enquanto ele não responde, as leituras das tabelas da fonte falham e nomeiam o host.

**Se o código mudar.**

- Um contêiner que volta executando outro código é recusado até que um steward revise o código e o fixe de novo pelo formulário.
- Para mudar o próprio código, volte a fonte ao modo dev, salve o código novo e construa de novo.

### Os serviços do Provisa {: #provisas-services }

Seu código pode importar um módulo que o Provisa sempre oferece no servidor: `provisa_services` em Python, `@provisa/services` em Node.js, Bun e Deno. Não há nada a instalar, e ele não está em nenhum registro de pacotes. Fora do Provisa ele não existe, então seus testes o simulam.

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
| `settings.get(name, default)`, `settings.all()` | As configurações da fonte, de **Settings** no formulário, que um steward pode mudar sem mudar o código. Uma ausente sem valor padrão gera um erro. |
| `cache.get(key, default)`, `cache.set(key, value, ttl)`, `cache.delete(key)` | O cache próprio da fonte, compartilhado por todos os seus workers. Um valor é qualquer coisa que o JSON comporte, até 1 MB, guardado por `ttl` segundos ou até ser substituído. |
| `vectors.upsert(items, model)`, `vectors.search(query, k, model)`, `vectors.embed(texts, model)`, `vectors.delete(ids)` | A coleção de vetores própria da fonte, para seus próprios dados de referência. Um item tem um `id` e um `text` ou um `vector`; um texto é vetorizado por um dos modelos vetoriais da sua organização. Ela não pode buscar nas suas tabelas governadas, cujos dados só chegam ao código como parâmetros. |
| `http.get(url)`, `http.post(url, ...)` | Uma requisição que o Provisa faz pelo código, pelo proxy de saída e pelas autoridades certificadoras da implantação. O host precisa estar na lista de saída permitida, e cada requisição fica registrada na trilha de auditoria. |
| `llm.chat(model, messages)`, `llm.embeddings(model, input)`, `llm.base_url()` (`llm.baseUrl()` em JavaScript) | O serviço de LLM do Provisa. Veja [Chamar um modelo](#calling-a-model). |
| `log(message, level, fields)`, `metric(name, value, kind, attributes)` | Uma linha de log, e um ponto de métrica chamado `code_source.<name>`, anexados à requisição que seu código atende. |

- Uma chamada que o serviço recusa gera um `ServiceError` que nomeia o serviço e diz por quê.
- Os serviços respondem ao código que roda no servidor Provisa. Em um host remoto toda chamada gera um erro.

#### Segredos {: #secrets }

A chave de uma API paga pertence ao cofre da sua organização, não ao código.

- Em **Secrets** no formulário da fonte, escolha os segredos da organização que o código pode ler. Em um arquivo de configuração, liste-os no mapping da fonte:

    ```yaml
    mapping:
      secrets: [openrouter]
    ```

- Um nome sozinho é o segredo da organização com esse nome. Para ler em vez disso uma variável de ambiente da implantação, dê a referência: `secrets: {openrouter: "${env:OPENROUTER_API_KEY}"}`.
- A lista faz referência ao cofre e nunca copia um valor, então uma configuração que nomeia um segredo que ninguém guardou ainda carrega mesmo assim.

Seu código lê um segredo com `secrets.get(name)`, ou direto de `$PROVISA_SERVICES/secrets/<name>`:

```python
import os, urllib.request
key = urllib.request.urlopen(f"{os.environ['PROVISA_SERVICES']}/secrets/openrouter").read().decode()
```

```typescript
const key = await (await fetch(`${process.env.PROVISA_SERVICES}/secrets/openrouter`)).text();
```

- `PROVISA_SERVICES` é a única variável de ambiente que seu código recebe. A requisição passa pelo mesmo proxy de saída que o resto do tráfego dele, e o proxy a responde sozinho; nada mais do Provisa é alcançável assim.
- O endereço traz um token que só o worker desta fonte tem. Ele lê só os nomes que a fonte lista, e para de funcionar quando o worker para.
- Cada leitura resolve o segredo no cofre naquele momento, então um segredo rotacionado vale na leitura seguinte.
- Um nome que a fonte não lista é recusado, assim como um segredo que não pode ser resolvido. A recusa nomeia o segredo, nunca seu valor.
- Cada leitura fica registrada na trilha de auditoria da organização com a fonte, o nome e o horário. Nenhum valor é jamais gravado em um log.
- O host da API ainda precisa estar na lista de saída permitida.

#### Chamar um modelo {: #calling-a-model }

Seu código pode chamar um modelo de linguagem pelo Provisa em vez de guardar uma chave de fornecedor. `$PROVISA_SERVICES/llm/v1` é um endpoint compatível com OpenAI, então qualquer SDK da OpenAI funciona com a URL base apontada para lá. O endereço é a credencial, então a chave que o SDK pede pode ser qualquer texto.

```python
import os
from openai import OpenAI

client = OpenAI(base_url=f"{os.environ['PROVISA_SERVICES']}/llm/v1", api_key="unused")
reply = client.chat.completions.create(
    model="provisa:sql_generation",
    messages=[{"role": "user", "content": "Summarise this account's last quarter."}],
)
```

```typescript
import OpenAI from "openai";

const client = new OpenAI({ baseURL: `${process.env.PROVISA_SERVICES}/llm/v1`, apiKey: "unused" });
const reply = await client.chat.completions.create({
  model: "provisa:sql_generation",
  messages: [{ role: "user", content: "Summarise this account's last quarter." }],
});
```

- **Por papel.** Nomeie um modelo como `provisa:<role>` e você recebe o modelo que sua organização atribui a esse papel em **Admin → AI Models**; ele acompanha essa escolha quando um administrador a muda. Você também pode nomear um modelo diretamente quando um papel está atribuído a ele.
- **Embeddings.** `$PROVISA_SERVICES/llm/v1/embeddings` aceita qualquer um dos modelos vetoriais da sua organização.
- Qualquer outro modelo é recusado.
- A chamada sai pelos próprios fornecedores, endpoints e chaves da sua organização, e o que ela envia passa antes pelas regras de redação do Provisa.
- Streaming não é oferecido. Envie `stream: false`, que é o padrão.

## Proteções {: #safeguards }

### O sandbox {: #the-sandbox }

- Seu código roda no próprio processo worker, em um sandbox. Ele pode ler o próprio código instalado e nada mais no disco.
- Ele só alcança os hosts da lista de saída permitida da implantação, por um proxy que o Provisa executa, e nunca as portas próprias do Provisa.
- Um host que não consegue oferecer o sandbox não inicia o worker e nomeia o que falta; o código nunca roda sem confinamento. No Windows, as fontes de código são recusadas.
- Em um host remoto, a política de rede do contêiner é o limite de saída do código. A lista de saída permitida do servidor vale para o modo dev.

### Recusas {: #refusals }

Cada um destes casos é recusado, antes de o seu código rodar quando possível:

- Uma tag desconhecida ou um tipo que o Provisa não consegue mapear, todos os problemas de uma função de uma vez.
- Uma leitura que omite um parâmetro obrigatório.
- Uma declaração de delegação ou de `loader` que os tipos da função não podem aceitar.
- Um pacote com dependências e sem lockfile.
- Um diretório do servidor fora dos permitidos, ou em um servidor de muitas organizações.
- Um host remoto que executa um código diferente daquele ao qual a fonte está fixada.
- Um segredo que a fonte não lista, um modelo não permitido, uma chamada de serviço que o serviço não responde.

### Medição e limites {: #metering }

- Cada chamada de modelo fica registrada para a fonte, com o modelo, seus tokens e seu custo pela entrada do modelo em **Polly model prices**, e é gravada na trilha de auditoria da organização.
- Para limitar o que uma fonte gasta, defina **LLM monthly cap** no formulário, em USD. Em um arquivo de configuração ele vai no mapping:

    ```yaml
    mapping:
      llm: {cap_usd: 20}
    ```

- Uma fonte com limite só pode chamar modelos que tenham preço ali.
- Quando o gasto do mês (UTC) atinge o limite, suas chamadas são recusadas até o mês virar. Chamadas já em andamento quando o limite é atingido ainda terminam, então o mês pode acabar um pouco acima do limite.

### Governança sobre os resultados {: #governance }

- As linhas da sua função são verificadas contra as colunas da tabela.
- Visibilidade, máscaras e regras de linha então se aplicam a cada leitor, como em qualquer tabela.
- Seu código vê os dados de uma tabela governada só como os parâmetros que uma leitura lhe dá.

### Rastreamento {: #tracing }

- Cada chamada ao seu código é rastreada como parte da requisição que a disparou: a função, seu tipo, quantos argumentos recebeu (nunca seus valores), as linhas e os lotes que devolveu, sua duração e a CPU do worker.
- Cada chamada de serviço é rastreada da mesma forma.
- No detalhe de rastreamento normal esses dados ficam no span da requisição; no detalhe de depuração cada chamada é um span próprio.
- Se seu código usa OpenTelemetry, seus spans entram no mesmo trace.
