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

## Lendo uma tabela {: #reading-a-table }

A tabela de uma fonte de código é pousada, nunca lida no lugar. O Provisa chama a função com os argumentos da leitura, confere cada linha com as colunas e guarda as linhas sob esses argumentos pelo TTL de cache da tabela. Visibilidade, máscaras e regras de linha então se aplicam às linhas pousadas, como em qualquer tabela.

Você passa argumentos como passa qualquer parâmetro de tabela: como condições `_nf_` em SQL e como argumentos de campo em GraphQL.

## Chaves em lote: o parâmetro `loader` {: #batched-keys }

Um join pode chegar a uma tabela de código: as linhas de outra tabela fornecem os seus parâmetros, como clientes unidos à sua pontuação de churn. Por padrão, o Provisa chama a função uma vez para cada conjunto distinto de argumentos que as linhas unidas fornecem, e envia todas essas chamadas ao worker numa única requisição.

Uma função que recebe um parâmetro chamado `loader` é chamada, em vez disso, uma vez por lote, o padrão que o DataLoader do GraphQL tornou conhecido. `loader` é uma lista de objetos JSON, uma chave cada, com os valores das linhas unidas para os demais parâmetros da função. O Provisa elimina chaves duplicadas, então cada combinação distinta de valores aparece uma vez. A lista é esparsa: um campo sem valor fica de fora da sua chave em vez de ser enviado como null, de modo que vale o valor padrão da própria função, como numa chamada única.

A função devolve linhas comuns que trazem as suas colunas de chave, e o Provisa as une de volta às linhas unidas por essas colunas, nunca pela posição. As linhas podem voltar em qualquer ordem, e uma chave pode dar nenhuma linha ou muitas.

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
    plan: str
    region: str
    score: float


def churn_score(
    customer_id: str, plan: str = "basic", region: str = "US", loader: list[CustomerKey] | None = None
) -> list[Score]:
    """Churn score of each customer in the batch.

    @table
    """
    keys = loader or [{"customer_id": customer_id, "plan": plan, "region": region}]
    rows = [{"plan": "basic", "region": "US", **key} for key in keys]
    scores = MODEL.predict(features(rows))  # one vectorized call for the whole batch
    return [Score(r["customer_id"], r["plan"], r["region"], s) for r, s in zip(rows, scores)]
```

Unida a uma tabela de clientes, ela é chamada com `loader=[{"customer_id": "637", "plan": "pro", "region": "EU"}, {"customer_id": "638"}, ...]`. A segunda chave traz só `customer_id`, então o plano e a região recebem os valores padrão. Cada linha devolvida traz `customer_id`, `plan` e `region`, e o Provisa a associa ao seu cliente por eles.

A mesma função em TypeScript:

```typescript
export interface CustomerKey {
  customer_id: string;
  plan?: string;
  region?: string;
}

export interface Score {
  customer_id: string;
  plan: string;
  region: string;
  score: number;
}

/** Churn score of each customer in the batch.
 * @table
 */
export async function churnScore(
  customer_id: string,
  plan = "basic",
  region = "US",
  loader?: CustomerKey[],
): Promise<Score[]> {
  const keys = loader ?? [{ customer_id, plan, region }];
  const rows = keys.map((key) => ({ plan: "basic", region: "US", ...key }));
  const scores = await model.predict(rows); // one call for the whole batch
  return rows.map((row, i) => ({ ...row, score: scores[i] }) as Score);
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
