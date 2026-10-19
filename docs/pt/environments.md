# Ambientes {: #environments }

Um ambiente é uma cópia com nome do modelo governado de uma organização. Fisicamente, a cópia é um esquema PostgreSQL à parte: um esquema de verdade, sem coluna discriminadora nem prefixo. Por isso toda consulta de repositório existente está correta dentro de um ambiente sem ser reescrita, e um predicado esquecido não consegue deixar as linhas de um ambiente serem lidas a partir de outro.

Toda organização começa com um ambiente chamado `prod`. Ele não pode ser excluído nem renomeado.
Uma requisição que não nomeia ambiente algum é servida por `prod`; uma requisição que nomeia um ambiente inexistente
é recusada.

Ambientes estão disponíveis para organizações em um plano pago.

Para preencher um ambiente que não é de produção com dados seguros de usar, veja [Criar ambientes de teste](test-data.md).

## Nomes de ambiente {: #environment-names }

Um nome precisa corresponder a `[a-z][a-z0-9_]{1,31}`: de dois a trinta e dois caracteres entre letras minúsculas, dígitos e sublinhados, começando por uma letra. `prod` e nomes que começam com `pg_` são recusados. O comprimento máximo para uma organização depende do id dela. O PostgreSQL trunca em silêncio um identificador com mais de 63 bytes, e o limite protege contra o nome de esquema mais longo que um ambiente deriva.

## O que uma cópia carrega {: #what-a-copy-carries }

Toda tabela do esquema da organização cai em exatamente uma classe. A classificação é uma lista de permissões: uma tabela acrescentada depois não viaja até que alguém nomeie a classe dela aqui, então esquecer uma tabela aparece como um teste vermelho.

| Classe | Tabelas | O que acontece na cópia |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Copiadas por inteiro |
| IDENTITY_ONLY | sources, api_sources, kafka_sources | Campos de identidade e governança viajam; valores de conexão ficam para trás (veja Vínculos) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Copiadas apenas quando um ambiente é criado pela primeira vez; merges posteriores não as tocam |
| PARTIAL | org_settings | Copiadas por chave: configurações de governança viajam, chaves que nomeiam um destino externo ou runtime por ambiente ficam para trás |
| NEVER_SENSITIVE | org_secrets, user_directory | Nunca copiadas |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log, e outras | Nunca copiadas |

`SEEDED_AT_CREATION` existe para resolver um problema específico. Um ambiente novo precisa de funções e
atribuições ou abre sem ninguém capaz de agir. Mas um merge posterior que carregasse a linha `developer`
de `prod` sobrescreveria a versão restrita de que uma branch restrita poderia precisar, tornando o caminho de revisão
a rota de escalada. Então funções e atribuições viajam uma vez, na criação, e depois disso são a resposta
de cada ambiente.

## Vínculos {: #bindings }

Vínculos são as colunas que dizem para onde uma fonte aponta de fato: `host`, `port`, `database`, `username` e as demais. Eles nunca viajam numa cópia. Um ambiente ainda não vinculado é marcado como `unbound` em vez de ficar em branco, porque um host vazio não é o mesmo que nenhum host, e o montador de conexões o leria como `localhost:5432`.

Um ambiente criado de qualquer forma exceto Unbound copia a conexão de cada fonte do seu pai,
exatamente como o pai a escreveu: uma referência `${secret:...}` ou `${env:...}` é copiada como essa
referência, nunca resolvida. Após a criação a cópia é a linha própria do ambiente, editável na página
Fontes, e nada é resolvido por meio do pai quando o runtime do ambiente é construído.
O campo `binding` de cada fonte diz de onde veio sua conexão: `copied`, `own` (informada neste
ambiente) ou `unbound`. Editar uma conexão copiada a torna `own`. **Copiar novamente do pai**
(`POST /admin/orgs/{org}/environments/{name}/sources/recopy`) copia as conexões do pai de novo.
Um merge ou deploy não leva conexões, então uma fonte que ele introduz chega `unbound`.

## A projeção git {: #the-git-projection }

Toda escrita no modelo faz commit do resultado no branch git do ambiente. Fazer deploy de uma árvore exige uma chamada explícita: um pull request mesclado no host git não faz o próprio deploy.

Cada entidade ganha um arquivo. O caminho é o endereço `provisa://` da entidade sem o esquema e a organização: `provisa://acme/sales/tables/Order` vira `sales/tables/Order.yaml`. Fontes vão para `sources/`, commands para `commands/` e métricas para `metrics/`. Linhas filhas que dependem de um pai (colunas, relacionamentos, regras de RLS) são gravadas dentro do arquivo do pai, sem arquivos próprios.

Commands e suas atribuições de tag sobrevivem à ida e volta. Uma tag em um command é roteada para o
arquivo do próprio command (`commands/<name>.yaml`); uma tag que não pertence a arquivo algum desaparece da
projeção e seria excluída na próxima implantação daquela árvore.

Nenhuma chave substituta chega a um arquivo. `registered_tables.id` é um inteiro autoincrementado, então o mesmo modelo recebe inteiros diferentes em dois ambientes, e um dump ingênuo daria diferença consigo mesmo. Toda chave substituta é descartada, e toda referência a uma é gravada como o caminho do destino.

A serialização é determinística. Chaves são emitidas em ordem alfabética, coleções filhas ordenadas por
seu endereço, e o estilo YAML é fixo. Dois ambientes contendo o mesmo modelo produzem
árvores byte-idênticas.

## Merge

Mesclar o modelo de um ambiente em outro atualiza por identidade: todo objeto que a origem tem é criado ou atualizado no destino. Objetos que a origem não tem mais só são removidos quando quem chama pede remoções explicitamente. Uma mesclagem é uma única transação, então se falhar no meio o destino fica como estava.

Antes de aplicar, chame o endpoint de prévia (`GET /{name}/merge-preview`) ou passe `dry_run: true`.
A prévia roda o mesmo caminho de código que o merge usa; é um endpoint `GET` para que um script de CI que
erre a flag não possa acidentalmente aplicar o merge que pretendia apenas inspecionar.

Um merge deixa os vínculos, funções e segredos do destino exatamente como estavam. Um ambiente de dev
não perde suas próprias conexões de banco de dados ao receber um modelo mais novo de prod. Prod não adquire
as concessões de dev.

### O que o relatório nomeia {: #what-the-report-names }

O relatório da mesclagem lista, por caminho, o que foi adicionado, alterado, removido e deixado como estava. Ele também nomeia os **conflitos**: objetos que os dois lados mudaram desde o último commit em comum. Um conflito é informado, e a origem vence, que é o que uma mesclagem num destino significa. O Provisa não oferece resolução de conflitos, marcadores de mesclagem nem escolha por objeto. A lista de conflitos vale como sinal de que duas pessoas estavam editando o mesmo objeto sem saber.

Quando os dois lados mudaram um objeto para o mesmo valor, eles concordam, e não há conflito. Quando os dois ambientes não têm nenhum ancestral em comum, a base no relatório é `None`, e uma lista de conflitos vazia significa que nada foi comparado, e não que nada colidiu.

A mesclagem chega como um único commit comprimido no branch do destino. A mensagem do commit é obrigatória e não pode ficar em branco, porque é o único relato do trabalho que esse commit representa. Os commits da origem ficam onde estão e ainda podem receber deploy por SHA depois.

## Pull

Fazer pull pega o que o remote contém para um ambiente e o torna o modelo. Ele não avança a branch
local diretamente; ele aplica a árvore buscada pelo caminho de implantação comum,
de modo que a mesma validação e auditoria que governam uma implantação manual governam um pull.

Como uma mesclagem, um pull informa o que sobrescreveu: objetos que a árvore que chega mudou e que o ambiente local também tinha mudado desde o último commit em comum das duas linhas. Uma mudança local que nunca foi commitada deixa o ambiente divergente (veja Histórico abaixo), e um pull a nomeia no relatório como uma mudança comum.

Um pull é recusado quando as duas linhas **divergiram**, ou seja, cada uma tem commits que a outra não tem. A recusa lista os objetos que os dois lados mexeram, para que quem precisa decidir qual trabalho fica saiba quais objetos olhar.

## Histórico {: #history }

Todo deploy avança o cursor do ambiente na sua própria linha de commits. Desfazer volta um commit, e refazer avança de novo em direção ao ponto de onde o desfazer partiu. Nenhum dos dois remove um commit: voltar acrescenta uma posição e deixa o histórico como está.

Uma branch é semeada na ponta do ambiente do qual foi criada, então um desfazer para naquele
ponto de semeadura e não caminha para os commits do ambiente pai.

As flags `can_undo` e `can_redo` vêm com a resposta da lista de ambientes. As duas valem `false` quando a projeção não tem o commit que o plano de controle nomeia, um estado que o desenho admite e chama de **divergente**. Um nó cujo armazenamento de repositório nunca recebeu um certo commit continua listando seus ambientes; só as respostas do histórico mudam.

## Autorização {: #authorization }

Ambientes são governados por dois direitos. Nenhum deles é de um analista por padrão.

| Direito | Quem o detém (semeado) | O que ele governa |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Criar e excluir ambientes |
| `environment_switch` | org_admin, developer | Ser servido por qualquer ambiente que não seja prod |

`prod` não precisa de direito nenhum. É ele que atende uma requisição que não nomeia ambiente, e recusá-lo seria recusar toda requisição.

A verificação acontece onde o ambiente é escolhido, antes de chegar a qualquer rota. Um membro sem `environment_switch` é recusado em todas as superfícies de uma vez (HTTP, GraphQL, SQL e os protocolos de rede), porque o ambiente é fixado no middleware e não em handlers individuais.

Um analista sem direito de ambiente algum pode consultar `prod` e não vê o seletor de ambiente.
Um contratado a quem se concede a função de analista não vê interface de ambientes e não pode criar nem entrar em
nenhum ambiente que não seja produção.

### Autoridade do dono do ambiente {: #environment-owner-authority }

Criar um ambiente é o único caminho pelo qual um membro só de leitura ganha direitos de edição do modelo. Dentro de um ambiente que criou, o criador tem as capacidades do papel `developer`, menos os direitos sobre dados (`write`, `full_results`, `usage`): direitos para construir o modelo, sem direitos sobre os dados.

A concessão é derivada de `environments.created_by` no momento da autorização, nunca escrita em uma
tabela de concessões. Excluir o ambiente a remove no mesmo ato.

A participação em domínios ainda limita o que o dono pode alterar. Fazer branch muda o que um membro pode fazer;
nunca muda em quais domínios ele pode fazê-lo.

## Ambientes protegidos {: #protected-environments }

Um ambiente pode ser protegido. Um merge ou uma implantação em um ambiente protegido não é aplicado
quando solicitado; ele é proposto, e alguém diferente de quem solicitou precisa aprová-lo.

`prod` fica protegido automaticamente assim que a organização tem mais de um membro. Uma organização de um só membro não consegue cumprir "alguém que não o solicitante", então a regra não vale ali; ela tornaria impossível mesclar em `prod`. Um org_admin pode marcar qualquer ambiente como protegido.

Um pedido de mesclagem é uma linha gravada, e nenhuma caixa de confirmação o substitui. Quem aprova é, por definição, outra pessoa que não o solicitante e não está presente no momento do pedido. Uma confirmação passageira forçaria a aprovação dentro da sessão do solicitante, que é justamente o que a regra proíbe.

A linha da requisição carrega o relatório de merge ao lado da mensagem de quem solicitou. A obsolescência é derivada
no momento da leitura, nunca armazenada: replanejar na leitura e comparar com o relatório armazenado é
a única versão que não pode estar errada. Uma requisição obsoleta precisa ser refeita. Quem solicita não pode
aprovar a própria requisição.

Estados do ciclo de vida da requisição: `requested` → `approved`/`rejected` → `applied`. `stale` é derivado.

Deploys a partir de uma ref do repositório passam pela mesma porta, e o pedido fixa o SHA no momento da proposta. Se a ref andar entre a proposta e a decisão, quem aprova lê o relatório do commit fixado, e não o do novo.

!!! note
    A UI de requisições de merge fica na aba **Merge requests** do painel de Ambientes.
    A coluna **Report** mostra o que mudaria em contagem; a linha se expande para mostrar detalhe
    por objeto.

## Os comandos de CLI `env` {: #the-env-cli-commands }

`provisa env deploy` envia o modelo de uma ref para um ambiente. Ele sai com 0 quando o deploy foi aplicado ou era um teste, e com 2 quando o ambiente é protegido e o deploy só foi proposto. Um pipeline que tratasse uma aprovação pendente como um deploy liberado estaria errado, e o código de saída distingue os dois casos.

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` traz as branches remotas da organização para o repositório local. Uma implantação pode então
nomear `origin/<branch>`.

```
provisa env fetch --org acme --api <url> --token <token>
```

Ambos os comandos aceitam `--api` (a URL da API do Provisa) e `--token` (um token bearer). Defina
`PROVISA_API_URL` e `PROVISA_API_TOKEN` no ambiente para evitar passá-los em toda chamada.

O pipeline de CI típico para um fluxo apoiado em repositório:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## Veja também {: #see-also }

- [Implantação](deployment.md) — como levantar o plano de controle ao qual os ambientes se conectam
- [Commands](commands.md) — funções rastreadas e webhooks que aparecem na árvore de cada ambiente
- [Criar ambientes de teste](test-data.md) — perfilar a produção, declarar dados falsos, gerar dados sintéticos para um ambiente que não é de produção
- [Métodos de dados falsos](fake-methods.md) — cada tipo de dado falso e cada método que uma coluna pode declarar
