# Ambientes

Um ambiente é uma cópia nomeada do modelo governado de uma organização. A cópia é fisicamente um
esquema PostgreSQL separado — não uma coluna discriminadora, não um prefixo, um esquema de verdade — de modo que toda
consulta de repositório existente está correta dentro de um ambiente sem nada reescrito, e as linhas de um
ambiente não podem alcançar a leitura de outro por um predicado esquecido (REQ-1487, REQ-1488).
[tool-verified: `environments.py` module docstring; `org_schema()` at environments.py lines 86-96]

Toda organização começa com um ambiente chamado `prod`. Ele não pode ser excluído nem renomeado.
Uma requisição que não nomeia ambiente algum é servida por `prod`; uma requisição que nomeia um ambiente inexistente
é recusada. [tool-verified: `PROD = "prod"` at environments.py line 44; `select_environment()`
at env_routing.py lines 93-129]

Ambientes estão disponíveis para organizações em um plano pago. [inferred: REQ-1507]

## Nomes de ambiente

Um nome deve corresponder a `[a-z][a-z0-9_]{1,31}` — de dois a trinta e dois caracteres de letras minúsculas,
dígitos e sublinhados, começando por letra. `prod` e nomes começando com `pg_` são recusados.
O comprimento máximo para uma dada organização depende do id da própria organização: o PostgreSQL trunca silenciosamente um identificador
acima de 63 bytes, e o nome de esquema mais longo que um ambiente deriva é o que o limite protege.
[tool-verified: `ENV_NAME_PATTERN` at environments.py line 59; `validate_env_name()` at
environments.py lines 119-142; `max_env_name_length()` at environments.py lines 108-116]

## O que uma cópia carrega

Toda tabela no esquema da organização cai em exatamente uma classe (REQ-1489). A classificação é uma
lista de permissões, não uma lista de exclusão: uma tabela adicionada depois não viaja até que alguém nomeie sua
classe aqui, então o modo de falha para uma tabela esquecida é um teste vermelho. [tool-verified: `CLASSIFIED`
constant and module docstring, env_classes.py lines 19-22]

| Classe | Tabelas | O que acontece na cópia |
| --- | --- | --- |
| CARRIED | domains, naming_rules, registered_tables, table_columns, relationships, metrics, roles, rls_rules, tags, tag_param_values, tag_assignments, glossary terms, materialized_views, calendars, api_endpoints, tracked_functions, tracked_webhooks, table_meta_links | Copiadas por inteiro |
| IDENTITY_ONLY | sources, api_sources, kafka_sources, kafka_sinks | Campos de identidade e governança viajam; valores de conexão ficam para trás (veja Vínculos) |
| SEEDED_AT_CREATION | roles, user_role_assignments | Copiadas apenas quando um ambiente é criado pela primeira vez; merges posteriores não as tocam |
| PARTIAL | org_settings | Copiadas por chave: configurações de governança viajam, chaves que nomeiam um destino externo ou runtime por ambiente ficam para trás |
| NEVER_SENSITIVE | org_secrets, user_directory | Nunca copiadas |
| NEVER_RUNTIME | mv_refresh_log, relationship_candidates, admin_audit_log, e outras | Nunca copiadas |

[tool-verified: `CARRIED`, `IDENTITY_ONLY`, `SEEDED_AT_CREATION`, `PARTIAL`, `NEVER_SENSITIVE`,
`NEVER_RUNTIME` frozensets, env_classes.py lines 29-113]

`SEEDED_AT_CREATION` existe para resolver um problema específico. Um ambiente novo precisa de funções e
atribuições ou abre sem ninguém capaz de agir. Mas um merge posterior que carregasse a linha `developer`
de `prod` sobrescreveria a versão restrita de que uma branch restrita poderia precisar, tornando o caminho de revisão
a rota de escalada. Então funções e atribuições viajam uma vez, na criação, e depois disso são a resposta
de cada ambiente. [tool-verified: env_classes.py lines 65-71; env_copy.py lines 41-44]

## Vínculos

Vínculos são as colunas que dizem para onde uma fonte de fato aponta — `host`, `port`, `database`,
`username` e o restante. Elas nunca viajam em cópia alguma. Um ambiente que não foi vinculado é
marcado como `unbound` em vez de deixado em branco: um host vazio não é um host ausente, e o construtor de
conexão o leria como `localhost:5432`. [tool-verified: `BOUND_COLUMN = "bound"` at
env_classes.py line 143; `BINDING_COLUMNS` dict at env_classes.py lines 155-172]

As fontes de um ambiente resolvem de uma entre duas maneiras.

**Base** — o ambiente carrega suas próprias credenciais. Um org_admin cria uma base e então vincula
cada fonte explicitamente. [tool-verified: `CreateEnvBody.inherit_connections = False` (default) at
environments_router.py line 227; "binding a base is an org_admin's act" comment at line 358]

**Branch** — o ambiente herda as credenciais da base por referência. Nada é copiado.
Quando uma consulta precisa de uma conexão, a resolução sobe a cadeia `branched_from` e para no
primeiro ambiente cuja linha esteja vinculada. Girar uma credencial na base propaga para toda branch
dela sem nenhuma ação necessária. Revogá-la revoga para todas de uma vez. Nenhum segredo é jamais
materializado em lugar algum de onde uma branch, uma exportação ou um repositório pudesse levá-lo embora.
[tool-verified: `resolve()` at env_bindings.py lines 114-151; `lineage()` at env_bindings.py
lines 74-102; env_bindings.py module docstring lines 11-33]

Para criar uma branch, marque **Inherit connections** no painel de Ambientes. O padrão é desligado.
[tool-verified: `environmentsTab.json` key `inheritConnections`; `inheritHelp2` string]

## A projeção git

Toda escrita no modelo faz commit do resultado na branch git do ambiente. O repositório é uma
projeção do modelo, nunca sua autoridade: o Provisa lê e escreve no plano de controle; o
repositório é o registro, não a fonte. Implantar uma árvore exige uma chamada explícita — um pull request
com merge no host git não se implanta sozinho (REQ-1524, REQ-1526). [tool-verified:
deploy endpoint docstring at environments_router.py lines 777-791]

Cada entidade ganha um arquivo. O caminho é a URI do REQ-1385 com esquema e organização removidos:
`provisa://acme/sales/tables/Order` vira `sales/tables/Order.yaml`. Fontes pousam em `sources/`,
commands em `commands/`, métricas em `metrics/`. Linhas filhas que cascateiam de um pai — colunas,
relacionamentos, regras de RLS — são escritas dentro do arquivo do pai, não como arquivos próprios.
[tool-verified: `table_path()` at env_files.py line 109-115; `kind_path()` at env_files.py
lines 118-120; `COMMANDS_DIR = "commands"` at env_project.py line 71; env_files.py module
docstring lines 17-24]

Commands e suas atribuições de tag sobrevivem à ida e volta. Uma tag em um command é roteada para o
arquivo do próprio command (`commands/<name>.yaml`); uma tag que não pertence a arquivo algum desaparece da
projeção e seria excluída na próxima implantação daquela árvore. [tool-verified:
env_project.py lines 346-364; `owner_command_name` routing in `_assignments_for()` at
env_project.py lines 137-164]

Nenhuma chave substituta chega a um arquivo. `registered_tables.id` é um inteiro autoincrementado — o mesmo
modelo em dois ambientes recebe inteiros diferentes, então um dump ingênuo faz diff contra si mesmo. Toda
substituta é descartada e toda referência a uma é escrita como o caminho do destino.
[tool-verified: `STORAGE_COLUMNS` and `_model_columns()` at env_files.py lines 62-128;
env_project.py docstring lines 26-27]

A serialização é determinística. Chaves são emitidas em ordem alfabética, coleções filhas ordenadas por
seu endereço, e o estilo YAML é fixo. Dois ambientes contendo o mesmo modelo produzem
árvores byte-idênticas. [tool-verified: `dump()` at env_files.py lines 131-143]

## Merge

Fazer merge do modelo de um ambiente em outro atualiza por identidade: todo objeto que a origem tem
é criado ou atualizado no destino. Objetos que a origem não tem mais são removidos somente quando quem
chama solicita remoções explicitamente. Um merge que falha no meio deixa o destino como estava — uma
transação. [tool-verified: `copy_model()` at env_copy.py lines 216-234; REQ-1490 description]

Antes de aplicar, chame o endpoint de prévia (`GET /{name}/merge-preview`) ou passe `dry_run: true`.
A prévia roda o mesmo caminho de código que o merge usa; é um endpoint `GET` para que um script de CI que
erre a flag não possa acidentalmente aplicar o merge que pretendia apenas inspecionar. [tool-verified:
`preview_merge()` docstring at environments_router.py lines 1086-1095]

Um merge deixa os vínculos, funções e segredos do destino exatamente como estavam. Um ambiente de dev
não perde suas próprias conexões de banco de dados ao receber um modelo mais novo de prod. Prod não adquire
as concessões de dev. [tool-verified: env_copy.py lines 269-287; REQ-1490 scenario]

### O que o relatório nomeia

O relatório de merge lista, por caminho, o que foi adicionado, alterado, removido e deixado inalterado. Ele também
nomeia quaisquer **conflitos** — objetos que ambos os lados alteraram desde o último commit em comum. Um conflito
é reportado e não resolvido: a origem vence, que é o que um merge em um destino significa. O Provisa
não oferece resolução de conflito, nem marcadores de merge, nem escolha por objeto. O valor da lista de
conflitos é o sinal — duas pessoas estavam editando o mesmo objeto sem saber (REQ-1555).
[tool-verified: `CopyReport.conflicts` at env_copy.py lines 151-165; `detect_conflicts()` called
at env_copy.py lines 261-263; REQ-1555 description]

Um objeto que ambos os lados alteraram para o mesmo valor é acordo, não conflito. Quando os dois
ambientes não compartilham ancestral algum, a base é `None` no relatório e a lista vazia de conflitos
significa que nada foi comparado, não que nada colidiu. [tool-verified: `CopyReport.compared`
property at env_copy.py lines 164-166; env_copy.py lines 255-264]

O merge chega como um único commit esmagado na branch do destino. A mensagem de commit é obrigatória
e não pode ser vazia — é o único relato do intervalo de trabalho que o squash representa.
Os commits da origem permanecem onde estão e continuam implantáveis por SHA depois.
[tool-verified: `_squash()` docstring at environments_router.py lines 663-680;
`MergeBody.message` comment at environments_router.py lines 258-260]

## Pull

Fazer pull pega o que o remote contém para um ambiente e o torna o modelo. Ele não avança a branch
local diretamente; ele aplica a árvore buscada pelo caminho de implantação comum,
de modo que a mesma validação e auditoria que governam uma implantação manual governam um pull.
[tool-verified: `pull_environment()` docstring at environments_router.py lines 1450-1462]

Como um merge, um pull reporta o que sobrescreveu — objetos que a árvore recebida alterou e que o ambiente
local também havia alterado desde o último commit em comum entre as duas linhas. Uma alteração local não commitada
é um ambiente à deriva (veja Histórico abaixo); um pull a nomeia como uma alteração comum no relatório.
[tool-verified: REQ-1556 description; `pull_environment()` at environments_router.py
lines 1485-1519]

Um pull é recusado quando as duas linhas **divergiram** — ambas contêm commits que a outra não tem.
A recusa carrega a lista de objetos que ambos os lados tocaram, para que quem agora precisa decidir
qual trabalho sobrevive saiba em quais objetos olhar. [tool-verified: `state["diverged"]` check at
environments_router.py lines 1491-1503; `_collisions()` at environments_router.py
lines 1581-1602]

## Histórico

Toda implantação move o cursor do ambiente adiante em sua própria linha de commits. Um desfazer volta
um commit; um refazer avança de novo rumo à posição de onde o desfazer partiu. Nenhuma das duas
operações remove um commit — voltar adiciona uma posição, não reescreve o histórico.
[tool-verified: `_move()` docstring at environments_router.py lines 854-868]

Uma branch é semeada na ponta do ambiente do qual foi criada, então um desfazer para naquele
ponto de semeadura e não caminha para os commits do ambiente pai. [tool-verified:
`origin_sha` comment at environments_router.py lines 428-448; `_move()` at
environments_router.py lines 907-916]

As flags `can_undo` e `can_redo` viajam com a resposta da lista de ambientes. Ambas reportam `false`
quando a projeção não contém o commit que o plano de controle nomeia — um estado que o projeto admite,
chamado **à deriva**. Um nó cujo armazenamento de repositório nunca recebeu um dado commit ainda lista
seus ambientes; apenas as respostas de histórico mudam (REQ-1561). [tool-verified: `_with_history()`
at environments_router.py lines 316-344; REQ-1561 description]

## Autorização

Ambientes são governados por dois direitos. Nenhum deles é de um analista por padrão (REQ-1573).
[tool-verified: REQ-1573 description; `MANAGE_CAPABILITY = "environment_management"` and
`SWITCH_CAPABILITY = "environment_switch"` at environments_router.py line 110 and
env_routing.py line 53]

| Direito | Quem o detém (semeado) | O que ele governa |
| --- | --- | --- |
| `environment_management` | org_admin, developer | Criar e excluir ambientes |
| `environment_switch` | org_admin, developer | Ser servido por qualquer ambiente que não seja prod |

`prod` não precisa de direito — é por ele que uma requisição que não nomeia nada é servida, e recusá-lo
recusaria toda requisição.

A imposição acontece no ponto de seleção, antes de qualquer rota ser alcançada. Um membro sem
`environment_switch` é recusado em todas as interfaces de uma vez — HTTP, GraphQL, SQL e os protocolos
de fio — porque o ambiente é vinculado no middleware, não em handlers individuais.
[tool-verified: `select_environment()` at env_routing.py lines 93-129; env_routing.py
module docstring lines 28-34]

Um analista sem direito de ambiente algum pode consultar `prod` e não vê o seletor de ambiente.
Um contratado a quem se concede a função de analista não vê interface de ambientes e não pode criar nem entrar em
nenhum ambiente que não seja produção. [tool-verified: REQ-1573 use_case and scenario]

### Autoridade do dono do ambiente

Criar um ambiente é o único caminho pelo qual um membro somente leitura adquire direitos de edição de modelo
(REQ-1528). Dentro do ambiente que criou, o criador detém as capacidades da função `developer` — menos
os direitos de dados (`write`, `full_results`, `usage`). Direitos de construção de modelo,
não direitos de dados. [tool-verified: `ENVIRONMENT_OWNER_CAPABILITIES` at env_authority.py lines 75-77;
`_DATA_RIGHTS` at env_authority.py lines 74-77; env_authority.py module docstring lines 14-38]

A concessão é derivada de `environments.created_by` no momento da autorização, nunca escrita em uma
tabela de concessões. Excluir o ambiente a remove no mesmo ato.
[tool-verified: env_authority.py module docstring lines 39-42; `environment_owner()` at
env_authority.py lines 84-98]

A participação em domínios ainda limita o que o dono pode alterar. Fazer branch muda o que um membro pode fazer;
nunca muda em quais domínios ele pode fazê-lo (REQ-1530).
[tool-verified: `domains_within()` at env_authority.py lines 121-145]

## Ambientes protegidos (REQ-1504)

Um ambiente pode ser protegido. Um merge ou uma implantação em um ambiente protegido não é aplicado
quando solicitado; ele é proposto, e alguém diferente de quem solicitou precisa aprová-lo.

`prod` é protegido automaticamente assim que a organização tem mais de um membro. Uma organização de um único membro
não consegue satisfazer "alguém diferente de quem solicitou", então a regra não é aplicada ali — ela tornaria
`prod` impossível de mesclar. Qualquer ambiente pode ser marcado como protegido por um org_admin.
[tool-verified: `is_protected()` at env_approvals.py lines 79-96; `protectedHelp2` UI string
in environmentsTab.json line 28]

Uma requisição de merge é uma linha, não uma caixa de diálogo de confirmação. Quem aprova é por definição uma pessoa
diferente de quem solicitou e não está presente no momento da solicitação; uma confirmação
efêmera forçaria a aprovação dentro da sessão de quem solicitou, que é o único arranjo que o
requisito proíbe. [tool-verified: env_approvals.py module docstring lines 11-17]

A linha da requisição carrega o relatório de merge ao lado da mensagem de quem solicitou. A obsolescência é derivada
no momento da leitura, nunca armazenada: replanejar na leitura e comparar com o relatório armazenado é
a única versão que não pode estar errada. Uma requisição obsoleta precisa ser refeita. Quem solicita não pode
aprovar a própria requisição. [tool-verified: `STALE` constant and `effective_state()` at
env_approvals.py lines 53, 215-243; `decide()` lines 265-268]

Estados do ciclo de vida da requisição: `requested` → `approved`/`rejected` → `applied`. `stale` é derivado.
[tool-verified: `REQUESTED`, `APPROVED`, `REJECTED`, `APPLIED`, `STALE` at env_approvals.py
lines 47-53]

A mesma porta trata implantações a partir de um ref de repositório: a requisição fixa o SHA no momento da proposta.
Se o ref se mover entre a proposta e a decisão, quem aprova lê o relatório do commit
fixado, não do novo. [tool-verified: `request_deploy()` at env_approvals.py lines
150-189; env_approvals.py docstring lines 26-27]

!!! note
    A UI de requisições de merge fica na aba **Merge requests** do painel de Ambientes.
    A coluna **Report** mostra o que mudaria em contagem; a linha se expande para mostrar detalhe
    por objeto. [tool-verified: `environmentsTab.json` keys `requestsTitle`, `colReport`,
    `approve`, `reject`]

## Os comandos de CLI `env`

`provisa env deploy` envia o modelo em um ref para um ambiente. Ele sai com 0 quando a implantação foi
aplicada ou foi uma execução de teste, e com 2 quando o ambiente é protegido e a implantação foi apenas proposta
— um pipeline que tratasse uma aprovação pendente como uma implantação liberada estaria errado, e o código de saída
diz isso. [tool-verified: `_cmd_env_deploy()` at cli.py lines 389-411]

```
provisa env deploy --org acme --env prod --ref main --token <token> --api <url>
```

`provisa env fetch` traz as branches remotas da organização para o repositório local. Uma implantação pode então
nomear `origin/<branch>`. [tool-verified: `_cmd_env_fetch()` at cli.py lines 414-426]

```
provisa env fetch --org acme --api <url> --token <token>
```

Ambos os comandos aceitam `--api` (a URL da API do Provisa) e `--token` (um token bearer). Defina
`PROVISA_API_URL` e `PROVISA_API_TOKEN` no ambiente para evitar passá-los em toda chamada.
[inferred: shared `_api_call()` helper]

O pipeline de CI típico para um fluxo apoiado em repositório:

```bash
provisa env fetch --org acme --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
provisa env deploy --org acme --env prod --ref "origin/main" \
  --message "release: $GIT_COMMIT_MSG" \
  --api "$PROVISA_API_URL" --token "$PROVISA_API_TOKEN"
```

---

## Veja também

- [Implantação](deployment.md) — como levantar o plano de controle ao qual os ambientes se conectam
- [Commands](commands.md) — funções rastreadas e webhooks que aparecem na árvore de cada ambiente
