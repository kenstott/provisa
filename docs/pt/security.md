# Modelo de segurança {: #security-model }

Toda consulta passa pela mesma governança, seja qual for a linguagem em que é escrita (GraphQL, SQL, Cypher) e o caminho por onde chega (REST, gRPC, Arrow Flight, JDBC, WebSocket). Não há como contorná-la.

As verificações acontecem em ordem. Uma requisição precisa passar por cada uma antes que a próxima seja examinada.

## Modelo em camadas {: #layered-model }

### Camada 0 — Filtragem de introspecção {: #layer-0-introspection-filtering }

Um papel vê apenas as tabelas que seu `domain_access` alcança e as colunas que suas regras `visible_to` permitem. O resto não existe para esse papel: não dá para consultar, autocompletar nem adivinhar. O esquema GraphQL, o catálogo SQL e o navegador de esquema do editor de consultas mostram todos a mesma visão restrita.

Veja [Visibilidade do esquema](#schema-visibility).

### Camada 1 — Acesso público {: #layer-1-public-access }

Uma coluna publicada com o escopo `public` é servida a todos os papéis da organização, até a um cujo `domain_access` não alcança o domínio da tabela. Esse papel vê a tabela com as colunas públicas e mais nada. Nada é público até que quem registrou a tabela decida que é. Veja [Colunas públicas](#public-columns).

### Camada 2 — Acesso por domínio {: #layer-2-domain-access }

Cada papel tem uma lista `domain_access` de IDs de domínio, e uma consulta que toca uma tabela de outro domínio é recusada antes de rodar. É a fronteira grossa de propriedade. Um papel de RH não alcança as tabelas de finanças, não importa como o SQL seja escrito.

Veja [Modelo de direitos](#rights-model).

### Camada 3 — Segurança em nível de linha {: #layer-3-row-level-security }

Resolvido o acesso por domínio, o filtro de linhas do papel para cada tabela é acrescentado a todo `SELECT` como predicado `WHERE` e avaliado sobre os dados brutos. Um gerente regional que roda `SELECT *` na tabela compartilhada de pedidos continua recebendo só as linhas da sua região.

Veja [Segurança em nível de linha (RLS)](#row-level-security-rls).

### Camada 4 — Visibilidade e mascaramento de colunas {: #layer-4-column-visibility-and-masking }

Uma coluna cuja lista `visible_to` deixa seu papel de fora some do resultado. Uma coluna mascarada tem os valores substituídos antes de saírem do servidor, por uma expressão regular, uma constante ou um truncamento. Isso vale em toda linguagem de consulta e todo formato de saída.

Veja [Modelo de permissões de coluna](#column-permission-model) e [Mascaramento de colunas](#column-level-masking).

### Camada 5 — Proteção de predicados {: #layer-5-predicate-guard }

Você não pode filtrar por uma coluna mascarada: `WHERE` e `HAVING` a recusam já na análise da consulta. Do contrário, quem chama poderia recuperar o valor real por busca binária, um filtro de cada vez, mesmo que todo resultado visto estivesse mascarado.

### Governança de relacionamentos (V002) {: #relationship-governance-v002 }

Um join de SQL precisa seguir um relacionamento registrado e aprovado, e qualquer outro join é recusado. Cada relacionamento traz um motivo e uma descrição que explicam a pessoas e agentes por que aquele caminho existe. Trate isso como política de governança, não como fronteira de segurança. As camadas 2 a 5 valem seja como forem os joins, então contornar a verificação não expõe nada que o papel não pudesse ler com duas consultas separadas. As tentativas de contorno ficam registradas.

Há duas maneiras de passar pelo V002. A primeira é uma capacidade: um papel com `ignore_relationships` pode unir relações que o catálogo não cobre. Dos papéis já criados, só `modeler` a tem, porque seu trabalho é descobrir o modelo; `analyst` não tem.

A segunda exige duas coisas ao mesmo tempo:

1. **Flag do papel**: `relationship_guard: false` na definição do papel (o padrão é `true`).
2. **Exclusão por consulta**: o comentário `--relationship-guard=false` no SQL.

Qualquer uma sozinha não faz nada.

**O modo de alta segurança trava a verificação.** Com `security.mode: high`, nenhuma das duas maneiras funciona. `ignore_relationships` e `relationship_guard: false` são ambos ignorados, e todo join precisa estar no catálogo aprovado. A redundância é proposital: um papel de produção que recebeu a capacidade por engano continua sem conseguir sair do modelo.

**GraphQL** pula o V002. Seus relacionamentos vêm do modelo, então já estão aprovados.

**SQL e Cypher** aplicam o V002 por padrão, com a exclusão de duas condições descrita acima.

**pgwire** aplica a mesma verificação que o SQL. O comentário `--relationship-guard=false` é removido antes de a consulta chegar ao banco.

---

As camadas se somam. Um papel com acesso por domínio, filtros de linhas e colunas mascaradas tem tudo isso em vigor ao mesmo tempo. Adicionar uma fonte, uma coluna ou um relacionamento não obriga a mexer em cada regra, porque cada camada é configurada à parte e vale para qualquer consulta que alcance o que ela governa.

---

## Modelo de direitos {: #rights-model }

Capacidades atribuídas de forma independente, com uma hierarquia de funções opcional via `parent_role_id`. `admin` concede tudo.

| Capacidade | Descrição |
| ----------- | ------------- |
| `source_registration` | Registrar fontes de dados |
| `table_registration` | Registrar tabelas e colunas |
| `create_relationship` | Definir relacionamentos FK |
| `access_config` | Configurar RLS e mascaramento |
| `query_development` | Executar consultas |
| `write` | Invocar mutações registradas (controle grosso; veja Autorização de mutações) |
| `full_results` | Ignorar os limites de amostragem |
| `ignore_relationships` | Ignorar a governança de relacionamentos (V002). Tida apenas por `modeler` entre as funções de sistema, e totalmente ignorada no modo de alta segurança |
| `admin` | Superusuário — concede tudo |

### Herança de funções {: #role-inheritance }

Funções podem herdar de uma função pai via `parent_role_id`, definida na configuração ou na página Segurança como "Herda de". A cadeia é dobrada na inicialização: um filho tem a união das capacidades e do acesso a domínios dos seus ancestrais, cada concessão de coluna, métrica, função e webhook que nomeia um ancestral, e as regras RLS dos ancestrais por tabela, com precedência do filho — a função mais próxima na cadeia com uma regra para uma tabela fornece o predicado dessa tabela, e uma regra salva para o filho substitui a do pai para essa tabela.

```yaml
roles:
  - id: basic_user
    description: Queries the public domain.
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    description: Analyses sales; reads full results.
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```
## Funções: concedidas, derivadas e meta {: #roles-granted-derived-and-meta }

Três tipos de função trabalham juntos no modelo de acesso do Provisa. A página Funções lista as duas primeiras juntas; uma meta-função nunca é listada.

### Funções concedidas {: #granted-roles }

Uma função concedida é atribuída diretamente a um usuário. Ela tem direitos: capacidades, acesso a domínios, entradas de mascaramento, filtros de linha. A herança (`parent_role_id`) dobra na inicialização os direitos de um ancestral no filho. Direitos só se somam; não há subtração. Para restringir um leitor, conceda a um composto que nomeie tanto a função quanto uma condição, ou coloque uma entrada de máscara antes da permissiva.

### Atribuições que expiram e sem uso {: #expiring-and-unused-grants }

Uma atribuição que o Provisa mantém pode terminar. Uma atribuição concedida com expiração deixa de ser mantida a partir desse instante, em cada solicitação. Uma função pode indicar `inactivity_days`: uma atribuição dessa função da qual nenhuma solicitação dependeu por mais tempo, contado a partir do mais recente entre o último uso e a criação, é removida. As funções que vêm das reivindicações ou dos mapeamentos de funções de um provedor de identidade são limitadas no tempo pelo provedor; defina lá a expiração delas. Funções derivadas e meta-funções nunca expiram.

Uma solicitação dependeu de uma função quando essa função a atendeu sozinha ou, quando a solicitação atua como várias funções, quando à função é concedida uma tabela que a instrução leu. Atuar como «Todas» não conta como usada cada função mantida. O último uso de uma atribuição é registrado no máximo uma vez por dia.

A cada 15 minutos, a varredura de cada organização exclui as atribuições expiradas e sem uso e registra cada uma no registro administrativo como `role.expired` ou `role.removed_inactive`. Onde o e-mail de saída está configurado, inclusive numa implantação com uma só organização, o titular e os administradores da organização recebem um e-mail `auth.role_grant_notice_days` dias (padrão 7) antes de uma atribuição terminar, e os administradores recebem um quando uma é removida; onde não há e-mail configurado, a entrada do registro é o comprovante. Esses avisos são enviados qualquer que seja a preferência de e-mail do usuário.

### Funções derivadas {: #derived-roles }

Uma função derivada vem dos atributos de um usuário. A organização declara quais atributos viram funções (`derived_roles`), cada um com uma descrição. Para cada atributo declarado `a` cujo valor `v` os atributos do usuário fornecem, o usuário tem a função pai `a` e a sua função de valor `a__v`: `region = eu` dá `region` e `region__eu`. Sem valor, nenhuma das duas.

A resolução dos atributos importa. O registro próprio da organização sobre um membro prevalece sobre o que o provedor de identidade fornece: quando os dois nomeiam o mesmo atributo, o valor da organização é usado e o do provedor é marcado como substituído.

Funções derivadas não trazem direitos e nunca são atribuídas à mão. Uma lista de concessão, uma máscara ou uma regra de linha as nomeia exatamente como nomeia uma função concedida; uma requisição as nomeia em `X-Provisa-Role` e age como elas.

Na página Funções, cada atributo declarado é uma função pai marcada como *derivada*, com suas funções de valor listadas abaixo à medida que seus valores aparecem. **+ Função → Derivada de um atributo** declara uma. O painel dela mostra o atributo, sua descrição, os valores vistos até agora e cada máscara, regra de linha e concessão que nomeia ela ou uma de suas funções de valor. Ela não tem controles de concessão. A descrição de uma função de valor é gerada: "Mantida quando o region do usuário é eu".

Declare-as na configuração como uma lista:

```yaml
derived_roles:
- attribute: region
  description: The subsidiary a member works for; region__eu marks EU staff.
```

Pense em um JWT: seus claims são fatos sobre a pessoa. Funções derivadas transformam fatos escolhidos em funções, para que uma concessão possa se dirigir a eles sem criar uma função por combinação.

### Meta-funções {: #meta-roles }

Quando uma requisição nomeia mais de uma função, o Provisa monta uma meta-função efêmera identificada como `meta:<ids sorted, joined by +>`. Ela é um único filho de cada função do conjunto nomeado. O pipeline inteiro então roda com ela como uma função única comum.

Os direitos são a união dos de cada membro. Os filtros de linha são o OR do filtro resolvido de cada membro; se algum membro que lê uma tabela não tem filtro, a meta-função também não tem — sem filtro significa todas as linhas. Uma meta-função nunca é armazenada, listada nem exportada. Ela é remontada a partir dos seus membros quando o modelo muda.

As colunas de cada linha são mascaradas e exibidas conforme os papéis cujo filtro de linhas admitiu essa linha: desses papéis, aplica-se a leitura menos restritiva da coluna, e um papel que não admitiu a linha não contribui em nada para como ela é lida. As máscaras se aplicam onde a tabela é lida, antes de qualquer join ou agregação, de modo que uma agregação lê os valores mascarados. As escritas são verificadas da mesma forma: uma coluna é escrita em uma linha apenas onde um papel que concede essa coluna admite a linha.

O registro de auditoria nomeia cada função atuante, não o id da meta-função. Como uma meta-função nunca é armazenada, ela não tem descrição própria: onde quer que uma seja mostrada, sua descrição é a de seus membros, cada id com a sua.

### Descrições {: #descriptions }

Toda função e toda regra de linha diz para que serve. Uma revisão de acesso lê quem pode ver o quê e por quê. Uma função ou regra que ninguém sabe explicar é uma que ninguém pode revisar, então a descrição é obrigatória: uma função, uma função pai derivada ou uma regra de linha com descrição vazia é recusada pelo carregamento da configuração, pela API de administração e pela UI, com o motivo. As listas Funções e Regras de linha mostram cada descrição, e seus painéis de detalhe a mostram e editam.

Uma função tem uma descrição, não uma por domínio. Uma função geral que não descreve suas responsabilidades dentro de um domínio específico significa que esse domínio precisa de uma função própria, geralmente uma filha da função geral (`parent_role_id`), que herda seus direitos e descreve o que faz ali.

### Como uma requisição nomeia funções {: #how-a-request-names-roles }

Um cliente envia uma função, ou um conjunto separado por vírgulas, em `X-Provisa-Role`. Cada função nomeada precisa ser tida. Uma função não atribuída à identidade é recusada pelo nome.

Direitos e dados seguem apenas as funções com que a requisição age: ter uma função não é agir como ela. Uma função é essa função; duas ou mais agem como sua meta-função. Uma função atuante que trouxesse junto os direitos de cada função tida não significaria nada, então uma requisição recebe o menor privilégio e o registro de auditoria nomeia exatamente o que agiu.

Funções derivadas nomeadas entram como qualificadores. Um qualificador não tem direitos, acesso a domínios, máscaras ou filtros de linha próprios, então não é membro dos direitos da meta-função: como membro sem máscaras, desmascararia toda coluna, e como membro que não lê nenhuma tabela, encerraria todo OR de filtros de linha. Ele entra só nas concessões: uma concessão ou entrada de máscara que nomeia `region__eu` alcança uma requisição que nomeia esse qualificador junto com uma função concedida.

Um agente MCP nunca age além dos papéis que lhe foram dados. Um agente remoto só nomeia papéis que o principal do seu token detém. Um agente stdio local só nomeia papéis dentro do conjunto fixado em `mcp.role` (`PROVISA_MCP_ROLE`), e age como esse conjunto quando não nomeia nenhum; qualquer outro papel é recusado pelo nome.

Um agente pergunta com qual função age por meio da ferramenta MCP `describe_role`. A resposta é essa função e para que ela serve (uma meta-função com cada um de seus membros), e as funções para as quais o agente pode mudar, cada uma com sua descrição. Ela nunca traz uma regra de linha, máscara, concessão ou condição. O Provisa nunca diz a um agente por que um resultado foi restringido, e um resultado não traz nenhum indicador de "filtrado": os dados de uma função são toda a sua visão, e um motivo para o que falta convidaria a sondá-lo.

### O que `+` significa {: #what-means }

`+` aparece em dois lugares, com significado diferente em cada um.

**Em uma entrada de concessão.** `analyst+region__eu` em uma lista `visible_to` ou `to` significa que o leitor tem todas elas — `analyst` E `region__eu` ao mesmo tempo. Uma entrada que não é um composto é uma função única. Entre as entradas de uma lista a regra é qualquer uma: um leitor tem a concessão se tiver qualquer entrada. E dentro, OU entre elas.

**Em um id de meta-função.** `meta:analyst+engineer` é o identificador do servidor para a meta-função do conjunto {analyst, engineer}. Seus direitos são a união dos dois. Um cliente nunca nomeia diretamente o id da meta-função; ele nomeia as funções que tem, e o servidor monta a meta-função.

### Regras de coluna ABAC como RBAC simples {: #abac-column-rules-as-plain-rbac }

Uma política baseada em atributos pergunta: "este usuário, com estes atributos, pode ver esta coluna?" O RBAC pergunta: "quais funções podem ver esta coluna?" Concessões compostas e funções derivadas as tornam a mesma pergunta.

"Desmascarar este e-mail só para um analista na UE" é uma entrada de máscara `{to: [analyst+region__eu], mask: null}`. Qualquer leitor que tenha tanto a função `analyst` quanto a função derivada `region__eu` lê a coluna sem máscara. Sem linguagem de política. Sem uma função por combinação.

A troca é cálculo em vez de enumeração. Um catálogo de funções pequeno mantém a manutenção baixa, mas "quem pode ver este e-mail da UE?" vira uma resposta calculada: entradas compostas são avaliadas contra as atribuições e os claims de cada usuário. O Provisa consegue responder nas duas direções porque o modelo é uma única definição. A verificação de acesso do editor de colunas (`POST /admin/masks/check`) executa essa avaliação para um conjunto de funções escolhido.

### Benefícios {: #benefits }

- Uma regra de coluna ABAC vira uma entrada de concessão RBAC.
- Um catálogo de funções pequeno em vez de uma função por combinação.
- Uma única definição coordenando as regras de cada região.
- Menor privilégio e um registro de auditoria exato por requisição.

### Boas práticas {: #best-practices }

- Mantenha o catálogo de funções tão pequeno quanto a organização permitir.
- Herde dentro de um eixo (senioridade, nível de habilitação). Componha entre eixos independentes (função + região, função + departamento).
- Coloque primeiro as entradas de máscara mais específicas — uma entrada anterior vence mesmo quando o leitor tem uma posterior.
- Verifique o acesso efetivo com `POST /admin/masks/check` antes de colocar uma combinação em produção.

## Modelo de permissões de coluna {: #column-permission-model }

Cada coluna tem um modelo de permissões que controla o acesso de leitura, escrita e mascaramento por função.

### Visibilidade em três níveis {: #three-tier-visibility }

| Nível | Condição | Resultado |
| ------ | ----------- | -------- |
| **Oculta** | Função fora de `visible_to` | Coluna ausente do esquema e do catálogo |
| **Mascarada** | Função em `visible_to`; a primeira entrada de máscara que ela tem traz uma `mask` | Coluna visível, valor substituído na saída |
| **Sem máscara** | Função em `visible_to`; a primeira entrada de máscara que ela tem não traz `mask`, ou ela não tem nenhuma | Acesso completo de leitura |

### Colunas públicas {: #public-columns }

Toda coluna tem um `scope`, definido no arquivo do modelo, na API de administração ou na página Tabelas. Os três escopos são ordenados por quem atendem: `restricted`, depois `domain`, depois `public`. Um editor de tabela (`table_registration` no domínio da tabela) pode restringir o escopo de uma coluna. Ampliá-lo — de `restricted` para `domain` ou `public`, de `domain` para `public` — serve a coluna a mais funções e exige o direito que concedê-la exige: `column_grant` ou `access_config` no domínio da tabela. Uma coluna sendo registrada é comparada com `domain`, então registrá-la como `public` é uma ampliação. Em uma coluna com uma tag sensível, qualquer mudança de escopo exige `sensitive_data`. Uma mudança recusada nomeia a coluna e o direito.

| Escopo | Servida a |
| ------- | ----------- |
| `domain` (padrão) | As funções que alcançam o domínio da tabela, como diz `visible_to`. Um `visible_to` vazio é cada uma dessas funções. |
| `public` | As mesmas funções, e também as que não alcançam o domínio da tabela. |
| `restricted` | Apenas as funções que `visible_to` nomeia. Um `visible_to` vazio é ninguém. |

Publicar uma coluna remove a condição de domínio para essa coluna e nada mais:

- Uma função fora do domínio recebe a tabela apenas com suas colunas públicas. As outras colunas da tabela, e as tabelas que não publicam nada, ficam fora do seu esquema, do seu catálogo e do seu SQL.
- O próprio `visible_to` da coluna ainda decide quem: uma coluna pública concedida a uma função é pública só para essa função.
- Máscaras, filtros de linha, limites de linha e a proteção de relacionamentos se aplicam como dentro do domínio. Sem filtro de linha para a função, as colunas publicadas são lidas sem filtro; um filtro de linha na tabela ou no seu domínio continua valendo.
- Um relacionamento é servido quando as colunas das suas duas pontas são.
- Uma coluna de parâmetro de uma tabela de API (um argumento de caminho ou de consulta) não é dado e não publica nada.
- O domínio `ops` exige uma concessão explícita para cada coluna; nada nele é servido entre domínios, seja qual for o escopo.

A regra é a mesma em toda superfície, porque cada uma lê o esquema compilado da função: GraphQL, SQL (HTTP, pgwire, Flight, JDBC), Cypher e Bolt, gRPC, REST e JSON:API, os catálogos de Flight e MCP e as listagens de administração. Um escopo que não é um dos três valores é recusado quando uma coluna é salva.

### Permissões de escrita {: #write-grants }

`visible_to` controla a leitura de uma coluna (vazio significa que todas as funções podem ler). A escrita é controlada por tabela com `write_grants`, não por coluna. Por padrão, uma tabela não tem nenhuma permissão, então nenhuma função pode escrever nela.

Uma permissão de escrita indica o que uma função pode escrever em uma tabela por uma operação (`insert`, `update` ou `delete`):

| Campo | Finalidade |
| ------- | --------- |
| `columns` | As colunas que um `INSERT` pode listar ou um `UPDATE` pode definir |
| `rows` | As linhas que um `UPDATE` ou `DELETE` pode tocar (um predicado SQL) |
| `check` | O que toda linha escrita por um `INSERT`, ou deixada por um `UPDATE`, deve satisfazer (um predicado SQL) |
| `presets` | Colunas que a própria permissão define, a partir de uma variável de sessão, de um literal ou do momento da escrita. A instrução não pode fornecê-las. |

Se você omitir `rows` ou `check`, a permissão assume o filtro de linhas de leitura da função sobre a tabela, de modo que uma função escreve as linhas que lê. `"TRUE"` significa todas as linhas.

Uma única admissão decide cada escrita em cada superfície, nesta ordem: a função tem o direito `write`; tem uma permissão para a operação sobre a tabela; cada coluna que a instrução escreve está na permissão; os presets são adicionados; `rows` é combinado com AND no `WHERE`; e `check` é avaliado sobre as novas linhas antes de qualquer coisa ser enviada à fonte. Uma recusa nomeia a tabela, a função e a operação.

Instruções mais amplas exigem permissões mais amplas. `TRUNCATE` exige uma permissão `delete` sobre todas as linhas. `MERGE` exige uma permissão `insert` e uma `update` sobre todas as linhas. `INSERT ... ON CONFLICT DO UPDATE` exige uma permissão `update` sobre todas as linhas sem presets. `COPY` exige uma permissão `insert` sem presets.

O GraphQL oferece a cada função um campo de mutação exatamente para as operações em que ela tem permissões, com as colunas da permissão. Uma função filha herda as permissões de sua função pai, as suas próprias primeiro. Uma meta-função mantém a dobra das permissões de seus membros: a união das colunas, o OR de `rows` e `check`, e apenas os presets que todos os membros compartilham. Na interface de administração, a seção **Gravações** de uma tabela lista e edita suas permissões.

### Exemplo {: #example }

```yaml
tables:
  - source_id: hr-pg
    domain_id: people
    schema: public
    table: employees
    columns:
      - name: id
        visible_to: []                # all can read
      - name: email
        visible_to: [admin, analyst, viewer]
        masks:
          - to: [admin]               # admin sees raw value (no mask)
          - to: ["*"]                 # everyone else sees it masked
            mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}
      - name: salary
        visible_to: [admin, hr]
        masks:
          - to: [admin, hr]           # admin and hr see real value
          - to: ["*"]                 # all other visible roles see 0
            mask: {type: constant, value: 0}
      - name: created_at
        visible_to: []                # all can read
    write_grants:
      - role: admin
        operation: update
        columns: [email]
        rows: "TRUE"
      - role: hr
        operation: update
        columns: [salary]
      - role: hr
        operation: insert
        columns: [email, salary]
        presets:
          - {column: created_at, source: now}
```

Neste exemplo:

- `email`: admin vê `alice@example.com` e pode atualizá-lo em qualquer linha; analyst e viewer veem `a***@example.com` e não podem escrever
- `salary`: admin e hr veem o valor real; hr pode atualizá-lo nas linhas que hr pode ler, porque a permissão não indica `rows`; qualquer outra função visível vê `0`
- `created_at`: todos podem lê-lo, nenhuma função o fornece, e um insert de hr o define como o momento da escrita

## Autorização de mutações {: #mutation-authorization }

Uma mutação registrada (GraphQL remoto, OpenAPI, gRPC, Hasura) passa por duas verificações. O papel precisa ter a capacidade global `write`, e o comando precisa estar atribuído a ele por `visible_to`; uma lista vazia o atribui a todos os papéis. Um comando não tem lista `writable_by`, e a configuração recusa uma pelo nome.

O que decide se algo é uma escrita é o seu contrato, não o que diz quem chama. Um `SELECT` que chama uma função do tipo mutação é tratado como escrita e passa pelas mesmas duas verificações, então você não consegue rodar uma mutação disfarçada de leitura. Marcar uma mutação como segura para leitura exige a capacidade `access_config` e fica registrado como decisão de governança. Nenhuma requisição pode se isentar.

## Visibilidade do esquema {: #schema-visibility }

Cada papel tem o próprio esquema GraphQL. Só aparecem tabelas dos domínios do seu `domain_access` (`"*"` são todos), e uma coluna fora de `visible_to` para o papel também fica fora do SDL. O que o papel não pode ler, ele não vê.

## Segurança em nível de linha (RLS) {: #row-level-security-rls }

Um filtro de linhas é um predicado `WHERE` para uma tabela e um papel, acrescentado depois da compilação e antes da execução.

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
    description: Analysts read only their own region's orders.
```

O filtro entra com AND na cláusula `WHERE` da consulta, tanto em leituras quanto em `UPDATE` e `DELETE`.

Um filtro lê a requisição por `current_setting('provisa.<name>')`. A requisição vincula `user_id` a partir da identidade conectada, `role` a partir do papel em ação e cada claim escalar do token, em minúsculas e com sublinhados no lugar de hífens. `provisa.user_id` é sempre a identidade conectada, e nenhum claim pode substituí-la.

As variáveis de sessão do Hasura mantêm o nome. `X-Hasura-User-Id` é `hasura_user_id`, lido do claim `x-hasura-user-id` do token (no nível superior ou dentro do objeto de claims do Hasura, como `https://hasura.io/jwt/claims`) ou do atributo `hasura_user_id` registrado para o membro. Assim, um filtro do Hasura importado sobre `X-Hasura-User-Id` lê `provisa.hasura_user_id`.

Debaixo dos valores da requisição ficam os `session_vars` configurados do papel, que a requisição sobrescreve. Um papel que herda recebe as constantes dos antecessores sob as suas, e para cada nome vence o papel mais próximo. `user_id`, `role`, `region` e `org` vêm da requisição ou do nó e não podem ser configurados como constantes. No modo sem segurança, um cabeçalho `x-provisa-session-<name>` vincula `<name>`. Uma variável não vinculada em lugar nenhum é NULL, e um filtro sobre ela não encontra linhas.

Uma constante de papel funciona ao contrário de um papel derivado. Um papel derivado transforma em papel um valor que um membro tem. Uma constante de papel dá um valor a toda requisição feita como aquele papel, e uma regra de linhas o lê do mesmo jeito que lê um claim. A constante pode ser uma referência `${env:NAME}` ou `${secret:NAME}`, resolvida pelo mesmo resolvedor de qualquer outra referência quando uma consulta a usa. O modelo guarda a referência, nunca o valor, e uma referência que não resolve para nada faz a consulta falhar, nomeando a constante. Depois de resolvido, o valor faz parte da consulta: os registros do próprio Provisa ficam cobertos pela ocultação central, mas os logs próprios de uma fonte federada ficam fora do alcance do Provisa.

## Mascaramento em nível de coluna {: #column-level-masking }

O mascaramento é definido como uma lista ordenada em cada coluna. A primeira entrada que o leitor tem decide o que ele vê.

| Tipo de máscara | Tipos de coluna suportados | Expressão SQL |
| ----------- | ----------------------- | --------------- |
| `regex` | String (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Qualquer | Valor literal (`NULL`, `0`, uma string personalizada) |
| `truncate` | Data/Timestamp | `DATE_TRUNC(precision, col)` |

Cada entrada tem uma lista `to` que nomeia os leitores a que se dirige, e uma `mask` opcional. Uma entrada sem `mask` significa sem máscara para esses leitores. `*` em `to` corresponde a qualquer leitor.

A expressão de máscara é levada para a projeção SQL SELECT. O banco de dados retorna o valor mascarado; dados sem máscara nunca atravessam a rede para funções mascaradas. Colunas mascaradas também são bloqueadas em cláusulas `WHERE` e `HAVING` (proteção de predicados da camada 5) para impedir a dedução de valores por filtragem.

**Uma máscara cobre todos os nomes da coluna, não uma coluna relacionada.** Uma coluna lida pelo seu alias ou pelo seu nome físico é mascarada da mesma forma. Uma máscara não oculta uma chave que um relacionamento aprovado une a uma coluna que o papel pode ver: o join compara os valores reais, então a coluna relacionada mostra o que a chave mascarada contém. Mascare também a coluna relacionada, ou não aprove o relacionamento para esse papel.

**Nenhuma entrada tida significa sem máscara.** Um leitor que não tem nenhuma entrada lê a coluna sem máscara. Termine uma lista com uma entrada `*` quando todo leitor deve receber alguma máscara. O editor de colunas avisa quando falta uma entrada `*` em uma lista (`unlisted_unmasked`).

**Negar pela ordem.** Uma entrada anterior vence mesmo quando o leitor também tem uma posterior mais permissiva. Para que prestadores de serviço vejam sempre um salário mascarado, quaisquer que sejam suas outras funções, coloque a entrada deles antes da entrada sem máscara. Uma função concedida nunca perde o que o pai concede por herança, mas uma entrada de máscara colocada antes na lista se aplica primeiro — é assim que a restrição funciona aqui.

**Meta-funções.** A cadeia de uma meta-função inclui as funções de cada membro, seus ancestrais e quaisquer funções derivadas nomeadas. Uma entrada concedida a qualquer membro alcança a meta-função. Uma entrada anterior, tida por outro membro, ainda se aplica primeiro.

**Verificar o acesso.** `POST /admin/masks/check` aceita uma lista de máscaras e um conjunto opcional de funções, e retorna qual entrada se aplica para essas funções, mais as entradas que nenhum leitor consegue alcançar. Protegido por `masking_config`.

```yaml
# email column: analyst+region__eu sees raw; everyone else sees masked
masks:
  - to: [analyst+region__eu]          # composite: analyst AND in EU region
  - to: ["*"]                         # all others
    mask: {type: regex, pattern: "(.).*@", replace: "$1***@"}

# hire_date column: hr sees raw; all other visible roles see year only
masks:
  - to: [hr]
  - to: ["*"]
    mask: {type: truncate, precision: year}
```
## Governança das respostas de ações {: #governing-action-responses }

O que uma função rastreada ou um webhook retorna é governado como as linhas de uma tabela. O Provisa vincula as linhas retornadas como uma relação cujas colunas são a saída declarada da ação: `output_columns` para uma função, `inline_return_type` para um webhook, ou as propriedades do objeto do `return_schema` do tipo array de uma função. Essa relação passa então pela mesma etapa de governança de qualquer leitura de tabela, sobre uma CTE VALUES com as linhas, de modo que segurança de linhas, mascaramento e visibilidade não são reimplementados para ações.

Valem três regras, na mesma ordem de uma tabela:

- **Filtro de linhas.** A regra RLS do papel salva para a ação pelo nome (`upsertRlsRule` com `actionName`), ou a regra do domínio da ação quando ela não tem uma própria. O predicado é verificado contra o contrato ao ser salvo, como uma regra de tabela.
- **Visibilidade de colunas.** Uma coluna do contrato que declara `visible_to` é retirada para qualquer papel que a lista deixe de fora. Uma coluna que não declara faz parte da forma pública da ação e permanece.
- **Máscaras.** Uma coluna do contrato traz `masks`, a mesma lista ordenada de uma coluna de tabela.

A herança de papéis resolve a cadeia para as três. Se uma resposta retornar uma coluna fora do contrato declarado, a chamada falha com 502 em vez de deixá-la passar sem governança. Uma ação que não declara colunas retorna um escalar, que não tem nada a vincular, e ele volta como está. A invocação de teste da administração informa quais filtros, exclusões e máscaras se aplicaram, e a instrução governada vai para o log de auditoria de consultas.

## Amostragem {: #sampling }

Todo papel recebe uma amostra dos resultados, 100 linhas por padrão, a menos que tenha a capacidade `full_results`. Defina o tamanho com a variável de ambiente `PROVISA_SAMPLE_SIZE`.

## Registro de auditoria {: #audit-logging }

Toda consulta que toca um ativo de domínio é escrita em `query_audit_log`. O log pertence à organização, não à plataforma: o registro da própria plataforma, com organizações, usuários e convites, não guarda entradas de auditoria. O log de cada organização é uma tabela no esquema dela, no armazenamento que a organização indica para seus registros em cada região que escolhe. Uma organização sem regiões o mantém no banco do plano de controle da implantação, ao lado do modelo.

Uma linha registra `tenant_id` (a organização), `user_id`, `role_id`, um hash SHA-256 do texto da consulta, o próprio texto (criptografado), `table_ids` e `table_names`, `source`, `status_code`, `duration_ms`, a rota e a contagem de linhas, o que foi aplicado à instrução e `logged_at`. O texto é criptografado com a chave da organização, se ela tiver definido uma, e com a da implantação caso contrário. Uma implantação sem provedor de criptografia o guarda sem criptografia. Em nenhum caso ele aparece nas tabelas de operação consultáveis, e só é descriptografado em leituras administrativas autorizadas.

O log aceita só acréscimos, no próprio banco: regras do PostgreSQL transformam `DELETE` e `UPDATE` em `query_audit_log` em operações sem efeito. Dois índices, `(tenant_id, logged_at)` e `(user_id, logged_at)`, deixam rápidas as consultas de conformidade por organização e por usuário dentro de um intervalo de tempo.

As linhas são mantidas para sempre. O Provisa não tem configuração de retenção e nunca expurga o log, e como `DELETE` não faz nada, apagar linhas antigas significa um operador excluir ou arquivar os dados da tabela fora do Provisa.

## Ocultação {: #redaction }

O que o Provisa registra sobre o próprio trabalho passa por uma lista de regras de ocultação antes de ser escrito em qualquer lugar. O texto que uma regra reconhece é substituído por `[REDACTED:<rule>]`, que nomeia a regra correspondente.

A lista tem quatro partes:

- **Padrões integrados.** Números de cartão que passam na verificação de Luhn, números de Seguro Social dos EUA, tokens bearer, tokens de acesso do Provisa, chaves da AWS, GCP, Azure, GitHub e Slack, e chaves privadas. São um conjunto inicial, não uma garantia.
- **As regras da implantação**, que valem em todas as organizações. Alterá-las exige o direito `platform_settings`.
- **As regras próprias da organização.** Alterá-las exige o direito `org_settings`.
- **A regra automática** sobre os segredos da organização: todo valor que o armazenamento do próprio Provisa guarda para a organização, todo `${secret:NAME}` que o modelo e as configurações da organização nomeiam (em qualquer backend de segredos), e cada valor `${secret:…}` ou `${user:…}` a partir da primeira resolução. Nenhuma regra precisa nomeá-los.

Uma regra é uma expressão regular nomeada ou uma referência `${secret:NAME}` / `${env:NAME}` nomeada, que reconhece o valor para o qual a referência se resolve. Uma referência que não se resolve é registrada em `ERROR` pelo nome e mostrada como não resolvida; as demais regras continuam valendo. Valores com menos de quatro caracteres não são reconhecidos.

O Provisa não tem como conhecer todos os formatos sensíveis. Os padrões integrados cobrem formas comuns; uma organização acrescenta padrões para os próprios formatos, como matrículas de funcionários, identificadores de conta ou nomes de host internos. A ocultação é feita na medida do possível: um valor escrito numa forma que nenhuma regra descreve passa sem alteração.

As regras ficam na página **Segredos da organização**, em **Regras de ocultação**. A página lista os padrões integrados, as regras da implantação (editáveis com `platform_settings`), as regras da organização, os segredos da regra automática apenas pelo nome e toda referência que não se resolve. As regras são carregadas quando o servidor inicia e quando o runtime de uma organização é construído, e recarregadas quando o modelo ou as configurações mudam e quando um segredo da organização é salvo ou removido.

A lista vale em cada saída que o Provisa escreve:

- cada span, como uma cópia ocultada entregue a cada exportador, e a visão de spans recentes;
- cada registro de log no pipeline de logs OpenTelemetry do Provisa, e cada linha que um handler de logging escreve, incluindo a saída padrão e os logs dos contêineres;
- o texto da consulta no log de auditoria, antes de ter o hash calculado e ser criptografado, de modo que nem o texto nem o hash revelam um valor reconhecido; e o detalhe da trilha administrativa;
- as mensagens de erro que um cliente recebe por HTTP, pgwire, gRPC, Arrow Flight e MCP.

A telemetria e os erros de cliente não estão ligados a uma única organização quando são escritos, por isso são ocultados pelas regras de todas as organizações em conjunto. Dois erros passam sem ocultação: um corpo de erro HTTP maior que 1 MiB e o erro de um stream do Arrow Flight que falha depois de começar a enviar.

Uma fonte federada mantém os próprios logs, que o Provisa não escreve e não alcança. Os registros que uma fonte guarda das instruções que o Provisa lhe envia cabe a essa fonte proteger.

## Estatística pelo MCP {: #statistics-over-mcp }

As ferramentas de estatística do MCP (`describe`, `correlate`, `regress`, `compare`, `forecast`) executam o SQL que um agente define pelo pipeline governado com o papel do agente, então as regras de linha, o mascaramento e a ocultação se aplicam ao que analisam. As ferramentas de estatística devolvem apenas resultados, então as linhas por trás de uma análise nunca entram no contexto do modelo: minimização de dados por padrão.

É o controle de acesso que limita o que um agente pode saber. Para manter identidades fora do alcance de um agente, conceda ao papel dele apenas as colunas que não identificam ninguém; um papel que pode ler uma coluna pode agregá-la, e agregados sobre grupos pequenos ainda podem identificar pessoas.

## Cobertura de colunas sensíveis {: #sensitive-column-coverage }

A view `sensitive_coverage` no domínio meta lista cada coluna sensível da organização com o valor fictício e a regra sintética que declara e um status: `covered`, `no_fake`, ou `inadequate` quando uma das verificações de adequação do Provisa falhou. As verificações procuram um valor fictício que deixa os valores reais inalterados, um valor fictício cujo tipo não corresponde ao que a coluna contém, e um valor fictício ou uma regra que lê o valor real de outra coluna sensível. Um data steward vê as colunas sem valor fictício no detalhe de cada ambiente, e um ambiente Test (fake) se recusa a servir dados até que cada coluna sensível tenha um. As verificações e a view são descritas em [Construir ambientes de teste](test-data.md#coverage-and-adequacy-checks).

## Limitação de taxa {: #rate-limiting }

Você define limites por papel em `provisa.yaml`: requisições por segundo, assinaturas SSE simultâneas e fluxos Arrow Flight simultâneos. Eles são verificados na API, antes de qualquer compilação ou execução, e uma requisição acima do limite recebe HTTP 429 com um cabeçalho `Retry-After`.

Consultas em linguagem natural (`POST /query/nl`) têm um limite próprio, `nl.rate_limit`, em requisições por minuto por papel. Uma requisição acima dele é recusada antes de qualquer chamada ao LLM.

As contagens ficam no Redis (`cache.redis_url`) como janelas deslizantes, sem nada guardado por instância, então um limite vale não importa quantas instâncias do Provisa você rode.

## Limite de complexidade de consulta {: #query-complexity-limit }

Um limite de taxa limita quantas requisições uma função envia. O limite de complexidade limita o que uma instrução pode pedir.

Toda instrução tem uma pontuação de complexidade: as relações que lê, seus joins, as colunas que seleciona (um `*` conta como as colunas que representa) e as consultas aninhadas nela. Uma relação lida de uma fonte de API remota (OpenAPI, GraphQL remoto, gRPC remoto) conta 10, porque cada leitura gasta um orçamento que o sistema remoto define para todos que usam essa fonte.

O limite é `max_query_complexity` no `rate_limit` de uma função. A configuração de toda a organização `limits.max_query_complexity` é um teto para cada função: uma função pode definir um limite menor, e um maior não tem efeito. Nenhum dos dois é definido por padrão.

```yaml
roles:
  - id: analyst
    rate_limit:
      max_query_complexity: 200
```
A pontuação é medida depois que uma requisição é analisada e antes de ser governada ou executada, então um mesmo limite vale em toda interface por onde uma instrução pode chegar: GraphQL, SQL sobre HTTP, pgwire, Arrow Flight, gRPC, Cypher, JSON:API e MCP. Uma instrução acima do limite é recusada com sua pontuação, o limite e o que pediu. As interfaces HTTP respondem 413. A recusa é registrada como uma negação de política.

## Autenticação {: #authentication }

Provedores de autenticação plugáveis:

| Provedor | Credencial | Caso de uso |
| ---------- | ----------- | ---------- |
| `none` | Cabeçalho X-Provisa-Role | Somente desenvolvimento |
| `basic` | Contas locais (bcrypt) + JWT de sessão | Implantações autônomas |
| `firebase` | Token de ID do Firebase | Produção |
| `keycloak` | JWT do Keycloak | Empresa |
| `oauth` / `oidc` | JWT OIDC | PingFed, Okta, Azure AD, Auth0 |
| `ldap` | Nome de usuário e senha do diretório (LDAP ou Active Directory) | Diretórios corporativos |
| `saml` | Asserção assinada SAML 2.0, depois um JWT de sessão | Single sign-on corporativo |
| `simple` | bcrypt + JWT | Testes |

O `ldap` autentica um usuário por search-then-bind: o Provisa se conecta como uma conta de serviço, encontra a única entrada que o nome de usuário indica, depois se conecta como essa entrada com a senha apresentada, de modo que o diretório a verifica. Os grupos do usuário se tornam suas funções. A conta e seus grupos são lidos de novo toda vez que um token de sessão retorna, então um usuário removido do diretório, ou de um grupo, perde esse acesso antes de o token expirar. `ldaps://` e StartTLS são ambos suportados.

O `saml` é o Web Browser SSO iniciado pelo SP. A assinatura XML da resposta é verificada contra os metadados do provedor de identidade, e os claims são lidos apenas do elemento assinado. Issuer, Audience, a URL do ACS, a expiração e `InResponseTo` são todos verificados. Respostas não solicitadas (iniciadas pelo IdP), asserções cifradas, documentos com DOCTYPE, múltiplas asserções e assinaturas SHA-1 são recusados. Uma sessão SAML não é verificada de novo com o provedor de identidade: um usuário removido ali mantém o acesso até o token de sessão expirar.

Mapeamento de funções: claims de identidade → função do Provisa via regras configuráveis. O campo `assignments_source` controla de onde vêm as atribuições de funções: `claims` as lê dos claims do token JWT (padrão), `provisa` as lê do armazenamento interno de atribuições do Provisa. Uma implantação multi-tenant (`multitenancy: true`) concede as funções ela mesma, por organização, então precisa usar `assignments_source: provisa`: com `claims` ela não inicia, e uma mudança de configuração que produziria essa combinação é recusada. Uma implantação single-tenant pode usar qualquer uma.

Um superusuário configurado em `provisa.yaml` (nome de usuário mais uma senha de um segredo de ambiente) sempre recebe a função admin e todas as capacidades, seja qual for o provedor configurado — um caminho de bootstrap para a configuração inicial.

### Superfícies e credenciais {: #surfaces-and-credentials }

Toda superfície autentica quem chama pelo mesmo provedor, então uma credencial que funciona em uma funciona em todas, onde quer que o protocolo tenha lugar para levá-la.

| Superfície | Senha | Token do provedor | Token de acesso pessoal | Certificado de cliente (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | via proxy de terminação |
| pgwire | campo de senha (texto puro ou SCRAM) | campo de senha, implantações OIDC | campo de senha | sim |
| Bolt | esquema `basic` | esquema `bearer` | esquema `bearer` | sim |
| Arrow Flight | — | `token` no handshake ou no payload do ticket; cabeçalho de chamada `authorization: Bearer` em `list_flights`, `get_flight_info` e `get_schema` | o mesmo | sim |
| gRPC | — | metadados `authorization` | metadados `authorization` | sim |
| Airport (extensão `airport` do DuckDB, opcional) | — | `authorization: Bearer` (o `auth_token` do segredo) | o mesmo | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | via proxy de terminação |

A extensão `airport` do DuckDB só consegue enviar o `auth_token` bearer do seu segredo. Um cliente DuckDB coloca ali um token do provedor ou um token de acesso pessoal e age como o papel para o qual sua identidade é resolvida. Qualquer cliente que consiga definir cabeçalhos de chamada pode pedir um dos seus papéis, ou um conjunto separado por vírgulas, em `x-provisa-role`.

Toda superfície diz que tipo de recusa você recebeu. Uma credencial ausente, rejeitada ou inutilizável é uma recusa de autenticação: HTTP 401, gRPC `UNAUTHENTICATED`, Arrow Flight e Airport `Unauthenticated`. Uma credencial válida que pede para agir como um papel que sua identidade não tem é uma recusa de autorização, e nomeia o papel: HTTP 403, gRPC `PERMISSION_DENIED`, Arrow Flight e Airport `Unauthorized`. Uma exceção vem da biblioteca cliente do Arrow, não do servidor. Quando o `get_schema` do Flight é recusado, um cliente Python vê um erro genérico do Arrow que traz o motivo do servidor mas não o tipo; `list_flights`, `get_flight_info` e `do_get` entregam o tipo.

Um `—` na tabela significa que o protocolo não tem campo de nome de usuário para parear com uma senha, e as formas de token cobrem o caso. O pgwire é o caso oposto. Seu pacote de inicialização tem um único campo secreto e nenhum esquema, então o próprio segredo decide o método: um token de acesso pessoal é reconhecido pelo prefixo, o segredo é lido como token bearer quando o provedor configurado emite tokens, e todo o resto é uma senha. Essa escolha é feita uma vez, e uma credencial recusada pelo validador escolhido não é tentada em outro.

### Tokens de acesso pessoal {: #personal-access-tokens }

Um token de acesso pessoal é um segredo bearer de longa duração que você cria para um cliente que não consegue fazer login interativo: um script, uma ferramenta de BI, um driver. Ele traz a própria organização e o próprio papel, e toda superfície o verifica com o mesmo validador, então nenhuma precisa saber o que ele é.

Na rede, ele é `provisa_pat_` seguido de 43 caracteres base64 seguros para URL. O prefixo é o que manda um segredo apresentado para o armazenamento de tokens em vez do provedor de identidade, e deixa um token vazado fácil de achar em logs e repositórios.

Só o SHA-256 do segredo é guardado. Você vê o segredo uma vez, ao criá-lo, e depois ele não pode ser recuperado; a lista dos seus tokens mostra um prefixo de exibição e datas, nunca uma credencial que funcione. Você cria, lista e revoga os próprios tokens com `POST /auth/tokens`, `GET /auth/tokens` e `DELETE /auth/tokens/{token_hash}`, ou na seção de tokens do seu perfil na interface de administração.

Um token válido é resolvido para a conta do dono, com id de usuário, e-mail e nome de exibição, de modo que uma linha de auditoria ou um relatório de uso gravado com um token nomeia a pessoa, não a credencial. Qual dos tokens dela agiu fica registrado à parte, em `raw_claims["token_name"]`. Um token pode ter validade, depois da qual é recusado, e remover a associação de um usuário revoga também os tokens dele.

### SCRAM-SHA-256 no pgwire {: #scram-sha-256-on-pgwire }

Com o provedor `basic`, definir `auth.scram: true` faz o pgwire anunciar SASL (código de autenticação 10) com o mecanismo `SCRAM-SHA-256`, para que uma senha seja provada em vez de enviada. O channel binding (`SCRAM-SHA-256-PLUS`) não é oferecido.

O SCRAM precisa de um verificador RFC 5802, que não pode ser derivado de um hash bcrypt. Um verificador é gravado sempre que uma senha passa em texto puro — cadastro, login, troca de senha, redefinição por administrador — então uma implantação que ativa o SCRAM coleta verificadores à medida que seus usuários se autenticam, e a primeira conexão SCRAM de cada usuário vem depois da sua próxima digitação de senha. Um usuário ainda sem verificador recebe uma troca simulada indistinguível de uma real, para que a rede não revele quem já migrou.

### TLS mútuo {: #mutual-tls }

A verificação do certificado do cliente move a primeira checagem para o handshake TLS: um chamador sem um certificado assinado pela CA da implantação nunca chega à camada de credenciais. Está disponível no pgwire, Bolt, gRPC e Arrow Flight — os quatro transportes que terminam o próprio TLS.

| Variável | Significado |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | Pacote PEM das CAs autorizadas a assinar certificados de cliente |
| `PROVISA_MTLS_MODE` | `required` (o padrão quando uma CA é definida) ou `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Se verdadeiro, o common name do certificado deve ser igual ao nome de usuário com que a conexão então se autentica |

As substituições por protocolo seguem a mesma nomenclatura das configurações de TLS. Nada é deduzido: um modo definido sem CA se recusa a iniciar, e um modo não reconhecido se recusa a iniciar em vez de ser lido como o vizinho mais seguro — uma implantação que acredita exigir certificados de cliente e não exige está em situação pior que uma que não inicia.

### Versões do TLS {: #tls-versions }

Todo listener que o Provisa termina (a API HTTP e a UI, pgwire, Bolt, MCP, gRPC e Arrow Flight) aceita TLS 1.2 e TLS 1.3 e recusa TLS 1.0 e 1.1. O Provisa não define versão de protocolo nem lista de cifras próprias. Os listeners HTTP, pgwire e Bolt usam os padrões de servidor do módulo `ssl` do Python (mínimo TLS 1.2). gRPC e Arrow Flight usam os padrões do gRPC (mínimo TLS 1.2). As versões não são configuráveis.

### Limitação de logins {: #login-throttling }

Adivinhar senhas não depende do protocolo: a mesma conta pode ser atacada por HTTP, pgwire e Bolt. O contador fica, portanto, na camada de validação de credenciais, não em uma superfície específica, de modo que um bloqueio obtido em qualquer lugar é aplicado em todo lugar.

Ela fica ligada por padrão — cinco falhas em cinco minutos bloqueiam o sujeito por quinze minutos — e é ajustada em `auth.login_throttle`. Um sujeito bloqueado é recusado antes mesmo de a credencial ser examinada, e uma autenticação bem-sucedida apaga o histórico desse sujeito.

A chave é o principal que o protocolo traz. Uma superfície só de bearer não traz principal, então a chave é um digest da própria credencial; o que isso impede é que um token ruim seja reapresentado sem limite. O armazenamento é por processo, então uma implantação com vários workers de API permite até `max_attempts` por worker — a limitação freia as tentativas, não é uma cota distribuída.

### Sondagem de papéis {: #role-probing }

Nomear um papel que o autor da chamada não detém é tratado como um evento de segurança detectado de gravidade alta, em todas as superfícies: a API HTTP, MCP, pgwire, Bolt, Arrow Flight, o serviço Flight do airport e gRPC. Um cliente legítimo nomeia os papéis que lhe foram dados; um que nomeia outro está com defeito ou sondando.

A solicitação é recusada. O autor da chamada recebe apenas "not permitted", nada sobre a existência do papel, e a conexão ou sessão termina onde houver uma: a conexão pgwire, a sessão Bolt, a chamada gRPC, a sessão MCP. A exceção é a API HTTP com qualquer credencial que não seja um token de acesso pessoal: ali o navegador apresenta sua sessão ou o token do seu provedor de identidade, por isso a recusa continua nomeando o papel.

O restante depende da credencial e do transporte:

| Credencial | Transporte | Suspenso |
|---|---|---|
| O token do visitante de um sandbox de demonstração | a API HTTP | nada |
| Token de acesso pessoal | qualquer | o token |
| Token assinado por um provedor de identidade externo | MCP, pgwire, Bolt, Flight, gRPC | o acesso por token do principal à organização |
| Token assinado por um provedor de identidade externo | a API HTTP | nada |
| Uma sessão ou senha que esta implantação verifica | qualquer | nada |
| Uma sessão MCP local (sem bearer) | MCP | nada; o evento vai para o log do operador |

A API HTTP é a exceção porque o navegador apresenta o mesmo token bearer que um cliente de API, de modo que o servidor não consegue distingui-los, e uma aba que ainda nomeia um papel que o usuário perdeu (depois que um administrador mudou seus papéis, ou após uma troca de organização) é rotineira. MCP e os demais transportes nunca são uma sessão de navegador. Lá o evento é recusado, registrado e notificado como qualquer outro; apenas a suspensão é omitida. O token do visitante de um sandbox de demonstração é um token de acesso pessoal, mas é a única credencial da aba do navegador do visitante; por isso, na API HTTP é tratado como uma sessão: recusado e registrado, nunca suspenso.

Um token suspenso é recusado como um revogado. Um principal cujo acesso por token está suspenso é recusado em MCP, pgwire, Bolt, Flight e gRPC nessa organização, e ainda pode entrar na interface. Somente um administrador levanta uma suspensão; nada a levanta por tempo. Equipe → Acesso suspenso lista cada suspensão com o evento que a causou e a levanta (`GET /admin/orgs/{org}/suspensions`, `POST /admin/orgs/{org}/suspensions/lift`).

Cada evento é gravado no registro administrativo da organização (ação `security.role_probe`) com o principal, o identificador da credencial, o endereço do cliente, o transporte, o endpoint ou a ferramenta, o papel nomeado, os papéis detidos, a hora e os identificadores das instruções auditadas mais recentes do principal. O texto delas permanece no log de auditoria de consultas e é lido com o direito `view_governance`.

Os administradores da organização recebem um e-mail onde a implantação envia e-mail. O evento também é enviado como JSON ao webhook de segurança do operador quando `security.webhook_url` está definido. O que sai da organização nomeia o principal, o identificador da credencial, o transporte, o endpoint, o papel nomeado, a hora e a gravidade, com um link para a visão administrativa; não contém nenhuma instrução nem nenhum papel detido. Um aviso que não pode ser entregue é registrado em log e nunca desfaz a recusa nem a suspensão.

### Nomear a organização {: #naming-the-org }

Em multi-tenant, toda requisição nomeia a organização a que se destina. Pertencer a exatamente uma organização não a nomeia: uma requisição que não nomeia nenhuma é recusada, e a recusa diz o que enviar. Cada superfície nomeia uma organização assim:

| Superfície | Como um cliente nomeia a organização |
| ------- | -------------------------- |
| HTTP (interface, REST, GraphQL) | O nome de host próprio da organização, `acme.provisa.dev`, ou o cabeçalho `X-Org-Provisa` no host do plano de controle |
| pgwire | O nome do banco de dados (`psql -d acme`, o campo de banco de dados de uma ferramenta de BI), ou o nome de host próprio da organização discado via TLS |
| Bolt | O nome do banco de dados, `<org>.provisa_<role>` (ou `<org>.provisa_ops_<role>`), ou o nome de host próprio da organização discado via TLS |
| Arrow Flight | `"org"` no ticket |
| gRPC | O cabeçalho de metadados `x-provisa-org` |
| MCP (transporte HTTP) | O nome de host próprio da organização, ou o cabeçalho `X-Org-Provisa` |
| Cliente Python | `org=` em `ProvisaClient`, `connect()` e `adbc_connect()`; `?org=` em uma URL do SQLAlchemy |

Um cliente pgwire que disca um nome de host via TLS o envia no ClientHello do TLS, e o Provisa lê a organização dali. Quando o nome de host e o nome do banco de dados nomeiam ambos uma organização e diferem, a conexão é recusada; nenhum dos dois tem preferência. O nome de banco de dados `provisa`, o padrão, não nomeia nenhuma organização, e uma ferramenta de BI conectada a uma organização mostra
essa organização como seu banco de dados. No Bolt, `SHOW DATABASES` lista um `<org>.provisa_<role>` por organização a que
o usuário pertence; um `provisa_<role>` sozinho é recusado em multi-tenant, e uma sessão permanece na
organização que nomeou primeiro.

Um nome é um pedido, não uma concessão. Ele chega em todo lugar ao mesmo resolvedor, que recusa uma organização da qual o principal autenticado não é membro nem tem o direito entre organizações.

Um token de acesso pessoal é emitido para uma organização e a nomeia ele mesmo. Ele abre só essa organização: uma requisição que nomeia outra é recusada, mesmo quando o dono do token pertence às duas.

Uma implantação single-tenant não tem organização a nomear. Nenhuma superfície lê ali um nome de host, um nome de banco de dados ou um cabeçalho para isso.

## Segredos entre organizações {: #secrets-across-organizations }

Os segredos de uma organização são lidos só por essa organização. No armazenamento próprio do Provisa cada segredo fica sob a sua organização. Em um serviço central (Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) de uma implantação de muitas organizações, toda leitura é feita dentro do prefixo da organização que lê, e uma referência que sairia dele é recusada. Os prefixos e os passos para mover os segredos existentes para debaixo deles estão em [Segredos](secrets.md#one-organizations-secrets-in-a-central-service).

## Telemetria entre organizações {: #telemetry-across-organizations }

Numa implantação compartilhada por muitas organizações, os sinais operacionais (`"ops"."traces"` e, no motor Trino, as métricas e os logs ao lado) são um único armazenamento onde chega o trabalho de todas as organizações. Cada span registra a organização para a qual foi iniciado, guardada como `tenant_id`, e toda leitura dos sinais responde apenas as linhas da organização que lê, seja qual for o papel. Um span iniciado fora do trabalho de qualquer organização (a inicialização do servidor, por exemplo) não pertence a nenhuma e nenhuma o lê. O administrador da plataforma não tem acesso a dados em nenhuma organização e não lê nenhuma pelo Provisa.

Os sinais operacionais são mantidos indefinidamente nos dois motores, como o log de auditoria: a compactação reescreve os arquivos em que estão guardados e não remove nenhuma de suas linhas. No Trino eles são as tabelas Iceberg do catálogo `otel`. Num motor nativo (DuckDB) o servidor grava os próprios registros de trace como arquivos Parquet na sua pasta de telemetria e o motor os lê com o mesmo nome, `"ops"."traces"`, com a visão de linhagem de atualizações `"ops"."mv_refresh_lineage"` sobre eles; ambos são lidos pela mesma regra por organização.

Os sinais são mantidos em claro no seu armazenamento. Uma carga de trabalho que precise de mais do que um limite por organização dentro de uma implantação roda numa implantação dedicada; uma muito sensível ou regulada é auto-hospedada.

## Trabalho em segundo plano entre organizações {: #background-work-across-organizations }

Um processo que atende muitas organizações executa o trabalho em segundo plano de cada uma como essa organização. O processamento de mudanças de cada organização (o loop de eventos que grava as mudanças das fontes e atualiza as visões sobre elas), suas construções de réplicas e os gatilhos de tabela que disparam pertencem ao seu próprio runtime: uma mudança é processada pela tarefa da sua própria organização, lê sua fonte com o endereço e a credencial dessa organização e grava no armazenamento dessa organização. Cada ambiente de uma organização tem seu próprio slot de replicação lógica do PostgreSQL e sua publicação, nomeados pela organização, pelo ambiente e pela fonte, de modo que duas organizações que dão o mesmo nome a uma fonte num mesmo banco leem cada uma as próprias mudanças.

## Residência de dados {: #data-residency }

Numa implantação com regiões, uma tabela pode nomear uma região como sua casa, e seus dados pessoais são armazenados e processados lá.

- Só a região de casa constrói e atualiza a réplica da tabela. Qualquer outra região lê essa réplica no lugar, com as regras de linha e coluna do leitor aplicadas na região de casa, de modo que só sai dela o que o leitor pode ver.
- Uma região de casa cuja réplica não foi construída, ou cujos armazenamentos não podem ser alcançados, recusa a leitura (`query.home_region_unavailable`). A fonte nunca é lida ao vivo de outra região no lugar.
- Um rascunho só é pré-visualizado onde vive. Uma tabela em rascunho que nomeia uma região é lida para a pré-visualização do seu responsável só nos nós dessa região e recusada em outros lugares; a cópia que a pré-visualização grava é mantida lá para a sua liberação. Uma cópia que qualquer região tenha de uma tabela, rascunho ou publicada, cuja casa agora é outra região é retirada e depois removida.
- Definir, mudar ou remover a região de uma tabela exige o direito `data_residency` para as regiões envolvidas, além de ser dono da tabela pelo seu domínio. Uma mudança pode passar pelo rascunho, de modo que a tabela não é servida em lugar nenhum no intervalo.

Regras de linha e coluna escritas sobre a região do nó que responde (`current_setting('provisa.region')`) e sobre o atributo `region` dos membros decidem o que atravessa entre regiões. [Regiões](regions.md) descreve por completo regiões, a atribuição de casa e essas regras.

## Fortalecimento da implantação {: #deployment-hardening }

### Credenciais dos stores e onde eles escutam {: #store-credentials-and-where-the-stores-listen }

Os stores incluídos não têm credenciais padrão. O control plane PostgreSQL, o Redis, o MinIO, a conta de administrador do Grafana e o segredo compartilhado do cluster Trino recebem cada um um valor gerado para a implantação:

- Os launchers (`provisa start`, os instaladores de desktop e de servidor) os geram uma vez e os mantêm em `~/.provisa/store-credentials.env` (modo 600). Um valor já presente no ambiente é usado como está, e um nó secundário, que usa os stores do primário, se recusa a iniciar sem os do primário.
- Os módulos Terraform os geram com `random_password` e dão os mesmos valores a cada nó. Eles ficam no estado do Terraform, então proteja o estado como protegeria as senhas.
- O chart Helm não define nenhum. Ele exige um Secret que você cria, nomeado por `credentials.existingSecret`, contendo `pg-password`, `redis-password`, `minio-access-key`, `minio-secret-key`, `grafana-admin-password` e `trino-shared-secret`.

O Redis exige a sua senha, e a senha do papel PostgreSQL é definida de novo a cada início, de modo que um volume criado antes acompanhe a senha atual da implantação. Os stores, o motor de federação e os serviços de telemetria publicam suas portas em um só endereço, `PROVISA_STORE_BIND_IP`: `127.0.0.1` em um nó isolado, ou o endereço privado do primário quando secundários se conectam a ele. A conta de administrador do Grafana usa a senha gerada; a stack compose deixa visitantes anônimos verem os painéis, e o chart Helm não admite visitante anônimo.

### Criptografia de transporte {: #transport-encryption }

Todo listener que o Provisa termina serve TLS (veja [Versões do TLS](#tls-versions)). Um nó servidor usa o certificado em `PROVISA_TLS_CERT`/`PROVISA_TLS_KEY`, ou gera um autoassinado. O chart Helm serve TLS por padrão (`tls.enabled: true`) a partir do Secret `kubernetes.io/tls` que `tls.secretName` nomeia.

### O PostgreSQL embarcado {: #the-embedded-postgresql }

As camadas desktop e nativa executam o PostgreSQL embarcado. No macOS e no Linux ele escuta em um unix socket dentro do seu diretório de dados (modo 700), que só a conta dona do diretório alcança. Por TCP (Windows, onde escuta em 127.0.0.1), toda conexão deve fornecer uma senha gerada para o diretório de dados e mantida nele (`provisa.password`, modo 600). Ele nunca é iniciado como root.

### O motor de federação (Trino) {: #the-federation-engine-trino }

- **Entre nós.** Um coordenador e seus workers se autenticam mutuamente com o segredo compartilhado gerado da implantação (`internal-communication.shared-secret`). Criptografar o tráfego entre eles é uma opção do operador, desativada por padrão: desativada, o tráfego fica dentro da rede privada ou do cluster da implantação. Ative-a onde um requisito de conformidade ou de zero-trust peça criptografia dentro da implantação. O Trino documenta que ela torna mais lentas as consultas com muito shuffle (joins distribuídos, agregações, funções de janela) de 10% a mais de 100%. Em uma implantação compose é a configuração `engine.trino_internal_tls` (Configurações da implantação → Engine), com a rede do motor e o endereço fixado do coordenador ao lado, e vale a partir do próximo início. Com o chart Helm é o valor `trino.internalTls`; o chart gera a CA e o certificado que os nós usam, mantém-nos entre atualizações e endereça cada nó pelo nome DNS do seu pod. Os motores da plataforma hospedada são coordenadores de nó único, sem tráfego entre nós, e a recusam.
- **Dados em spool.** A execução tolerante a falhas criptografa os dados de exchange que grava em spool com uma chave gerada para cada consulta e descartada quando ela termina (`fault-tolerant-execution.exchange-encryption-enabled=true`, definido explicitamente). O spill legado em disco não é usado.
- **Organizações em um só motor.** Os catálogos de cada organização são dela, nomeados com a organização, e as instruções de cada organização são executadas em seu próprio resource group.
- **Os catálogos, e as credenciais neles, vivem só na memória do motor.** O Provisa cria cada catálogo do Trino em tempo de execução, e cada um guarda as propriedades de conexão da sua fonte, senhas incluídas. Todo motor que o Provisa executa os mantém em memória (`catalog.store=memory`): nenhum catálogo é gravado no disco do motor, e nenhum é montado a partir de um arquivo. Um coordenador que reinicia volta sem catálogos; cada processo do Provisa percebe na sua próxima consulta, e o observador do motor em até um minuto, e recria os catálogos que serve. Uma fonte Kafka com credenciais SASL precisa de um arquivo: o conector Kafka do Trino lê as configurações de cliente apenas de um arquivo, então o Provisa as grava, senha incluída, em `kafka-client/<catalog>.properties`. O arquivo é legível só pelo usuário do Trino (modo 0600 num diretório 0700). Na pilha de contêineres o diretório é um volume em memória compartilhado pela API e pelo motor. Nos motores da plataforma hospedada é o próprio Secret do Kubernetes do motor, mantido na memória do nó e legível só pelo grupo do Trino (modo 0440). De qualquer forma, o arquivo nunca chega a um disco. Nos motores hospedados uma fonte Kafka deve nomear um registro de esquemas: uma cujas colunas dos tópicos só o modelo declara é recusada pelo nome ao ser salva ou aplicada, pois suas descrições de tabela exigiriam um diretório que não pode ser dado ao motor em execução. Iniciar a pilha remove os arquivos de catálogo e de cliente Kafka que versões anteriores deixaram em disco.

### Fontes em uma implantação compartilhada {: #sources-on-a-shared-deployment }

Em uma implantação que abriga muitas organizações, as fontes de uma organização só podem alcançar endereços públicos: nunca a rede privada da plataforma, onde estão seu control plane, cache e armazenamento de objetos. A verificação se aplica quando uma fonte é salva, pela UI, pela API de administração ou por uma aplicação de configuração, e de novo a cada conexão e a cada redirecionamento HTTP. As fontes de um sandbox de demonstração são ainda presas ao endereço em que foram verificadas. A organização da própria implantação, cujas fontes o operador configura, não é restringida.

## Criptografia em repouso e as chaves da própria organização {: #encryption-at-rest-and-the-organizations-own-keys }

A criptografia sob uma chave da própria organização está disponível e fica desativada até que a organização a ative. Nada é criptografado sob uma chave da organização a menos que um administrador da organização crie uma.

**A chave de criptografia da organização** envolve os segredos da organização, suas colunas criptografadas e o texto de suas consultas de auditoria. Um administrador da organização a cria em **Admin → Org Secrets → Org Key**, ou com `PUT /admin/org-encryption`: sem corpo o Provisa gera a chave, que nunca é exibida, e um corpo `key_b64` fornece 32 bytes seus. `GET /admin/org-encryption` informa só a sua impressão digital. Sem uma chave da organização, os dados da organização são envolvidos pela chave da implantação (`encryption.provider` em `provisa.yaml`). Uma implantação que não configura nenhum provedor de criptografia guarda colunas criptografadas e texto de consultas de auditoria sem criptografia; segredos nunca são guardados em texto claro (veja [Segredos](secrets.md#provisas-built-in-store)).

**A pasta da organização** guarda seus arquivos enviados e aterrissados, os arquivos de spill do seu motor e seus arquivos temporários. Um administrador da organização ativa a criptografia dela em **Admin → Org Secrets → File folder encryption**, ou com `PUT /admin/org-files` (`{"encrypted": true, "key_reference": "${secret:NAME}"}`). `POST /admin/org-files/generate-key` responde uma só vez com uma nova chave de 256 bits, para ser guardada no cofre. A chave fica no cofre da própria organização (AWS Secrets Manager, GCP Secret Manager, Azure Key Vault ou HashiCorp Vault): o Provisa guarda só a referência, e o gocryptfs mantém os arquivos da pasta criptografados em disco sob a chave. Isso exige FUSE, portanto um host Linux ou contêiner com `/dev/fuse`; no macOS a configuração é recusada.

Em uma implantação auto-hospedada, que normalmente executa uma organização em motores próprios, ativar as duas criptografa os segredos, arquivos, arquivos de spill e arquivos temporários dessa organização sob as suas chaves:

- O motor DuckDB que uma organização executa faz spill na pasta da organização.
- As linhas que um row filter busca e o código enviado são preparados (staged) na pasta da organização.
- A execução tolerante a falhas do Trino criptografa os dados de exchange em spool com uma chave gerada para cada consulta e descartada quando ela termina (`fault-tolerant-execution.exchange-encryption-enabled=true`). O Provisa não ativa o spill legado do Trino em disco.
- O Trino mantém os catálogos da organização, com suas credenciais, em memória e não grava nenhum em disco (`catalog.store=memory`); as configurações de cliente SASL de uma fonte Kafka ficam num arquivo que só o usuário do Trino lê, em memória na pilha de contêineres (veja [O motor de federação](#the-federation-engine-trino)).
- Um motor de federação PostgreSQL grava seus arquivos temporários onde aquele servidor PostgreSQL os guarda, sob a criptografia de armazenamento do próprio servidor, não na pasta da organização.

### Plataformas hospedadas compartilhadas e dedicadas {: #shared-and-dedicated-hosted-platforms }

A plataforma hospedada compartilhada isola as organizações logicamente. Uma implantação atende a todas, e os dados de cada organização continuam seus por meio da governança, de seus próprios schemas, catálogos e resource groups no motor, de suas chaves, de chaves por consulta para dados de motor em spool e da recusa de suas fontes nos endereços privados da plataforma. É recomendada para experimentação e pequenas empresas. Uma plataforma hospedada dedicada é fisicamente separada: máquinas, rede e stores próprios, atendendo a um só cliente. Para cargas de trabalho muito sensíveis ou reguladas, auto-hospede o Provisa dentro do seu próprio perímetro, onde seus próprios operadores detêm cada chave e cada host. As plataformas hospedadas não têm um relatório SOC 2. Se você precisa de SOC 2, recomendamos a auto-hospedagem.

### Python no navegador {: #python-in-the-browser }

As abas Python e Notebook do Explore executam Python no navegador do visitante. Cada leitura que fazem vai ao servidor como o usuário conectado, com seu papel ativo, sua organização e seu ambiente, e é governada exatamente como a mesma instrução de qualquer outro cliente.

## Modo de alta segurança {: #high-security-mode }

`security.mode: high` em `provisa.yaml` afirma uma garantia: o backend do Provisa nunca manipula dados em texto puro. Toda coluna que importa é cifrada na fonte, e só um cliente que tem a chave de decifração pode lê-la. Essa garantia tem consequências que uma implantação precisa planejar.

**O que o modo faz:**

- **Os endpoints de dados exigem prova de decifração no cliente.** Tudo sob `/data/` retorna 403 a menos que o chamador apresente o cabeçalho `X-Provisa-KMS-Key` — a marca de um cliente JDBC ou Python configurado para decifrar localmente. Um navegador ou um consumidor REST em texto puro não traz essa chave e é recusado. O controle é uma negação por padrão sobre a árvore inteira: uma rota adicionada amanhã fica protegida no dia em que é lançada, e uma exceção precisa ser justificada.
- **Os endpoints de metadados de esquema continuam abertos.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` e `/data/compile` não retornam dados de linha, e um cliente precisa ler o esquema — incluindo quais campos são `@encrypted` — antes de poder se conectar.
- **gRPC e Arrow Flight continuam servindo, com a mesma prova.** São os transportes que os clientes que cifram realmente usam; fechá-los deixaria uma implantação de alta segurança sem protocolo de transmissão. Uma chamada de dados em qualquer um deles precisa levar a mesma chave KMS como metadados de chamada.
- **pgwire, Bolt e MCP não iniciam.** Nenhum dos três tem um handshake por conexão capaz de levar um contexto de decifração: um conjunto de linhas do pgwire e um resultado Cypher trafegam em texto puro, e uma chamada de ferramenta MCP entrega seus resultados a um modelo como texto. Uma porta configurada para qualquer um deles é recusada na inicialização em vez de servida.
- **A proteção de relacionamentos não pode ser contornada.** `ignore_relationships` e `relationship_guard: false` são ambos ignorados; veja [Governança de relacionamentos](#relationship-governance-v002).

**Verificar se uma implantação está no modo:** o log de inicialização o nomeia, uma requisição `/data/sql` sem chave KMS responde 403 com uma mensagem que nomeia o modo de alta segurança, e as portas do pgwire, Bolt e MCP não estão escutando.

## Hook de aprovação ABAC {: #abac-approval-hook }

Um hook de política externo opcional que dispara antes da execução da consulta. Quando configurado, o Provisa chama o seu motor de políticas com a identidade do usuário, as funções, as tabelas, as colunas e a operação. A resposta determina se a consulta prossegue. Um comando registrado com **Requer aprovação** é submetido ao mesmo hook antes de cada chamada; veja [Chamadas de comandos](#command-calls).

### Escopo {: #scoping }

O hook só dispara quando a consulta toca uma tabela ou fonte incluída — nenhum custo extra para todo o resto.

| Configuração | Efeito |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Toda consulta dispara o hook |
| `sources[].approval_hook: true` | Todas as tabelas dessa fonte disparam o hook |
| `tables[].approval_hook: true` | Essa tabela dispara o hook |

### Protocolos {: #protocols }

Três transportes são suportados:

| Tipo | Caso de uso | Campo de configuração |
| ------ | ---------- | ------------- |
| `webhook` | Qualquer serviço de política com HTTP (OPA, próprio) | `url` |
| `unix_socket` | OPA ou sidecar de política na mesma máquina | `socket_path` + `url` |
| `grpc` | Serviço de política co-localizado de alto throughput | `url` (host:port) |

O transporte gRPC usa o contrato `provisa.auth.ApprovalService` definido em `provisa/auth/approval.proto`. Implemente este serviço no seu motor de políticas:

```proto
service ApprovalService {
  rpc Evaluate (ApprovalRequest) returns (ApprovalResponse);
}

message ApprovalRequest {
  string user = 1;
  repeated string roles = 2;
  repeated string tables = 3;
  repeated string columns = 4;
  string operation = 5;
  map<string, string> session_vars = 6;
  string command = 7;         // a command call's name; empty for a query
  string arguments_json = 8;  // a command call's arguments as a JSON object
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```
O canal gRPC é persistente — um canal por instância do Provisa, reutilizado em todas as chamadas a esse endpoint do hook.

### Requisição / resposta {: #request-response }

Os três transportes levam o mesmo payload:

| Campo | Tipo | Descrição |
| ------- | ------ | ------------- |
| `user` | string | Identidade do usuário autenticado |
| `roles` | string[] | Funções do Provisa do usuário |
| `tables` | string[] | Ids das tabelas referenciadas na consulta |
| `columns` | string[] | Colunas selecionadas na consulta |
| `operation` | string | `"query"` ou `"mutation"`; `"command"` para uma chamada de comando |
| `command` | string | O nome do comando em uma chamada de comando; vazio em uma consulta |
| `arguments` | object | Os argumentos com que um comando é chamado; vazio em uma consulta |

Os transportes webhook e socket Unix trocam JSON, com `command` e `arguments` como chaves. No gRPC os argumentos viajam como o texto JSON `arguments_json`. A resposta precisa incluir `approved` (bool) e, opcionalmente, `reason` (string).

### Chamadas de comandos {: #command-calls }

Um comando com `requires_approval` definido (a chave **Requer aprovação** no formulário do comando) vai ao hook antes de cada chamada, em toda superfície. Ele só executa se o hook aprovar. A requisição leva `operation: "command"`, o nome do comando e seus argumentos; `tables` e `columns` ficam vazios.

```json
{
  "user": "analyst",
  "roles": ["analyst"],
  "tables": [],
  "columns": [],
  "operation": "command",
  "command": "create_issue",
  "arguments": {"input": {"title": "Crash on save"}}
}
```

Duas recusas, ambas 403:

- `functions.approval_unavailable`: nenhum hook está configurado. Diferente de uma consulta, um comando que precisa de aprovação nunca passa sem um.
- `functions.approval_denied`: o hook respondeu `approved: false`. O motivo está na mensagem.

### Timeout e fallback {: #timeout-and-fallback }

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```
Em caso de timeout ou erro de transporte, a política `fallback` se aplica. Um circuit breaker (padrão: abre após 5 falhas consecutivas, meio aberto após 30 s) evita falhas em cascata por um endpoint de hook lento.

### Exemplo de configuração {: #configuration-example }

```yaml
auth:
  approval_hook:
    type: webhook
    url: "http://opa.internal:8181/v1/data/provisa/allow"
    timeout_ms: 300
    fallback: deny

sources:
  - id: analytics_pg
    approval_hook: true   # all tables on this source require hook approval

tables:
  - id: salary_data
    approval_hook: true   # this table always requires hook approval
```
## Segredos {: #secrets }

As credenciais usam a sintaxe `${env:VAR_NAME}`, resolvida em tempo de execução. Senhas nunca são armazenadas no banco de dados de configuração.

Para o serviço completo de segredos — cofres, sintaxe de referência e provedores — veja [Segredos](secrets.md).
