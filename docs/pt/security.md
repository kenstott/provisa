# Modelo de segurança {: #security-model }

O Provisa aplica um modelo de segurança em várias camadas em toda linguagem de consulta (GraphQL, SQL, Cypher) e todo transporte (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) A governança é aplicada de forma uniforme — não existe caminho de consulta que a contorne. (REQ-002, REQ-266)

As camadas se aplicam em ordem. Uma requisição precisa passar por cada camada antes que a próxima seja avaliada.

## Modelo em camadas {: #layered-model }

### Camada 0 — Filtragem da introspecção {: #layer-0-introspection-filtering }

O esquema e o catálogo apresentados a uma função contêm apenas as tabelas da sua lista `domain_access` e as colunas que passam pelas regras `visible_to` de cada coluna. (REQ-039) Objetos fora do acesso de uma função são invisíveis no momento da descoberta — não podem ser consultados, autocompletados nem ter sua existência deduzida. (REQ-039) Isso vale para o esquema GraphQL, o catálogo SQL e o navegador de esquema do editor de consultas. (REQ-039, REQ-363)

Veja [Visibilidade do esquema](#schema-visibility).

### Camada 1 — Acesso público {: #layer-1-public-access }

Uma coluna publicada com o escopo `public` é servida a toda função da organização, inclusive funções cujo `domain_access` não alcança o domínio da tabela; uma função assim vê a tabela apenas com suas colunas públicas. Nada é público a menos que quem registra uma tabela diga isso. (REQ-1959) Veja [Colunas públicas](#public-columns).

### Camada 2 — Acesso a domínios {: #layer-2-domain-access }

Cada função traz uma lista `domain_access` de ids de domínio. Uma consulta que toca uma tabela fora desses domínios é recusada antes da execução. (REQ-038, REQ-039) Essa é a fronteira grossa de propriedade — uma função de RH não alcança tabelas financeiras, seja como for escrito o SQL. (REQ-002)

Veja [Modelo de direitos](#rights-model).

### Camada 3 — Segurança em nível de linha {: #layer-3-row-level-security }

Depois que o acesso ao domínio é confirmado, predicados `WHERE` por tabela e por função são injetados em cada `SELECT` no momento da execução. (REQ-041, REQ-263) Os predicados são avaliados sobre os dados brutos. Um gerente regional que consulta uma tabela de pedidos compartilhada vê apenas as linhas da sua região, mesmo com `SELECT *`. (REQ-264)

Veja [Segurança em nível de linha (RLS)](#row-level-security-rls).

### Camada 4 — Visibilidade e mascaramento de colunas {: #layer-4-column-visibility-and-masking }

Colunas com uma lista `visible_to` que exclui a função solicitante são removidas da saída da consulta. (REQ-040, REQ-263) Colunas com uma regra de mascaramento têm seus valores substituídos — supressão por regex, substituição por constante ou truncamento — antes que os resultados deixem o servidor. (REQ-263) O mascaramento se aplica em todas as linguagens de consulta e formatos de saída. (REQ-263)

Veja [Modelo de permissões de coluna](#column-permission-model) e [Mascaramento em nível de coluna](#column-level-masking).

### Camada 5 — Proteção de predicados {: #layer-5-predicate-guard }

Colunas mascaradas são recusadas em cláusulas `WHERE` e `HAVING`. (REQ-263) Sem isso, um chamador poderia deduzir o valor não mascarado buscando-o por bisseção em um filtro, mesmo com a saída mascarada. A recusa é aplicada na análise da consulta, antes da execução. (REQ-531)

### Governança de relacionamentos (V002) {: #relationship-governance-v002 }

Condições JOIN em SQL precisam corresponder a um relacionamento registrado e aprovado entre tabelas. (REQ-001) Joins não aprovados são recusados. Cada relacionamento traz um motivo e uma descrição legíveis — uma orientação para usuários e agentes autônomos sobre por que um caminho de travessia existe. Isso é uma política de governança, não uma fronteira rígida de segurança: as camadas 2–5 valem independentemente da estrutura do join, então um contorno deliberado não expõe dados que a função não pudesse alcançar com duas consultas separadas. Tentativas de contorno são registradas e auditáveis.

**Mecanismos de contorno** — o V002 pode ser contornado de duas formas. A primeira é uma capacidade: uma função que tem `ignore_relationships` faz joins entre relações que o catálogo não cobre. Entre as funções de sistema pré-criadas, só `modeler` a tem — a função de descoberta, cujo trabalho é determinar o modelo em vez de aplicá-lo. (REQ-1297) `analyst` não a tem.

A segunda é uma exclusão com duas condições, que precisam ser verdadeiras ambas:

1. **Flag da função** — `relationship_guard: false` na definição da função (padrão: `true`).
2. **Exclusão por consulta** — o SQL contém o comentário `--relationship-guard=false`.

A flag da função sozinha não contorna o V002; o comentário sozinho não contorna o V002.

**O modo de alta segurança fixa a proteção.** Com `security.mode: high` nenhum contorno se aplica: `ignore_relationships` é ignorado, `relationship_guard: false` é ignorado, e todo join precisa existir no catálogo de relacionamentos aprovados. (REQ-693) É uma redundância deliberada — uma função de produção que recebeu a capacidade por engano ainda não consegue sair do modelo.

**Caminho GraphQL** — o V002 é sempre ignorado em consultas GraphQL. Relacionamentos definidos no SDL são pré-aprovados por concepção; a verificação é redundante e não é aplicada.

**Caminhos SQL e Cypher** — o V002 fica ativo por padrão. Tanto `endpoint_dev.py` quanto `cypher_router.py` aplicam a verificação de duas condições antes de chamar `validate_sql`.

**Caminho pgwire** — a mesma verificação de duas condições do SQL. O comentário `--relationship-guard=false` é removido da consulta antes da execução; ele não chega ao banco de dados.

---

Essas camadas se combinam. Uma função com acesso a domínios, RLS e colunas mascaradas tem as cinco restrições ativas ao mesmo tempo. Adicionar uma fonte de dados, coluna ou relacionamento não exige atualizar cada regra — cada camada é configurada de forma independente e se aplica automaticamente a qualquer consulta que toque objetos governados.

---

## Modelo de direitos {: #rights-model }

Capacidades atribuídas de forma independente, com uma hierarquia de funções opcional via `parent_role_id`. `admin` concede tudo. (REQ-042)

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

Funções podem herdar de uma função pai via `parent_role_id`, definida na configuração ou na página Segurança como "Herda de". (REQ-215) A cadeia é dobrada na inicialização: um filho tem a união das capacidades e do acesso a domínios dos seus ancestrais, cada concessão de coluna, métrica, função e webhook que nomeia um ancestral, e as regras RLS dos ancestrais por tabela, com precedência do filho — a função mais próxima na cadeia com uma regra para uma tabela fornece o predicado dessa tabela, e uma regra salva para o filho substitui a do pai para essa tabela. (REQ-1677)

```yaml
roles:
  - id: basic_user
    capabilities: [query_development]
    domain_access: [public]
  - id: analyst
    capabilities: [full_results]
    domain_access: [sales, analytics]
    parent_role_id: basic_user   # inherits query_development + public domain
```
## Funções: concedidas, pseudo e meta {: #roles-granted-pseudo-and-meta }

Três tipos de função trabalham juntos no modelo de acesso do Provisa.

### Funções concedidas {: #granted-roles }

Uma função concedida é atribuída diretamente a um usuário. Ela tem direitos: capacidades, acesso a domínios, entradas de mascaramento, filtros de linha. A herança (`parent_role_id`) dobra na inicialização os direitos de um ancestral no filho. Direitos só se somam; não há subtração. Para restringir um leitor, conceda a um composto que nomeie tanto a função quanto uma condição, ou coloque uma entrada de máscara antes da permissiva. (REQ-215, REQ-1677)

### Pseudo-funções {: #pseudo-roles }

Uma pseudo-função é derivada dos atributos de um usuário. A organização declara quais tipos de atributo viram funções (`claim_roles`). Para cada tipo declarado `t` cujo valor `v` os atributos do usuário fornecem, o usuário tem a função pai `t` e a sua subfunção `t__v`. Sem valor, nenhuma das duas.

A resolução dos atributos importa. O registro próprio da organização sobre um membro prevalece sobre o que o provedor de identidade fornece: quando os dois nomeiam o mesmo atributo, o valor da organização é usado e o do provedor é marcado como substituído.

Pseudo-funções não trazem direitos e nunca são atribuídas à mão. Uma lista de concessão as nomeia exatamente como nomeia uma função concedida; uma requisição as nomeia em `X-Provisa-Role` e age como elas. (REQ-2057)

Pense em um JWT: seus claims são fatos sobre a pessoa. Pseudo-funções transformam fatos escolhidos em funções, para que uma concessão possa se dirigir a eles sem criar uma função por combinação.

### Meta-funções {: #meta-roles }

Quando uma requisição nomeia mais de uma função, o Provisa monta uma meta-função efêmera identificada como `meta:<ids sorted, joined by +>`. Ela é um único filho de cada função do conjunto nomeado. O pipeline inteiro então roda com ela como uma função única comum.

Os direitos são a união dos de cada membro. Os filtros de linha são o OR do filtro resolvido de cada membro; se algum membro que lê uma tabela não tem filtro, a meta-função também não tem — sem filtro significa todas as linhas. Uma meta-função nunca é armazenada, listada nem exportada. Ela é remontada a partir dos seus membros quando o modelo muda. (REQ-1620)

O registro de auditoria nomeia cada função atuante, não o id da meta-função.

### Como uma requisição nomeia funções {: #how-a-request-names-roles }

Um cliente envia uma função, ou um conjunto separado por vírgulas, em `X-Provisa-Role`. Cada função nomeada precisa ser tida. Uma função não atribuída à identidade é recusada pelo nome.

Direitos e dados seguem apenas as funções com que a requisição age: ter uma função não é agir como ela. Uma função é essa função; duas ou mais agem como sua meta-função. Uma função atuante que trouxesse junto os direitos de cada função tida não significaria nada, então uma requisição recebe o menor privilégio e o registro de auditoria nomeia exatamente o que agiu.

Pseudo-funções nomeadas entram como qualificadores. Um qualificador não tem direitos, acesso a domínios, máscaras ou filtros de linha próprios, então não é membro dos direitos da meta-função: como membro sem máscaras, desmascararia toda coluna, e como membro que não lê nenhuma tabela, encerraria todo OR de filtros de linha. Ele entra só nas concessões: uma concessão ou entrada de máscara que nomeia `region__eu` alcança uma requisição que nomeia esse qualificador junto com uma função concedida. (REQ-2057)

### O que `+` significa {: #what-means }

`+` aparece em dois lugares, com significado diferente em cada um.

**Em uma entrada de concessão.** `analyst+region__eu` em uma lista `visible_to` ou `to` significa que o leitor tem todas elas — `analyst` E `region__eu` ao mesmo tempo. Uma entrada que não é um composto é uma função única. Entre as entradas de uma lista a regra é qualquer uma: um leitor tem a concessão se tiver qualquer entrada. E dentro, OU entre elas.

**Em um id de meta-função.** `meta:analyst+engineer` é o identificador do servidor para a meta-função do conjunto {analyst, engineer}. Seus direitos são a união dos dois. Um cliente nunca nomeia diretamente o id da meta-função; ele nomeia as funções que tem, e o servidor monta a meta-função.

### Regras de coluna ABAC como RBAC simples {: #abac-column-rules-as-plain-rbac }

Uma política baseada em atributos pergunta: "este usuário, com estes atributos, pode ver esta coluna?" O RBAC pergunta: "quais funções podem ver esta coluna?" Concessões compostas e pseudo-funções as tornam a mesma pergunta.

"Desmascarar este e-mail só para um analista na UE" é uma entrada de máscara `{to: [analyst+region__eu], mask: null}`. Qualquer leitor que tenha tanto a função `analyst` quanto a pseudo-função `region__eu` lê a coluna sem máscara. Sem linguagem de política. Sem uma função por combinação.

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

Cada coluna tem um modelo de permissões que controla o acesso de leitura, escrita e mascaramento por função. (REQ-042, REQ-249)

### Visibilidade em três níveis {: #three-tier-visibility }

| Nível | Condição | Resultado |
| ------ | ----------- | -------- |
| **Oculta** | Função fora de `visible_to` | Coluna ausente do esquema e do catálogo |
| **Mascarada** | Função em `visible_to`; a primeira entrada de máscara que ela tem traz uma `mask` | Coluna visível, valor substituído na saída |
| **Sem máscara** | Função em `visible_to`; a primeira entrada de máscara que ela tem não traz `mask`, ou ela não tem nenhuma | Acesso completo de leitura |

### Colunas públicas {: #public-columns }

Toda coluna tem um `scope`, definido no arquivo do modelo, na API de administração ou na página Tabelas. Os três escopos são ordenados por quem atendem: `restricted`, depois `domain`, depois `public`. Um editor de tabela (`table_registration` no domínio da tabela) pode restringir o escopo de uma coluna. Ampliá-lo — de `restricted` para `domain` ou `public`, de `domain` para `public` — serve a coluna a mais funções e exige o direito que concedê-la exige: `column_grant` ou `access_config` no domínio da tabela. Uma coluna sendo registrada é comparada com `domain`, então registrá-la como `public` é uma ampliação. Em uma coluna com uma tag sensível, qualquer mudança de escopo exige `sensitive_data`. Uma mudança recusada nomeia a coluna e o direito. (REQ-1959, REQ-1943, REQ-1944)

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
- O domínio `ops` exige uma concessão explícita para cada coluna (REQ-1133); nada nele é servido entre domínios, seja qual for o escopo.

A regra é a mesma em toda superfície, porque cada uma lê o esquema compilado da função: GraphQL, SQL (HTTP, pgwire, Flight, JDBC), Cypher e Bolt, gRPC, REST e JSON:API, os catálogos de Flight e MCP e as listagens de administração. Um escopo que não é um dos três valores é recusado quando uma coluna é salva.

### Permissões de escrita {: #write-grants }

`visible_to` controla a leitura de uma coluna (vazio significa que todas as funções podem ler). A escrita é controlada por tabela com `write_grants`, não por coluna. Por padrão, uma tabela não tem nenhuma permissão, então nenhuma função pode escrever nela. (REQ-2074)

Uma permissão de escrita indica o que uma função pode escrever em uma tabela por uma operação (`insert`, `update` ou `delete`):

| Campo | Finalidade |
| ------- | --------- |
| `columns` | As colunas que um `INSERT` pode listar ou um `UPDATE` pode definir |
| `rows` | As linhas que um `UPDATE` ou `DELETE` pode tocar (um predicado SQL) |
| `check` | O que toda linha escrita por um `INSERT`, ou deixada por um `UPDATE`, deve satisfazer (um predicado SQL) |
| `presets` | Colunas que a própria permissão define, a partir de uma variável de sessão, de um literal ou do momento da escrita. A instrução não pode fornecê-las. |

Se você omitir `rows` ou `check`, a permissão assume o filtro de linhas de leitura da função sobre a tabela, de modo que uma função escreve as linhas que lê. `"TRUE"` significa todas as linhas.

Uma única admissão decide cada escrita em cada superfície, nesta ordem: a função tem o direito `write`; tem uma permissão para a operação sobre a tabela; cada coluna que a instrução escreve está na permissão; os presets são adicionados; `rows` é combinado com AND no `WHERE`; e `check` é avaliado sobre as novas linhas antes de qualquer coisa ser enviada à fonte. Uma recusa nomeia a tabela, a função e a operação. (REQ-2074)

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

As mutações registradas (GraphQL remoto, OpenAPI, gRPC, Hasura) são protegidas por duas verificações independentes. (REQ-868) Uma função só pode invocar uma mutação se tiver a capacidade global `write` E o comando estiver atribuído a ela por `visible_to` (uma lista vazia o atribui a todas as funções). (REQ-868) Um comando não tem lista `writable_by`; a configuração recusa uma pelo nome. [tool-verified: `provisa/security/mutation_authz.py` `command_assigned`, `admit_command`; `provisa/core/models.py` `_one_role_list`]

Mutações são classificadas como escritas por contrato, não pela declaração do chamador. (REQ-869) Um `SELECT` que referencia uma função do tipo mutação é promovido a escrita e sujeito à mesma verificação de duas portas, então um chamador não consegue invocar uma mutação disfarçando-a de leitura. (REQ-869) Reclassificar uma mutação como segura para leitura exige a capacidade `access_config` e é registrado como uma decisão de governança; não há exclusão por requisição. (REQ-870)

## Visibilidade do esquema {: #schema-visibility }

Esquemas GraphQL por função ocultam conteúdo não autorizado: (REQ-039)

- **Acesso a domínios**: uma função vê tabelas apenas nos seus domínios `domain_access` (`"*"` = todos) (REQ-039)
- **Visibilidade de colunas**: colunas fora de `visible_to` para uma função são omitidas do SDL (REQ-039)
- Tabelas/colunas não autorizadas não aparecem no esquema (REQ-039)

## Segurança em nível de linha (RLS) {: #row-level-security-rls }

Injeção de cláusulas SQL WHERE por tabela e por função. Aplicada depois da compilação, antes da execução. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```
O filtro é combinado com AND na cláusula WHERE da consulta. Funciona para consultas e mutações (UPDATE/DELETE). (REQ-035, REQ-041)

Um termo `current_setting('provisa.<name>')` é resolvido contra as variáveis de sessão que a requisição vincula: `user_id` a partir da identidade autenticada, `role` a partir da função atuante, e cada claim escalar do token pelo seu nome em minúsculas sem o prefixo `x-hasura-`, de modo que um filtro Hasura importado sobre `X-Hasura-User-Id` lê `provisa.user_id`. As `session_vars` configuradas de uma função são as constantes de base; os vínculos da requisição as sobrepõem. No modo sem segurança, um cabeçalho `x-provisa-session-<name>` vincula `<name>`. Uma variável não vinculada em lugar nenhum resolve para NULL e não corresponde a nenhuma linha. (REQ-1682)

## Mascaramento em nível de coluna {: #column-level-masking }

O mascaramento é definido como uma lista ordenada em cada coluna. A primeira entrada que o leitor tem decide o que ele vê. (REQ-2059, REQ-263)

| Tipo de máscara | Tipos de coluna suportados | Expressão SQL |
| ----------- | ----------------------- | --------------- |
| `regex` | String (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Qualquer | Valor literal (`NULL`, `0`, uma string personalizada) |
| `truncate` | Data/Timestamp | `DATE_TRUNC(precision, col)` |

Cada entrada tem uma lista `to` que nomeia os leitores a que se dirige, e uma `mask` opcional. Uma entrada sem `mask` significa sem máscara para esses leitores. `*` em `to` corresponde a qualquer leitor.

A expressão de máscara é levada para a projeção SQL SELECT. O banco de dados retorna o valor mascarado; dados sem máscara nunca atravessam a rede para funções mascaradas. (REQ-263) Colunas mascaradas também são bloqueadas em cláusulas `WHERE` e `HAVING` (proteção de predicados da camada 5) para impedir a dedução de valores por filtragem. (REQ-263, REQ-531)

**Uma máscara cobre todos os nomes da coluna, não uma coluna relacionada.** Uma coluna lida pelo seu alias ou pelo seu nome físico é mascarada da mesma forma. Uma máscara não oculta uma chave que um relacionamento aprovado une a uma coluna que o papel pode ver: o join compara os valores reais, então a coluna relacionada mostra o que a chave mascarada contém. Mascare também a coluna relacionada, ou não aprove o relacionamento para esse papel. (REQ-263, REQ-019)

**Nenhuma entrada tida significa sem máscara.** Um leitor que não tem nenhuma entrada lê a coluna sem máscara. Termine uma lista com uma entrada `*` quando todo leitor deve receber alguma máscara. O editor de colunas avisa quando falta uma entrada `*` em uma lista (`unlisted_unmasked`).

**Negar pela ordem.** Uma entrada anterior vence mesmo quando o leitor também tem uma posterior mais permissiva. Para que prestadores de serviço vejam sempre um salário mascarado, quaisquer que sejam suas outras funções, coloque a entrada deles antes da entrada sem máscara. Uma função concedida nunca perde o que o pai concede por herança, mas uma entrada de máscara colocada antes na lista se aplica primeiro — é assim que a restrição funciona aqui.

**Meta-funções.** A cadeia de uma meta-função inclui as funções de cada membro, seus ancestrais e quaisquer pseudo-funções nomeadas. Uma entrada concedida a qualquer membro alcança a meta-função. Uma entrada anterior, tida por outro membro, ainda se aplica primeiro.

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

A resposta de uma função rastreada ou de um webhook é governada como as linhas de uma tabela. (REQ-1679) As linhas que uma ação retorna são vinculadas como uma relação cujas colunas são o contrato de saída declarado da ação — `output_columns` para uma função, `inline_return_type` para um webhook, ou as propriedades de objeto do `return_schema` de tipo array de uma função — e essa relação passa pela mesma etapa de governança que toda leitura de tabela, sobre uma CTE VALUES com as linhas. (REQ-1679) Nada de RLS, mascaramento ou visibilidade é reimplementado para as ações.

Três coisas se aplicam, na mesma ordem que para uma tabela. (REQ-1679)

- Um filtro de linha: a regra RLS da função salva contra a ação pelo nome (`upsertRlsRule` com `actionName`), ou, quando a ação não tem uma própria, a regra de domínio do domínio da ação. O predicado é validado contra o contrato ao salvar, como uma regra de tabela. (REQ-1676)
- Visibilidade de colunas: uma coluna do contrato que declara `visible_to` é removida para uma função que a lista não nomeia; uma coluna que não declara nenhum faz parte da forma pública da ação e fica.
- Máscaras: uma coluna do contrato traz `masks`, a mesma lista ordenada de máscaras que uma coluna de tabela. (REQ-2059)

A herança de funções resolve a cadeia para as três. (REQ-1677) Uma resposta que retorna uma coluna fora do contrato declarado é recusada com um 502 em vez de passar sem governança. Uma ação que não declara contrato de colunas retorna um escalar e não tem nada a vincular; ela é retornada como está. A invocação de teste da administração informa os filtros, as exclusões e as máscaras aplicados, e a instrução governada é gravada no log de auditoria de consultas.

## Amostragem {: #sampling }

Todas as funções veem resultados amostrados (padrão: 100 linhas) a menos que tenham a capacidade `full_results`. (REQ-554) Controlado pela variável de ambiente `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Registro de auditoria {: #audit-logging }

Toda consulta que toca um ativo de domínio é registrada em `query_audit_log`. (REQ-596, REQ-613) O log de auditoria pertence à organização, não à plataforma: o registro próprio da plataforma de organizações, usuários e convites não contém registros de auditoria. (REQ-1922) O `query_audit_log` de cada organização é uma tabela no schema próprio da organização, no store que a organização nomeia para o seu registro em cada região que seleciona. Uma organização que não declara regiões mantém esse schema no banco de dados do control plane da implantação, junto ao seu modelo.

Cada linha guarda `tenant_id` (a organização), `user_id`, `role_id`, um hash SHA-256 do texto da consulta, o próprio texto da consulta cifrado (REQ-689), `table_ids` e `table_names`, `source`, `status_code`, `duration_ms`, a rota e a contagem de linhas, o que foi imposto à instrução, e `logged_at`. O texto da consulta é cifrado com a chave da organização quando a organização definiu uma, caso contrário com a chave da implantação (REQ-1574). Uma implantação que não configura nenhum provedor de cifragem o armazena sem cifrar. O texto nunca é exposto nas tabelas ops consultáveis e é decifrado apenas em leituras de administração autorizadas.

O log é somente de acréscimo no nível do banco de dados: regras do PostgreSQL transformam `DELETE` e `UPDATE` em `query_audit_log` em operações nulas. (REQ-596, REQ-613) Dois índices, `(tenant_id, logged_at)` e `(user_id, logged_at)`, dão suporte a consultas de conformidade por organização e por usuário em intervalos de tempo. (REQ-596, REQ-613)

Retenção: as linhas são mantidas indefinidamente. O Provisa não tem configuração de retenção e nunca apaga o log de auditoria; como `DELETE` é uma operação nula, remover linhas antigas significa que um operador descarta ou arquiva os dados da tabela fora do Provisa.

## Limitação de taxa {: #rate-limiting }

Limites de taxa por função são configurados em `provisa.yaml`: máximo de requisições por segundo, máximo de assinaturas SSE simultâneas e máximo de fluxos Arrow Flight simultâneos. (REQ-369) Os limites são aplicados na camada de API antes da compilação ou execução; requisições acima do limite são recusadas com HTTP 429 e um cabeçalho `Retry-After`. (REQ-369)

O serviço de consultas NL (`POST /query/nl`) tem um limite independente via `nl.rate_limit` (requisições por minuto por função). Requisições acima do limite são recusadas antes de qualquer chamada a um LLM. (REQ-370)

O estado dos limites de taxa fica no Redis (`cache.redis_url`) como um contador de janela deslizante — sem estado por instância — então os limites valem em todas as instâncias horizontais do Provisa. (REQ-371)

## Limite de complexidade de consulta {: #query-complexity-limit }

Um limite de taxa limita quantas requisições uma função envia. O limite de complexidade limita o que uma instrução pode pedir. (REQ-1174)

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

Provedores de autenticação plugáveis: (REQ-120)

| Provedor | Credencial | Caso de uso |
| ---------- | ----------- | ---------- |
| `none` | Cabeçalho X-Provisa-Role | Somente desenvolvimento |
| `basic` | Contas locais (bcrypt) + JWT de sessão | Implantações autônomas |
| `firebase` | Token de ID do Firebase | Produção |
| `keycloak` | JWT do Keycloak | Empresa |
| `oauth` / `oidc` | JWT OIDC | PingFed, Okta, Azure AD, Auth0 |
| `ldap` | Nome de usuário e senha do diretório (LDAP ou Active Directory) | Diretórios corporativos (REQ-1265) |
| `saml` | Asserção assinada SAML 2.0, depois um JWT de sessão | Single sign-on corporativo (REQ-1265) |
| `simple` | bcrypt + JWT | Testes |

O `ldap` autentica um usuário por search-then-bind: o Provisa se conecta como uma conta de serviço, encontra a única entrada que o nome de usuário indica, depois se conecta como essa entrada com a senha apresentada, de modo que o diretório a verifica. Os grupos do usuário se tornam suas funções. A conta e seus grupos são lidos de novo toda vez que um token de sessão retorna, então um usuário removido do diretório, ou de um grupo, perde esse acesso antes de o token expirar. `ldaps://` e StartTLS são ambos suportados.

O `saml` é o Web Browser SSO iniciado pelo SP. A assinatura XML da resposta é verificada contra os metadados do provedor de identidade, e os claims são lidos apenas do elemento assinado. Issuer, Audience, a URL do ACS, a expiração e `InResponseTo` são todos verificados. Respostas não solicitadas (iniciadas pelo IdP), asserções cifradas, documentos com DOCTYPE, múltiplas asserções e assinaturas SHA-1 são recusados. Uma sessão SAML não é verificada de novo com o provedor de identidade: um usuário removido ali mantém o acesso até o token de sessão expirar.

Mapeamento de funções: claims de identidade → função do Provisa via regras configuráveis. (REQ-120) O campo `assignments_source` controla de onde vêm as atribuições de funções: `claims` as lê dos claims do token JWT (padrão), `provisa` as lê do armazenamento interno de atribuições do Provisa. (REQ-551) Uma implantação multi-tenant (`multitenancy: true`) concede as funções ela mesma, por organização, então precisa usar `assignments_source: provisa`: com `claims` ela não inicia, e uma mudança de configuração que produziria essa combinação é recusada. Uma implantação single-tenant pode usar qualquer uma.

Um superusuário configurado em `provisa.yaml` (nome de usuário mais uma senha de um segredo de ambiente) sempre recebe a função admin e todas as capacidades, seja qual for o provedor configurado — um caminho de bootstrap para a configuração inicial. (REQ-125)

### Superfícies e credenciais {: #surfaces-and-credentials }

Toda superfície se autentica pelo mesmo contrato de provedor, então uma credencial que funciona em uma funciona em todas, onde quer que o protocolo possa levá-la. (REQ-124, REQ-1263) Esta tabela é a única referência; a documentação de cada superfície não a repete.

| Superfície | Senha | Token do provedor | Token de acesso pessoal | Certificado de cliente (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | via proxy de terminação |
| pgwire | campo de senha (texto puro ou SCRAM) | campo de senha, implantações OIDC | campo de senha | sim |
| Bolt | esquema `basic` | esquema `bearer` | esquema `bearer` | sim |
| Arrow Flight | — | `token` no handshake ou no payload do ticket; cabeçalho de chamada `authorization: Bearer` em `list_flights`, `get_flight_info` e `get_schema` | o mesmo | sim |
| gRPC | — | metadados `authorization` | metadados `authorization` | sim |
| Airport (extensão `airport` do DuckDB, opcional) | — | `authorization: Bearer` (o `auth_token` do segredo) | o mesmo | — |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | via proxy de terminação |

A única opção de autenticação da extensão `airport` do DuckDB é o `auth_token` bearer do seu segredo, então um cliente DuckDB apresenta ali um token do provedor ou um token de acesso pessoal e age como a função para a qual sua identidade se resolve. Um cliente que pode definir cabeçalhos de chamada pode pedir uma função tida, ou um conjunto separado por vírgulas, em `x-provisa-role`. (REQ-1120, REQ-1263)

Uma recusa diz de que tipo é em cada uma dessas superfícies. Uma credencial ausente, rejeitada ou inutilizável é uma recusa de autenticação: HTTP 401, gRPC `UNAUTHENTICATED`, Arrow Flight e Airport `Unauthenticated`. Uma credencial válida que pede para agir como uma função que sua identidade não tem é uma recusa de autorização, com a função nomeada: HTTP 403, gRPC `PERMISSION_DENIED`, Arrow Flight e Airport `Unauthorized`. (REQ-273, REQ-1263) Uma exceção está na biblioteca cliente do Arrow, não no servidor: uma recusa da chamada `get_schema` do Flight chega a um cliente Python como um erro genérico do Arrow com o motivo do servidor, sem o seu tipo; `list_flights`, `get_flight_info` e `do_get` entregam o tipo.

Onde uma célula mostra `—`, o protocolo não traz um campo de nome de usuário para parear com uma senha; as formas de token cobrem isso. O pgwire é o caso inverso: o pacote de inicialização tem um campo secreto e nenhum esquema, então o que o segredo *é* escolhe o método — um PAT é reconhecido pelo prefixo, o segredo é lido como token bearer quando o provedor configurado é um provedor de tokens, e qualquer outra coisa é uma senha. A escolha é feita uma vez — uma credencial que o validador escolhido recusa não é tentada de novo com outro.

A matriz é imposta por `tests/unit/test_auth_surface_conformance.py`, que exercita o ponto de entrada real de validação de cada superfície e falha quando uma nova superfície é adicionada sem linha.

### Tokens de acesso pessoal {: #personal-access-tokens }

Um PAT é um segredo bearer de longa duração que um usuário gera para um cliente que não consegue concluir um login interativo — um script, uma ferramenta de BI, um driver. (REQ-1263) Ele traz sua própria organização e função, e toda superfície o resolve pelo mesmo validador, então nenhuma superfície precisa saber o que é um PAT.

A forma de transmissão é `provisa_pat_` seguido de 43 caracteres base64 seguros para URL. O prefixo é o que direciona um segredo apresentado ao armazenamento de tokens em vez do provedor de identidade, e torna um token vazado localizável em logs e repositórios.

- **Armazenamento** — só o SHA-256 do segredo é guardado. O segredo em si é mostrado exatamente uma vez, na criação, e não pode ser recuperado. A listagem traz o prefixo de exibição e os timestamps do ciclo de vida, nunca uma credencial funcional.
- **Emissão e revogação** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}`, mais a seção de autoatendimento no próprio perfil do usuário na interface de administração. Gerar e revogar uma credencial é um ato do titular do token.
- **Atribuição** — um PAT validado é resolvido para a conta do seu dono: id de usuário, e-mail e nome de exibição. Uma linha de auditoria ou um relatório de uso escrito sob um PAT nomeia, portanto, a pessoa, não a credencial. Qual dos tokens dessa pessoa agiu é levado à parte, em `raw_claims["token_name"]`.
- **Expiração** — um token pode ter uma expiração; um token expirado é recusado na validação. Excluir a associação de um usuário revoga seus tokens junto.

### SCRAM-SHA-256 no pgwire {: #scram-sha-256-on-pgwire }

Com o provedor `basic`, definir `auth.scram: true` faz o pgwire anunciar SASL (código de autenticação 10) com o mecanismo `SCRAM-SHA-256`, para que uma senha seja provada em vez de enviada. (REQ-1394) O channel binding (`SCRAM-SHA-256-PLUS`) não é oferecido.

O SCRAM precisa de um verificador RFC 5802, que não pode ser derivado de um hash bcrypt. Um verificador é gravado sempre que uma senha passa em texto puro — cadastro, login, troca de senha, redefinição por administrador — então uma implantação que ativa o SCRAM coleta verificadores à medida que seus usuários se autenticam, e a primeira conexão SCRAM de cada usuário vem depois da sua próxima digitação de senha. Um usuário ainda sem verificador recebe uma troca simulada indistinguível de uma real, para que a rede não revele quem já migrou.

### TLS mútuo {: #mutual-tls }

A verificação do certificado do cliente move a primeira checagem para o handshake TLS: um chamador sem um certificado assinado pela CA da implantação nunca chega à camada de credenciais. (REQ-1228) Está disponível no pgwire, Bolt, gRPC e Arrow Flight — os quatro transportes que terminam o próprio TLS.

| Variável | Significado |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | Pacote PEM das CAs autorizadas a assinar certificados de cliente |
| `PROVISA_MTLS_MODE` | `required` (o padrão quando uma CA é definida) ou `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Se verdadeiro, o common name do certificado deve ser igual ao nome de usuário com que a conexão então se autentica |

As substituições por protocolo seguem a mesma nomenclatura das configurações de TLS. Nada é deduzido: um modo definido sem CA se recusa a iniciar, e um modo não reconhecido se recusa a iniciar em vez de ser lido como o vizinho mais seguro — uma implantação que acredita exigir certificados de cliente e não exige está em situação pior que uma que não inicia.

### Versões do TLS {: #tls-versions }

Todo listener que o Provisa termina (a API HTTP e a UI, pgwire, Bolt, MCP, gRPC e Arrow Flight) aceita TLS 1.2 e TLS 1.3 e recusa TLS 1.0 e 1.1. O Provisa não define versão de protocolo nem lista de cifras próprias. Os listeners HTTP, pgwire e Bolt usam os padrões de servidor do módulo `ssl` do Python (mínimo TLS 1.2). gRPC e Arrow Flight usam os padrões do gRPC (mínimo TLS 1.2). As versões não são configuráveis.

### Limitação de logins {: #login-throttling }

Adivinhar senhas não depende do protocolo: a mesma conta pode ser atacada por HTTP, pgwire e Bolt. O contador fica, portanto, na camada de validação de credenciais, não em uma superfície específica, de modo que um bloqueio obtido em qualquer lugar é aplicado em todo lugar. (REQ-1393)

Ela fica ligada por padrão — cinco falhas em cinco minutos bloqueiam o sujeito por quinze minutos — e é ajustada em `auth.login_throttle`. Um sujeito bloqueado é recusado antes mesmo de a credencial ser examinada, e uma autenticação bem-sucedida apaga o histórico desse sujeito.

A chave é o principal que o protocolo traz. Uma superfície só de bearer não traz principal, então a chave é um digest da própria credencial; o que isso impede é que um token ruim seja reapresentado sem limite. O armazenamento é por processo, então uma implantação com vários workers de API permite até `max_attempts` por worker — a limitação freia as tentativas, não é uma cota distribuída.

### Nomear a organização {: #naming-the-org }

Em multi-tenant, toda requisição nomeia a organização a que se destina. Pertencer a exatamente uma organização não a nomeia: uma requisição que não nomeia nenhuma é recusada, e a recusa diz o que enviar. (REQ-1235) Cada superfície nomeia uma organização assim:

| Superfície | Como um cliente nomeia a organização |
| ------- | -------------------------- |
| HTTP (interface, REST, GraphQL) | O nome de host próprio da organização, `acme.provisa.dev`, ou o cabeçalho `X-Org-Provisa` no host do plano de controle |
| pgwire | O nome do banco de dados (`psql -d acme`, o campo de banco de dados de uma ferramenta de BI), ou o nome de host próprio da organização discado via TLS |
| Bolt | O nome do banco de dados, `<org>.provisa_<role>` (ou `<org>.provisa_ops_<role>`), ou o nome de host próprio da organização discado via TLS |
| Arrow Flight | `"org"` no ticket |
| gRPC | O cabeçalho de metadados `x-provisa-org` |
| MCP (transporte HTTP) | O nome de host próprio da organização, ou o cabeçalho `X-Org-Provisa` |
| Cliente Python | `org=` em `ProvisaClient`, `connect()` e `adbc_connect()`; `?org=` em uma URL do SQLAlchemy |

Um cliente pgwire que disca um nome de host via TLS o envia no ClientHello do TLS, e o Provisa lê a organização dali (REQ-1234). Quando o nome de host e o nome do banco de dados nomeiam ambos uma organização e diferem, a conexão é recusada; nenhum dos dois tem preferência. O nome de banco de dados `provisa`, o padrão, não nomeia nenhuma organização, e uma ferramenta de BI conectada a uma organização mostra
essa organização como seu banco de dados. No Bolt, `SHOW DATABASES` lista um `<org>.provisa_<role>` por organização a que
o usuário pertence; um `provisa_<role>` sozinho é recusado em multi-tenant, e uma sessão permanece na
organização que nomeou primeiro.

Um nome é um pedido, não uma concessão. Ele chega em todo lugar ao mesmo resolvedor, que recusa uma organização da qual o principal autenticado não é membro nem tem o direito entre organizações.

Um token de acesso pessoal é emitido para uma organização e a nomeia ele mesmo. Ele abre só essa organização: uma requisição que nomeia outra é recusada, mesmo quando o dono do token pertence às duas.

Uma implantação single-tenant não tem organização a nomear. Nenhuma superfície lê ali um nome de host, um nome de banco de dados ou um cabeçalho para isso.

## Segredos entre organizações {: #secrets-across-organizations }

Os segredos de uma organização são lidos só por essa organização. No armazenamento próprio do Provisa cada segredo fica sob a sua organização. Em um serviço central (Vault, AWS Secrets Manager, GCP Secret Manager, Azure Key Vault) de uma implantação de muitas organizações, toda leitura é feita dentro do prefixo da organização que lê, e uma referência que sairia dele é recusada. (REQ-1557) Os prefixos e os passos para mover os segredos existentes para debaixo deles estão em [Segredos](secrets.md#one-organizations-secrets-in-a-central-service).

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

- **Entre nós.** Um coordenador e seus workers se autenticam mutuamente com o segredo compartilhado gerado da implantação (`internal-communication.shared-secret`). Criptografar o tráfego entre eles é uma opção do operador, desativada por padrão: desativada, o tráfego fica dentro da rede privada ou do cluster da implantação. Ative-a onde um requisito de conformidade ou de zero-trust peça criptografia dentro da implantação. O Trino documenta que ela torna mais lentas as consultas com muito shuffle (joins distribuídos, agregações, funções de janela) de 10% a mais de 100%. Em uma implantação compose é a configuração `engine.trino_internal_tls` (Configurações da implantação → Engine), com a rede do motor e o endereço fixado do coordenador ao lado, e vale a partir do próximo início. Com o chart Helm é o valor `trino.internalTls`, com `trino.coordinatorClusterIP`. Os motores da plataforma hospedada são coordenadores de nó único, sem tráfego entre nós, e a recusam. (REQ-2072)
- **Dados em spool.** A execução tolerante a falhas criptografa os dados de exchange que grava em spool com uma chave gerada para cada consulta e descartada quando ela termina (`fault-tolerant-execution.exchange-encryption-enabled=true`, definido explicitamente). O spill legado em disco não é usado.
- **Organizações em um só motor.** Os catálogos de cada organização são dela, nomeados com a organização, e as instruções de cada organização são executadas em seu próprio resource group.

### Fontes em uma implantação compartilhada {: #sources-on-a-shared-deployment }

Em uma implantação que abriga muitas organizações, as fontes de uma organização só podem alcançar endereços públicos: nunca a rede privada da plataforma, onde estão seu control plane, cache e armazenamento de objetos. A verificação se aplica quando uma fonte é salva, pela UI, pela API de administração ou por uma aplicação de configuração, e de novo a cada conexão e a cada redirecionamento HTTP. As fontes de um sandbox de demonstração são ainda presas ao endereço em que foram verificadas. A organização da própria implantação, cujas fontes o operador configura, não é restringida.

## Criptografia em repouso e as chaves da própria organização {: #encryption-at-rest-and-the-organizations-own-keys }

A criptografia sob uma chave da própria organização está disponível e fica desativada até que a organização a ative. Nada é criptografado sob uma chave da organização a menos que um administrador da organização crie uma.

**A chave de criptografia da organização** (REQ-1574) envolve os segredos da organização, suas colunas criptografadas e o texto de suas consultas de auditoria. Um administrador da organização a cria em **Admin → Org Secrets → Org Key**, ou com `PUT /admin/org-encryption`: sem corpo o Provisa gera a chave, que nunca é exibida, e um corpo `key_b64` fornece 32 bytes seus. `GET /admin/org-encryption` informa só a sua impressão digital. Sem uma chave da organização, os dados da organização são envolvidos pela chave da implantação (`encryption.provider` em `provisa.yaml`). Uma implantação que não configura nenhum provedor de criptografia guarda colunas criptografadas e texto de consultas de auditoria sem criptografia; segredos nunca são guardados em texto claro (veja [Segredos](secrets.md#provisas-built-in-store)).

**A pasta da organização** (REQ-1992) guarda seus arquivos enviados e aterrissados, os arquivos de spill do seu motor e seus arquivos temporários. Um administrador da organização ativa a criptografia dela em **Admin → Org Secrets → File folder encryption**, ou com `PUT /admin/org-files` (`{"encrypted": true, "key_reference": "${secret:NAME}"}`). `POST /admin/org-files/generate-key` responde uma só vez com uma nova chave de 256 bits, para ser guardada no cofre. A chave fica no cofre da própria organização (AWS Secrets Manager, GCP Secret Manager, Azure Key Vault ou HashiCorp Vault): o Provisa guarda só a referência, e o gocryptfs mantém os arquivos da pasta criptografados em disco sob a chave. Isso exige FUSE, portanto um host Linux ou contêiner com `/dev/fuse`; no macOS a configuração é recusada.

Em uma implantação auto-hospedada, que normalmente executa uma organização em motores próprios, ativar as duas criptografa os segredos, arquivos, arquivos de spill e arquivos temporários dessa organização sob as suas chaves:

- O motor DuckDB que uma organização executa faz spill na pasta da organização.
- As linhas que um row filter busca e o código enviado são preparados (staged) na pasta da organização.
- A execução tolerante a falhas do Trino criptografa os dados de exchange em spool com uma chave gerada para cada consulta e descartada quando ela termina (`fault-tolerant-execution.exchange-encryption-enabled=true`). O Provisa não ativa o spill legado do Trino em disco.
- Um motor de federação PostgreSQL grava seus arquivos temporários onde aquele servidor PostgreSQL os guarda, sob a criptografia de armazenamento do próprio servidor, não na pasta da organização.

### Plataformas hospedadas compartilhadas e dedicadas {: #shared-and-dedicated-hosted-platforms }

A plataforma hospedada compartilhada isola as organizações logicamente. Uma implantação atende a todas, e os dados de cada organização continuam seus por meio da governança, de seus próprios schemas, catálogos e resource groups no motor, de suas chaves, de chaves por consulta para dados de motor em spool e da recusa de suas fontes nos endereços privados da plataforma. É recomendada para experimentação e pequenas empresas. Uma plataforma hospedada dedicada é fisicamente separada: máquinas, rede e stores próprios, atendendo a um só cliente.

### Python no navegador {: #python-in-the-browser }

A área Data Science executa Python no navegador do visitante. Toda leitura que ela faz vai ao servidor como o usuário conectado, com o seu papel de atuação, organização e ambiente, e é governada exatamente como a mesma instrução de qualquer outro cliente.

## Modo de alta segurança {: #high-security-mode }

`security.mode: high` em `provisa.yaml` afirma uma garantia: o backend do Provisa nunca manipula dados em texto puro. (REQ-693) Toda coluna que importa é cifrada na fonte, e só um cliente que tem a chave de decifração pode lê-la. Essa garantia tem consequências que uma implantação precisa planejar.

**O que o modo faz:**

- **Os endpoints de dados exigem prova de decifração no cliente.** Tudo sob `/data/` retorna 403 a menos que o chamador apresente o cabeçalho `X-Provisa-KMS-Key` — a marca de um cliente JDBC ou Python configurado para decifrar localmente. Um navegador ou um consumidor REST em texto puro não traz essa chave e é recusado. O controle é uma negação por padrão sobre a árvore inteira: uma rota adicionada amanhã fica protegida no dia em que é lançada, e uma exceção precisa ser justificada.
- **Os endpoints de metadados de esquema continuam abertos.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` e `/data/compile` não retornam dados de linha, e um cliente precisa ler o esquema — incluindo quais campos são `@encrypted` — antes de poder se conectar.
- **gRPC e Arrow Flight continuam servindo, com a mesma prova.** São os transportes que os clientes que cifram realmente usam; fechá-los deixaria uma implantação de alta segurança sem protocolo de transmissão. Uma chamada de dados em qualquer um deles precisa levar a mesma chave KMS como metadados de chamada.
- **pgwire, Bolt e MCP não iniciam.** Nenhum dos três tem um handshake por conexão capaz de levar um contexto de decifração: um conjunto de linhas do pgwire e um resultado Cypher trafegam em texto puro, e uma chamada de ferramenta MCP entrega seus resultados a um modelo como texto. Uma porta configurada para qualquer um deles é recusada na inicialização em vez de servida.
- **A proteção de relacionamentos não pode ser contornada.** `ignore_relationships` e `relationship_guard: false` são ambos ignorados; veja [Governança de relacionamentos](#relationship-governance-v002).

**Verificar se uma implantação está no modo:** o log de inicialização o nomeia, uma requisição `/data/sql` sem chave KMS responde 403 com uma mensagem que nomeia REQ-693, e as portas do pgwire, Bolt e MCP não estão escutando.

## Hook de aprovação ABAC {: #abac-approval-hook }

Um hook de política externo opcional que dispara antes da execução da consulta. (REQ-203) Quando configurado, o Provisa chama o seu motor de políticas com a identidade do usuário, as funções, as tabelas, as colunas e a operação. A resposta determina se a consulta prossegue. (REQ-203) Um comando registrado com **Requer aprovação** é submetido ao mesmo hook antes de cada chamada; veja [Chamadas de comandos](#command-calls). (REQ-1924)

### Escopo {: #scoping }

O hook só dispara quando a consulta toca uma tabela ou fonte incluída — nenhum custo extra para todo o resto. (REQ-204)

| Configuração | Efeito |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Toda consulta dispara o hook |
| `sources[].approval_hook: true` | Todas as tabelas dessa fonte disparam o hook |
| `tables[].approval_hook: true` | Essa tabela dispara o hook |

### Protocolos {: #protocols }

Três transportes são suportados: (REQ-246)

| Tipo | Caso de uso | Campo de configuração |
| ------ | ---------- | ------------- |
| `webhook` | Qualquer serviço de política com HTTP (OPA, próprio) | `url` |
| `unix_socket` | OPA ou sidecar de política na mesma máquina | `socket_path` + `url` |
| `grpc` | Serviço de política co-localizado de alto throughput | `url` (host:port) |

O transporte gRPC usa o contrato `provisa.auth.ApprovalService` definido em `provisa/auth/approval.proto`. Implemente este serviço no seu motor de políticas: (REQ-246)

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
O canal gRPC é persistente — um canal por instância do Provisa, reutilizado em todas as chamadas a esse endpoint do hook. (REQ-555)

### Requisição / resposta {: #request-response }

Os três transportes levam o mesmo payload: (REQ-246)

| Campo | Tipo | Descrição |
| ------- | ------ | ------------- |
| `user` | string | Identidade do usuário autenticado |
| `roles` | string[] | Funções do Provisa do usuário |
| `tables` | string[] | Ids das tabelas referenciadas na consulta |
| `columns` | string[] | Colunas selecionadas na consulta |
| `operation` | string | `"query"` ou `"mutation"`; `"command"` para uma chamada de comando |
| `command` | string | O nome do comando em uma chamada de comando; vazio em uma consulta |
| `arguments` | object | Os argumentos com que um comando é chamado; vazio em uma consulta |

Os transportes webhook e socket Unix trocam JSON, com `command` e `arguments` como chaves. No gRPC os argumentos viajam como o texto JSON `arguments_json`. A resposta precisa incluir `approved` (bool) e, opcionalmente, `reason` (string). (REQ-246)

### Chamadas de comandos {: #command-calls }

Um comando com `requires_approval` definido (a chave **Requer aprovação** no formulário do comando) vai ao hook antes de cada chamada, em toda superfície. Ele só executa se o hook aprovar. A requisição leva `operation: "command"`, o nome do comando e seus argumentos; `tables` e `columns` ficam vazios. (REQ-1924) [tool-verified: `provisa/api/data/action_exec.py` `_require_approval` (`operation="command"`, `tables=[]`, `columns=[]`, `command=fn["name"]`, `arguments=args`)]

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
[inferred: the payload also carries `session_vars`; values shown are placeholders]

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
Em caso de timeout ou erro de transporte, a política `fallback` se aplica. (REQ-247) Um circuit breaker (padrão: abre após 5 falhas consecutivas, meio aberto após 30 s) evita falhas em cascata por um endpoint de hook lento. (REQ-556)

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

As credenciais usam a sintaxe `${env:VAR_NAME}`, resolvida em tempo de execução. (REQ-557) Senhas nunca são armazenadas no banco de dados de configuração. (REQ-557)

Para o serviço completo de segredos — cofres, sintaxe de referência e provedores — veja [Segredos](secrets.md).
