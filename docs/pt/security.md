# Modelo de Segurança

O Provisa aplica um modelo de segurança em múltiplas camadas em cada linguagem de consulta (GraphQL, SQL, Cypher) e cada transporte (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) A governança é aplicada uniformemente — não há caminho de consulta que a contorne. (REQ-002, REQ-266)

As camadas se aplicam em ordem. Uma requisição deve passar por cada camada antes que a próxima seja avaliada.

## Modelo em Camadas

### Camada 0 — Filtragem de introspecção

O esquema e o catálogo apresentados a uma função contêm apenas as tabelas em sua lista `domain_access` e as colunas que passam nas regras `visible_to` por coluna. (REQ-039) Objetos fora do acesso de uma função são invisíveis no momento da descoberta — não podem ser consultados, autocompletados, ou inferidos como existentes. (REQ-039) Isso se aplica ao esquema GraphQL, ao catálogo SQL, e ao navegador de esquema do editor de consultas. (REQ-039, REQ-363)

Veja [Visibilidade de Esquema](#visibilidade-de-esquema).

### Camada 1 — Acesso público

Tabelas em domínios sem restrição `domain_access` são visíveis para todas as identidades autenticadas sem configuração adicional. Zero fricção para dados genuinamente públicos.

### Camada 2 — Acesso a domínio

Cada função carrega uma lista `domain_access` de IDs de domínio. Uma consulta que toca uma tabela fora desses domínios é rejeitada antes da execução. (REQ-038, REQ-039) Esta é a fronteira grosseira de propriedade — uma função de RH não pode alcançar tabelas financeiras independentemente de como o SQL é escrito. (REQ-002)

Veja [Modelo de Direitos](#modelo-de-direitos).

### Camada 3 — Segurança em nível de linha

Após o acesso a domínio ser confirmado, predicados `WHERE` por tabela por função são injetados em todo `SELECT` no momento da execução. (REQ-041, REQ-263) Os predicados avaliam contra os dados brutos. Um gerente regional consultando uma tabela de pedidos compartilhada vê apenas as linhas de sua região, mesmo em um `SELECT *`. (REQ-264)

Veja [Segurança em Nível de Linha (RLS)](#seguranca-em-nivel-de-linha-rls).

### Camada 4 — Visibilidade e mascaramento de coluna

Colunas com uma lista `visible_to` que exclui a função solicitante são removidas da saída da consulta. (REQ-040, REQ-263) Colunas com uma regra de mascaramento têm seus valores substituídos — redação por regex, substituição constante, ou truncamento — antes que os resultados deixem o servidor. (REQ-263) O mascaramento se aplica em todas as linguagens de consulta e formatos de saída. (REQ-263)

Veja [Modelo de Permissão de Coluna](#modelo-de-permissao-de-coluna) e [Mascaramento em Nível de Coluna](#mascaramento-em-nivel-de-coluna).

### Camada 5 — Guarda de predicado

Colunas mascaradas são rejeitadas em cláusulas `WHERE` e `HAVING`. (REQ-263) Sem isso, um chamador poderia inferir o valor não mascarado buscando-o binariamente em um filtro, mesmo que a saída esteja mascarada. A rejeição é aplicada no momento do parse da consulta, antes da execução. (REQ-531)

### Governança de relacionamento (V002)

Condições JOIN em SQL devem corresponder a um relacionamento registrado e aprovado entre tabelas. (REQ-001) Joins não aprovados são rejeitados. Cada relacionamento carrega um motivo e descrição legíveis por humanos — orientação tanto para usuários quanto para agentes autônomos sobre por que um caminho de travessia existe. Esta é uma política de governança, não uma fronteira de segurança rígida: as Camadas 2–5 se mantêm independentemente da estrutura do join, então uma burla deliberada não expõe dados que a função não pudesse alcançar através de duas consultas separadas. Tentativas de burla são registradas e auditáveis.

**Mecanismos de burla** — o V002 pode ser burlado de duas maneiras. A primeira é uma capacidade: uma função que detém `ignore_relationships` faz junções sobre relações que o catálogo não cobre. Entre as funções de sistema semeadas apenas `modeler` a detém — a função de descoberta cujo trabalho é determinar o modelo em vez de aplicá-lo. (REQ-1297) `analyst` não a detém. [tool-verified: `provisa/core/db.py:84`]

A segunda é uma exclusão de duas condições, em que ambas devem ser verdadeiras:

1. **Flag de função** — `relationship_guard: false` na definição da função (padrão: `true`). [tool-verified: `provisa/core/models.py:349`]
2. **Opt-out por consulta** — o SQL contém o comentário `--relationship-guard=false`. [tool-verified: `provisa/compiler/params.py:80`]

A flag de função sozinha não burla o V002; o comentário sozinho não burla o V002.

**O modo de alta segurança fixa a proteção.** Sob `security.mode: high` nenhuma das burlas se aplica: `ignore_relationships` é ignorado, `relationship_guard: false` é ignorado, e toda junção precisa existir no catálogo de relacionamentos aprovados. (REQ-693) Trata-se de redundância deliberada — uma função de produção que recebeu a capacidade por engano continua sem conseguir sair do modelo. [tool-verified: `provisa/pgwire/_pipeline.py:377`]

**Caminho GraphQL** — o V002 é incondicionalmente ignorado para consultas GraphQL. Relacionamentos definidos em SDL são pré-aprovados por design; a verificação é redundante e não é aplicada. [tool-verified: `provisa/api/data/endpoint.py:468`]

**Caminhos SQL e Cypher** — o V002 está ativo por padrão. Tanto `endpoint_dev.py` quanto `cypher_router.py` aplicam a verificação de duas condições antes de chamar `validate_sql`. [tool-verified: `provisa/api/data/endpoint_dev.py:127`, `provisa/api/rest/cypher_router.py:260`]

**Caminho pgwire** — mesma verificação de duas condições que o SQL. O comentário `--relationship-guard=false` é removido da consulta antes da execução; ele não alcança o banco de dados. [tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

Essas camadas compõem. Uma função com acesso a domínio, RLS, e colunas mascaradas tem todas as cinco restrições ativas simultaneamente. Adicionar uma nova fonte de dados, coluna, ou relacionamento não exige atualizar cada regra — cada camada é configurada independentemente e se aplica automaticamente a qualquer consulta que toque objetos governados.

---

## Modelo de Direitos

Capacidades atribuídas independentemente com hierarquia de função opcional via `parent_role_id`. `admin` concede tudo. (REQ-042)

| Capacidade | Descrição |
| ----------- | ------------- |
| `source_registration` | Registrar fontes de dados |
| `table_registration` | Registrar tabelas, colunas |
| `create_relationship` | Definir relacionamentos FK |
| `access_config` | Configurar RLS, mascaramento |
| `query_development` | Executar consultas |
| `write` | Invocar mutações registradas (gate grosseiro; veja Autorização de Mutação) |
| `full_results` | Ignorar limites de amostragem |
| `ignore_relationships` | Ignorar governança de relacionamento (V002). Detida apenas por `modeler` entre as funções de sistema, e ignorada por completo no modo de alta segurança |
| `admin` | Superusuário — concede tudo |

### Herança de Função

Funções podem herdar capacidades e acesso a domínio de uma função pai via `parent_role_id`. (REQ-215) A hierarquia é achatada na inicialização — funções filhas mesclam as capacidades e acesso a domínio de seus pais com os próprios. (REQ-215)

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

## Modelo de Permissão de Coluna

Cada coluna tem um modelo de permissão de quatro campos controlando acesso de leitura, escrita, e mascaramento por função. (REQ-042, REQ-249)

### Visibilidade de Três Níveis

| Nível | Condição | Resultado |
| ------ | ----------- | -------- |
| **Oculta** | Função não em `visible_to` | Coluna ausente do SDL GraphQL |
| **Mascarada** | Função em `visible_to`, tem regra de mascaramento, função não em `unmasked_to` | Coluna visível mas dados mascarados em SQL |
| **Não mascarada** | Função em `visible_to` E função em `unmasked_to` (ou sem regra de mascaramento) | Acesso de leitura completo |

### Permissões de Escrita

| Campo | Vazio significa | Propósito |
| ------- | ------------ | --------- |
| `visible_to` | Todas as funções podem ler | Controla quem vê a coluna (mascarada ou não) |
| `unmasked_to` | Nenhuma função vê não mascarado | Controla quem ignora o mascaramento |
| `writable_by` | Nenhuma função pode escrever | Controla quem pode mutar (INSERT/UPDATE) |

A permissão de escrita é aplicada no pipeline de mutação. Uma função não em `writable_by` recebe um erro 403 ao tentar escrever em uma coluna restrita. (REQ-033, REQ-034)

### Exemplo

```yaml
columns:
  - name: email
    visible_to: [admin, analyst, viewer]
    writable_by: [admin]
    unmasked_to: [admin]
    mask_type: regex
    mask_pattern: "(.).*@"
    mask_replace: "$1***@"
  - name: salary
    visible_to: [admin, hr]
    writable_by: [hr]
    unmasked_to: [admin, hr]
    mask_type: constant
    mask_value: "0"
  - name: created_at
    visible_to: []           # all can read
    writable_by: []          # nobody can write (auto-set)
```

Neste exemplo:

- `email`: admin vê `alice@example.com` e pode editar; analyst/viewer veem `a***@example.com`
- `salary`: admin e hr veem o valor real; hr pode editar; todas as outras funções não veem a coluna
- `created_at`: todos podem ler, ninguém pode escrever

## Autorização de Mutação

Mutações registradas (GraphQL remoto, OpenAPI, gRPC, Hasura) são controladas por duas verificações independentes. (REQ-867, REQ-868) Uma função pode invocar uma mutação apenas se possuir a capacidade global `write` E aparecer na lista `writable_by` dessa mutação. (REQ-868) Um `writable_by` vazio é negação por padrão — nenhuma função pode invocá-la. (REQ-867)

Mutações são classificadas como escritas por contrato, não por declaração do chamador. (REQ-869) Um `SELECT` que referencia uma função do tipo mutação é promovido a escrita e sujeito à mesma verificação de duas comportas, de modo que um chamador não pode invocar uma mutação disfarçando-a como leitura. (REQ-869) Reclassificar uma mutação como segura para leitura exige a capacidade `access_config` e é registrada como uma decisão de governança; não há opt-out por requisição. (REQ-870)

## Visibilidade de Esquema

Esquemas GraphQL por função ocultam conteúdo não autorizado: (REQ-039)

- **Acesso a domínio**: Função vê tabelas apenas em seus domínios `domain_access` (`"*"` = todos) (REQ-039)
- **Visibilidade de coluna**: Colunas não em `visible_to` para uma função são omitidas do SDL (REQ-039)
- Tabelas/colunas não autorizadas não aparecem no esquema (REQ-039)

## Segurança em Nível de Linha (RLS)

Injeção de cláusula WHERE SQL por tabela por função. Aplicada após a compilação, antes da execução. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

O filtro é unido por AND à cláusula WHERE da consulta. Funciona tanto para consultas quanto para mutações (UPDATE/DELETE). (REQ-035, REQ-041)

## Mascaramento em Nível de Coluna

O mascaramento é definido uma vez por coluna — é uma propriedade da coluna, não da função. O campo `unmasked_to` controla quais funções o ignoram. (REQ-249)

| Tipo de Máscara | Tipos Suportados | Expressão SQL |
| ----------- | ---------------- | ---------------- |
| `regex` | String (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Qualquer | Valor literal (NULL, 0, personalizado) |
| `truncate` | Date/Timestamp | `DATE_TRUNC(precision, col)` |

O mascaramento é empurrado para a projeção SELECT do SQL — o banco de dados retorna os dados já mascarados. (REQ-263) Dados não mascarados nunca cruzam a rede para funções mascaradas. (REQ-263) Colunas mascaradas também são bloqueadas de cláusulas `WHERE` e `HAVING` (guarda de predicado da Camada 5) para prevenir a inferência do valor não mascarado através de filtragem. (REQ-263, REQ-531)

## Amostragem

Todas as funções veem resultados amostrados (padrão: 100 linhas), a menos que tenham a capacidade `full_results`. (REQ-554) Controlado via a variável de ambiente `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Log de Auditoria

Toda consulta que toca um ativo de domínio é registrada no `query_audit_log` somente-anexação. (REQ-596, REQ-613) Cada linha captura `tenant_id`, `user_id`, `role_id`, um hash SHA-256 do texto da consulta, `table_ids`, `source`, `status_code`, `duration_ms`, e `logged_at`. (REQ-596) O texto da consulta nunca é armazenado literalmente — apenas seu hash. (REQ-596)

O log é somente-anexação no nível do banco de dados: regras do PostgreSQL bloqueiam `DELETE` e `UPDATE`. (REQ-596, REQ-613) Dois índices — `(tenant_id, logged_at)` e `(user_id, logged_at)` — suportam consultas de conformidade com escopo de tenant e por usuário em intervalo de tempo. (REQ-596, REQ-613)

Quando a criptografia está habilitada, a coluna de hash do texto da consulta é armazenada criptografada e descriptografada apenas em leituras autorizadas de admin. (REQ-689)

## Limitação de Taxa

Limites de taxa por função são configurados em `provisa.yaml`: máximo de requisições por segundo, máximo de subscriptions SSE concorrentes, e máximo de streams Arrow Flight concorrentes. (REQ-369) Os limites são aplicados na camada de API antes da compilação ou execução; requisições acima do limite são rejeitadas com HTTP 429 e um cabeçalho `Retry-After`. (REQ-369)

O serviço de consulta NL (`POST /query/nl`) tem um limite independente via `nl.rate_limit` (requisições por minuto por função). Requisições acima do limite são rejeitadas antes que qualquer chamada LLM seja feita. (REQ-370)

O estado de limitação de taxa reside no Redis (`cache.redis_url`) como um contador de janela deslizante — sem estado por instância — de modo que os limites se mantêm em todas as instâncias horizontais do Provisa. (REQ-371)

## Autenticação

Provedores de autenticação plugáveis: (REQ-120)

| Provedor | Tipo de Token | Caso de Uso |
| ---------- | ----------- | ---------- |
| `none` | Cabeçalho X-Provisa-Role | Desenvolvimento |
| `basic` | Contas locais bcrypt + JWT | Implantações autocontidas |
| `firebase` | Token de ID Firebase | Produção |
| `keycloak` | JWT Keycloak | Empresarial |
| `oauth` | JWT OIDC | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | Testes |

Mapeamento de função: claims de identidade → função Provisa via regras configuráveis. (REQ-120) O campo `assignments_source` controla de onde vêm as atribuições de função: `claims` as lê das claims do token JWT (padrão), `provisa` as lê do armazenamento interno de atribuições do Provisa. (REQ-551)

Um superusuário configurado em `provisa.yaml` (usuário mais uma senha de um segredo de ambiente) sempre recebe a função admin e todas as capacidades independentemente do provedor configurado — um caminho de inicialização para configuração inicial. (REQ-125)

### Superfícies e credenciais

Toda superfície autentica pelo mesmo contrato de provedor, de modo que uma credencial que funciona em uma funciona em todas onde o protocolo consiga carregá-la. (REQ-124, REQ-1263) Esta tabela é a referência única; os documentos de cada superfície não a repetem.

| Superfície | Senha | Token de provedor | Token de acesso pessoal | Certificado de cliente (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | via proxy de terminação |
| pgwire | campo de senha (texto claro ou SCRAM) | campo de senha, implantações OIDC | campo de senha | sim |
| Bolt | esquema `basic` | esquema `bearer` | esquema `bearer` | sim |
| Arrow Flight | — | `token` no handshake ou na carga do ticket | idem | sim |
| gRPC | — | metadados `authorization` | metadados `authorization` | sim |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | via proxy de terminação |

Onde uma célula traz `—`, o protocolo não carrega campo de nome de usuário para emparelhar com uma senha; as formas de token cobrem o caso. O pgwire é o caso espelhado: o pacote de inicialização tem um único campo de segredo e nenhum esquema, então *o que* o segredo é escolhe o método — um PAT é reconhecido pelo prefixo, o segredo é lido como token bearer quando o provedor configurado é um provedor de tokens, e qualquer outra coisa é uma senha. A escolha é feita uma vez — uma credencial que o validador selecionado recusa não é tentada novamente contra outro.

A matriz é imposta por `tests/unit/test_auth_surface_conformance.py`, que aciona o ponto de entrada de validação real de cada superfície e falha quando uma nova superfície é adicionada sem linha.

### Tokens de acesso pessoal

Um PAT é um segredo bearer de longa duração que um usuário cunha para um cliente incapaz de completar um login interativo — um script, uma ferramenta de BI, um driver. (REQ-1263) Ele carrega sua própria organização e função, e toda superfície o resolve pelo mesmo validador, de modo que nenhuma superfície precisa saber o que é um PAT.

A forma no fio é `provisa_pat_` seguido de 43 caracteres base64 seguros para URL. O prefixo é o que roteia um segredo apresentado para o repositório de tokens em vez do provedor de identidade, e torna um token vazado localizável por grep em logs e repositórios.

- **Armazenamento** — apenas o SHA-256 do segredo é guardado. O segredo em si é exibido exatamente uma vez, na criação, e não pode ser recuperado. A listagem traz o prefixo de exibição e as marcas temporais do ciclo de vida, nunca uma credencial utilizável.
- **Emissão e revogação** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}`, além da seção de autoatendimento no próprio perfil do usuário na UI de administração. Cunhar e revogar uma credencial é ato de quem a detém.
- **Atribuição** — um PAT validado resolve para a conta de seu proprietário: id de usuário, e-mail e nome de exibição. Uma linha de auditoria ou um relatório de uso escrito sob um PAT nomeia, portanto, a pessoa, não a credencial. Qual dos tokens dessa pessoa agiu é carregado separadamente, em `raw_claims["token_name"]`.
- **Expiração** — um token pode carregar uma expiração; um token expirado é recusado na validação. Excluir a participação de um usuário revoga seus tokens junto.

### SCRAM-SHA-256 no pgwire

Sob o provedor `basic`, definir `auth.scram: true` faz o pgwire anunciar SASL (código de autenticação 10) com o mecanismo `SCRAM-SHA-256`, de modo que uma senha é provada em vez de enviada. (REQ-1394) A vinculação de canal (`SCRAM-SHA-256-PLUS`) não é oferecida.

O SCRAM precisa de um verificador RFC 5802, que não pode ser derivado de um hash bcrypt. Um verificador é gravado sempre que uma senha passa em texto claro — cadastro, login, troca de senha, redefinição por administrador — de modo que uma implantação que liga o SCRAM vai coletando verificadores conforme seus usuários se autenticam da próxima vez, e a primeira conexão SCRAM de cada usuário vem depois da próxima digitação de sua senha. A um usuário ainda sem verificador responde-se com uma troca simulada indistinguível de uma real, de forma que o fio não revela quem já migrou.

### TLS mútuo

A verificação de certificado de cliente move a primeira checagem para o handshake TLS: um chamador sem certificado assinado pela CA da implantação nunca alcança a camada de credenciais. (REQ-1228) Está disponível em pgwire, Bolt, gRPC e Arrow Flight — os quatro transportes que terminam o próprio TLS.

| Variável | Significado |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | Pacote PEM da(s) CA(s) autorizada(s) a assinar certificados de cliente |
| `PROVISA_MTLS_MODE` | `required` (o padrão assim que uma CA é definida) ou `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Quando verdadeiro, o common name do certificado precisa ser igual ao nome de usuário com que a conexão então se autentica |

Sobreposições por protocolo seguem a mesma nomenclatura das configurações TLS. Nada é inferido: um modo definido sem CA recusa iniciar, e um modo não reconhecido recusa iniciar em vez de ser lido como o vizinho mais seguro — uma implantação que acredita exigir certificados de cliente e não exige está pior do que uma que não inicia.

### Limitação de tentativas de login

Adivinhar senhas independe do protocolo: a mesma conta pode ser martelada por HTTP, pgwire e Bolt. O contador vive, portanto, na camada de validação de credenciais, e não em uma superfície qualquer, de modo que um bloqueio conquistado em qualquer lugar é aplicado em todos. (REQ-1393)

Está ligada por padrão — cinco falhas em cinco minutos bloqueiam o sujeito por quinze minutos — e é ajustada sob `auth.login_throttle`. Um sujeito bloqueado é recusado antes mesmo de a credencial ser examinada, e uma autenticação bem-sucedida limpa o histórico daquele sujeito.

A chave é o principal que o protocolo carrega. Uma superfície apenas-bearer não carrega principal, então a chave é um resumo da própria credencial; o que isso impede é um token ruim ser repetido sem limite. O repositório é por processo, então uma implantação com vários workers de API permite até `max_attempts` por worker — a limitação é um freio ao chute, não uma cota distribuída.

### Endereçar uma organização em um protocolo de fio

Sob multitenancy uma organização é endereçada por nome de host: `acme.provisa.dev` é a organização `acme`. Sobre HTTP esse nome chega no cabeçalho `Host`. Um cliente pgwire ou Bolt não envia tal cabeçalho, mas envia o nome de host que discou no ClientHello do TLS, e o Provisa lê a organização de lá. (REQ-1234) Nada muda no cliente — conectar-se a `acme.provisa.dev` é tudo o que basta.

O nome de host é um pedido, não uma concessão. Ele chega ao mesmo resolvedor a que chega o cabeçalho `Host`, que recusa qualquer organização da qual o principal autenticado não seja membro nem detenha o direito entre organizações. Discar um nome de host no qual você não tem participação não alcança dado algum. Um cliente que se conectou por endereço IP não envia nome de host e resolve sua organização apenas pelo principal, o que vale para toda conexão em uma implantação de organização única.

gRPC, Arrow Flight e MCP entregam seus certificados a bibliotecas que não expõem callback de nome de host; esses transportes nomeiam uma organização com o cabeçalho de metadados `x-provisa-org`.

## Modo de alta segurança

`security.mode: high` em `provisa.yaml` afirma uma garantia: o backend do Provisa nunca manipula dados em texto claro. (REQ-693) Toda coluna que importa está cifrada na origem, e apenas um cliente que detenha a chave de decifragem pode lê-la. Essa garantia tem consequências que uma implantação precisa planejar.

**O que o modo faz:**

- **Endpoints de dados exigem prova de decifragem no cliente.** Tudo sob `/data/` devolve 403 a menos que o chamador apresente o cabeçalho `X-Provisa-KMS-Key` — a marca de um cliente JDBC ou Python configurado para decifrar localmente. Um navegador ou um consumidor REST em texto claro não carrega tal chave e é recusado. O portão é um negar-por-padrão sobre toda a árvore: uma rota adicionada amanhã fica protegida no dia em que é publicada, e uma isenção precisa ser argumentada.
- **Endpoints de metadados de esquema seguem abertos.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` e `/data/compile` não devolvem dados de linha, e um cliente precisa ler o esquema — inclusive quais campos são `@encrypted` — antes mesmo de poder se conectar.
- **gRPC e Arrow Flight continuam servindo, sob a mesma prova.** São os transportes que os clientes que cifram de fato usam; fechá-los deixaria uma implantação de alta segurança sem protocolo de fio. Uma chamada de dados em qualquer um deles precisa carregar a mesma chave KMS como metadado da chamada.
- **pgwire, Bolt e MCP não iniciam.** Nenhum dos três tem handshake por conexão capaz de carregar um contexto de decifragem: um conjunto de linhas pgwire e um resultado Cypher vão em texto claro pelo fio, e uma chamada de ferramenta MCP entrega seus resultados a um modelo como texto. Uma porta configurada para qualquer um deles é recusada na inicialização em vez de servida.
- **A proteção de relacionamentos não pode ser burlada.** `ignore_relationships` e `relationship_guard: false` são ambos ignorados; veja [Governança de relacionamento](#relationship-governance-v002).

**Verificar se uma implantação está no modo:** o log de inicialização o nomeia, uma requisição `/data/sql` sem chave KMS responde 403 com uma mensagem citando REQ-693, e as portas pgwire, Bolt e MCP não estão escutando.

## Hook de Aprovação ABAC

Um hook de política externa opcional que dispara antes da execução da consulta. (REQ-203) Quando configurado, o Provisa chama seu motor de política com a identidade do usuário, funções, tabelas, colunas, e operação. A resposta determina se a consulta prossegue. (REQ-203)

### Escopo

O hook dispara apenas quando a consulta toca uma tabela ou fonte no escopo — zero overhead para todo o resto. (REQ-204)

| Config | Efeito |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Toda consulta aciona o hook |
| `sources[].approval_hook: true` | Todas as tabelas nessa fonte acionam o hook |
| `tables[].approval_hook: true` | Essa tabela aciona o hook |

### Protocolos

Três transportes são suportados: (REQ-246)

| Tipo | Caso de uso | Campo de config |
| ------ | ---------- | ------------- |
| `webhook` | Qualquer serviço de política com capacidade HTTP (OPA, personalizado) | `url` |
| `unix_socket` | OPA ou sidecar de política na mesma máquina | `socket_path` + `url` |
| `grpc` | Serviço de política colocalizado de alta vazão | `url` (host:porta) |

O transporte gRPC usa o contrato `provisa.auth.ApprovalService` definido em `provisa/auth/approval.proto`. Implemente este serviço em seu motor de política: (REQ-246)

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
}

message ApprovalResponse {
  bool approved = 1;
  string reason = 2;
}
```

O canal gRPC é persistente — um canal por instância Provisa, reutilizado em todas as chamadas para esse endpoint de hook. (REQ-555)

### Requisição / Resposta

Os três transportes carregam o mesmo payload: (REQ-246)

| Campo | Tipo | Descrição |
| ------- | ------ | ------------- |
| `user` | string | Identidade do usuário autenticado |
| `roles` | string[] | Funções Provisa do usuário |
| `tables` | string[] | IDs de tabela referenciados na consulta |
| `columns` | string[] | Colunas selecionadas na consulta |
| `operation` | string | `"query"` ou `"mutation"` |

Os transportes webhook e Unix socket trocam JSON. A resposta deve incluir `approved` (bool) e opcionalmente `reason` (string). (REQ-246)

### Timeout e Fallback

```yaml
auth:
  approval_hook:
    type: grpc          # webhook | grpc | unix_socket
    url: "localhost:50051"
    timeout_ms: 500     # default 5000
    fallback: deny      # allow | deny — applied on timeout or error
    scope: ""           # "" = use per-table/per-source flags; "all" = every query
```

Em caso de timeout ou erro de transporte, a política `fallback` se aplica. (REQ-247) Um disjuntor (padrão: abre após 5 falhas consecutivas, meio-aberto após 30s) previne falhas em cascata a partir de um endpoint de hook lento. (REQ-556)

### Exemplo de Configuração

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

## Segredos

Credenciais usam a sintaxe `${env:VAR_NAME}`, resolvida em tempo de execução. (REQ-557) Senhas nunca são armazenadas no BD de configuração. (REQ-557)
