# Modelo de Segurança

O Provisa aplica um modelo de segurança em múltiplas camadas em toda linguagem de consulta (GraphQL, SQL, Cypher) e todo transporte (REST, gRPC, Arrow Flight, JDBC, WebSocket). (REQ-001, REQ-266) A governança é aplicada de modo uniforme — não existe caminho de consulta que a contorne. (REQ-002, REQ-266)

As camadas se aplicam em ordem. Uma requisição precisa passar por cada camada antes que a seguinte seja avaliada.

## Modelo em Camadas

### Camada 0 — Filtragem de introspecção

O esquema e o catálogo apresentados a uma função contêm apenas as tabelas em sua lista `domain_access` e as colunas que passam nas regras `visible_to` de cada coluna. (REQ-039) Objetos fora do acesso de uma função são invisíveis no momento da descoberta — não podem ser consultados, autocompletados nem deduzidos como existentes. (REQ-039) Isso vale para o esquema GraphQL, o catálogo SQL e o navegador de esquema do editor de consultas. (REQ-039, REQ-363)

Veja [Visibilidade de Esquema](#visibilidade-de-esquema).

### Camada 1 — Acesso público

Tabelas em domínios sem restrição de `domain_access` são visíveis a todas as identidades autenticadas, sem configuração adicional. Atrito zero para dados genuinamente públicos.

### Camada 2 — Acesso a domínio

Cada função carrega uma lista `domain_access` de IDs de domínio. Uma consulta que toca uma tabela fora desses domínios é rejeitada antes da execução. (REQ-038, REQ-039) Esta é a fronteira grosseira de propriedade — uma função de RH não alcança tabelas de finanças, independentemente de como o SQL seja escrito. (REQ-002)

Veja [Modelo de Direitos](#modelo-de-direitos).

### Camada 3 — Segurança em nível de linha

Depois de confirmado o acesso ao domínio, predicados `WHERE` por tabela e por função são injetados em todo `SELECT` no momento da execução. (REQ-041, REQ-263) Os predicados avaliam contra os dados brutos. Um gerente regional que consulta uma tabela de pedidos compartilhada vê apenas as linhas da sua região, mesmo em um `SELECT *`. (REQ-264)

Veja [Segurança em Nível de Linha (RLS)](#seguranca-em-nivel-de-linha-rls).

### Camada 4 — Visibilidade e mascaramento de coluna

Colunas com uma lista `visible_to` que exclui a função solicitante são removidas da saída da consulta. (REQ-040, REQ-263) Colunas com uma regra de mascaramento têm seus valores substituídos — redação por regex, substituição por constante ou truncamento — antes que os resultados deixem o servidor. (REQ-263) O mascaramento se aplica em todas as linguagens de consulta e formatos de saída. (REQ-263)

Veja [Modelo de Permissão de Coluna](#modelo-de-permissao-de-coluna) e [Mascaramento em Nível de Coluna](#mascaramento-em-nivel-de-coluna).

### Camada 5 — Guarda de predicado

Colunas mascaradas são rejeitadas em cláusulas `WHERE` e `HAVING`. (REQ-263) Sem isso, quem chama poderia deduzir o valor não mascarado por busca binária em um filtro, mesmo que a saída esteja mascarada. A rejeição é aplicada no momento da análise da consulta, antes da execução. (REQ-531)

### Governança de relacionamento (V002)

Condições de JOIN em SQL precisam corresponder a um relacionamento registrado e aprovado entre tabelas. (REQ-001) Junções não aprovadas são rejeitadas. Cada relacionamento carrega um motivo e uma descrição legíveis por pessoas — orientação, tanto para usuários quanto para agentes autônomos, sobre por que existe um caminho de travessia. Isso é política de governança, não uma fronteira rígida de segurança: as camadas 2–5 valem independentemente da estrutura de junção, então uma evasão deliberada não expõe dados que a função não pudesse alcançar por duas consultas separadas. Tentativas de evasão são registradas e auditáveis.

**Mecanismos de contorno** — o V002 pode ser contornado de duas formas. A primeira é uma capacidade: uma função que detém `ignore_relationships` faz junções entre relações que o catálogo não cobre. Entre as funções de sistema semeadas, apenas `modeler` a detém — a função de descoberta, cujo trabalho é determinar o modelo em vez de aplicá-lo. (REQ-1297) `analyst` não a detém. [tool-verified: `provisa/core/db.py:84`]

A segunda é uma renúncia de duas condições, em que ambas precisam ser verdadeiras:

1. **Sinalizador de função** — `relationship_guard: false` na definição da função (padrão: `true`). [tool-verified: `provisa/core/models.py:349`]
2. **Renúncia por consulta** — o SQL contém o comentário `--relationship-guard=false`. [tool-verified: `provisa/compiler/params.py:80`]

O sinalizador de função sozinho não contorna o V002; o comentário sozinho não contorna o V002.

**O modo de alta segurança fixa a guarda.** Sob `security.mode: high` nenhum dos contornos se aplica: `ignore_relationships` é ignorado, `relationship_guard: false` é ignorado e toda junção precisa existir no catálogo de relacionamentos aprovados. (REQ-693) Isso é redundância deliberada — uma função de produção que recebeu a capacidade por engano ainda assim não consegue escapar do modelo. [tool-verified: `provisa/pgwire/_pipeline.py:377`]

**Caminho GraphQL** — o V002 é incondicionalmente ignorado em consultas GraphQL. Relacionamentos definidos em SDL são pré-aprovados por projeto; a verificação é redundante e não é aplicada. [tool-verified: `provisa/api/data/endpoint.py:468`]

**Caminhos SQL e Cypher** — o V002 está ativo por padrão. Tanto `endpoint_dev.py` quanto `cypher_router.py` aplicam a verificação de duas condições antes de chamar `validate_sql`. [tool-verified: `provisa/api/data/endpoint_dev.py:127`, `provisa/api/rest/cypher_router.py:260`]

**Caminho pgwire** — a mesma verificação de duas condições do SQL. O comentário `--relationship-guard=false` é removido da consulta antes da execução; ele não chega ao banco de dados. [tool-verified: `provisa/pgwire/_pipeline.py:60`]

---

Essas camadas se compõem. Uma função com acesso a domínio, RLS e colunas mascaradas tem as cinco restrições ativas ao mesmo tempo. Adicionar uma nova fonte de dados, coluna ou relacionamento não exige atualizar todas as regras — cada camada é configurada de forma independente e se aplica automaticamente a qualquer consulta que toque objetos governados.

---

## Modelo de Direitos

Capacidades atribuídas de forma independente, com hierarquia de função opcional via `parent_role_id`. `admin` concede todas. (REQ-042)

| Capacidade | Descrição |
| ----------- | ------------- |
| `source_registration` | Registrar fontes de dados |
| `table_registration` | Registrar tabelas, colunas |
| `create_relationship` | Definir relacionamentos de FK |
| `access_config` | Configurar RLS, mascaramento |
| `query_development` | Executar consultas |
| `write` | Invocar mutações registradas (portão grosseiro; veja Autorização de Mutação) |
| `full_results` | Ignorar limites de amostragem |
| `ignore_relationships` | Contornar a governança de relacionamento (V002). Detida apenas por `modeler` entre as funções de sistema, e totalmente ignorada em modo de alta segurança |
| `admin` | Superusuário — concede todas |

### Herança de Função

Funções podem herdar capacidades e acesso a domínio de uma função pai via `parent_role_id`. (REQ-215) A hierarquia é achatada na inicialização — funções filhas mesclam as capacidades e o acesso a domínio do pai com os seus. (REQ-215)

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

Cada coluna tem um modelo de permissão de quatro campos que controla acesso de leitura, escrita e mascaramento por função. (REQ-042, REQ-249)

### Visibilidade de Três Níveis

| Nível | Condição | Resultado |
| ------ | ----------- | -------- |
| **Oculta** | Função ausente de `visible_to` | Coluna ausente do SDL GraphQL |
| **Mascarada** | Função em `visible_to`, com regra de mascaramento, função ausente de `unmasked_to` | Coluna visível, mas dados mascarados no SQL |
| **Não mascarada** | Função em `visible_to` E função em `unmasked_to` (ou sem regra de mascaramento) | Acesso de leitura completo |

### Permissões de Escrita

| Campo | Vazio significa | Finalidade |
| ------- | ------------ | --------- |
| `visible_to` | Todas as funções podem ler | Controla quem vê a coluna (mascarada ou não) |
| `unmasked_to` | Nenhuma função vê sem máscara | Controla quem contorna o mascaramento |
| `writable_by` | Nenhuma função pode escrever | Controla quem pode alterar (INSERT/UPDATE) |

A permissão de escrita é aplicada no pipeline de mutação. Uma função ausente de `writable_by` recebe um erro 403 ao tentar escrever em uma coluna restrita. (REQ-033, REQ-034)

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

- `email`: o admin vê `alice@example.com` e pode editar; analyst/viewer veem `a***@example.com`
- `salary`: admin e hr veem o valor real; hr pode editar; todas as outras funções não veem a coluna
- `created_at`: todos podem ler, ninguém pode escrever

## Autorização de Mutação

Mutações registradas (GraphQL remoto, OpenAPI, gRPC, Hasura) passam por dois controles independentes. (REQ-867, REQ-868) Uma função só pode invocar uma mutação se detiver a capacidade global `write` E aparecer na lista `writable_by` daquela mutação. (REQ-868) Um `writable_by` vazio é negação por padrão — nenhuma função pode invocá-la. (REQ-867)

Mutações são classificadas como escritas por contrato, não por declaração de quem chama. (REQ-869) Um `SELECT` que referencia uma função do tipo mutação é promovido a escrita e sujeito ao mesmo controle duplo, de modo que quem chama não consegue invocar uma mutação disfarçando-a de leitura. (REQ-869) Reclassificar uma mutação como segura para leitura exige a capacidade `access_config` e é registrada como decisão de governança; não há renúncia por requisição. (REQ-870)

## Visibilidade de Esquema

Esquemas GraphQL por função ocultam conteúdo não autorizado: (REQ-039)

- **Acesso a domínio**: a função vê tabelas apenas nos domínios do seu `domain_access` (`"*"` = todos) (REQ-039)
- **Visibilidade de coluna**: colunas ausentes de `visible_to` para uma função são omitidas do SDL (REQ-039)
- Tabelas/colunas não autorizadas não aparecem no esquema (REQ-039)

## Segurança em Nível de Linha (RLS)

Injeção de cláusula WHERE SQL por tabela e por função. Aplicada após a compilação, antes da execução. (REQ-041, REQ-263)

```yaml
rls_rules:
  - table_id: orders
    role_id: analyst
    filter: "region = current_setting('provisa.user_region')"
```

O filtro entra com AND na cláusula WHERE da consulta. Funciona tanto para consultas quanto para mutações (UPDATE/DELETE). (REQ-035, REQ-041)

## Mascaramento em Nível de Coluna

O mascaramento é definido uma vez por coluna — é uma propriedade da coluna, não da função. O campo `unmasked_to` controla quais funções o contornam. (REQ-249)

| Tipo de Máscara | Tipos Suportados | Expressão SQL |
| ----------- | ---------------- | ---------------- |
| `regex` | String (varchar, char, text) | `REGEXP_REPLACE(col, pattern, replace)` |
| `constant` | Qualquer | Valor literal (NULL, 0, personalizado) |
| `truncate` | Data/Timestamp | `DATE_TRUNC(precision, col)` |

O mascaramento é empurrado para a projeção SELECT do SQL — o banco de dados retorna dados mascarados. (REQ-263) Dados não mascarados nunca cruzam a rede para funções mascaradas. (REQ-263) Colunas mascaradas também são bloqueadas em cláusulas `WHERE` e `HAVING` (guarda de predicado da camada 5) para impedir a dedução do valor não mascarado por filtragem. (REQ-263, REQ-531)

## Amostragem

Todas as funções veem resultados amostrados (padrão: 100 linhas), a menos que tenham a capacidade `full_results`. (REQ-554) Controlado pela variável de ambiente `PROVISA_SAMPLE_SIZE`. (REQ-554)

## Log de Auditoria

Toda consulta que toca um ativo de domínio é registrada no `query_audit_log`, que só aceita acréscimos. (REQ-596, REQ-613) Cada linha captura `tenant_id`, `user_id`, `role_id`, um hash SHA-256 do texto da consulta, `table_ids`, `source`, `status_code`, `duration_ms` e `logged_at`. (REQ-596) O texto da consulta nunca é armazenado literalmente — apenas seu hash. (REQ-596)

O log só aceita acréscimos no nível do banco de dados: regras do PostgreSQL bloqueiam `DELETE` e `UPDATE`. (REQ-596, REQ-613) Dois índices — `(tenant_id, logged_at)` e `(user_id, logged_at)` — sustentam consultas de conformidade por faixa de tempo, com escopo de inquilino e por usuário. (REQ-596, REQ-613)

Quando a criptografia está habilitada, a coluna do hash do texto da consulta é armazenada criptografada e só é descriptografada em leituras administrativas autorizadas. (REQ-689)

## Limitação de Taxa

Limites de taxa por função são configurados em `provisa.yaml`: máximo de requisições por segundo, máximo de assinaturas SSE simultâneas e máximo de fluxos Arrow Flight simultâneos. (REQ-369) Os limites são aplicados na camada de API, antes da compilação ou execução; requisições acima do limite são rejeitadas com HTTP 429 e um cabeçalho `Retry-After`. (REQ-369)

O serviço de consulta NL (`POST /query/nl`) tem um limite independente via `nl.rate_limit` (requisições por minuto por função). Requisições acima do limite são rejeitadas antes que qualquer chamada de LLM seja feita. (REQ-370)

O estado do limite de taxa fica no Redis (`cache.redis_url`) como um contador de janela deslizante — sem estado por instância — de modo que os limites valem em todas as instâncias horizontais do Provisa. (REQ-371)

## Autenticação

Provedores de autenticação plugáveis: (REQ-120)

| Provedor | Tipo de Token | Caso de Uso |
| ---------- | ----------- | ---------- |
| `none` | Cabeçalho X-Provisa-Role | Desenvolvimento |
| `basic` | Contas locais bcrypt + JWT | Implantações autocontidas |
| `firebase` | Token de ID do Firebase | Produção |
| `keycloak` | JWT do Keycloak | Empresarial |
| `oauth` | JWT OIDC | PingFed, Okta, Azure AD, Auth0 |
| `simple` | bcrypt + JWT | Testes |

Mapeamento de funções: claims de identidade → função do Provisa por regras configuráveis. (REQ-120) O campo `assignments_source` controla de onde vêm as atribuições de função: `claims` as lê dos claims do token JWT (padrão), `provisa` as lê do armazenamento interno de atribuições do Provisa. (REQ-551)

Um superusuário configurado em `provisa.yaml` (nome de usuário mais uma senha vinda de um segredo de ambiente) sempre recebe a função de administrador e todas as capacidades, independentemente do provedor configurado — um caminho de bootstrap para a configuração inicial. (REQ-125)

### Superfícies e credenciais

Toda interface autentica pelo mesmo contrato de provedor, então uma credencial que funciona em uma funciona em todas, onde quer que o protocolo consiga carregá-la. (REQ-124, REQ-1263) Esta tabela é a referência única; as documentações de cada interface não a repetem.

| Interface | Senha | Token do provedor | Token de acesso pessoal | Certificado de cliente (mTLS) |
| --------- | ---------- | ---------------- | ----------------------- | --------------------------- |
| HTTP (REST, JSON:API, GraphQL) | `Authorization: Basic` | `Authorization: Bearer` | `Authorization: Bearer` | por proxy terminador |
| pgwire | campo de senha (texto claro ou SCRAM) | campo de senha, implantações OIDC | campo de senha | sim |
| Bolt | esquema `basic` | esquema `bearer` | esquema `bearer` | sim |
| Arrow Flight | — | `token` no handshake ou no payload do ticket | idem | sim |
| gRPC | — | metadados `authorization` | metadados `authorization` | sim |
| MCP | — | `Authorization: Bearer` | `Authorization: Bearer` | por proxy terminador |

Onde uma célula traz `—`, o protocolo não carrega campo de nome de usuário para parear com uma senha; as formas de token cobrem o caso. O pgwire é o caso espelhado: o pacote de inicialização tem um único campo de segredo e nenhum esquema, então o que o segredo *é* escolhe o método — um PAT é reconhecido pelo prefixo, o segredo é lido como token bearer quando o provedor configurado é um provedor de token, e qualquer outra coisa é uma senha. A escolha é feita uma única vez — uma credencial que o validador selecionado recusa não é tentada de novo contra outro.

A matriz é garantida por `tests/unit/test_auth_surface_conformance.py`, que aciona o ponto de entrada real de validação de cada interface e falha quando uma nova interface é adicionada sem uma linha.

### Tokens de acesso pessoal

Um PAT é um segredo bearer de vida longa que um usuário emite para um cliente incapaz de completar um login interativo — um script, uma ferramenta de BI, um driver. (REQ-1263) Ele carrega sua própria organização e função, e toda interface o resolve pelo mesmo validador, de modo que nenhuma interface precisa saber o que é um PAT.

A forma de rede é `provisa_pat_` seguido de 43 caracteres base64 seguros para URL. O prefixo é o que roteia um segredo apresentado para o armazenamento de tokens em vez do provedor de identidade, e torna um token vazado localizável por grep em logs e repositórios.

- **Armazenamento** — apenas o SHA-256 do segredo é guardado. O segredo em si é exibido exatamente uma vez, na criação, e não pode ser recuperado. A listagem traz o prefixo de exibição e as marcações de ciclo de vida, nunca uma credencial funcional.
- **Emissão e revogação** — `POST /auth/tokens`, `GET /auth/tokens`, `DELETE /auth/tokens/{token_hash}`, além da seção de autoatendimento no próprio perfil do usuário na interface de administração. Emitir e revogar uma credencial é ato do portador do token.
- **Atribuição** — um PAT validado resolve para a conta do seu dono: id de usuário, e-mail e nome de exibição. Uma linha de auditoria ou relatório de uso escrito sob um PAT nomeia, portanto, a pessoa, não a credencial. Qual dos tokens dessa pessoa agiu é carregado à parte, em `raw_claims["token_name"]`.
- **Expiração** — um token pode carregar uma expiração; um token expirado é recusado na validação. Excluir a associação de um usuário revoga os tokens dele junto.

### SCRAM-SHA-256 no pgwire

Sob o provedor `basic`, definir `auth.scram: true` faz o pgwire anunciar SASL (código de autenticação 10) com o mecanismo `SCRAM-SHA-256`, de modo que a senha é provada em vez de enviada. (REQ-1394) A ligação de canal (`SCRAM-SHA-256-PLUS`) não é oferecida.

O SCRAM precisa de um verificador RFC 5802, que não pode ser derivado de um hash bcrypt. Um verificador é gravado sempre que uma senha passa em texto puro — cadastro, login, troca de senha, redefinição pelo administrador — então uma implantação que liga o SCRAM coleta verificadores conforme seus usuários autenticam na próxima vez, e a primeira conexão SCRAM de cada usuário vem depois da próxima digitação de senha. Um usuário que ainda não tem verificador recebe uma troca simulada indistinguível de uma real, de modo que a rede não revela quem já migrou.

### TLS mútuo

A verificação de certificado de cliente move o primeiro controle para o handshake TLS: quem chama sem um certificado assinado pela CA da implantação nunca alcança a camada de credenciais. (REQ-1228) Está disponível em pgwire, Bolt, gRPC e Arrow Flight — os quatro transportes que terminam seu próprio TLS.

| Variável | Significado |
| ---------- | --------- |
| `PROVISA_MTLS_CLIENT_CA` | Pacote PEM da(s) CA(s) autorizada(s) a assinar certificados de cliente |
| `PROVISA_MTLS_MODE` | `required` (o padrão assim que uma CA é definida) ou `optional` |
| `PROVISA_MTLS_BIND_PRINCIPAL` | Quando verdadeiro, o common name do certificado precisa ser igual ao nome de usuário com que a conexão então se autentica |

Sobreposições por protocolo seguem a mesma nomenclatura das configurações de TLS. Nada é deduzido: um modo definido sem CA recusa iniciar, e um modo não reconhecido recusa iniciar em vez de ser lido como o vizinho mais seguro — uma implantação que acredita exigir certificados de cliente e não exige está em pior situação do que uma que não sobe.

### Limitação de tentativas de login

A adivinhação de senha independe do protocolo: a mesma conta pode ser martelada por HTTP, pgwire e Bolt. O contador, portanto, vive na camada de validação de credenciais, e não em uma interface específica, de modo que um bloqueio conquistado em qualquer lugar é aplicado em todos. (REQ-1393)

Está ligada por padrão — cinco falhas em cinco minutos bloqueiam o sujeito por quinze minutos — e é ajustada em `auth.login_throttle`. Um sujeito bloqueado é recusado antes que a credencial seja sequer examinada, e uma autenticação bem-sucedida limpa o histórico daquele sujeito.

A chave é o principal que o protocolo carrega. Uma interface só de bearer não carrega principal, então a chave é um digest da própria credencial; o que isso impede é a repetição sem limite de um token comprometido. O armazenamento é por processo, então uma implantação com vários workers de API permite até `max_attempts` por worker — a limitação é um freio na adivinhação, não uma cota distribuída.

### Endereçar uma organização em um protocolo de fio

Sob multi-inquilino, uma organização é endereçada pelo nome de host: `acme.provisa.dev` é a organização `acme`. Por HTTP esse nome chega no cabeçalho `Host`. Um cliente pgwire ou Bolt não envia tal cabeçalho, mas envia o nome de host que discou no ClientHello do TLS, e o Provisa lê a organização de lá. (REQ-1234) Nada muda no cliente — conectar-se a `acme.provisa.dev` é tudo o que é preciso.

O nome de host é um pedido, não uma concessão. Ele chega ao mesmo resolvedor a que o cabeçalho `Host` chega, o qual recusa qualquer organização de que o principal autenticado não seja membro nem detenha o direito entre organizações. Discar um nome de host em que você não tem associação não alcança dado algum. Um cliente que se conectou por endereço IP não envia nome de host e resolve sua organização apenas pelo principal, o que é o caso de toda conexão em uma implantação de organização única.

gRPC, Arrow Flight e MCP entregam seus certificados a bibliotecas que não expõem callback de nome de host; esses transportes nomeiam uma organização com o cabeçalho de metadados `x-provisa-org`.

## Modo de alta segurança

`security.mode: high` em `provisa.yaml` afirma uma garantia: o backend do Provisa nunca manipula dados em texto puro. (REQ-693) Toda coluna que importa é criptografada na fonte, e só um cliente que detém a chave de descriptografia consegue lê-la. Essa garantia tem consequências para as quais uma implantação precisa se planejar.

**O que o modo faz:**

- **Endpoints de dados exigem prova de descriptografia no cliente.** Tudo sob `/data/` retorna 403 a menos que quem chama apresente o cabeçalho `X-Provisa-KMS-Key` — a marca de um cliente JDBC ou Python configurado para descriptografar localmente. Um navegador ou um consumidor REST em texto puro não carrega tal chave e é recusado. O portão é uma negação por padrão sobre toda a árvore: uma rota adicionada amanhã já nasce protegida, e uma exceção precisa ser justificada.
- **Endpoints de metadados de esquema seguem abertos.** `/data/sdl`, `/data/introspection`, `/data/schema-version`, `/data/domains`, `/data/proto` e `/data/compile` não retornam dados de linha, e um cliente precisa ler o esquema — inclusive quais campos são `@encrypted` — antes mesmo de conseguir se conectar.
- **gRPC e Arrow Flight seguem servindo, sob a mesma prova.** São os transportes que os clientes que criptografam de fato usam; fechá-los deixaria uma implantação de alta segurança sem nenhum protocolo de fio. Uma chamada de dados em qualquer um dos dois precisa carregar a mesma chave KMS como metadado da chamada.
- **pgwire, Bolt e MCP não iniciam.** Nenhum dos três tem um handshake por conexão capaz de carregar um contexto de descriptografia: um conjunto de linhas pgwire e um resultado Cypher trafegam em texto puro, e uma chamada de ferramenta MCP entrega seus resultados a um modelo como texto. Uma porta configurada para qualquer um deles é recusada na inicialização em vez de servida.
- **A guarda de relacionamento não pode ser contornada.** `ignore_relationships` e `relationship_guard: false` são ambos ignorados; veja [Governança de relacionamento](#governanca-de-relacionamento-v002).

**Verificar se uma implantação está no modo:** o log de inicialização o nomeia, uma requisição a `/data/sql` sem chave KMS responde 403 com uma mensagem que nomeia o REQ-693, e as portas pgwire, Bolt e MCP não estão escutando.

## Hook de Aprovação ABAC

Um hook externo de política, opcional, que dispara antes da execução da consulta. (REQ-203) Quando configurado, o Provisa chama seu motor de política com a identidade do usuário, as funções, as tabelas, as colunas e a operação. A resposta determina se a consulta prossegue. (REQ-203)

### Escopo

O hook só dispara quando a consulta toca uma tabela ou fonte no escopo — sobrecarga zero para todo o resto. (REQ-204)

| Configuração | Efeito |
| -------- | -------- |
| `auth.approval_hook.scope: all` | Toda consulta dispara o hook |
| `sources[].approval_hook: true` | Todas as tabelas daquela fonte disparam o hook |
| `tables[].approval_hook: true` | Aquela tabela dispara o hook |

### Protocolos

Três transportes são suportados: (REQ-246)

| Tipo | Caso de uso | Campo de configuração |
| ------ | ---------- | ------------- |
| `webhook` | Qualquer serviço de política com HTTP (OPA, próprio) | `url` |
| `unix_socket` | OPA ou sidecar de política na mesma máquina | `socket_path` + `url` |
| `grpc` | Serviço de política colocalizado de alta vazão | `url` (host:porta) |

O transporte gRPC usa o contrato `provisa.auth.ApprovalService` definido em `provisa/auth/approval.proto`. Implemente este serviço no seu motor de política: (REQ-246)

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

O canal gRPC é persistente — um canal por instância do Provisa, reutilizado em todas as chamadas àquele endpoint de hook. (REQ-555)

### Requisição / Resposta

Os três transportes carregam o mesmo payload: (REQ-246)

| Campo | Tipo | Descrição |
| ------- | ------ | ------------- |
| `user` | string | Identidade do usuário autenticado |
| `roles` | string[] | Funções do usuário no Provisa |
| `tables` | string[] | IDs de tabela referenciados na consulta |
| `columns` | string[] | Colunas selecionadas na consulta |
| `operation` | string | `"query"` ou `"mutation"` |

Os transportes webhook e socket Unix trocam JSON. A resposta precisa incluir `approved` (bool) e, opcionalmente, `reason` (string). (REQ-246)

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

Em caso de timeout ou erro de transporte, aplica-se a política de `fallback`. (REQ-247) Um disjuntor (padrão: abre após 5 falhas consecutivas, meio aberto após 30s) evita falhas em cascata a partir de um endpoint de hook lento. (REQ-556)

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

Credenciais usam a sintaxe `${env:VAR_NAME}`, resolvida em runtime. (REQ-557) Senhas nunca são armazenadas no banco de dados de configuração. (REQ-557)

Para o serviço de segredos completo — cofres, sintaxe de referência e provedores — veja [Segredos](secrets.md).
