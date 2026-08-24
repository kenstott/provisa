# Segredos

**Nomes entram. Valores nunca saem.**

Nenhum endpoint de API retorna um valor de segredo armazenado. Nenhuma tela oferece um botão "mostrar". Quem perdeu um valor o substitui — essa é a mesma chamada que o criou, pelo mesmo formulário. Isto não é uma decisão de política: o caminho de leitura simplesmente não existe no código. (REQ-1558)

---

## Sintaxe de referência

Três formas de referência são válidas onde quer que o Provisa resolva credenciais:

| Forma | Resolve a partir de | Quem pode usar |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | O ambiente do processo do servidor | Somente configuração de implantação |
| `${secret:NAME}` | O cofre da organização — compartilhado por todos os membros | Qualquer campo que aceite uma referência de credencial |
| `${user:NAME}` | O cofre pessoal de quem está agindo | Qualquer campo que aceite uma referência de credencial |

A resolução é de falha fechada de ponta a ponta. Um nome de provedor desconhecido, um nome não definido e um backend inacessível levantam erro. Uma referência que não pôde ser resolvida nunca é silenciosamente substituída por uma string vazia. (REQ-1557) [tool-verified: `provisa/core/secrets.py:92-117`]

### Formato do nome

Nomes de segredo devem corresponder a `[A-Za-z_][A-Za-z0-9_]*` — letras, dígitos e sublinhados, começando por letra ou sublinhado. A restrição é prática: `${secret:NAME}` é analisado pela gramática de referência, que lê até o `}` de fechamento. Um nome contendo chave, espaço ou dois-pontos produziria uma referência que é analisada como outra coisa. [tool-verified: `provisa/core/secrets_store.py:61`]

---

## Dois cofres, um serviço

Toda organização tem dois cofres. Ambos vivem dentro do mesmo serviço de segredos. (REQ-1560)

**Cofre da organização** — A credencial que um administrador da organização guarda aqui é compartilhada. Todo membro que referencia `${secret:DATABASE_TOKEN}` recebe o mesmo valor. Isto é para credenciais que a *organização* possui: uma senha de banco de dados compartilhada, uma chave de conta de serviço, um token de implantação. O cofre da organização exige a capacidade `org_settings` para leitura ou escrita.

**Cofre pessoal** — Uma credencial guardada aqui pertence a exatamente uma pessoa. Quando duas pessoas mantêm cada uma um `GIT_TOKEN`, `${user:GIT_TOKEN}` resolve para o de quem estiver agindo. O mesmo texto de referência entrega a cada pessoa a sua própria credencial. Quem não guardou nada recebe um erro, não o valor de outra pessoa. Nenhuma capacidade controla o cofre pessoal — guardar a sua própria credencial não é um privilégio que um administrador concede. E não há sintaxe de requisição para nomear o cofre de outra pessoa. [tool-verified: `provisa/api/admin/secrets_router.py:86-103`]

O escopo faz parte da referência, não é uma permissão em torno dela. `${secret:NAME}` e `${user:NAME}` nunca respondem um pelo outro.

---

## Escolhendo um serviço de segredos

**Administração → Segurança → Serviço de segredos.** O painel é visível a quem detém a capacidade `platform_settings`. Todo backend que o build conhece é listado, esteja o SDK instalado ou não. Uma linha esmaecida diz qual pacote Python está faltando — o painel o nomeia em vez de esconder a opção por completo.

Cinco backends acompanham o produto:

| Chave | Rótulo | Requer |
| ----- | ------- | ------- |
| `provisa` | Provisa (built-in, encrypted) | Nada; este é o padrão |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (secrets) | `azure-keyvault-secrets` |

[tool-verified: `provisa/core/secrets_registry.py:161-299`]

A seleção é de falha fechada: um backend desconhecido ou indisponível levanta erro na inicialização em vez de recair silentemente em outro. (REQ-1557)

### A credencial do próprio backend

A credencial de conexão de um backend central é configuração de processo. Ela vem de `${env:...}` apenas — nunca de `${secret:...}`. Um serviço de segredos cuja própria credencial vive dentro dele mesmo não pode ser aberto, então a cadeia de confiança termina no ambiente do host por design. O registro impõe isto: qualquer valor de configuração na especificação de um backend é resolvido com `providers=("env",)` antes de o backend ser construído. [tool-verified: `provisa/core/secrets_registry.py:128-141`]

Exemplo — configuração do Vault em `provisa.yaml`:

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Serviço central vs. built-in

Quando um serviço central está configurado, o Provisa lê dele mas não escreve nele. O serviço central é dono da criação e da exclusão de entradas — essas operações pertencem à ferramenta dele. A página de Segredos diz isso e não oferece botão de criação. (REQ-1557)

Quando o backend `provisa` built-in está ativo, a página de Segredos é totalmente gravável: criar, substituir e excluir pela UI ou via API.

---

## O armazenamento built-in do Provisa

O padrão quando nenhum serviço central está configurado. Cada linha em `secrets_store` guarda um blob de envelope criptografado — a coluna `value` é binária, não texto, e a chave de descriptografia vive no ambiente do processo, não no banco de dados. Uma cópia do plano de controle sem a chave mestra da implantação guarda texto cifrado e nada mais. (REQ-1558)

A criptografia nunca é opcional. Quando nenhuma chave de criptografia de processo está configurada, o armazenamento recorre a um keychain local. Se o host não tiver keychain para guardar uma chave, o armazenamento se recusa a escrever em vez de guardar o valor às claras. [tool-verified: `provisa/core/secrets_store.py:130-159`]

**Formato de armazenamento** [tool-verified: `provisa/core/schema_admin.py:493-505`]:

| Coluna | Tipo | Finalidade |
| -------- | ------ | --------- |
| `org_id` | Text | A organização dona deste segredo |
| `owner_id` | Text | `"*"` para o cofre da organização; id de usuário para o cofre pessoal |
| `name` | Text | O nome da referência |
| `value` | LargeBinary | Blob de envelope criptografado |
| `description` | Text | Para que serve o segredo — nunca derivado do valor |
| `updated_by` | Text | Quem o definiu por último |

A coluna `value` não é selecionada em nenhuma consulta de listagem. [tool-verified: `provisa/core/secrets_store.py:214-235`]

---

## Endpoints da API

Todas as rotas ficam sob `/admin/orgs/{org_id}`. O cofre da organização exige `org_settings` naquela organização. O cofre pessoal não exige capacidade alguma — o dono é lido da identidade autenticada; não há parâmetro de requisição para nomear o cofre de outra pessoa.

| Método | Caminho | O que faz |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | Lista nomes e referências do cofre da organização |
| `PUT` | `/secrets/{name}` | Cria ou substitui um segredo da organização |
| `DELETE` | `/secrets/{name}` | Exclui um segredo da organização |
| `GET` | `/my-secrets` | Lista os nomes e referências pessoais de quem chama |
| `PUT` | `/my-secrets/{name}` | Cria ou substitui um segredo de quem chama |
| `DELETE` | `/my-secrets/{name}` | Exclui um segredo de quem chama |

Toda resposta devolve metadados — nome, descrição, `updated_at`, `updated_by` e a string de `reference` para colar — mas nunca o valor. O corpo do `PUT` carrega `value` (obrigatório) e `description` (opcional). Uma substituição é a mesma chamada que uma criação: o nome é a identidade, não um ID à parte.

Toda escrita é registrada no log de auditoria. A entrada do log nomeia o ator e o nome do segredo. O valor não é registrado, nem mesmo o seu tamanho. [tool-verified: `provisa/api/admin/secrets_router.py:106-117`]

---

## Onde `${secret:NAME}` é resolvido

A resolução acontece dentro de uma operação vinculada a um contexto, não no momento do import nem na inicialização. O armazenamento lê e descriptografa os segredos da organização uma vez no início dessa operação e mantém o mapa em um `ContextVar` durante a sua duração. Fora de uma operação vinculada, `${secret:NAME}` levanta erro. (REQ-1557) [tool-verified: `provisa/core/secrets_store.py:269-290`]

Dois pontos de chamada estabelecem o vínculo:

**Operações de remote git.** Quando a URL do remote do repositório de uma organização contém uma referência `${secret:...}` ou `${user:...}` — por exemplo, um token de push embutido na URL — o roteador de ambientes vincula tanto o cofre da organização quanto o cofre pessoal do usuário que está agindo em torno da chamada git. A forma `${user:GIT_TOKEN}` significa que um commit chega sob a credencial de quem o enviou, não de uma conta de serviço compartilhada. [tool-verified: `provisa/api/admin/environments_router.py:1263`]

**Leituras da chave de API do fornecedor de IA.** Quando o Provisa lê a chave do fornecedor de LLM de uma organização e essa chave está guardada como referência `${secret:NAME}`, `bound_to_request_org` estabelece o cofre da organização para aquela requisição. A referência é resolvida na saída; o próprio texto da referência nunca é enviado ao fornecedor. (REQ-1580) [tool-verified: `provisa/core/org_secrets.py:76-79`]

---

## Chaves de fornecedor de IA da organização como referências de segredo

A chave de fornecedor de IA de uma organização (Anthropic, OpenAI e outros) pode ser guardada como referência `${secret:NAME}` em vez de uma chave literal. (REQ-1580)

Guarde a chave no cofre da organização primeiro:

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Depois configure a IA da organização para referenciá-la:

```
vendor key field → ${secret:OPENAI_KEY}
```

A referência é guardada criptografada em `org_secrets`. No momento da consulta o Provisa resolve `${secret:OPENAI_KEY}` contra o cofre da organização e entrega a chave literal ao SDK do fornecedor. Girar a entrada do cofre tem efeito imediato — nenhuma mudança de configuração do lado das configurações da organização. [tool-verified: `provisa/core/org_secrets.py:64-79`]

---

## Acesso do administrador da plataforma

Um administrador da plataforma operando o plano de controle não tem leitura de nenhum valor de segredo de nenhuma organização. A guarda `org_settings` recusa explicitamente `cross_org` e o bypass de plataforma: administrar o ciclo de vida de uma organização não é uma leitura das credenciais que aquela organização guarda. O servidor impõe isto independentemente da UI. (REQ-1361) [tool-verified: `provisa/api/admin/secrets_router.py:53-83`]

---

## Veja também

- [Modelo de segurança](security.md) — controle de acesso em camadas, autenticação e log de auditoria
- [Referência de configuração](configuration.md) — sintaxe `${env:VAR}` para credenciais em nível de processo
