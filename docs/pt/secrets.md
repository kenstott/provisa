# Segredos {: #secrets }

**Nomes entram. Valores nunca saem.**

Nenhum endpoint da API devolve o valor de um segredo guardado, e nenhuma UI oferece um botão "mostrar". Quem perdeu um valor o substitui, com a mesma chamada e o mesmo formulário que o criaram. Não é uma decisão de política: o caminho de leitura simplesmente não existe no código.

---

## Sintaxe de referência {: #reference-syntax }

Três formas de referência são válidas onde quer que o Provisa resolva credenciais:

| Forma | Resolve a partir de | Quem pode usar |
| ------ | -------------- | --------------- |
| `${env:VAR_NAME}` | O ambiente do processo do servidor | Somente configuração de implantação |
| `${secret:NAME}` | O cofre da organização — compartilhado por todos os membros | Qualquer campo que aceite uma referência de credencial |
| `${user:NAME}` | O cofre pessoal de quem está agindo | Qualquer campo que aceite uma referência de credencial |

Num deployment multi-inquilino, mantenha as credenciais da plataforma num serviço de segredos e fora das variáveis de ambiente. Um admin de inquilino pode escrever `${env:...}` num campo de credencial próprio, então qualquer valor do ambiente do processo está ao alcance de todo inquilino. Deixe no ambiente só a credencial do próprio backend de segredos.

A resolução é de falha fechada de ponta a ponta. Um nome de provedor desconhecido, um nome não definido e um backend inacessível levantam erro. Uma referência que não pôde ser resolvida nunca é silenciosamente substituída por uma string vazia.

### Formato do nome {: #name-format }

Nomes de segredos precisam corresponder a `[A-Za-z_][A-Za-z0-9_]*`: letras, dígitos e sublinhados, começando por letra ou sublinhado. O motivo é prático. `${secret:NAME}` é analisado pela gramática de referências, que lê até a `}` de fechamento, então um nome com chave, espaço ou dois-pontos geraria uma referência analisada como outra coisa.

---

## Dois cofres, um serviço {: #two-vaults-one-service }

Toda organização tem dois cofres. Ambos vivem dentro do mesmo serviço de segredos.

**Cofre da organização.** A credencial que um administrador da organização guarda aqui é compartilhada, então todo membro que referencia `${secret:DATABASE_TOKEN}` recebe o mesmo valor. Use-o para credenciais que a *organização* possui, como uma senha de banco de dados compartilhada, uma chave de conta de serviço ou um token de implantação. Ler ou escrever no cofre da organização exige a capacidade `org_settings`.

**Cofre pessoal.** Uma credencial guardada aqui pertence a exatamente uma pessoa. Quando duas pessoas têm cada uma um `GIT_TOKEN`, `${user:GIT_TOKEN}` resolve para o de quem está agindo, então o mesmo texto de referência entrega a cada pessoa a sua própria credencial. Quem não guardou nada recebe um erro, nunca o valor de outra pessoa. Nenhuma capacidade protege o cofre pessoal, porque ter a própria credencial não é um privilégio que um administrador concede, e nenhuma sintaxe de requisição nomeia o cofre de outra pessoa.

O escopo faz parte da própria referência, e nenhuma permissão a envolve. `${secret:NAME}` e `${user:NAME}` nunca respondem um pelo outro.

---

## Escolhendo um serviço de segredos {: #choosing-a-secrets-service }

**Administração → Segurança → Serviço de segredos.** Qualquer pessoa com a capacidade `platform_settings` vê o painel. Ele lista todo backend que a build conhece, com ou sem o SDK instalado, e uma linha acinzentada nomeia o pacote Python que falta em vez de esconder a opção.

Cinco backends acompanham o produto:

| Chave | Rótulo | Requer |
| ----- | ------- | ------- |
| `provisa` | Provisa (built-in, encrypted) | Nada; este é o padrão |
| `hashicorp_vault` | HashiCorp Vault (KV v2) | `hvac` |
| `aws_secrets_manager` | AWS Secrets Manager | `boto3` |
| `gcp_secret_manager` | Google Secret Manager | `google-cloud-secret-manager` |
| `azure_key_vault` | Azure Key Vault (secrets) | `azure-keyvault-secrets` |

A seleção falha de forma fechada: um backend desconhecido ou indisponível interrompe a inicialização com um erro, e o Provisa nunca recorre a outro em silêncio.

### A credencial do próprio backend {: #the-backends-own-credential }

A credencial de conexão de um backend central é configuração do processo e vem só de `${env:...}`. `${secret:...}` não pode fornecê-la, porque um serviço de segredos cuja própria credencial vive dentro dele nunca poderia ser aberto. A cadeia de confiança termina no ambiente do host por design. O Provisa resolve cada valor de configuração de um backend a partir do ambiente antes de construir o backend.

Por exemplo, uma configuração do Vault em `provisa.yaml`:

```yaml
secrets:
  provider: hashicorp_vault
  hashicorp_vault:
    url: https://vault.internal:8200
    token: ${env:VAULT_TOKEN}   # process env only — never ${secret:...}
    mount: secret
```

### Serviço central vs. built-in {: #central-service-vs-built-in }

Com um serviço central configurado, o Provisa lê dele e nunca escreve nele. Criar e apagar entradas cabe às ferramentas do próprio serviço, então a página de segredos diz isso e não oferece botão de criar.

Com o backend embutido `provisa` ativo, a página de segredos aceita escrita completa. Você cria, substitui e apaga segredos pela UI ou pela API.

---

### Os segredos de uma organização em um serviço central {: #one-organizations-secrets-in-a-central-service }

Uma implantação que hospeda muitas organizações e lê um serviço central o lê, para cada organização, dentro do prefixo próprio dessa organização. Uma referência continua sendo o nome curto (`${secret:db#password}`); o prefixo é adicionado na leitura.

| Serviço | Onde fica o segredo `db` da organização `acme` |
|---|---|
| HashiCorp Vault (KV v2) | `orgs/acme/db` |
| AWS Secrets Manager | `orgs/acme/db` |
| GCP Secret Manager | `org-acme--db` |
| Azure Key Vault | `o4-acme-db` (`o`, o comprimento do id da organização, o id com `_` escrito como `-`, depois o nome) |

Uma referência é recusada, pelo nome, quando:

- seu nome sairia do prefixo: começa com `/`, contém um segmento `..` ou uma barra invertida, ou contém `/` no GCP ou no Azure, cujos nomes são planos;
- seu nome é longo demais para o serviço depois de prefixado (GCP 255 caracteres, Azure 127); isso é recusado quando a referência é salva, com o limite informado;
- nenhuma organização está vinculada à operação que a lê.

Uma implantação de uma única organização lê uma referência exatamente como está escrita, sem prefixo: o serviço ao qual está ligada é o daquela organização, e nada precisa ser movido.

**Passar para o prefixo.** Em uma implantação de muitas organizações, cada segredo que uma organização referencia precisa estar sob o prefixo dessa organização. Copie cada um para lá com as ferramentas próprias do serviço e depois encurte a referência para o nome dentro do prefixo (`${secret:teams/data/db}`, guardado em `orgs/acme/db`, passa a ser `${secret:db}`). Uma referência cujo segredo não foi movido falha como não encontrada; ela não é lida de onde estava.

## O armazenamento built-in do Provisa {: #provisas-built-in-store }

É o padrão quando nenhum serviço central está configurado. Cada linha de `secrets_store` guarda um blob de envelope cifrado. A coluna `value` é binária, e a chave de decifração fica no ambiente do processo, fora do banco de dados. Uma cópia do plano de controle sem a chave mestra do deployment contém texto cifrado e nada mais.

A criptografia nunca é opcional. Sem uma chave de criptografia configurada para todo o processo, o armazenamento usa um chaveiro local. Se o host não tem chaveiro para guardar uma chave, o armazenamento se recusa a escrever, e o valor nunca é guardado em claro.

**Formato de armazenamento**:

| Coluna | Tipo | Finalidade |
| -------- | ------ | --------- |
| `org_id` | Text | A organização dona deste segredo |
| `owner_id` | Text | `"*"` para o cofre da organização; id de usuário para o cofre pessoal |
| `name` | Text | O nome da referência |
| `value` | LargeBinary | Blob de envelope criptografado |
| `description` | Text | Para que serve o segredo — nunca derivado do valor |
| `updated_by` | Text | Quem o definiu por último |

Uma organização que criou a própria chave de criptografia tem seus segredos envolvidos por essa chave; caso contrário, são envolvidos pela chave da implantação. Veja [Criptografia em repouso](security.md#encryption-at-rest-and-the-organizations-own-keys).

A coluna `value` não é selecionada em nenhuma consulta de listagem.

---

## Endpoints da API {: #api-endpoints }

Todas as rotas ficam sob `/admin/orgs/{org_id}`. O cofre da organização exige `org_settings` nessa organização. O cofre pessoal não exige capacidade, porque o dono é lido da identidade autenticada, e nenhum parâmetro de requisição nomeia o cofre de outra pessoa.

| Método | Caminho | O que faz |
| -------- | ------ | ------------- |
| `GET` | `/secrets` | Lista nomes e referências do cofre da organização |
| `PUT` | `/secrets/{name}` | Cria ou substitui um segredo da organização |
| `DELETE` | `/secrets/{name}` | Exclui um segredo da organização |
| `GET` | `/my-secrets` | Lista os nomes e referências pessoais de quem chama |
| `PUT` | `/my-secrets/{name}` | Cria ou substitui um segredo de quem chama |
| `DELETE` | `/my-secrets/{name}` | Exclui um segredo de quem chama |

Toda resposta devolve metadados e nunca o valor: o nome, a descrição, `updated_at`, `updated_by` e a string `reference` para colar. O corpo do `PUT` leva `value` (obrigatório) e `description` (opcional). Substituir é a mesma chamada que criar, porque o nome é a identidade e não há um ID separado.

Toda escrita fica registrada no log de auditoria. A entrada nomeia quem agiu e o segredo. Nem o valor nem seu comprimento são registrados.

---

## Onde `${secret:NAME}` é resolvido {: #where-secretname-resolves }

A resolução acontece dentro de uma operação vinculada. Nada é resolvido na importação nem na inicialização. O armazenamento lê e decifra os segredos da organização uma vez, quando a operação começa, e guarda o mapa enquanto ela dura. Fora de uma operação vinculada, `${secret:NAME}` gera um erro.

Dois pontos de chamada estabelecem o vínculo:

**Operações remotas do Git.** A URL remota do repositório de uma organização pode conter uma referência `${secret:...}` ou `${user:...}`, como um token de push embutido na URL. O roteador de ambientes vincula então o cofre da organização e o cofre pessoal do usuário que age em torno da chamada ao git. Com a forma `${user:GIT_TOKEN}`, um commit fica sob a credencial de quem fez o push. Nenhuma conta de serviço compartilhada participa.

**Leitura de chaves de API de fornecedores de IA.** Quando o Provisa lê a chave do fornecedor de LLM de uma organização e essa chave está guardada como referência `${secret:NAME}`, ele vincula o cofre da organização para essa requisição. A referência é resolvida na saída, e o texto da referência nunca chega ao fornecedor.

---

## Chaves de fornecedor de IA da organização como referências de segredo {: #org-ai-vendor-keys-as-secret-references }

A chave de fornecedor de IA de uma organização (Anthropic, OpenAI e outros) pode ser guardada como referência `${secret:NAME}` em vez de uma chave literal.

Guarde a chave no cofre da organização primeiro:

```
PUT /admin/orgs/{org_id}/secrets/OPENAI_KEY
{ "value": "sk-...", "description": "OpenAI production key" }
```

Depois configure a IA da organização para referenciá-la:

```
vendor key field → ${secret:OPENAI_KEY}
```

A referência é guardada cifrada em `org_secrets`. No momento da consulta, o Provisa resolve `${secret:OPENAI_KEY}` contra o cofre da organização e entrega a chave literal ao SDK do fornecedor. Rotacionar a entrada do cofre vale na hora, sem mudar a configuração da organização.

---

## As credenciais dos stores da própria implantação {: #the-deployments-own-store-credentials }

As senhas dos stores que o Provisa executa (PostgreSQL, Redis, MinIO, a conta de administrador do Grafana e o segredo compartilhado do cluster Trino) não são segredos em nenhum dos dois cofres. Elas são geradas por implantação e mantidas pela implantação: em `~/.provisa/store-credentials.env` (modo 600), no estado do Terraform, ou no Secret do Kubernetes que `credentials.existingSecret` do chart Helm nomeia. Veja [Fortalecimento da implantação](security.md#deployment-hardening).

---

## Acesso do administrador da plataforma {: #platform-admin-access }

Um admin de plataforma que opera o plano de controle não consegue ler os valores secretos de nenhuma organização. A guarda `org_settings` recusa de saída `cross_org` e o bypass de plataforma, porque administrar o ciclo de vida de uma organização não dá leitura das credenciais que ela guarda. O servidor impõe isso por conta própria, seja o que for que a UI mostre.

---

## Veja também {: #see-also }

- [Modelo de segurança](security.md) — controle de acesso em camadas, autenticação e log de auditoria
- [Referência de configuração](configuration.md) — sintaxe `${env:VAR}` para credenciais em nível de processo
