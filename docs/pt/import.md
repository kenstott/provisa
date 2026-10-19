# Importando do Hasura {: #importing-from-hasura }

O Provisa consegue converter metadados existentes do Hasura em um `config.yaml` do Provisa, preservando tabelas rastreadas, relacionamentos, permissões, e esquemas remotos.

## Importação interativa (Admin → Import Hasura Config) {: #interactive-import-admin-import-hasura-config }

A superfície de admin roda os mesmos conversores, então uma importação não exige acesso a shell nem
ida e volta de arquivo de config. Exige a capacidade `org_settings`; a importação é aplicada na
organização em que a sessão está atuando.

1. **Upload.** Escolha um diretório de metadados do Hasura v2 zipado, um projeto DDN zipado, uma
   exportação de metadados consolidada (`.yaml`/`.json`, incluindo o envelope `{resource_version, metadata}`
   que a API de metadados retorna), ou um único `.hml`. Deixe o formato em *Detect automatically*,
   a menos que o upload seja ambíguo.
2. **Mapear domínios** (opcional). Cada par mapeia um esquema v2 ou um subgrafo DDN para um domínio
   Provisa; o que não for mapeado mantém o nome original.
3. **Converter e visualizar.** O servidor converte e retorna as contagens, o relatório da conversão (veja [O relatório](#the-report)) e a configuração gerada. Nada é gravado nesta etapa.
   configuração gerada. Nada é escrito nesta etapa.
4. **Revisar e editar.** A configuração é editável no local: detalhes de conexão, nomes de domínio, nomes de função. O que você aplica é o que é mostrado.
5. **Aplicar.** *Aplicar a esta organização* carrega a configuração e reconstrói os esquemas da organização. Uma importação sempre faz merge com o que a organização já tem.

Endpoints: `POST /admin/import/hasura/preview` e `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Exportar Metadados {: #export-metadata }

Os dois layouts de exportação são convertidos.

**O diretório de metadados da CLI.** `hasura metadata export` grava um diretório. `databases/databases.yaml` lista cada fonte com seu tipo e sua conexão, e aponta para suas tabelas e funções por meio de índices `!include`:

```bash
hasura metadata export
```

```yaml
# metadata/databases/databases.yaml
- name: default
  kind: postgres
  configuration:
    connection_info:
      database_url:
        from_env: HASURA_GRAPHQL_DATABASE_URL
  tables: "!include default/tables/tables.yaml"
  functions: "!include default/functions/functions.yaml"
```

O layout plano (`tables.yaml` e `actions.yaml` na raiz) também é convertido.

**O documento único da API de metadados.** A API de metadados retorna toda a exportação como um único objeto, com a `configuration` por fonte que o layout em diretório não tem onde colocar:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

Tanto o objeto simples quanto o envelope `{resource_version, metadata}` são aceitos. A importação de administração aceita esta forma (veja [Importação interativa](#interactive-import-admin-import-hasura-config)); o conversor de linha de comando lê apenas diretórios.

### Converter {: #convert }

O conversor de linha de comando lê um **diretório** de metadados do Hasura (o layout produzido por `hasura metadata export`, ou o layout plano `tables.yaml` / `actions.yaml`) e grava uma configuração do Provisa:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omita `-o` para gravar a configuração em stdout. Quando algo fica para refazer à mão, o relatório é impresso em stderr.

Flags:

| Flag | Finalidade |
| ------ | --------- |
| `-o`, `--output` | Caminho de saída YAML (padrão: stdout) |
| `--source-overrides` | Arquivo YAML com sobreposições de conexão por fonte (host, porta, credenciais), indexado pelo nome da fonte |
| `--domain-map` | Mapeamentos de esquema para domínio como pares `SCHEMA=DOMAIN` |
| `--auth-env-file` | Arquivo `.env` com config de autenticação; converte JWT/JWK, segredo de admin, e mapa de claims |
| `--dry-run` | Analisa e valida sem escrever saída |

### O Relatório {: #the-report }

Toda conversão separa o que encontra em três partes. Leia-as nesta ordem.

1. **Levado como está.** O objeto tem uma contrapartida direta no Provisa e é convertido sem mudar de significado: fontes, tabelas, relacionamentos aprovados, permissões de select, cron triggers.
2. **Mapeado para um recurso melhor do Provisa.** O Provisa tem um recurso que faz o trabalho melhor que o do Hasura, então o objeto vira esse recurso. Cada item diz por quê. Event triggers, actions, remote schemas, nomes de campo raiz adicionais e funções herdadas de várias funções ficam aqui.
3. **Refazer à mão.** O objeto não tem equivalente no Provisa, ou falta à exportação o que a conversão precisa. Cada item diz o que fazer. Estes são os avisos.

A pré-visualização retorna as três partes como listas. O conversor de linha de comando as imprime em stderr como `Carried as`, `Mapped to` e `Redo by hand`, cada uma com uma contagem. Uma conversão sem nada na terceira parte está concluída.

### O Que É Convertido {: #what-gets-converted }

| Conceito Hasura | Parte | Resultado no Provisa |
| --------------- | ---- | -------------- |
| Fonte (`kind`, `connection_info`) | levado | `sources[]`. Uma conexão escrita por extenso na exportação é convertida em host, porta, banco de dados e credenciais. Uma lida de uma variável de ambiente não é adivinhada (veja [Conexões e domínios](#connections-and-domains-on-the-import-tab)). |
| Tabela rastreada | levado | Uma tabela registrada. Seu nome personalizado (ou seu campo raiz `select`) vira o alias da tabela. |
| Nomes de coluna personalizados (`column_config`, ou o antigo `custom_column_names`) | levado | `alias` da coluna. |
| Nomes de campo raiz além de `select` (`select_by_pk`, `select_aggregate`, `insert`, ...) | mapeado | Informados pelo nome. O Provisa serve uma tabela como um único campo: uma leitura por chave é um filtro sobre ele, um agregado o seu campo `_aggregate`, uma escrita a sua mutação. Verifique se as consultas do app têm essa forma. |
| Relationship de objeto e de array | levado | Um relacionamento aprovado (`many-to-one` para objeto, `one-to-many` para array) cujo campo GraphQL é o nome da relationship do Hasura. Os casos que diferem estão abaixo. |
| Permissão de select | levado | Permissões de coluna (`visible_to`) e uma regra de linhas. Um termo com variável de sessão (`X-Hasura-User-Id`) vira `current_setting('provisa.hasura_user_id')`, que a requisição vincula em tempo de consulta aos claims da identidade ou aos atributos do membro. Uma variável de sessão do Hasura mantém o próprio nome; `provisa.user_id` continua sendo a identidade autenticada. |
| Regra de linhas através de uma relationship | levado | Uma subconsulta `EXISTS` sobre a tabela relacionada. |
| Permissão de insert, update e delete | levado | Permissões de escrita sobre a tabela: uma por função e operação. `columns` viram as colunas da permissão, `filter` o seu `rows`, `check` o seu `check` e `set` os seus presets. |
| Função herdada de uma função | levado | Essa função como `parent_role_id`. |
| Função herdada de várias funções | mapeado | Uma função que mantém a união de suas permissões, escrita por extenso: uma função do Provisa tem um único pai. |
| Computed field | refazer | Uma coluna de uma view sobre a tabela. |
| Função rastreada | levado | Um comando; veja abaixo. Caso contrário, refazer. |
| Action | mapeado | Um comando que chama o handler da action. |
| Cron trigger | levado | Um trigger agendado. |
| Event trigger | mapeado | Um trigger sobre as alterações da tabela. |
| Remote schema | mapeado | Uma fonte `graphql_remote` cujos campos raiz aterrissam como tabelas governadas. |

Casos de relationships:

- **Colisão.** Quando a tabela já tem um campo com o nome da relationship, a relationship assume o nome que o Provisa lhe dá, e o relatório avisa. Renomeie-a na página Relationships se o app precisar de outro nome.
- **O mesmo join duas vezes.** Uma segunda relationship que declara um join já declarado é mesclada com a primeira. O relatório informa isso entre os itens levados.
- **Vários pares de colunas.** Um relacionamento do Provisa une por um par de colunas. Uma relationship que une por vários é avisada e não é criada, mas as regras de linhas através dela são levadas.
- **Apenas coluna de chave estrangeira.** Uma relationship de objeto declarada por `foreign_key_constraint_on: artist_id` não nomeia destino na exportação. O conversor a resolve pela relationship de array inversa e avisa quando não há nenhuma.

Detalhes das permissões de escrita:

- Um `filter` ou `check` vazio significa todas as linhas, escrito `TRUE`.
- Um valor de `set` que é uma variável de sessão (`X-Hasura-User-Id`) vira um preset `session` da variável de sessão correspondente (`X-Hasura-User-Id` vira `hasura_user_id`). Qualquer outro valor vira um preset `literal`. Uma coluna com preset não está também nas colunas da permissão.
- O admin implícito do Hasura tem todas as permissões. Ele vira a função `org_admin`, que mantém uma permissão de insert, update e delete sobre todas as colunas e todas as linhas, e é nomeada em cada coluna importada.
- Cada função com uma permissão de insert, update ou delete recebe também o direito `write`.

Detalhes dos comandos:

- **Função rastreada.** A exportação nomeia a função e as funções que a chamam, não seus argumentos nem o que ela retorna. A pré-visualização lê ambos do catálogo do banco de dados e registra um comando com a assinatura encontrada. Quando a fonte não é alcançada, não é PostgreSQL, ou a função não existe, está sobrecarregada ou retorna linhas de uma tabela que a importação não registra, a função vai para refazer. O conversor de linha de comando nunca alcança um banco de dados, então toda função rastreada que ele encontra vai para refazer.
- **Action.** O comando chama o handler da action com um `POST`. Um `{{VAR}}` na URL do handler é escrito `${env:VAR}` e resolvido quando o comando é chamado. O comando é visível às funções que as permissões da action nomeiam, ou apenas a `org_admin` quando elas não nomeiam nenhuma. Uma action assíncrona é avisada: seu comando espera a resposta do handler. Um tipo de retorno cujos campos a exportação não traz é avisado.
- **Event trigger.** O trigger dispara a cada alteração da tabela, uma vez por lote de alterações, e envia a tabela e as linhas alteradas. Ele não dispara por linha com valores antigos e novos. Um trigger do Hasura sobre menos que as três operações é avisado, pois o trigger do Provisa dispara em todas. As novas tentativas não são levadas: um trigger do Hasura com `num_retries` é avisado, porque um trigger do Provisa chama uma vez. Tente novamente no handler se uma chamada perdida importar.
- **Cron trigger.** A expressão cron, a URL, o payload e o flag enabled são levados. As novas tentativas não são levadas e são avisadas da mesma forma.

Um documento consolidado também contém seções para as quais o conversor não tem destino: `query_collections`, `allowlist`, `rest_endpoints`, `network` e `opentelemetry`. Quando uma está presente, o relatório a lista entre os itens a refazer. O leitor de diretório nunca abre seus arquivos.

### Conexões e domínios na aba de importação {: #connections-and-domains-on-the-import-tab }

Uma conexão que a exportação escreve por extenso é convertida como está. Quando a exportação lê uma URL de banco de dados de uma variável de ambiente (`database_url: {from_env: HASURA_GRAPHQL_DATABASE_URL}`), o valor da variável não está na exportação, então a conversão não adivinha nenhum. A fonte é registrada com os campos de conexão vazios, e o relatório manda refazê-la: informe host, porta, banco de dados, usuário e senha da fonte nas configurações de fontes da importação e converta novamente. Apenas os campos que você alterar viajam, como sobreposições de fonte. As linhas de domínio cobrem cada esquema, subgrafo e remote schema que o upload traz; cada uma é um seletor sobre os domínios existentes da organização que também aceita um nome digitado, marcado como "novo domínio" quando não corresponde a nenhum. Aplicar mescla com o que a organização já tem.

### Os tipos vêm da fonte na pré-visualização {: #types-come-from-the-source-at-preview }

Uma exportação do Hasura nomeia colunas sem tipos, e uma tabela rastreada sem permissão não nomeia colunas. A pré-visualização roda com as conexões de fonte que você fornece, então ela lê o `information_schema.columns` de cada fonte SQL alcançável: toda coluna sem tipo recebe o tipo da fonte mapeado para o vocabulário IR, e uma tabela sem colunas recebe toda coluna que a fonte tem, visível somente a `org_admin`, já que o Hasura não a expôs a nenhuma outra função. Uma fonte que a pré-visualização não consegue alcançar é reportada como um aviso `[sources]` e suas colunas permanecem sem tipo para você concluir antes de aplicar.

### Limitações {: #limitations }

- **Computed fields** são refeitos à mão: a exportação nomeia a função do banco de dados, mas não a define. Adicione-a como coluna de uma view sobre a tabela, ou calcule-a no app.
- **Remote schemas** aterrissam como tabelas a partir dos SDLs de permissão das funções. Um remote schema sem permissões não aterrissa nada, pois a exportação não traz outra declaração de sua forma.
- **Funções rastreadas** precisam do banco de dados na pré-visualização para ler sua assinatura; offline, são refeitas na página Commands.
- **As novas tentativas de event e cron triggers** não são levadas.

---

## Hasura DDN (v3)

### Localizar o projeto HML {: #locate-the-hml-project }

O conversor DDN lê o **diretório** do projeto DDN de arquivos `.hml` diretamente, sem passo de build de supergrafo. O primeiro componente de diretório sob a raiz do projeto é tomado como o nome do subgrafo; arquivos sob `globals/` são atribuídos ao subgrafo `globals`.

### Converter {: #convert_1 }

```bash
python -m provisa.ddn ./my-ddn-project -o config.yaml
```

Omita `-o` para escrever a config no stdout.

Flags:

| Flag | Propósito |
| ------ | --------- |
| `-o`, `--output` | Caminho de saída YAML (padrão: stdout) |
| `--source-overrides` | Arquivo YAML com sobreposições de conexão por fonte |
| `--domain-map` | Mapeamentos de subgrafo para domínio como pares `SUBGRAPH=DOMAIN` |
| `--aggregates-output` | Caminho de saída para o sidecar de expressões de agregado (padrão: `<output>-aggregates.yaml`) |
| `--dry-run` | Analisa e valida sem escrever saída |

Metadados de `AggregateExpression` são preservados em um arquivo sidecar `*-aggregates.yaml`.

### O Que É Convertido {: #what-gets-converted_1 }

| Conceito DDN | Equivalente Provisa |
| ------------ | ------------------- |
| Modelo de subgrafo | `tables[]` sob uma fonte |
| Relacionamento | `relationships[]` |
| Regra de permissão | Filtro RLS |
| Command | Mutação de webhook ou view |
| Conector | Entrada de fonte com detalhes de conexão |

### Limitações {: #limitations_1 }

- **Conectores Lambda** (funções TypeScript/Python) exigem configuração manual de webhook
- **Plugins de ciclo de vida** não têm equivalente direto
- **Modos de autenticação DDN** mapeiam para provedores de autenticação Provisa mas caminhos de claim JWT podem precisar de ajuste

---

## Após a Importação {: #after-import }

1. Revise o `config.yaml` gerado e o relatório. Comece pelos itens de `Redo by hand`
2. Verifique as credenciais de conexão. Uma conexão que a exportação lia de uma variável de ambiente chega vazia; o relatório a nomeia
3. Inicie o Provisa e confirme que as tabelas aparecem no Explorer
4. Rode suas consultas GraphQL existentes; o esquema é compatível para padrões comuns
5. Envie consultas para aprovação via API de Administração ou UI antes de habilitar a governança de produção
