# Importando do Hasura

O Provisa consegue converter metadados existentes do Hasura em um `config.yaml` do Provisa, preservando tabelas rastreadas, relacionamentos, permissões, e esquemas remotos.

## Importação interativa (Admin → Import Hasura Config)

A superfície de admin roda os mesmos conversores, então uma importação não exige acesso a shell nem
ida e volta de arquivo de config. Exige a capacidade `org_settings`; a importação é aplicada na
organização em que a sessão está atuando.

1. **Upload.** Escolha um diretório de metadados do Hasura v2 zipado, um projeto DDN zipado, uma
   exportação de metadados consolidada (`.yaml`/`.json`, incluindo o envelope `{resource_version, metadata}`
   que a API de metadados retorna), ou um único `.hml`. Deixe o formato em *Detect automatically*,
   a menos que o upload seja ambíguo.
2. **Mapear domínios** (opcional). Cada par mapeia um esquema v2 ou um subgrafo DDN para um domínio
   Provisa; o que não for mapeado mantém o nome original.
3. **Converter e pré-visualizar.** O servidor converte e retorna contagens, avisos do conversor, e a
   configuração gerada. Nada é escrito nesta etapa.
4. **Revisar e editar.** A configuração é editável no local — detalhes de conexão, nomes de domínio,
   nomes de função. O que você aplica é o que é mostrado.
5. **Aplicar.** *Replace the existing semantic layer* exclui toda fonte, tabela, função e regra
   ausente da configuração; deixado desmarcado, a importação faz merge com o que a organização já tem.
   Aplicar carrega a configuração e reconstrói os esquemas da organização.

Endpoints: `POST /admin/import/hasura/preview` e `POST /admin/import/hasura/apply`.

---

## Hasura v2

### Exportar Metadados

Do seu console ou CLI Hasura:

```bash
hasura metadata export --output metadata.yaml
```

Ou use a API Hasura:

```bash
curl -X POST http://localhost:8080/v1/metadata \
  -H "X-Hasura-Admin-Secret: <secret>" \
  -d '{"type":"export_metadata","args":{}}' \
  > metadata.json
```

### Converter

O conversor v2 lê um **diretório** de metadados Hasura (o layout produzido por `hasura metadata export`, ou o layout plano `tables.yaml` / `actions.yaml`) e escreve uma config Provisa:

```bash
python -m provisa.hasura_v2 ./metadata -o config.yaml
```

Omita `-o` para escrever a config no stdout.

Flags:

| Flag | Propósito |
| ------ | --------- |
| `-o`, `--output` | Caminho de saída YAML (padrão: stdout) |
| `--source-overrides` | Arquivo YAML com sobreposições de conexão por fonte (host, porta, credenciais) |
| `--domain-map` | Mapeamentos de esquema para domínio como pares `SCHEMA=DOMAIN` |
| `--auth-env-file` | Arquivo `.env` com config de autenticação; converte JWT/JWK, segredo de admin, e mapa de claims |
| `--dry-run` | Analisa e valida sem escrever saída |

### O Que É Convertido

| Conceito Hasura | Equivalente Provisa |
| --------------- | ------------------- |
| Tabela rastreada | `tables[]` com `publish: true` |
| Relacionamento de objeto | `relationships[]` com `cardinality: many-to-one`. Um declarado apenas por coluna de FK (`foreign_key_constraint_on: artist_id`) não nomeia um alvo na exportação; o conversor o resolve através do relacionamento de array inverso, e o descarta com um aviso `[relationships]` quando não há nenhum. (REQ-1680) |
| Relacionamento de array | `relationships[]` com `cardinality: one-to-many` |
| Permissão de select | Visibilidade de função + filtro RLS. Um termo de variável de sessão (`X-Hasura-User-Id`) se torna `current_setting('provisa.user_id')`, que a requisição vincula ao id de usuário e às claims da identidade no momento da consulta. (REQ-1682) |
| Permissão de coluna | `visible_to` / `writable_by` |
| Permissão de insert/update/delete | Mutação `writable_by` + RLS |
| Esquema remoto | Registro de fonte `graphql_remote` mais uma tabela pousada por campo raiz de Query que os SDLs de função expõem; uma coluna fica visível a toda função cujo SDL a expõe, um argumento raiz não anulável se torna uma coluna de filtro nativo `_nf_`, campos aninhados são nomeados em um aviso. (REQ-1681) |
| Campo computado | Entrada `functions[]` com `kind: query` |

### Conexões e domínios na aba de importação

A exportação nomeia seus bancos de dados por variável de ambiente, então, após a primeira conversão, a aba lista cada fonte SQL com a conexão que a conversão adivinhou. Preencha o host, a porta, o banco de dados, o usuário e a senha, e converta novamente; somente os campos que você alterou viajam, como sobreposições de fonte. As linhas de domínio cobrem todo esquema, subgrafo e esquema remoto que o upload carrega; cada uma é um seletor sobre os domínios existentes da organização que também aceita um nome digitado, marcado como *new domain* quando não corresponde a nenhum. Aplicar faz merge com o que a organização já tem, a menos que a caixa de seleção replace esteja marcada. (REQ-1687)

### Os tipos vêm da fonte na pré-visualização

Uma exportação do Hasura nomeia colunas sem tipos, e uma tabela rastreada sem permissão não nomeia colunas. A pré-visualização roda com as conexões de fonte que você fornece, então ela lê o `information_schema.columns` de cada fonte SQL alcançável: toda coluna sem tipo recebe o tipo da fonte mapeado para o vocabulário IR, e uma tabela sem colunas recebe toda coluna que a fonte tem, visível somente a `org_admin`, já que o Hasura não a expôs a nenhuma outra função. Uma fonte que a pré-visualização não consegue alcançar é reportada como um aviso `[sources]` e suas colunas permanecem sem tipo para você concluir antes de aplicar. (REQ-1691, REQ-1684)

### Limitações

- **Actions** convertem automaticamente: actions com handler HTTP se tornam mutações `webhooks[]`; actions com handler não-HTTP (banco de dados) se tornam um placeholder `functions[]` e emitem um aviso para revisar o handler
- **Event triggers** convertem para config `event_triggers` por tabela (operações, URL do webhook, política de retry) e emitem um aviso observando fidelidade limitada
- **Esquemas remotos** convertem para entradas de fonte `graphql_remote` e são pousados como tabelas a partir dos SDLs de permissão de função; um esquema remoto sem permissões não pousa nada, já que a exportação não carrega nenhuma outra declaração de seu formato (REQ-1681)
- **Funções SQL personalizadas** exigem revisão — casos simples convertem para entradas `functions[]`, casos complexos exigem trabalho manual
- **Cron triggers** convertem para entradas de config `scheduler`, preservando a expressão cron e a flag enabled

---

## Hasura DDN (v3)

### Localizar o projeto HML

O conversor DDN lê o **diretório** do projeto DDN de arquivos `.hml` diretamente — nenhum passo de build de supergrafo é exigido. O primeiro componente de diretório sob a raiz do projeto é tomado como o nome do subgrafo; arquivos sob `globals/` são atribuídos ao subgrafo `globals`.

### Converter

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

### O Que É Convertido

| Conceito DDN | Equivalente Provisa |
| ------------ | ------------------- |
| Modelo de subgrafo | `tables[]` sob uma fonte |
| Relacionamento | `relationships[]` |
| Regra de permissão | Filtro RLS |
| Command | Mutação de webhook ou view |
| Conector | Entrada de fonte com detalhes de conexão |

### Limitações

- **Conectores Lambda** (funções TypeScript/Python) exigem configuração manual de webhook
- **Plugins de ciclo de vida** não têm equivalente direto
- **Modos de autenticação DDN** mapeiam para provedores de autenticação Provisa mas caminhos de claim JWT podem precisar de ajuste

---

## Após a Importação

1. Revise o `config.yaml` gerado — preste atenção aos `warnings` do conversor
2. Verifique as credenciais de conexão (o conversor usa valores placeholder)
3. Inicie o Provisa e confirme que as tabelas aparecem no Explorer
4. Rode suas consultas GraphQL existentes — o esquema é compatível para padrões comuns
5. Envie consultas para aprovação via API de Administração ou UI antes de habilitar a governança de produção
