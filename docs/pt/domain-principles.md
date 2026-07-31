# Princípios do Modelo de Domínio

---

## 1. Governança

### Princípios Centrais

1. **Todo recurso deve pertencer a um domínio.** Tabelas, views, e relacionamentos são todos ativos
   de domínio. Não há recursos flutuantes não governados. O domínio é a unidade de responsabilidade.
2. **Todo domínio deve ter um steward.** Um domínio pode existir em um estado pendente até que um
   steward seja designado, mas não pode servir dados governados sem um.
3. **O admin é dono das fontes.** Fontes são infraestrutura, não recursos de domínio. O admin
   registra e gerencia conexões a sistemas de dados externos.
4. **Stewards podem reivindicar tabelas para um domínio.** Reivindicar é exclusivo — uma tabela
   pertence a exatamente um domínio. Este é o ato governado que conecta a infraestrutura e a camada
   semântica.
5. **Stewards podem criar views intradomínio a partir de ativos de domínio.** Views expressam
   lógica de negócio — joins, agregações, métricas derivadas — sobre ativos que o steward possui
   dentro do mesmo domínio. Views criam novo significado semântico e requerem aprovação do steward.
6. **Analistas podem criar consultas entre domínios a partir de relacionamentos aprovados.**
   Consultas são views interdomínio expressas em qualquer linguagem de consulta suportada. Elas não
   criam nova semântica — percorrem caminhos de relacionamento aprovados. Nenhuma aprovação
   adicional é necessária: a governança é tratada upstream nas camadas de Relacionamento e
   visibilidade de coluna. O catálogo é o mecanismo de aplicação: o compilador rejeita travessias
   que não estejam no catálogo de relacionamento aprovado.
7. **Qualquer um pode solicitar acesso a um recurso de domínio.** O acesso é concedido no nível do
   recurso, não no nível da consulta. Se você tem acesso a um recurso, pode consultá-lo. A
   governança é aplicada em tempo de execução através do pipeline.

### Recursos: Tabelas e Views como Pares

A distinção entre uma tabela e uma view é somente de origem — uma tabela é reivindicada de uma
fonte, uma view é definida por um steward. Uma vez que qualquer uma exista como um ativo de
domínio, o modelo de governança as trata de forma idêntica:

- Ambas são ativos de domínio de primeira classe visíveis no catálogo
- Ambas podem ser o alvo de um relacionamento
- Ambas podem ser concedidas sob o Princípio 6
- Ambas estão sujeitas ao mesmo pipeline de governança

Um steward pode reivindicar tabelas privadamente e expor somente views curadas como produtos de
dados voltados ao público.

### Composição de View

Uma view sempre pertence a um único domínio — há somente um tipo de view, sempre intradomínio. Uma
view existe para um de dois propósitos:

- **Importação entre domínios** — a fonte está fora do domínio. Dados entre domínios só podem
  entrar em um domínio via uma view, que atua como um adaptador somente-leitura nomeando os dados
  externos como um conceito de negócio do domínio.
- **Derivação local** — a fonte é do mesmo domínio. A view deriva dados novos ou calculados a
  partir de ativos de domínio existentes. Dados novos ou derivados só podem existir como uma view.

Uma view pode referenciar:
- Tabelas reivindicadas dentro do mesmo domínio
- Campos importados de outro domínio sob uma concessão de acesso de campo
- Uma outra view dentro do mesmo domínio, onde a variação é proposital: restrição de campo,
  agregação, ou enriquecimento via um join adicional

A profundidade de composição não é tecnicamente aplicada — o julgamento do steward durante a
revisão HITL é o mecanismo de controle de qualidade.

Toda view carrega um propósito de negócio declarado, estabelecido no momento da criação:
- Parte do artefato governado — stewards aprovam sabendo para que a view serve
- Referenciado por solicitações de acesso sob o Princípio 7 para que o steward possa avaliar a
  adequação
- Acompanha a view desde a criação através de todo o fluxo de governança

### Consultas (Queries)

Uma Query percorre caminhos de relacionamento aprovados sobre ativos de domínio. Diferente das
Views, Queries não criam novo significado semântico — elas percorrem a estrutura aprovada do
modelo. Queries podem ser expressas em qualquer linguagem de consulta suportada (SQL, GraphQL,
Cypher).

**Aplicação estrutural:** O catálogo de relacionamento é o mecanismo de aplicação. O compilador
valida toda travessia contra entradas de catálogo aprovadas e rejeita consultas que referenciam
caminhos não aprovados. A governança é estrutural, não uma verificação em tempo de execução.

**Nenhuma aprovação necessária:** A governança acontece upstream — nas camadas de Relacionamento e
visibilidade de coluna. Se um usuário tem acesso às colunas e o caminho de travessia é aprovado, a
Query é um uso válido. Nenhum portão adicional.

**Distinção de Views:**
- Views: intradomínio, introduzem novo significado semântico, curadas por steward
- Queries: percorrem relacionamentos aprovados, sem nova semântica, sem portão de aprovação

**Expressão de domínio por linguagem de consulta:**

Cada linguagem suportada expõe o domínio como um namespace estrutural nativo àquela linguagem:

| Linguagem | Expressão de domínio | Exemplo |
|---|---|---|
| GraphQL | Prefixo de nome de tipo e campo | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Nome de esquema | `SELECT * FROM sales.orders` |
| Cypher | Rótulo de nó adicional (domínio só necessário quando o nome do tipo é ambíguo) | `MATCH (o:Sales:Order)` |

O compilador resolve a associação de domínio a partir dessas posições estruturais — nenhuma
anotação ou dica é necessária.

### Relacionamentos

Um relacionamento é um caminho de travessia aprovado entre dois ativos. Fronteiras de domínio são
irrelevantes para o que um relacionamento é — elas só determinam quem o aprova.

**Aprovação:**
- A aprovação é necessária de todo steward distinto que possui um ativo envolvido no
  relacionamento
- Se um steward possui ambos os ativos, uma aprovação é necessária. Se dois stewards estão
  envolvidos, duas aprovações são necessárias
- Não há classificação intradomínio/entre domínios — a propriedade determina o ônus de aprovação
  naturalmente
- Aprovar um relacionamento constrói o grafo de dependência de cada steward, habilitando
  notificações proativas de evolução de esquema

Relacionamentos são criados por demanda, não especulativamente. A primeira equipe com a necessidade
de negócio faz o trabalho; equipes subsequentes herdam a infraestrutura.

**Consequência de otimização:** Uma declaração de relacionamento não é somente um artefato de
governança — também é uma descrição estrutural de uma forma de join. As duas tabelas, duas colunas,
e tipo de join que definem um relacionamento são exatamente o que o otimizador de consulta precisa
para pré-materializar aquele join. Relacionamentos entre fontes geram automaticamente tabelas de
join pré-materializadas; relacionamentos na mesma fonte podem optar por isso via
`materialize: true`. Stewards que pensam bem e aprovam relacionamentos válidos ganham aceleração de
consulta como um subproduto direto — trabalho de governança e trabalho de otimização são o mesmo
ato.

### Concessões de Acesso de Campo

Uma concessão de acesso de campo é uma permissão domínio-para-domínio — o Domínio A pode usar
campos específicos do Domínio B em suas views.

**Ciclo de vida da concessão:**
- Provocada pela criação de view quando campos externos são identificados como necessários
- Aprovada uma vez pelo steward do domínio alvo
- Pertence ao domínio solicitante, não à view que a provocou
- Qualquer view subsequente no domínio solicitante pode usar os campos concedidos sem
  envolvimento adicional entre domínios
- Campos adicionais não concedidos requerem uma nova solicitação

**Notificação pós-uso:** Quando uma view é criada usando campos concedidos, o steward de origem é
notificado — não solicitado a aprovar. A notificação inclui o nome da view, o propósito de negócio
declarado, os campos específicos usados, e qual steward a aprovou. Isso dá ao steward de origem:
- **Visibilidade** — consciência de como seus dados estão sendo usados
- **Supervisão** — fundamento para levantar uma preocupação se o uso parecer inapropriado
- **Recurso** — capacidade de revogar a concessão, invalidando views dependentes

O tradeoff: o domínio de origem aprova acesso de campo sem conhecer todo uso futuro. Aprovação
por-view é correta em teoria e inviável na prática.

### Fluxo de Trabalho de Criação de Consulta

Três estágios, em ordem.

**Estágio 1 — Modelagem (shaping) (descoberta SQL, a partir da página Relationships):**
- O analista abre a ferramenta de Shaping a partir da página Relationships para explorar caminhos
  de join potenciais em SQL bruto
- SQL é executado contra dados acessíveis, sujeito a RLS e mascaramento de coluna existentes
- JOINs no SQL são analisados e expostos como propostas de Relationship candidatas
- Candidatos sugeridos por máquina (inferência de FK, inferência semântica) são mostrados ao lado
  da exploração SQL do analista na mesma view
- O analista seleciona candidatos para promover a uma solicitação de Relationship formal

**Estágio 2 — Aprovação de relacionamento** (consequencial — estrutural e permanente):
- Levantada a todo steward distinto que possui um ativo envolvido no relacionamento
- Este é um caminho de travessia legítimo? O join é semanticamente válido?
- Todos os stewards implicados devem aprovar; o relacionamento se torna uma entrada permanente do
  catálogo

**Estágio 3 — Criação de consulta:**
- O analista constrói a Query em qualquer linguagem suportada (SQL, GraphQL, Cypher), percorrendo
  caminhos de relacionamento aprovados
- Somente relacionamentos de catálogo aprovados são percorríveis — o compilador aplica isso
  estruturalmente
- Nenhuma aprovação necessária — visibilidade de coluna e aprovação de relacionamento são os únicos
  portões

### HITL como o Controle Primário

Regras técnicas tratam do que é objetivo — rastreamento de proveniência de campo, aplicação de
fronteira de domínio, validação de compilador. O julgamento contextual permanece com o steward.
Restrições como profundidade de composição de view, requisitos de propósito por consulta, e
decisões de aprovação de relacionamento são preocupações HITL, não regras aplicadas pelo compilador.

**Neutralidade de domínio de origem:** O steward do domínio de origem aprova o relacionamento uma
vez e a concessão de campo uma vez. Depois disso, domínios a jusante operam dentro dessas
fronteiras concedidas:
- **Alta consideração** na decisão de cruzamento de fronteira
- **Consciência leve** depois via notificações e histórico de consulta

---

## 2. Descobribilidade

### Camadas de Descoberta

A descoberta é estruturada através de cinco camadas de governança crescente. Cada camada é um
pré-requisito para a próxima.

| Camada | Descrição | Estado de governança |
|---|---|---|
| 1 — Esquema de fonte registrado | Toda tabela, coluna, e tipo de uma fonte registrada. Visibilidade em nível de admin. | Nenhum — inventário bruto |
| 2 — Tabelas não reivindicadas | Tabelas introspectadas de fontes registradas sem proprietário de domínio. Visíveis para stewards com acesso à fonte. | Disponível mas não governado |
| 3 — Ativos de domínio | Tabelas reivindicadas e views definidas por steward. Totalmente governado, possuído, visível no catálogo. | Totalmente governado |
| 4 — Relacionamentos | Caminhos de travessia aprovados entre ativos da Camada 3. Pré-requisito para criação de view entre domínios. | Aprovado por ambos os stewards |
| 5 — Concessões de campo | Permissões de acesso de campo domínio-para-domínio. O acesso governado mais específico e deliberado. | Aprovado pelo steward de origem |

Uma tabela não reivindicada é um sinal de lacuna — se dados necessários existem somente na Camada
2, um steward deve reivindicá-los antes que a governança possa prosseguir. A ausência de qualquer
candidato em todas as camadas requer escalonamento ao admin.

### Restrições FK

Restrições FK são uma construção em nível de fonte — não podem abranger múltiplas fontes de dados.
Caminhos de join entre fontes são derivados inteiramente de relacionamentos de catálogo aprovados
(Camada 4), que são mais fortes, tendo sido validados por ambos os stewards.

Dentro de uma fonte:
- Restrições FK são expostas automaticamente como relacionamentos candidatos no registro da fonte
- Elas representam intenção explícita de modelagem — não aplicada na maioria dos sistemas SQL
  analíticos mas declarada propositalmente
- A validação do steward ainda é necessária antes que um candidato se torne um relacionamento
  aprovado

### Hierarquia de Confiança de Relacionamento

| Evidência | Confiança |
|---|---|
| Relacionamento de catálogo aprovado — entre fontes, validado por ambos os stewards | Mais alta |
| Restrição FK intra-fonte — intenção de modelagem explícita, não aplicada mas proposital | Alta |
| Inferência semântica intra-fonte — similaridade de nome/tipo de coluna dentro de um esquema consistente | Média |
| Inferência semântica entre fontes — convenções de nomenclatura divergem entre sistemas; alto risco de falso positivo | Baixa |

Sugestões corroboradas por múltiplos tipos de evidência acumulam confiança.

### Sondagem e Correlação de Dados

Para candidatos inferidos semanticamente, a sondagem de dados fornece uma etapa de validação:
- **Sobreposição de valor** — proporção de valores da coluna de origem que aparecem na coluna alvo
- **Cardinalidade** — se a distribuição corresponde ao tipo de relacionamento esperado
- **Taxa de nulo** — proporção da coluna de origem que é nula, indicando opcionalidade

Alta correlação eleva a confiança; baixa correlação suprime ou rebaixa o candidato. A sondagem é
evidência corroborante, não prova — faixas de inteiro podem se sobrepor coincidentemente e
integridade referencial parcial é comum em sistemas analíticos. Uma margem de erro significativa
permanece. O julgamento semântico do steward é a única verificação final confiável.

### Descoberta Assistida por LLM

O LLM opera em todas as cinco camadas simultaneamente, sugerindo relacionamentos, reivindicações
candidatas, e caminhos de travessia classificados por confiança.

**O que o LLM expõe:**
- Relacionamentos candidatos classificados por confiança
- Tabelas não reivindicadas que podem satisfazer uma necessidade de dados, com um prompt para
  iniciar a reivindicação
- Ausência de qualquer candidato — sinal para escalonar ao admin

**Design de view a partir de descrição de negócio:**

O analista fornece uma descrição em linguagem natural e restrições opcionais. O LLM produz uma
estrutura de view sugerida.

*Entrada:*
- Descrição de negócio: entidades, métricas, relacionamentos, intenção
- Restrições opcionais: filtros, janelas de tempo, agregações, campos excluídos, restrições de
  sensibilidade

*Exemplo:*
> "Volumes de negociação diários por contraparte nos últimos 30 dias, somente contrapartes ativas,
> mostrando nome legal da contraparte e classificação de crédito. Sem PII."

*Processo do LLM:*
1. Analisar — identificar entidades, métricas, dimensões, filtros, exclusões
2. Buscar — todas as camadas de catálogo por ativos correspondentes
3. Sugerir — ativos de domínio, relacionamentos, campos, estrutura de agregação
4. Pontuar — confiança por componente com base em evidência de camada
5. Pré-requisitos — lista ordenada de reivindicações, relacionamentos, e concessões de campo
   necessárias
6. Lacunas — entidades ou campos sem candidato em nenhuma camada, sinalizados para escalonamento
   ao admin

*Saída:*
- Consulta rascunho para revisão e refinamento do analista
- Pontuações de confiança por componente
- Lista de pré-requisitos ordenada
- Lista de lacunas

A descrição de negócio se torna o propósito de negócio declarado da view assim que a view é
formalmente criada.

**Descoberta de relacionamento SQL-first (ferramenta Modeling):**

Acessada como um modal a partir da página Relationships. A intenção é construir o modelo
semântico — identificando caminhos de join estruturais antes de formalizá-los como relacionamentos
governados.

1. O analista escreve SQL livre contra tabelas acessíveis (RLS e mascaramento ainda aplicados)
2. A AST do SQL é analisada — cada condição JOIN se torna uma proposta de Relationship candidata
3. A lista de candidatos é mostrada ao lado de candidatos sugeridos por máquina (inferência de FK,
   inferência semântica) para revisão unificada
4. O analista promove candidatos selecionados a solicitações de Relationship formais
5. Relationships aprovados são adicionados ao catálogo e se tornam percorríveis em Queries

A ferramenta Modeling pode mostrar todas as tabelas registradas para exploração estrutural, mesmo
onde o analista não consegue ver os dados subjacentes — a aprovação do steward governa o acesso
real aos dados, não a visibilidade de esquema.

---

## 3. Uso

### Trilha de Auditoria de Consulta

Toda consulta que toca um ativo de domínio é registrada em um `query_audit_log` somente-anexação.
Cada entrada captura:

- `tenant_id`, `user_id`, `role_id` — o contexto de identidade
- Um hash SHA-256 da consulta — o texto literal da consulta nunca é armazenado
- `table_ids` — os ativos de domínio que a consulta tocou
- `source`, `status_code`, `duration_ms`
- `logged_at` — o timestamp

O log é somente-anexação (DELETE e UPDATE bloqueados no nível do banco de dados) e indexado por
`(tenant_id, logged_at)` e `(user_id, logged_at)`.

O relatório de histórico de consulta do steward é uma view agregada sobre este log, filtrável por
ativo, função, e janela de tempo. O catálogo é um instrumento de governança ao vivo — stewards
mantêm consciência de como seus ativos são usados conforme acontece, não depois do fato.

**Dois mecanismos de visibilidade:**
- **Push** — notificações pós-uso para atos estruturais (uma nova view foi criada usando seus
  campos)
- **Pull** — histórico de consulta para padrões de uso em tempo de execução
