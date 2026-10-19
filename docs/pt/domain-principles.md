# Princípios do Modelo de Domínio {: #domain-model-principles }

---

## 1. Governança {: #1-governance }

### Princípios Centrais {: #core-principles }

1. **Todo recurso deve pertencer a um domínio.** Tabelas, views, e relacionamentos são todos ativos
   de domínio. Não há recursos flutuantes não governados. O domínio é a unidade de responsabilidade.
2. **Todo domínio deve ter um steward.** Um domínio pode existir em um estado pendente até que um
   steward seja designado, mas não pode servir dados governados sem um.
3. **O admin é dono das fontes.** Fontes são infraestrutura, não recursos de domínio. O admin
   registra e gerencia conexões a sistemas de dados externos.
4. **Stewards podem reivindicar tabelas para um domínio.** Uma reivindicação é exclusiva: uma tabela pertence a exatamente um domínio. Reivindicar é o ato governado que liga a infraestrutura ao modelo.
5. **Stewards podem criar views intradomínio a partir de ativos do domínio.** Views expressam lógica de negócio, como joins, agregações e métricas derivadas, sobre ativos que o steward possui no mesmo domínio. Uma view cria significado semântico novo e precisa de aprovação do steward.
6. **Analistas podem criar consultas entre domínios a partir de relacionamentos aprovados.** Consultas são views interdomínio escritas em qualquer linguagem de consulta suportada. Elas não criam semântica nova; percorrem caminhos de relacionamento aprovados. Nenhuma outra aprovação é necessária, porque a governança acontece antes, nas camadas de relacionamento e de visibilidade de colunas. É o catálogo que impõe: o compilador recusa qualquer travessia que não esteja no catálogo de relacionamentos aprovados.
7. **Qualquer pessoa pode pedir acesso a um recurso de domínio.** O acesso é concedido por recurso, nunca por consulta: se você tem acesso a um recurso, pode consultá-lo. A governança é imposta no momento da execução, pelo pipeline.

### Recursos: Tabelas e Views como Pares {: #resources-tables-and-views-as-peers }

Uma tabela e uma view só diferem na origem: uma tabela é reivindicada de uma fonte, e uma view é definida por um steward. Assim que qualquer uma delas é um ativo de domínio, o modelo de governança as trata da mesma forma:

- Ambas são ativos de domínio de primeira classe visíveis no catálogo
- Ambas podem ser o alvo de um relacionamento
- Ambas podem ser concedidas sob o Princípio 6
- Ambas estão sujeitas ao mesmo pipeline de governança

Um steward pode reivindicar tabelas privadamente e expor somente views curadas como produtos de
dados voltados ao público.

### Composição de View {: #view-composition }

Uma view sempre pertence a um único domínio. Só existe um tipo de view, e ele é sempre intradomínio. Uma view existe para um de dois propósitos:

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

Nada técnico limita a profundidade com que views se compõem. O controle de qualidade é o julgamento do steward na revisão HITL.

Toda view carrega um propósito de negócio declarado, estabelecido no momento da criação:

- Parte do artefato governado, então os stewards aprovam sabendo para que a view serve
- Referenciado por solicitações de acesso sob o Princípio 7 para que o steward possa avaliar a
  adequação
- Acompanha a view desde a criação através de todo o fluxo de governança

### Consultas (Queries) {: #queries }

Uma consulta percorre caminhos de relacionamento aprovados sobre ativos de domínio. Diferente de uma view, ela não cria significado semântico novo; segue a estrutura aprovada do modelo. Uma consulta pode ser escrita em qualquer linguagem de consulta suportada (SQL, GraphQL, Cypher).

**Imposição estrutural:** Quem impõe é o catálogo de relacionamentos. O compilador confere cada travessia com as entradas aprovadas do catálogo e recusa uma consulta que nomeia um caminho não aprovado. A governança está na estrutura, então não há uma verificação separada em tempo de execução.

**Sem aprovação:** A governança acontece antes, nas camadas de relacionamento e de visibilidade de colunas. Se um usuário pode ver as colunas e o caminho está aprovado, a consulta é um uso válido, sem outra barreira.

**Distinção de Views:**

- Views: intradomínio, introduzem novo significado semântico, curadas por steward
- Queries: percorrem relacionamentos aprovados, sem nova semântica, sem portão de aprovação

**Expressão de domínio por linguagem de consulta:**

Cada linguagem suportada expõe o domínio como um namespace estrutural nativo àquela linguagem:

| Linguagem | Expressão de domínio | Exemplo |
| --- | --- | --- |
| GraphQL | Prefixo de nome de tipo e campo | `type sales__Order { ... }`, `query { sales__orders { ... } }` |
| SQL | Nome de esquema | `SELECT * FROM sales.orders` |
| Cypher | Rótulo de nó adicional (domínio só necessário quando o nome do tipo é ambíguo) | `MATCH (o:Sales:Order)` |

O compilador deduz a qual domínio cada coisa pertence a partir dessas posições estruturais, então não é preciso anotação nem dica.

### Relacionamentos {: #relationships }

Um relacionamento é um caminho de travessia aprovado entre dois ativos. As fronteiras de domínio não mudam o que é um relacionamento; só decidem quem o aprova.

**Aprovação:**

- Uma solicitação de relacionamento é decidida pelos domínios que ela toca: o domínio da sua tabela de origem e o domínio da sua tabela de destino
- Uma aprovação só conta se vier de um usuário cujo direito de criar relacionamentos alcança um desses domínios; qualquer usuário assim pode rejeitá-la
- A solicitação é executada quando dois usuários diferentes, nenhum deles o solicitante, a aprovaram e cada domínio envolvido tem um aprovador: um relacionamento entre dois domínios precisa de um sim de cada lado, e um relacionamento dentro de um domínio precisa de dois aprovadores desse domínio
- Aprovar um relacionamento constrói o grafo de dependência de cada steward, habilitando
  notificações proativas de evolução de esquema

Relacionamentos são criados quando alguém precisa deles, nunca por especulação. A primeira equipe com a necessidade de negócio faz o trabalho, e as equipes seguintes o herdam.

**Consequência para a otimização:** Uma declaração de relacionamento é um artefato de governança e também uma descrição estrutural do formato de um join. As duas tabelas, as duas colunas e o tipo de join que definem um relacionamento são exatamente o que o otimizador de consultas precisa para pré-materializar esse join. Relacionamentos entre fontes ganham tabelas de join pré-materializadas automaticamente, e relacionamentos na mesma fonte podem optar por isso com `materialize: true`. Stewards que pensam e aprovam relacionamentos válidos ganham consultas mais rápidas como resultado direto: o trabalho de governança e o de otimização são o mesmo ato.

### Concessões de Acesso de Campo {: #field-access-grants }

Uma concessão de acesso a campos é uma permissão de um domínio para outro: o domínio A pode usar campos específicos do domínio B nas suas views.

**Ciclo de vida da concessão:**

- Provocada pela criação de view quando campos externos são identificados como necessários
- Aprovada uma vez pelo steward do domínio alvo
- Pertence ao domínio solicitante, não à view que a provocou
- Qualquer view subsequente no domínio solicitante pode usar os campos concedidos sem
  envolvimento adicional entre domínios
- Campos adicionais não concedidos requerem uma nova solicitação

**Aviso após o uso:** Quando uma view é criada com campos concedidos, o steward de origem é avisado, sem precisar aprovar. O aviso traz o nome da view, o propósito de negócio declarado, os campos usados e o steward que a aprovou. Isso dá ao steward de origem:

- **Visibilidade** — consciência de como seus dados estão sendo usados
- **Supervisão** — fundamento para levantar uma preocupação se o uso parecer inapropriado
- **Recurso** — capacidade de revogar a concessão, invalidando views dependentes

O tradeoff: o domínio de origem aprova acesso de campo sem conhecer todo uso futuro. Aprovação
por-view é correta em teoria e inviável na prática.

### Fluxo de Trabalho de Criação de Consulta {: #query-creation-workflow }

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

- Decidido pelos domínios que o relacionamento toca, como descrito em [Relacionamentos](#relationships)
- Este é um caminho de travessia legítimo? O join é semanticamente válido?
- Dois usuários diferentes, nenhum deles o solicitante, precisam aprovar, e todo domínio envolvido precisa ter um aprovador; aí o relacionamento vira uma entrada permanente do catálogo

**Estágio 3 — Criação de consulta:**

- O analista constrói a Query em qualquer linguagem suportada (SQL, GraphQL, Cypher), percorrendo
  caminhos de relacionamento aprovados
- Somente relacionamentos de catálogo aprovados são percorríveis, e o compilador aplica isso estruturalmente
- Nenhuma aprovação necessária: visibilidade de coluna e aprovação de relacionamento são os únicos portões

### HITL como o Controle Primário {: #hitl-as-the-primary-control }

Regras técnicas cuidam do que é objetivo: o rastreamento da procedência dos campos, a imposição das fronteiras de domínio e a validação do compilador. O julgamento que depende do contexto fica com o steward. Limites como a profundidade de composição das views, um propósito declarado para cada consulta e as decisões de aprovação de relacionamentos são questões de HITL, que o compilador não impõe.

**Neutralidade de domínio de origem:** O steward do domínio de origem aprova o relacionamento uma
vez e a concessão de campo uma vez. Depois disso, domínios a jusante operam dentro dessas
fronteiras concedidas:

- **Alta consideração** na decisão de cruzamento de fronteira
- **Consciência leve** depois via notificações e histórico de consulta

---

## 2. Descobribilidade {: #2-discoverability }

### Camadas de Descoberta {: #discovery-tiers }

A descoberta é estruturada através de cinco camadas de governança crescente. Cada camada é um
pré-requisito para a próxima.

| Camada | Descrição | Estado de governança |
| --- | --- | --- |
| 1 — Esquema de fonte registrado | Toda tabela, coluna, e tipo de uma fonte registrada. Visibilidade em nível de admin. | Nenhum — inventário bruto |
| 2 — Tabelas não reivindicadas | Tabelas introspectadas de fontes registradas sem proprietário de domínio. Visíveis para stewards com acesso à fonte. | Disponível mas não governado |
| 3 — Ativos de domínio | Tabelas reivindicadas e views definidas por steward. Totalmente governado, possuído, visível no catálogo. | Totalmente governado |
| 4 — Relacionamentos | Caminhos de travessia aprovados entre ativos da Camada 3. Pré-requisito para criação de view entre domínios. | Aprovado por ambos os stewards |
| 5 — Concessões de campo | Permissões de acesso de campo domínio-para-domínio. O acesso governado mais específico e deliberado. | Aprovado pelo steward de origem |

Uma tabela não reivindicada aponta uma lacuna. Se os dados de que você precisa só existem no nível 2, um steward precisa reivindicá-los antes que a governança possa avançar. Quando nenhum nível tem candidato, escale para um admin.

### Restrições FK {: #fk-constraints }

Restrições FK pertencem a uma única fonte, então não podem abranger fontes. Caminhos de join entre fontes vêm inteiramente de relacionamentos aprovados do catálogo (nível 4), que são uma evidência mais forte porque os dois stewards os validaram.

Dentro de uma fonte:

- Restrições FK são expostas automaticamente como relacionamentos candidatos no registro da fonte
- Elas representam intenção explícita de modelagem: não aplicada na maioria dos sistemas SQL analíticos mas declarada propositalmente
- A validação do steward ainda é necessária antes que um candidato se torne um relacionamento
  aprovado

### Hierarquia de Confiança de Relacionamento {: #relationship-confidence-hierarchy }

| Evidência | Confiança |
| --- | --- |
| Relacionamento de catálogo aprovado — entre fontes, validado por ambos os stewards | Mais alta |
| Restrição FK intra-fonte — intenção de modelagem explícita, não aplicada mas proposital | Alta |
| Inferência semântica intra-fonte — similaridade de nome/tipo de coluna dentro de um esquema consistente | Média |
| Inferência semântica entre fontes — convenções de nomenclatura divergem entre sistemas; alto risco de falso positivo | Baixa |

Sugestões corroboradas por múltiplos tipos de evidência acumulam confiança.

### Sondagem e Correlação de Dados {: #data-probing-and-correlation }

Para candidatos inferidos semanticamente, a sondagem de dados fornece uma etapa de validação:

- **Sobreposição de valor** — proporção de valores da coluna de origem que aparecem na coluna alvo
- **Cardinalidade** — se a distribuição corresponde ao tipo de relacionamento esperado
- **Taxa de nulo** — proporção da coluna de origem que é nula, indicando opcionalidade

Correlação alta aumenta a confiança, e correlação baixa suprime ou rebaixa o candidato. A sondagem corrobora; não prova. Faixas de inteiros podem se sobrepor por acaso, e integridade referencial parcial é comum em sistemas analíticos, então sobra muito espaço para erro. O julgamento semântico do steward é a única verificação final confiável.

### Descoberta Assistida por LLM {: #llm-assisted-discovery }

O LLM opera em todas as cinco camadas simultaneamente, sugerindo relacionamentos, reivindicações
candidatas, e caminhos de travessia classificados por confiança.

**O que o LLM expõe:**

- Relacionamentos candidatos classificados por confiança
- Tabelas não reivindicadas que podem satisfazer uma necessidade de dados, com um prompt para
  iniciar a reivindicação
- A ausência de qualquer candidato é o sinal para escalonar ao admin

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

Ele abre como modal na página de relacionamentos. O propósito é construir o modelo semântico: encontrar caminhos de join estruturais antes de formalizá-los como relacionamentos governados.

1. O analista escreve SQL livre contra tabelas acessíveis (RLS e mascaramento ainda aplicados)
2. A AST do SQL é analisada, e cada condição JOIN se torna uma proposta de Relationship candidata
3. A lista de candidatos é mostrada ao lado de candidatos sugeridos por máquina (inferência de FK,
   inferência semântica) para revisão unificada
4. O analista promove candidatos selecionados a solicitações de Relationship formais
5. Relationships aprovados são adicionados ao catálogo e se tornam percorríveis em Queries

Para a exploração estrutural, a ferramenta de modelagem pode mostrar toda tabela registrada, inclusive aquelas cujos dados o analista não pode ver. A visibilidade do esquema é outra questão; o acesso aos dados em si é regido pela aprovação do steward.

---

## 3. Uso {: #3-usage }

### Trilha de Auditoria de Consulta {: #query-audit-trail }

Toda consulta que toca um ativo de domínio é registrada em um `query_audit_log` somente-anexação.
Cada entrada captura:

- `tenant_id`, `user_id`, `role_id` — o contexto de identidade
- Um hash SHA-256 da consulta; o texto literal da consulta nunca é armazenado
- `table_ids` — os ativos de domínio que a consulta tocou
- `source`, `status_code`, `duration_ms`
- `logged_at` — o timestamp

O log é somente-anexação (DELETE e UPDATE bloqueados no nível do banco de dados) e indexado por
`(tenant_id, logged_at)` e `(user_id, logged_at)`.

O relatório de histórico de consultas do steward é uma visão agregada sobre este log, que você pode filtrar por ativo, papel e janela de tempo. Isso faz do catálogo um instrumento de governança vivo: os stewards veem como seus ativos são usados enquanto acontece, em vez de descobrir depois.

**Dois mecanismos de visibilidade:**

- **Push** — notificações pós-uso para atos estruturais (uma nova view foi criada usando seus
  campos)
- **Pull** — histórico de consulta para padrões de uso em tempo de execução
