# Ontologia {: #ontology }

A ontologia é um vocabulário vivo sobre seu modelo de dados. Toda coluna física corresponde a um termo, e colunas com o mesmo significado compartilham um termo, por mais diferente que o escrevam. Cada termo pode ter uma definição, relações tipadas com outros termos, uma lista de especialistas no assunto e tags como `pii` que toda coluna com esse significado carrega.

É um grafo de conceitos, das suas definições e das relações entre eles. O Provisa o associa às tabelas e colunas que guardam os valores, e é isso que lhe permite responder a perguntas sobre seus dados. Um termo é um rascunho até ter uma definição e chegar aos dados. A ontologia é separada das métricas: um termo diz o que é um conceito, uma métrica diz como um número é calculado.

Esse vocabulário compartilhado liga a linguagem do negócio aos dados físicos. Um agente de IA que sabe que «customer» nomeia toda coluna com um identificador de cliente não precisa adivinhar qual de `cust_id`, `customerId` e `CUSTOMER_KEY` é a certa. Todas correspondem ao mesmo termo, e o termo carrega a definição.

## Classes, propriedades e conceitos {: #classes-properties-and-concepts }

Todo termo tem exatamente um papel, derivado daquilo em que está ancorado. Você nunca o define.

| Papel | Ancorado em | Exemplo |
| --- | --- | --- |
| **Classe** | Uma ou mais tabelas ou views. Suas instâncias são as linhas delas. | `customer`, ancorado em `customers` e `crm_contacts` |
| **Propriedade** | Colunas que não são chaves. | `customer email`, com `customers.email` |
| **Conceito** | Nada diretamente. Chega aos dados por suas relações. | `revenue`, com arestas `PART_OF` de `order amount` e `refund amount` |

Um termo é uma coisa ou outra. O Provisa recusa ancorar uma tabela em um termo que tem colunas não chave, e recusa mover uma coluna não chave para um termo ancorado em tabelas. Cada recusa nomeia o termo e o que ele já tem.

### Toda tabela com chave propõe uma Classe {: #every-keyed-table-proposes-a-class }

Quando uma tabela com chave primária é salva, o Provisa a ancora em um termo Classe em rascunho, nomeado pelo conceito da tabela: `orders` propõe `order`, `order_lines` propõe `order line`. Tabelas do mesmo conceito compartilham uma Classe. Uma tabela sem chave primária não pode ser uma Classe, e sua ancoragem é recusada, nomeando a tabela.

Uma ancoragem que você mesmo faz, na interface, na configuração ou pelo Polly, é curada. A sincronização nunca a move, e ancorar uma tabela em um termo curado substitui sua proposta em rascunho. Quando um conceito já é uma Propriedade com colunas não chave, suas tabelas não recebem proposta; a ontologia as lista em **Para revisão**.

### As chaves pertencem à Classe {: #keys-belong-to-the-class }

Uma chave primária não tem termo próprio. Cada uma das suas colunas, incluindo as partes de uma chave composta, põe sua referência na Classe da sua tabela.

Uma chave estrangeira funciona da mesma forma. Cada coluna do lado «muitos» de uma relação declarada põe sua referência na Classe da tabela de destino, seja qual for o nome dela. `manager_id` em `employees`, que aponta de volta para `employees`, vai para `employee`; o papel «manager» vive no nome da relação. Uma coluna sem relação declarada é uma Propriedade, mesmo quando seu nome coincide com uma Classe.

O Provisa coloca as referências de chave apenas por derivação, sempre que uma tabela sincroniza, uma relação muda ou a Classe de uma tabela muda. Você não pode mover uma coluna chave à mão; a recusa nomeia a coluna, sua tabela e sua Classe. Ancore uma tabela em outro termo e suas chaves a seguem imediatamente.

### A mesma entidade em várias tabelas {: #the-same-entity-in-several-tables }

Instâncias de tabelas diferentes são distintas até você dizer o contrário. Quando `customers` e `crm_contacts` descrevem o mesmo cliente, marque como **mesma entidade** a relação que as une. Ela deve unir duas tabelas que a mesma Classe ancora, um para um, sobre uma chave de cada lado: a chave primária ou uma restrição de unicidade declarada.

Enquanto uma relação de mesma entidade existir em uma tabela, sua Classe não pode ser retirada nem substituída. Primeiro desmarque a relação.

Uma Classe ancorada em várias tabelas que as relações de mesma entidade não unem todas aparece em **Para revisão** como *tabelas não ligadas*.

## Como os termos são derivados {: #how-terms-are-derived }

O Provisa cria automaticamente um termo em rascunho a partir de cada nome de coluna, com uma regra de normalização determinística: uniformização de maiúsculas e minúsculas, tokenização por separadores e camelCase, expansão de abreviações e remoção de tokens proxy finais.

A **expansão de abreviações** converte abreviações empresariais comuns na forma completa: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` e assim por diante. Tanto `id` como `key` expandem para `identifier`. A tabela é fixa e cautelosa. Abreviações ambíguas como `st`, `min` e `no` ficam como estão, para que nada seja adivinhado errado.

A **remoção de tokens proxy** tira um token final `identifier`, `code`, `index` ou `reference`. Uma coluna chamada `cust_id` não nomeia o identificador em si; nomeia um cliente por meio de um valor substituto. Remover o proxy faz `cust_id` e `customerId` chegarem ao termo `customer`. Só os tokens finais saem, e nunca o último que resta, por isso uma coluna `id` sozinha expande para `identifier` e fica aí.

A **deduplicação** é o objetivo. A regra é determinística, então `cust_id`, `customerId` e `CUSTOMER_KEY` produzem todos `customer`. Cada coluna recebe uma referência a esse único termo, e a curadoria tem um único lugar para acrescentar a definição em vez de três.

As colunas chave saltam esta regra. Suas referências vão para uma Classe, como descrito acima.

### Frases genéricas {: #generic-phrases }

Algumas frases normalizadas são genéricas demais para serem um conceito por si só. Uma coluna `name`, `date` ou `identifier` sozinha nomeia um atributo do conceito da sua tabela, que não existe fora dessa tabela. Funcionários têm nomes e produtos têm nomes, mas não são a mesma coisa.

Quando uma frase está no conjunto genérico e há um contexto de tabela, o termo é qualificado como `<conceito da tabela> <frase>`: `employees.first_name` é normalizado para `employee first name`.

O conjunto genérico abrange substantivos de atributo (`name`, `date`, `status`, `type`, `amount`, `quantity`), frases de auditoria (`created_at`, `modified_by`, `submitted_timestamp`) e mais algumas que aparecem em quase toda tabela.

### Os termos seguem o nome de negócio {: #terms-follow-the-business-name }

Um termo derivado segue o **nome de negócio** da coluna: seu alias quando o modelador definiu um, e o nome físico caso contrário. Quando `usr_nm` tem o alias `user name`, o termo derivado é `user name`, onde expandir `usr_nm` poderia ter dado `user number`.

Dar um alias a uma coluna é a correção mais forte. Um alias chega a todas as superfícies que leem a coluna (SQL, GraphQL, agentes de IA, o catálogo), por isso o modelo se descreve corretamente em toda parte. Renomear o termo corrige uma entrada de catálogo e deixa a coluna chamada `usr_nm` para o próximo leitor. O aviso de Rascunho na interface diz isso: dê primeiro um alias à coluna, e renomeie o termo só quando o nome da coluna estiver certo e o vocabulário não.

Mudar o alias de uma coluna volta a derivar seu termo em rascunho, para que a ontologia acompanhe o modelo e nunca peça a mesma correção duas vezes. Depois que um curador acrescenta a um termo uma definição, uma relação, um especialista ou uma tag, uma edição de alias deixa a coluna onde está, porque esse trabalho é do curador e permanece.

### Nomes de tabela de caminho de acesso {: #access-path-table-names }

Alguns nomes de tabela descrevem um caminho de acesso em vez de um conceito: `user_by_name` é um usuário alcançado por uma pesquisa por nome, e não um tipo de entidade à parte. Quando o Provisa deriva o conceito de uma tabela, corta o nome no conector, por isso `user_by_name` passa a `user` e `orders_by_customer` passa a `order`.

Sem o corte, `user_by_name` proporia uma Classe `user name` em conflito com o atributo real `users.name`. O corte vale apenas para conceitos de tabela. Em um nome de coluna, `by` faz parte do substantivo composto, por isso `pet_by_name` e `pet_name` são normalizados para o mesmo termo, `pet name`.

## Estado {: #status }

Um termo tem um estado de cada vez.

| Estado | Significado | Como chega lá |
| --- | --- | --- |
| **Rascunho** | Indisponível para agentes, pesquisa e catálogos até ter uma definição e chegar aos dados. | Todo termo derivado começa aqui. |
| **Ativo** | Admitido: oferecido a agentes, pesquisa e catálogos. | Definido, ancorado e em serviço. |
| **Obsoleto** | A caminho da saída. | Um curador marca como obsoleto um termo ativo, ou um termo curado perde sua última coluna. |
| **Retirado** | Fora de serviço, mantido como registro. | Um curador o retira e nomeia um sucessor. |

### A regra de admissão {: #the-admission-rule }

Um termo está ativo quando cumpre três condições.

1. **Em serviço.** Não está retirado e não ficou obsoleto por perder sua última coluna. Um termo que um curador marcou como obsoleto continua em serviço até a sua retirada prevista.
2. **Definido.** Tem uma definição. Um termo derivado de um nome de coluna é apenas um token. Sem definição, é um rascunho à espera de um curador, nunca vocabulário em que um agente possa basear uma pergunta.
3. **Ancorado.** Está ligado, por termos em serviço, a um termo que tem uma coluna ou uma tabela. A ontologia é uma porta de entrada para os dados, por isso toda cadeia tem de terminar nos dados.

A conectividade espalha-se pelo grafo. Um Conceito chega aos dados por qualquer vizinho em serviço que chegue. Termos fora de serviço não conduzem, por isso um termo retirado não mantém vivos seus dependentes.

### Marcar um termo como obsoleto {: #deprecating-a-term }

Marque como obsoleto um termo ativo quando estiver de saída mas ainda em uso. Você indica um motivo, obrigatório, e opcionalmente uma data de retirada prevista, hoje ou mais tarde. O termo continua em serviço, e continua a contar para a regra de admissão, até essa data. Depois o agendador o retira.

Um termo vencido que ainda tem colunas ou tabelas não pode ser retirado pela data, porque só um sucessor as pode receber. Ele espera, e **Para revisão** o lista como *retirada bloqueada* até você o retirar com um sucessor.

Um termo curado que perde sua última coluna fica obsoleto automaticamente e é mantido. Sai de serviço, mantém o conteúdo que seus editores forneceram e volta se a mesma coluna reaparecer. Um termo não curado sem colunas é removido.

### Retirar um termo {: #retiring-a-term }

Retirar um termo move tudo o que está ligado a ele para um **sucessor** e depois o tira de serviço.

- As colunas de uma Propriedade passam para o sucessor. Ele tem de ser uma Propriedade ou um Conceito.
- As tabelas de uma Classe passam para o sucessor, e suas chaves as seguem. Ele tem de ser uma Classe ou um Conceito.
- Um Conceito não tem nada, por isso seu sucessor é opcional.

A retirada precisa de um motivo e escreve uma aresta `REPLACES` do sucessor para o termo retirado. O termo retirado fica como registro inerte: definição, tags, relações, especialistas e o motivo. Não conduz nada.

Como suas colunas agora pertencem ao sucessor, o painel de um termo retirado mostra a que foi **aplicado pela última vez**: suas colunas e tabelas como o modelo as tinha no commit anterior à retirada. O painel também leva ao termo que o substitui.

### Restaurar um termo {: #restoring-a-term }

**Restaurar** desfaz o que um curador declarou. Um termo obsoleto deixa de o ser. Um termo retirado volta como um Conceito vazio e em rascunho, já que suas colunas foram para o sucessor, e sua aresta `REPLACES` é removida.

### O que torna um termo curado {: #what-makes-a-term-curated }

Um termo nascido da normalização de colunas começa vazio. Passa a curado quando tem qualquer um destes:

- uma definição guardada
- uma aresta de relação
- um especialista no assunto atribuído
- uma tag, como `pii`
- uma retirada

Termos curados são mantidos quando suas colunas saem. A sincronização nunca volta a derivar as colunas de um termo curado para outro lugar.

## Ressincronizar a partir das tabelas {: #re-syncing-from-tables }

Sempre que uma tabela é salva ou recarregada, o Provisa concilia suas colunas com a ontologia. Colunas novas criam ou ligam termos, colunas que saem perdem suas referências, e todo termo deixado sem referências é removido ou marcado como obsoleto pela regra acima. As colunas chave vão para sua Classe, e uma tabela com chave sem Classe propõe uma.

Só os termos não curados são derivados de novo. Se você der um alias a uma coluna e seu termo em rascunho mudar, a coluna passa para o novo termo. Se o termo for curado, a coluna fica, porque uma edição de alias não se sobrepõe à escolha de um curador.

Quando remover um termo deixaria um Conceito sem caminho até os dados, o Provisa marca esse termo como obsoleto em vez disso, para que a estrutura que você construiu se mantenha até religá-la.

## Relações {: #relationships }

Os termos relacionam-se com outros termos por arestas tipadas.

| Tipo | Significado |
| --- | --- |
| `KIND_OF` | O termo de origem é um tipo do termo de destino. |
| `PART_OF` | O termo de origem é um componente do termo de destino. |
| `SYNONYM_OF` | Os dois termos são intercambiáveis neste domínio. |
| `RELATED_TO` | Uma associação solta, quando nenhuma afirmação mais forte se aplica. |
| `VALID_VALUE_OF` | A origem é um valor permitido da enumeração ou do domínio de destino. |
| `DERIVED_FROM` | A origem é calculada ou obtida a partir do destino. |
| `REPLACES` | A origem substitui o destino. Retirar um termo escreve uma a partir do seu sucessor. |
| `PREFERRED_TERM_FOR` | A origem é o termo preferido em relação ao destino desaconselhado. |
| `TRANSLATION_OF` | A origem é uma tradução do destino para outra localidade ou idioma. |
| `ANTONYM_OF` | A origem é o oposto semântico do destino. |

As relações têm direção. A interface mostra as arestas de saída (este termo → outro) e as de entrada (outro termo → este termo), rotulando cada direção com sua própria frase em linguagem simples.

As arestas ficam em uma tabela associativa declarada como relação de junção. Sua coluna de tipo de relação é o discriminador, por isso cada tipo acima é um tipo de relação Cypher distinto entre dois nós de termo. Nada nela é específico da ontologia: é declarada como você declararia uma junção sobre suas próprias tabelas, e o mesmo código a lê.

## Conceitos {: #concepts }

Um Conceito não tem colunas nem tabelas próprias. Use um para uma ideia de negócio que abrange vários termos concretos, e depois conecte-o aos termos que têm dados. `revenue`, por exemplo, pode ser um Conceito, com arestas `PART_OF` de `order amount`, `adjustment amount` e `refund amount`.

Um Conceito que não chega a nenhuma coluna ou tabela pelo grafo de relações não nomeia dados. Não aparece na pesquisa de termos dos agentes nem na exportação de metadados, e **Para revisão** o lista como *sem dados*. Os domínios aplicam-se apenas a um Conceito. Qualquer outro termo tira seus domínios das tabelas em que está ancorado ou das colunas que tem.

## PII pelo significado {: #pii-on-meanings }

PII é uma propriedade do que uma coluna significa, por isso o Provisa deixa você marcá-la uma vez, no termo, e cobre toda coluna com esse significado, incluindo colunas registradas depois.

A tag `pii` vai em uma coluna ou em um termo Propriedade, nunca em uma origem, em uma tabela, em uma Classe ou em um Conceito. Um termo tem de estar definido e em serviço (ativo ou obsoleto) para a receber. Marcar um termo em rascunho é recusado com «Defina *termo* para o marcar», e você não pode apagar a definição de um termo marcado até o desmarcar.

Toda coluna de um termo marcado carrega `pii`, e `sensitive` com ela, onde quer que as tags sejam lidas: governança, mascaramento, sensibilidade, exportação e as vistas de tags. Nada é copiado para as colunas. Uma coluna movida para o termo carrega a tag imediatamente, e uma coluna movida para fora deixa de a carregar. Cada coluna diz de onde vem sua tag: definida na coluna, com seu motivo, ou por um termo, nomeando-o.

Quando você marca uma coluna cujo termo é uma Propriedade, a interface pergunta se deve marcar as outras colunas com o mesmo significado.

- **Sim** põe a tag no termo.
- **Só esta** marca apenas a coluna e pergunta por quê.

Uma coluna que é PII pelo seu termo não pode ser desmarcada sozinha. Desmarque o termo, ou mova a coluna para o termo que corresponde ao que ela contém.

### Movimentos que perdem tags {: #moves-that-lose-tags }

Mover uma coluna para outro termo pode tirar dela uma tag: mover `customers.email` de `customer email` (marcado com `pii`) para um termo sem marca deixaria a coluna de e-mail desprotegida. Todo movimento que uma pessoa faz é verificado primeiro. Isso abrange reivindicar ou mover uma coluna, retirar um termo, e salvar ou eliminar uma relação que realoca colunas chave.

Quando uma coluna deixaria de carregar `pii` por completo, o movimento é recusado até você decidir. A interface mostra um resumo do que cada coluna movida perde e ganha, primeiro as perdas.

- **Mover** segue em frente, com cada tag perdida confirmada.
- **Mover e marcar** leva as tags perdidas para o termo para onde as colunas vão, na mesma alteração.
- **Cancelar** não move nada.

Uma coluna que mantém a tag pela sua própria marca não precisa de confirmação. Movimentos que o próprio modelo faz, como voltar a derivar as colunas de um rascunho, nunca mudam uma tag herdada.

## Para revisão {: #to-review }

A ontologia relata como constatações o que precisa da sua atenção. Um termo com alguma constatação está **Para revisão**, e a lista de termos filtra por isso. Um simples rascunho não é uma constatação; rascunhos são o estado normal da ontologia.

| Constatação | O que significa | A correção |
| --- | --- | --- |
| Tabelas não ligadas | Uma Classe ancora várias tabelas que as relações de mesma entidade não unem todas. | Marque relações de mesma entidade entre elas. |
| Classe e atributo | Uma Classe tem colunas que não são suas chaves, ou uma tabela com chave não tem Classe porque seu conceito é uma Propriedade. | Mova as colunas para uma Propriedade, ou volte a ancorar a tabela. |
| Tag no tipo errado | Um termo que não é uma Propriedade carrega uma tag que só uma Propriedade passa às colunas. | Remova a tag. |
| Retirada bloqueada | Um termo obsoleto passou da retirada prevista mas ainda tem colunas ou tabelas. | Retire-o com um sucessor. |
| Sem dados | Um Conceito em serviço não chega a nenhum dado. | Relacione-o com um termo que tenha dados. |

As constatações relatam; não mudam nada até você agir.

## Exportação de metadados {: #metadata-export }

A ontologia é publicada em catálogos de dados externos como parte da exportação de metadados. Só os termos ativos são publicados, avaliados pelas colunas e tabelas que realmente são publicadas. Um termo cujas colunas são todas retidas da exportação, porque suas tabelas não são produtos de dados ou porque filtros técnicos as excluem, não está ancorado para a exportação, mesmo que tenha colunas no plano de controle.

Um termo obsoleto é publicado com seu estado, o motivo e a data de retirada prevista, para que cada catálogo o associe aseu próprio marcador de obsolescência.

As arestas de relação só são publicadas quando os dois termos das pontas são publicados. Os ativos de coluna são exportados de forma independente, por isso excluir um termo não oculta os dados subjacentes.

### Excluir um termo da exportação {: #excluding-a-term-from-export }

Algumas colunas carregam canalização, como identificadores de lote de ETL, versões de linha e marcadores de data e hora de ingestão. Um termo derivado de uma coluna assim pode ter uma definição exata e mesmo assim não ser vocabulário de negócio. O controle **Excluir da exportação de metadados** retém o termo, e qualquer aresta de relação que termine nele, dos catálogos em que o Provisa publica. As colunas em si continuam a ser exportadas como ativos.

O que conta é se o negócio usa a palavra. Um identificador de lote de ETL tem um significado claro que pertence à ontologia para os engenheiros, mas não a um catálogo de negócio ao lado de `customer` e `revenue`.

## Trabalhar com a ontologia {: #working-with-the-ontology }

Abra **Ontologia** na barra de navegação. O painel esquerdo lista todo termo com seu papel e, salvo se estiver ativo, seu estado. As caixas **Obsoleto** e **Rascunho** mostram ou ocultam esses termos, e **Para revisão** restringe a lista aos termos com constatações. Clique em um termo para abrir seu painel.

O painel mostra o papel e o estado do termo e, para uma Propriedade, sua marca PII com o número de colunas que cobre. Um termo obsoleto ou retirado mostra quem mudou seu estado e por quê. Um termo com constatações abre com uma seção **Para revisão**: clique em uma constatação para ver o que foi encontrado e a ação que a corrige.

**Editar** abre o termo inteiro como um formulário. Salvar aplica todas as alterações juntas.

- **Renomeie** o termo para mudar sua redação sem mover suas colunas.
- **Acrescente uma definição** escrevendo-a, ou clique no botão de rascunho por IA para gerar um ponto de partida a partir do nome do termo, das suas colunas e das suas relações. O rascunho só é salvo quando você salva o formulário.
- **Reivindique colunas** de outros termos para este, ou escolha **Mover para termo** ao lado de uma das suas colunas. Cada coluna pertence a um termo. As colunas chave seguem sua Classe e não podem ser movidas.
- **Ancore tabelas** em uma Classe, ou remova-as.
- **Marque PII** em uma Propriedade.
- **Acrescente uma relação**, escolhendo seu tipo. Mude o tipo de uma aresta existente no lugar em vez de a apagar e voltar a criar.
- **Atribua especialistas** por ID de usuário, como `expert` ou `author`.

Ao lado de Editar, o painel oferece as ações de estado que o termo permite: **Marcar obsoleto** um termo ativo, **Retirar** qualquer termo em serviço e **Restaurar** um termo obsoleto ou retirado.

No menu ✨, **Definições** preenche toda definição vazia de uma vez e nunca sobrescreve o texto de uma pessoa. **Relações** propõe arestas tipadas para toda a lista de termos; propostas com termos desconhecidos, o mesmo termo duas vezes ou um tipo não reconhecido são descartadas.

O aviso **Rascunho** em um termo indica se ele está sem definição (dê um alias à coluna ou acrescente uma definição) ou sem âncora (relacione-o com um termo que tenha dados). Enquanto o vir, agentes e catálogos não conseguem chegar ao termo.

## Veja também {: #see-also }

- [Exportação de metadados](metadata-export.md) explica como termos e relações são publicados em catálogos de dados externos.
- [Linhagem no nível da coluna](lineage.md) explica o explorador de linhagem e como uma coluna física informa os termos ligados a ela.
