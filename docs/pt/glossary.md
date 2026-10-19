# Glossário de Negócios {: #business-glossary }

O glossário de negócios é um vocabulário vivo sobre o seu modelo de dados. Toda coluna física do modelo corresponde a um termo, e colunas que trazem o mesmo conceito compartilham um termo, por mais diferente que o escrevam. Cada termo pode ter uma definição, um conjunto de relacionamentos tipados com outros termos e uma lista de especialistas no assunto que são donos do seu significado.

O glossário é uma ontologia: um grafo de conceitos, suas definições e as relações entre eles. O Provisa o mapeia para as estruturas de banco de dados que guardam os valores, o que permite responder perguntas sobre seus dados. Um conceito é um rascunho até chegar aos dados, diretamente ou por meio de outros conceitos. O glossário é separado das métricas: um termo diz o que um conceito é, uma métrica diz como um número é calculado.

Esse vocabulário compartilhado liga a linguagem do negócio aos dados físicos. Um agente de IA que sabe que "customer" nomeia toda coluna com um identificador de cliente não precisa adivinhar qual de `cust_id`, `customerId` e `CUSTOMER_KEY` é a certa. Todas correspondem ao mesmo termo, e o termo traz a definição.

## Como os termos são derivados {: #how-terms-are-derived }

O Provisa deriva um termo de cada nome de coluna automaticamente, usando uma regra de normalização
determinística: dobra de maiúsculas e minúsculas, tokenização por separador e camelCase,
expansão de abreviações e remoção de tokens de proxy ao final.

A **expansão de abreviações** leva abreviações corporativas comuns à forma completa: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` e assim por diante. Tanto `id` quanto `key` viram `identifier`. A tabela é fixa e cautelosa: abreviações ambíguas como `st`, `min` e `no` ficam como estão, para não chutar errado.

**Remoção de token de proxy** retira um token final `identifier`, `code`, `index` ou `reference`.
Uma coluna chamada `cust_id` não está nomeando o identificador em si; ela está nomeando um cliente através de
um valor substituto. Remover o proxy faz `cust_id` e `customerId` pousarem ambos no termo `customer`.
Somente tokens finais são removidos, e nunca o último token restante: uma coluna `id` isolada expande para
`identifier` e ali permanece.

A **deduplicação** é o objetivo. A regra de normalização é determinística, então `cust_id`, `customerId` e `CUSTOMER_KEY` produzem todos `customer`. Cada coluna recebe uma referência a esse único termo, e a curadoria tem um só lugar para acrescentar a definição em vez de três.

### Frases genéricas {: #generic-phrases }

Algumas expressões normalizadas são genéricas demais para serem um conceito sozinhas. Uma coluna `name`, `date` ou `identifier` isolada nomeia um atributo do conceito da sua tabela, que não existe separado dessa tabela. Funcionários têm nome, produtos têm nome, mas não são a mesma coisa.

Quando uma frase cai no conjunto genérico e há contexto de tabela disponível, o termo se qualifica para
`<table concept> <phrase>`: `employees.first_name` normaliza para `employee first name`, e
`orders.id` normaliza para `order`, porque a remoção do proxy então colapsa a frase qualificada
sobre o conceito que ela identifica. Este último caso é importante: a chave primária de `orders` e cada
chave estrangeira `order_id` em outras tabelas pousam todas em `order`, sem necessidade de curadoria extra.

O conjunto genérico cobre substantivos de atributo (`name`, `date`, `status`, `type`, `amount`, `quantity`),
frases de trilha de auditoria (`created_at`, `modified_by`, `submitted_timestamp`) e um punhado de outras
que aparecem em quase toda tabela.

### O nome de negócio, não o nome físico {: #the-business-name-not-the-physical-name }

Um termo derivado segue o **nome de negócio** da coluna: o alias quando o modelador definiu um, e o nome físico caso contrário. Quando `usr_nm` tem o alias `user name`, o termo derivado é `user name`, onde expandir `usr_nm` poderia ter dado `user number`.

Dar um alias à coluna é a correção mais forte. Um alias chega a toda superfície que lê a coluna (SQL, GraphQL, agentes de IA, o catálogo), então o modelo se descreve corretamente em todo lugar. Renomear o termo corrige uma entrada do catálogo e deixa a coluna como `usr_nm` para o próximo leitor. O aviso de termo proposto na UI diz isso: primeiro dê um alias à coluna, e só renomeie o termo quando o nome da coluna estiver certo e o vocabulário não.

Trocar o alias de uma coluna deriva de novo o termo proposto, então o glossário acompanha o modelo e nunca pede a mesma correção duas vezes. Depois que um curador acrescentou a um termo uma definição, um relacionamento ou um especialista, editar o alias deixa a referência onde está, porque esse trabalho é do curador e permanece.

### Nomes de tabela de caminho de acesso {: #access-path-table-names }

Alguns nomes de tabela descrevem um caminho de acesso e não um conceito: `user_by_name` é um usuário alcançado por uma busca por nome, e não um tipo de entidade à parte. Quando o Provisa deriva o conceito da tabela para qualificar expressões genéricas, ele corta o nome no conectivo, então `user_by_name` vira `user` e `orders_by_customer` vira `order`.

Sem o corte, a chave substituta de `user_by_name` seria normalizada para `user name` e colidiria com o atributo real `users.name`, dando um termo que guarda ao mesmo tempo uma coisa e um dos seus próprios campos. O corte só vale para conceitos de tabela. Num nome de coluna, `by` faz parte do substantivo composto, então `pet_by_name` e `pet_name` são normalizados para o mesmo termo, `pet name`.

## O que torna um termo curado {: #what-makes-a-term-curated }

Um termo nascido da normalização de colunas começa em branco, como uma proposta que ainda não é vocabulário. Ele passa a ser curado quando qualquer uma destas condições é verdadeira:

- Uma definição foi salva.
- Uma aresta de relacionamento foi adicionada.
- Um especialista no assunto foi designado.
- Um curador o aposentou manualmente.

A curadoria importa para o ciclo de vida do termo. Quando a última coluna física de um termo curado sai do modelo, o termo é depreciado e mantido: sai de serviço, guarda o conteúdo que os editores forneceram e volta automaticamente se a mesma coluna reaparecer. Um termo não curado sem colunas restantes é simplesmente removido.

## Ressincronizando a partir das tabelas {: #re-syncing-from-tables }

Toda vez que uma tabela é salva ou recarregada, `sync_table_refs` reconcilia as colunas daquela tabela com
os refs existentes. Colunas novas criam ou vinculam termos; colunas que partiram descartam seus refs; e a
regra de remover-ou-depreciar resolve qualquer termo que perca seu último ref.

Só os termos não curados são derivados de novo. Se você deu um alias a uma coluna e o termo proposto mudou, a referência passa para o novo termo. Se o termo é curado, o vínculo fica, porque editar o alias não se sobrepõe à escolha de termo do curador.

Um termo abstrato cujo único caminho até dados físicos passava por um termo que parte é depreciado em vez
de removido, preservando a estrutura conceitual até que seja religado.

## Relacionamentos {: #relationships }

Termos se relacionam com outros termos através de arestas tipadas. Os tipos de relacionamento suportados são:

| Tipo | Significado |
| --- | --- |
| `KIND_OF` | O termo de origem é um tipo do termo de destino. |
| `PART_OF` | O termo de origem é um componente do termo de destino. |
| `SYNONYM_OF` | Os dois termos são intercambiáveis neste domínio. |
| `RELATED_TO` | Uma associação frouxa — nenhuma afirmação mais forte se aplica. |
| `VALID_VALUE_OF` | A origem é um valor permitido da enumeração ou domínio de destino. |
| `DERIVED_FROM` | A origem é calculada ou obtida a partir do destino. |
| `REPLACES` | A origem substitui o destino depreciado. |
| `PREFERRED_TERM_FOR` | A origem é o termo preferido sobre o destino desencorajado. |
| `TRANSLATION_OF` | A origem é uma tradução de locale ou idioma do destino. |
| `ANTONYM_OF` | A origem é o oposto semântico do destino. |

Relacionamentos são direcionais. A UI mostra tanto arestas de saída (este termo → outro) quanto arestas
de entrada (outro termo → este termo), rotulando cada direção com sua própria frase em linguagem simples.

As arestas vivem em `glossary_term_edges`, uma tabela associativa declarada como relação de junção: a coluna `rel_type` é o discriminador, de modo que cada um dos tipos acima é um tipo de relação Cypher distinto entre dois nós `GlossaryTerm`, e não uma propriedade de um nó reificado. A tabela é provisionada com o restante do esquema de metadados e não aparece como nó nos clientes de grafo: ela é a aresta. Nada nela é específico do glossário: é declarada do mesmo modo que você declararia uma junção sobre as suas próprias tabelas, e é lida pelo mesmo código.

## Termos abstratos {: #abstract-terms }

Um termo abstrato não tem refs de coluna física próprios. Use um para um conceito de negócio que abrange múltiplos termos concretos: um guarda-chuva que você então liga aos termos específicos que de fato contêm colunas. `revenue`, por exemplo, poderia ser abstrato, com arestas `PART_OF` vindas de `order amount`, `adjustment amount` e `refund amount` apontando para ele.

Um termo abstrato que não alcança nenhuma coluna física pelo grafo de relacionamentos é uma proposta solta. Ele não aparece na busca de termos dos agentes nem na exportação de metadados, porque um termo que não nomeia dados não pode responder nada.

## A regra de admissão para interfaces consumidoras {: #the-admission-rule-for-consuming-surfaces }

Um termo que uma interface consumidora pode oferecer precisa satisfazer três condições:

1. **Em serviço** — não aposentado (um curador o retirou de serviço) e não depreciado (perdeu sua
   última coluna e foi mantido apenas porque excluí-lo deixaria algo solto).
2. **Definido** — carrega uma definição. Um termo derivado de um nome de coluna é um token, não um
   significado. Sem uma definição, é uma proposta aguardando um curador, nunca vocabulário no qual um agente
   possa fundamentar uma pergunta.
3. **Fundamentado** — conectado, sobre termos em serviço, a pelo menos um termo que contenha um ref de coluna
   física. O glossário é um ponto de entrada para os dados, então toda cadeia precisa terminar em uma
   coluna.

A conectividade se propaga pelo grafo: um termo abstrato alcança os dados por qualquer vizinho em serviço que os alcance. Termos fora de serviço não conduzem, então um termo aposentado não mantém vivos os seus dependentes.

## Exportação de metadados {: #metadata-export }

O glossário é publicado em catálogos de dados externos como parte da exportação de metadados. Vale a mesma regra de admissão, com uma restrição: se um termo é enraizado se julga só pelas colunas que de fato são publicadas. Um termo cujas colunas ficam todas de fora da exportação, porque as tabelas delas não estão marcadas como data products ou porque filtros técnicos as excluem, não é enraizado para a exportação, mesmo que tenha referências no plano de controle.

Arestas de relacionamento publicam apenas quando ambos os termos das pontas publicam.

Ativos de coluna exportam independentemente. Um termo ser excluído não esconde os dados subjacentes.

### Excluindo um termo da exportação {: #excluding-a-term-from-export }

Algumas colunas carregam encanamento em vez de dados de negócio: identificadores de lote de ETL, versões de linha,
timestamps de ingestão. Um termo derivado de tal coluna pode ter uma definição perfeitamente precisa
que simplesmente não é vocabulário de negócios. O controle **Excluir da exportação de metadados**
retém o termo e quaisquer arestas de relacionamento que terminem nele dos catálogos em que o Provisa publica,
enquanto as próprias colunas ainda exportam como ativos.

O que conta é se o negócio usa a palavra; o quanto a definição é boa não decide isso. Um identificador de lote de ETL tem um significado claro que pertence ao glossário para os engenheiros, mas não a um catálogo de negócio ao lado de `customer` e `revenue`.

## Trabalhando com o glossário {: #working-with-the-glossary }

Abra **Administração → Glossário** na UI. O painel esquerdo lista todos os termos; clique em um para abrir sua
visão de detalhe. A partir daí:

- **Renomeie** o termo para mudar sua redação sem mover suas colunas.
- **Adicione uma definição** digitando uma ou clicando no botão de rascunho por IA para gerar um ponto de partida
  a partir do nome do termo, de suas colunas físicas e de seus relacionamentos. O rascunho não é salvo até
  você confirmá-lo.
- **Mova um ref** para consolidar dois termos: escolha o termo de destino no menu suspenso ao lado de qualquer
  ref físico. Se o termo de origem perder seu último ref, ele é resolvido pela regra de remover-ou-depreciar
  automaticamente.
- **Adicione um relacionamento** entre este termo e outro, escolhendo o tipo do conjunto fechado.
  Retipe uma aresta existente no lugar em vez de excluí-la e adicioná-la de novo.
- **Designe especialistas** por ID de usuário, com um tipo `expert` ou `author`.
- **Aposente** um termo para retirá-lo de serviço. Ele mantém suas colunas e continua editável aqui, mas
  a busca de termos por agentes e a exportação de metadados o ignoram. Restaure-o depois se o conceito voltar.
- **Gere definições em massa** para preencher toda definição em branco de uma vez. Somente definições vazias
  são escritas; texto humano nunca é sobrescrito.
- **Gere relacionamentos em massa** para propor arestas tipadas por toda a lista de termos. Propostas
  malformadas — nomes de termo desconhecidos, arestas para si mesmo, tipos não reconhecidos — são descartadas automaticamente.

O banner **Proposto** em um termo sem definição diz se o termo está indefinido
(dê alias à coluna ou adicione uma definição) ou não fundamentado (relacione-o a um termo que tenha colunas).
Quando você o vê, o termo ainda não é alcançável por agentes ou catálogos.

## Veja também {: #see-also }

- [Exportação de Metadados](metadata-export.md) — como termos e relacionamentos publicam em catálogos
  de dados externos, incluindo quais termos a regra de admissão da exportação admite.
- [Lineage em Nível de Coluna](lineage.md) — o explorador de lineage e como `columnDependents`
  reporta vínculos de glossário como dependentes de uma coluna física.
