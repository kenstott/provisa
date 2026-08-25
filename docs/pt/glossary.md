# Glossário de Negócios

O glossário de negócios é um vocabulário vivo sobre o seu modelo de dados. Toda coluna física na
camada semântica resolve para um termo — um único termo compartilhado sempre que múltiplas colunas
carregam o mesmo conceito, por mais diferente que o escrevam. Cada termo pode conter uma definição,
um conjunto de relacionamentos tipados com outros termos e uma lista de especialistas no assunto
que são donos do significado.

Esse vocabulário compartilhado é a ponte entre a linguagem de negócios e os dados físicos. Um agente
de IA que sabe que "customer" nomeia toda coluna que carrega um identificador de cliente não precisa
adivinhar qual de `cust_id`, `customerId` e `CUSTOMER_KEY` é a certa — todas resolvem para o mesmo
termo, e o termo carrega a definição.

## Como os termos são derivados

O Provisa deriva um termo de cada nome de coluna automaticamente, usando uma regra de normalização
determinística (REQ-1387): dobra de maiúsculas e minúsculas, tokenização por separador e camelCase,
expansão de abreviações e remoção de tokens de proxy ao final.

**Expansão de abreviações** mapeia abreviações corporativas comuns para suas formas completas: `cust` →
`customer`, `txn` → `transaction`, `qty` → `quantity`, e assim por diante. Tanto `id` quanto `key` expandem para
`identifier`. A tabela é fixa e conservadora — abreviações ambíguas como `st`, `min` e
`no` permanecem como escritas em vez de arriscar um palpite errado.

**Remoção de token de proxy** retira um token final `identifier`, `code`, `index` ou `reference`.
Uma coluna chamada `cust_id` não está nomeando o identificador em si; ela está nomeando um cliente através de
um valor substituto. Remover o proxy faz `cust_id` e `customerId` pousarem ambos no termo `customer`.
Somente tokens finais são removidos, e nunca o último token restante: uma coluna `id` isolada expande para
`identifier` e ali permanece.

**Deduplicação** é o objetivo. A regra de normalização é determinística, então `cust_id`,
`customerId` e `CUSTOMER_KEY` produzem todos `customer`. Cada coluna ganha um ref no único
termo resultante em vez de três termos separados. A curadoria então tem um único lugar para adicionar a definição,
não três.

### Frases genéricas

Algumas frases normalizadas são genéricas demais para serem um conceito por si só. Uma coluna `name`, `date` ou
`identifier` isolada nomeia um atributo do conceito da sua tabela, não um conceito independente daquela
tabela. Funcionários têm nomes; produtos têm nomes; não são a mesma coisa.

Quando uma frase cai no conjunto genérico e há contexto de tabela disponível, o termo se qualifica para
`<conceito da tabela> <frase>`: `employees.first_name` normaliza para `employee first name`, e
`orders.id` normaliza para `order`, porque a remoção do proxy então colapsa a frase qualificada
sobre o conceito que ela identifica. Este último caso é importante: a chave primária de `orders` e cada
chave estrangeira `order_id` em outras tabelas pousam todas em `order`, sem necessidade de curadoria extra.

O conjunto genérico cobre substantivos de atributo (`name`, `date`, `status`, `type`, `amount`, `quantity`),
frases de trilha de auditoria (`created_at`, `modified_by`, `submitted_timestamp`) e um punhado de outras
que aparecem em quase toda tabela.

### O nome de negócio, não o nome físico

Um termo derivado segue o **nome de negócio** da coluna — seu alias quando o modelador definiu um, seu
nome físico quando não definiu (REQ-1581). Quando `usr_nm` recebe o alias `user name`, o termo derivado
é `user name`, não `user number` nem alguma expansão de `usr_nm`.

Dar alias a uma coluna é a correção mais forte. Um alias viaja para toda interface que lê a
coluna — SQL, GraphQL, agentes de IA, o catálogo — de modo que o modelo se descreve corretamente em todo lugar.
Renomear um termo conserta uma entrada de catálogo e deixa a coluna lendo `usr_nm` para o próximo leitor.
O banner de termo proposto na UI diz isso diretamente: dê alias à coluna primeiro; renomeie o termo
apenas quando o nome da coluna estiver certo e o vocabulário não.

Realiasar uma coluna re-deriva seu termo proposto, então o glossário acompanha o modelo em vez de
pedir a mesma correção duas vezes. Uma vez que um curador tenha adicionado uma definição, um relacionamento ou um
especialista a um termo, uma edição de alias não move o ref — aquele trabalho é do curador, e permanece.

### Nomes de tabela de caminho de acesso

Alguns nomes de tabela descrevem um caminho de acesso em vez de um conceito: `user_by_name` é um usuário alcançado
através de uma busca por nome, não um tipo distinto de entidade. Quando o Provisa deriva o conceito da tabela para
qualificação de frase genérica, ele corta o nome no conectivo (REQ-1582). `user_by_name` vira
`user`; `orders_by_customer` vira `order`.

Sem o corte, a chave substituta em `user_by_name` normalizaria para `user name` e colidiria
com o atributo genuíno `users.name` — um termo contendo uma coisa e um dos seus próprios campos.
O corte se aplica somente a conceitos de tabela. Em um nome de coluna, `by` faz parte do substantivo composto:
`pet_by_name` e `pet_name` normalizam para o mesmo termo, `pet name`.

## O que torna um termo curado

Um termo nascido da normalização de coluna começa em branco — uma proposta, ainda não vocabulário. Ele se torna
curado quando qualquer uma das condições a seguir for verdadeira:

- Uma definição foi salva.
- Uma aresta de relacionamento foi adicionada.
- Um especialista no assunto foi designado.
- Um curador o aposentou manualmente.

A curadoria importa para o ciclo de vida do termo. Quando a última coluna física de um termo curado é removida
do modelo, o termo é depreciado em vez de excluído: ele sai de serviço, mantém o conteúdo
fornecido pelo editor e é revivido automaticamente se a mesma coluna reaparecer. Um termo não curado
sem mais colunas é simplesmente removido.

## Ressincronizando a partir das tabelas

Toda vez que uma tabela é salva ou recarregada, `sync_table_refs` reconcilia as colunas daquela tabela com
os refs existentes. Colunas novas criam ou vinculam termos; colunas que partiram descartam seus refs; e a
regra de remover-ou-depreciar resolve qualquer termo que perca seu último ref.

A re-derivação acontece somente para termos não curados. Se você deu alias a uma coluna e o termo proposto agora
difere, o ref se move para o novo termo. Se o termo for curado, o vínculo permanece — a edição de alias
não sobrepôs a escolha de termo do curador.

Um termo abstrato cujo único caminho até dados físicos passava por um termo que parte é depreciado em vez
de removido, preservando a estrutura conceitual até que seja religado.

## Relacionamentos

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

As arestas vivem em `glossary_term_edges`, uma tabela associativa declarada como relação de junção
(REQ-1586): a coluna `rel_type` é o discriminador, de modo que cada um dos tipos acima é um tipo de
relação Cypher distinto entre dois nós `GlossaryTerm`, e não uma propriedade de um nó reificado. A tabela
é provisionada com o restante do esquema de metadados e não aparece como nó nos clientes de grafo — ela
é a aresta. Nada nela é específico do glossário: é declarada do mesmo modo que você declararia uma junção
sobre as suas próprias tabelas, e é lida pelo mesmo código.
[tool-verified: `provisa/cypher/label_map.py:378-397`, `provisa/api/startup_seed.py:508-550`]

## Termos abstratos

Um termo abstrato não tem refs de coluna física próprios. Use um para um conceito de negócio que abrange
múltiplos termos concretos — um guarda-chuva que você então liga aos termos específicos que de fato contêm colunas.
`revenue`, por exemplo, poderia ser abstrato, com arestas `PART_OF` vindas de `order amount`, `adjustment
amount` e `refund amount` apontando para ele.

Um termo abstrato que não consegue alcançar nenhuma coluna física através do grafo de relacionamentos é uma proposta
solta. Ele não aparece na busca de termos por agentes nem na exportação de metadados — um termo que não nomeia dado algum
não pode responder nada.

## A regra de admissão para interfaces consumidoras

Um termo que uma interface consumidora pode oferecer precisa satisfazer três condições (REQ-1387):

1. **Em serviço** — não aposentado (um curador o retirou de serviço) e não depreciado (perdeu sua
   última coluna e foi mantido apenas porque excluí-lo deixaria algo solto).
2. **Definido** — carrega uma definição. Um termo derivado de um nome de coluna é um token, não um
   significado. Sem uma definição, é uma proposta aguardando um curador, nunca vocabulário no qual um agente
   possa fundamentar uma pergunta.
3. **Fundamentado** — conectado, sobre termos em serviço, a pelo menos um termo que contenha um ref de coluna
   física. O glossário é um ponto de entrada para os dados, então toda cadeia precisa terminar em uma
   coluna.

A conectividade se propaga pelo grafo: um termo abstrato alcança dados através de qualquer vizinho em serviço
que os alcance. Termos fora de serviço não conduzem — um termo aposentado não mantém seus
dependentes vivos.

## Exportação de metadados

O glossário publica em catálogos de dados externos como parte da exportação de metadados. A mesma regra de
admissão se aplica, com um estreitamento: o enraizamento de um termo é julgado apenas contra colunas que de fato
publicam. Um termo cujas colunas são todas retidas da exportação — porque suas tabelas não estão marcadas
como produtos de dados, ou porque filtros técnicos as excluem — não está enraizado para fins de exportação
mesmo que contenha refs no plano de controle.

Arestas de relacionamento publicam apenas quando ambos os termos das pontas publicam.

Ativos de coluna exportam independentemente. Um termo ser excluído não esconde os dados subjacentes.

### Excluindo um termo da exportação

Algumas colunas carregam encanamento em vez de dados de negócio: identificadores de lote de ETL, versões de linha,
timestamps de ingestão. Um termo derivado de tal coluna pode ter uma definição perfeitamente precisa
que simplesmente não é vocabulário de negócios (REQ-1583). O controle **Excluir da exportação de metadados**
retém o termo e quaisquer arestas de relacionamento que terminem nele dos catálogos em que o Provisa publica,
enquanto as próprias colunas ainda exportam como ativos.

O teste é se o negócio fala esta palavra, não se a definição é boa. Um identificador de
lote de ETL tem um significado claro que pertence ao glossário para engenheiros; ele não pertence
a um catálogo de negócios ao lado de `customer` e `revenue`.

## Trabalhando com o glossário

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

## Veja também

- [Exportação de Metadados](metadata-export.md) — como termos e relacionamentos publicam em catálogos
  de dados externos, incluindo quais termos a regra de admissão da exportação admite.
- [Lineage em Nível de Coluna](lineage.md) — o explorador de lineage e como `columnDependents`
  reporta vínculos de glossário como dependentes de uma coluna física.
