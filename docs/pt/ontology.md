# Ontologia {: #ontology }

A ontologia é um vocabulário vivo sobre o seu modelo de dados. Cada coluna física se resolve em um termo, e colunas que têm o mesmo significado compartilham um termo, por mais diferente que seja a grafia. Cada termo pode ter uma definição, relacionamentos tipados com outros termos, uma lista de especialistas no assunto e tags como `pii` que toda coluna com esse significado carrega.

É um grafo de conceitos, suas definições e os relacionamentos entre eles. O Provisa o mapeia para as tabelas e colunas que contêm os valores, e é isso que lhe permite responder a perguntas sobre os seus dados. Um termo é um rascunho até que um steward o aprove, e se ele alcança dados é determinado a partir do grafo. A ontologia é separada das métricas: um termo diz o que um conceito é, uma métrica diz como um número é calculado.

Esse vocabulário compartilhado conecta a linguagem do negócio aos dados físicos. Um agente de IA que sabe que "customer" nomeia toda coluna com um identificador de cliente não precisa adivinhar qual de `cust_id`, `customerId` e `CUSTOMER_KEY` é a certa. Todas se resolvem no mesmo termo, e o termo carrega a definição.

## Classes, propriedades e conceitos {: #classes-properties-and-concepts }

Todo termo tem exatamente um papel, derivado do que ele ancora. Você nunca o define.

| Papel | Ancorado em | Exemplo |
| --- | --- | --- |
| **Classe** | Uma ou mais tabelas ou views. Suas instâncias são as linhas delas. | `customer`, ancorando `customers` e `crm_contacts` |
| **Propriedade** | Colunas que não são chaves. | `customer email`, contendo `customers.email` |
| **Conceito** | Nada diretamente. Alcança os dados por meio de seus relacionamentos. | `revenue`, com arestas `PART_OF` a partir de `order amount` e `refund amount` |

Um termo é uma coisa ou outra. O Provisa se recusa a ancorar uma tabela em um termo que contém colunas que não são chaves, e se recusa a mover uma coluna que não é chave para um termo ancorado em tabelas. Cada recusa nomeia o termo e o que ele já contém.

### Toda tabela com chave propõe uma classe {: #every-keyed-table-proposes-a-class }

Quando uma tabela com chave primária é salva, o Provisa a ancora em um termo de classe em rascunho, com o nome do conceito da tabela: `orders` propõe `order`, `order_lines` propõe `order line`. Tabelas do mesmo conceito compartilham uma classe. Uma tabela sem chave primária não pode ser uma classe, e ancorá-la é recusado, nomeando a tabela.

Uma ancoragem que você mesmo faz, na UI, na configuração ou pelo Polly, é curada. A sincronização nunca a move, e ancorar uma tabela em um termo curado substitui a proposta em rascunho. Quando um conceito já é uma propriedade com colunas que não são chaves, suas tabelas não recebem proposta; a ontologia as lista em **Para revisão**.

### As chaves pertencem à classe {: #keys-belong-to-the-class }

Uma chave primária não tem termo próprio. Cada uma de suas colunas, incluindo as partes de uma chave composta, coloca sua referência na classe da sua tabela.

Uma chave estrangeira funciona do mesmo jeito. Cada coluna do lado "muitos" de um relacionamento declarado coloca sua referência na classe da tabela de destino, seja qual for o nome da coluna. `manager_id` em `employees`, apontando de volta para `employees`, vai para `employee`; o papel "manager" fica no nome do relacionamento. Uma coluna sem relacionamento declarado é uma propriedade, mesmo quando o nome coincide com uma classe.

O Provisa posiciona referências de chave apenas por derivação, sempre que uma tabela é sincronizada, um relacionamento muda ou a classe de uma tabela muda. Você não pode mover uma coluna de chave à mão, e a recusa nomeia a coluna, sua tabela e sua classe. Ancore uma tabela em outro termo e as chaves a acompanham na hora.

### A mesma entidade em várias tabelas {: #the-same-entity-in-several-tables }

Instâncias de tabelas diferentes são distintas até que você diga o contrário. Quando `customers` e `crm_contacts` descrevem ambas um mesmo cliente, marque o relacionamento que as une como **mesma entidade**. Ele precisa unir duas tabelas que a mesma classe ancora, um para um, por uma chave de cada uma: a chave primária ou uma restrição de unicidade declarada.

Enquanto houver um relacionamento de mesma entidade em uma tabela, a classe dela não pode ser retirada nem substituída. Desmarque o relacionamento primeiro.

Uma classe ancorada em várias tabelas que os relacionamentos de mesma entidade não unem todas é sinalizada em **Para revisão** como *tabelas não vinculadas*.

## Como os termos são derivados {: #how-terms-are-derived }

O Provisa cria automaticamente um termo em rascunho a partir de cada nome de coluna, usando uma regra de normalização determinística: unificação de maiúsculas e minúsculas, separação por separadores e camelCase, expansão de abreviações e remoção de tokens substitutos finais.

**A expansão de abreviações** mapeia abreviações corporativas comuns para a forma completa: `cust` → `customer`, `txn` → `transaction`, `qty` → `quantity` e assim por diante. Tanto `id` quanto `key` se expandem para `identifier`. A tabela é fixa e cautelosa. Abreviações ambíguas como `st`, `min` e `no` ficam como estão, para que nada seja adivinhado errado.

**A remoção de tokens substitutos** retira um token final `identifier`, `code`, `index` ou `reference`. Uma coluna chamada `cust_id` não nomeia o identificador em si; ela nomeia um cliente por meio de um valor substituto. Removendo o substituto, `cust_id` e `customerId` vão parar no termo `customer`. Só tokens finais são removidos, e nunca o último que resta, então uma coluna `id` sozinha se expande para `identifier` e fica assim.

**A deduplicação** é o objetivo. A regra é determinística, então `cust_id`, `customerId` e `CUSTOMER_KEY` produzem todos `customer`. Cada coluna recebe uma referência nesse único termo, e a curadoria tem um só lugar para adicionar a definição, em vez de três.

Colunas de chave pulam essa regra. Suas referências vão para uma classe, como descrito acima.

### Frases genéricas {: #generic-phrases }

Algumas frases normalizadas são genéricas demais para serem um conceito por si só. Uma coluna `name`, `date` ou `identifier` sozinha nomeia um atributo do conceito da sua tabela, que não existe fora dessa tabela. Funcionários têm nomes e produtos têm nomes, mas não são a mesma coisa.

Quando uma frase está no conjunto genérico e há um contexto de tabela disponível, o termo é qualificado como `<table concept> <phrase>`: `employees.first_name` normaliza para `employee first name`.

O conjunto genérico abrange substantivos de atributo (`name`, `date`, `status`, `type`, `amount`, `quantity`), frases de trilha de auditoria (`created_at`, `modified_by`, `submitted_timestamp`) e algumas outras que aparecem em quase toda tabela.

### Os termos seguem o nome de negócio {: #terms-follow-the-business-name }

Um termo derivado segue o **nome de negócio** da coluna: seu alias, quando o modelador definiu um, e seu nome físico, caso contrário. Quando `usr_nm` tem o alias `user name`, o termo derivado é `user name`, onde expandir `usr_nm` poderia ter dado `user number`.

Dar um alias à coluna é a correção mais forte. Um alias chega a toda superfície que lê a coluna (SQL, GraphQL, agentes de IA, o catálogo), então o modelo se descreve corretamente em toda parte. Renomear o termo corrige uma entrada do catálogo e deixa a coluna como `usr_nm` para o próximo leitor. O banner Rascunho na UI diz isso: dê um alias à coluna primeiro, e renomeie o termo só quando o nome da coluna estiver certo e o vocabulário não.

Mudar o alias de uma coluna deriva de novo o seu termo em rascunho, então a ontologia acompanha o modelo e nunca pede a mesma correção duas vezes. Depois que um curador adiciona a um termo uma definição, um relacionamento, um especialista ou uma tag, uma edição de alias deixa a coluna onde está, porque esse trabalho é do curador e permanece.

### Nomes de tabela de caminho de acesso {: #access-path-table-names }

Alguns nomes de tabela descrevem um caminho de acesso em vez de um conceito: `user_by_name` é um usuário alcançado por uma busca por nome, não um tipo separado de entidade. Quando o Provisa deriva o conceito de uma tabela, corta o nome no conector, então `user_by_name` vira `user` e `orders_by_customer` vira `order`.

Sem o corte, `user_by_name` proporia uma classe `user name` que colidiria com o atributo real `users.name`. O corte vale apenas para conceitos de tabela. Em um nome de coluna, `by` faz parte do substantivo composto, então `pet_by_name` e `pet_name` normalizam para o mesmo termo, `pet name`.

## Por que aprovação e ancoragem importam {: #why-approval-and-grounding-matter }

Agentes e outras automações encontram os dados com que trabalham pela ontologia, e a citam. Um agente começa em um termo que uma pessoa aprovou. Ele segue um caminho até dados cuja confiabilidade consegue ver, ancorados ou ainda aguardando uma aprovação. Chega a tabelas e colunas que lê sob as permissões, regras de linha e máscaras do seu papel. Cada passo desse caminho tem um nome que um agente pode colocar em uma citação.

Então dois fatos sobre um termo decidem o que um agente pode fazer com ele. Pessoas definem o seu **status**. O Provisa calcula a sua **ancoragem**. A página os mostra como dois chips separados, e você filtra por cada um.

## Status {: #status }

Uma pessoa define o status de um termo. Ele tem um de cada vez.

| Status | Significado | Como chega lá |
| --- | --- | --- |
| **Rascunho** | Ainda não aprovado. | Todo termo derivado começa aqui, assim como todo termo que você cria. |
| **Aprovado** | A escolha de um steward: este é vocabulário com que trabalhar. | Um steward o aprova. A aprovação exige uma definição. |
| **Obsoleto** | De saída. | Um curador marca como obsoleto um termo aprovado, ou um termo curado perde sua última coluna. |
| **Retirado** | Fora de serviço, mantido como registro. | Um curador o retira e nomeia um sucessor. |

Uma definição sozinha não aprova nada. Aprove um termo quando a definição disser o que o negócio entende por ele.

## Ancoragem {: #grounding }

A ancoragem diz se um termo alcança dados. Ninguém a define: o Provisa a calcula a partir dos relacionamentos do termo toda vez que o grafo muda.

A ancoragem de um termo olha para baixo pelos seus próprios caminhos até os dados. Um caminho vai do termo, pelos seus relacionamentos, até o primeiro termo que contém uma coluna ou uma tabela: o termo na ponta dos dados, que detém a vinculação. Só contam os termos estritamente entre as duas pontas. O próprio termo nunca conta contra a sua ancoragem, nem nada acima dele.

| Ancoragem | Marca | Significado |
| --- | --- | --- |
| **Ancorado** | ícone ⏚ verde | Algum caminho para baixo não tem termo em rascunho entre o termo e a ponta dos dados. |
| **Ancorado · caminho pendente de aprovação** | ícone ⏚ esmaecido | Todo caminho para baixo tem um termo em rascunho no meio. Passe o mouse sobre o ícone: ele diz "Alcança dados, mas só por termos ainda não aprovados". |
| **Não ancorado** | nenhuma | Nenhum caminho alcança dados. |

O ícone é o símbolo elétrico de terra. Ele aparece na lista de termos ao lado da contagem de colunas, na página do termo, antes do nome do termo nos grafos e na legenda do grafo. O texto de foco de um termo ancorado diz até onde ele alcança, por exemplo "Ancorado: alcança dados em 3 colunas"; um leitor de tela lê as mesmas palavras.

Alguns casos decorrem diretamente da regra:

- Um conceito em rascunho que você está redigindo sobre uma cadeia aprovada está ancorado. O chip de status continua dizendo Rascunho, e os termos abaixo não mudam.
- Um termo derivado de uma coluna é um rascunho na ponta dos dados. Ele nunca deixa pendente a cadeia acima.
- Um termo em rascunho no meio do único caminho deixa ⏚ esmaecido todo termo acima. Aprovar esse único rascunho os ancora.
- Um segundo caminho limpo basta. Um rascunho ao lado não muda nada.
- Termos obsoletos e retirados quebram a cadeia. Um termo cujo único caminho passava por um deles deixa de estar ancorado, e seus chips e filtros dizem isso.

## O que um agente vê {: #what-an-agent-sees }

A busca de termos do MCP, o vocabulário do SPARQL e toda outra superfície voltada a agentes mostram a um agente o mesmo recorte da ontologia:

- um termo aprovado em um caminho até dados;
- um termo em rascunho definido em um caminho até dados que contém pelo menos um termo aprovado, marcado como rascunho;
- um termo sem definição em tal caminho só como passo anônimo, sem nome nem definição. Cada passo continua distinto, então a forma do caminho não muda.

Nada mais chega a um agente. Uma cadeia sem termo aprovado fica oculta, assim como termos que não alcançam dados e termos obsoletos ou retirados. Pessoas veem todos os termos na página Ontologia, com status e ancoragem.

Cada resultado de busca traz o status e a ancoragem do termo, suas colunas e tabelas, seus especialistas e seus caminhos até os dados. Cada passo de um caminho é um termo nomeado ou um passo anônimo, e a ponta dos dados lista aquilo a que está vinculada. É esse caminho que um agente segue e cita.

### Marcar um termo como obsoleto {: #deprecating-a-term }

Marque como obsoleto um termo aprovado quando ele estiver de saída. Você informa um motivo, que é obrigatório, e opcionalmente uma data de retirada planejada, hoje ou depois. Nessa data o agendador o retira. Um termo obsoleto quebra a cadeia que passa por ele desde o momento em que você o marca, então vale ler antes a prévia de impacto.

Um termo com retirada vencida que ainda contém colunas ou tabelas não pode ser retirado por data, porque só um sucessor pode recebê-las. Ele espera, e **Para revisão** o lista como *retirada bloqueada* até que você o retire com um sucessor.

Um termo curado que perde a última coluna é marcado como obsoleto automaticamente e mantido. Ele sai de serviço, mantém o conteúdo fornecido pelos editores e volta se a mesma coluna reaparecer. Um termo não curado sem colunas é removido.

### Retirar um termo {: #retiring-a-term }

Retirar um termo move tudo o que está ligado a ele para um **sucessor** e então o tira de serviço.

- As colunas de uma propriedade passam para o sucessor. Ele precisa ser uma propriedade ou um conceito.
- As tabelas de uma classe passam para o sucessor, e as chaves as acompanham. Ele precisa ser uma classe ou um conceito.
- Um conceito não contém nada, então seu sucessor é opcional.

A retirada precisa de um motivo, e grava uma aresta `REPLACES` do sucessor para o termo retirado. O termo retirado fica como registro inerte: sua definição, tags, relacionamentos, especialistas e o motivo. Ele não conduz nada. O sucessor mantém o próprio status; uma retirada nunca o aprova.

Como suas colunas agora pertencem ao sucessor, o painel de um termo retirado mostra a que ele **se aplicava por último**: suas colunas e tabelas como o modelo as tinha no commit anterior à retirada. O painel também leva ao termo que o substitui.

### Restaurar um termo {: #restoring-a-term }

**Restaurar** desfaz o que um curador afirmou. Um termo obsoleto volta a ser aprovado. Um termo retirado volta como conceito vazio e em rascunho, já que suas colunas foram para o sucessor, e sua aresta `REPLACES` é removida.

### O que torna um termo curado {: #what-makes-a-term-curated }

Um termo nascido da normalização de colunas começa em branco. Ele se torna curado quando tem qualquer um destes:

- uma definição salva
- uma aresta de relacionamento
- um especialista no assunto atribuído
- uma tag, como `pii`
- uma retirada

Termos curados são mantidos quando suas colunas saem. A sincronização nunca deriva de novo, em outro lugar, as colunas de um termo curado.

## Ressincronizar a partir das tabelas {: #re-syncing-from-tables }

Toda vez que uma tabela é salva ou recarregada, o Provisa reconcilia as colunas dessa tabela com a ontologia. Colunas novas criam ou vinculam termos, colunas que saíram largam suas referências, e qualquer termo que fique sem referências é removido ou marcado como obsoleto pela regra acima. Colunas de chave vão para a sua classe, e uma tabela com chave sem classe propõe uma.

Só termos não curados são derivados de novo. Se você der um alias a uma coluna e o termo em rascunho mudar, a coluna passa para o termo novo. Se o termo for curado, a coluna fica, porque uma edição de alias não se sobrepõe à escolha de um curador.

Quando remover um termo deixaria um conceito sem caminho até os dados, o Provisa marca esse termo como obsoleto em vez disso, para que a estrutura que você construiu sobreviva até você religá-la.

## Relacionamentos {: #relationships }

Termos se relacionam com outros termos por arestas tipadas.

| Tipo | Significado |
| --- | --- |
| `KIND_OF` | O termo de origem é um tipo do termo de destino. |
| `PART_OF` | O termo de origem é um componente do termo de destino. |
| `SYNONYM_OF` | Os dois termos são intercambiáveis neste domínio. |
| `RELATED_TO` | Dois assuntos distintos que andam juntos, quando nenhum outro tipo se encaixa. Guarda um lugar e não ancora. |
| `VALID_VALUE_OF` | A origem é um valor permitido da enumeração ou do domínio de destino. |
| `DERIVED_FROM` | A origem é calculada ou obtida a partir do destino. |
| `REPLACES` | A origem substitui o destino. Retirar um termo grava uma a partir do seu sucessor. |
| `PREFERRED_TERM_FOR` | A origem é o termo preferido em relação ao destino desaconselhado. |
| `TRANSLATION_OF` | A origem é uma tradução do destino para um idioma ou localidade. |
| `ANTONYM_OF` | A origem é o oposto semântico do destino. |

Os relacionamentos têm direção. A UI mostra tanto as arestas de saída (este termo → outro) quanto as de entrada (outro termo → este termo), rotulando cada direção com sua própria frase em linguagem simples.

As arestas ficam em uma tabela associativa declarada como relacionamento de junção. A coluna de tipo de relacionamento é o discriminador, então cada tipo acima é um tipo de relacionamento Cypher distinto entre dois nós de termo. Nada nisso é específico da ontologia: ela é declarada como você declararia uma junção sobre as suas próprias tabelas, e o mesmo código a lê.

## Construir um glossário que ancore {: #building-a-glossary-that-grounds }

Um termo só ajuda um agente quando está ancorado, ou seja, quando algum caminho de relacionamentos leva dele a uma coluna ou tabela. Agentes só veem os termos em caminhos assim, como descreve [O que um agente vê](#what-an-agent-sees). Um termo ancorado diz onde estão os seus dados. Um não ancorado nomeia uma ideia e deixa você procurar os dados sozinho.

Os tipos de relacionamento que você escolhe decidem se esse caminho existe. A ancoragem vai de um termo mais amplo para um mais estreito. `revenue` alcança dados porque `order amount` é parte dele e `order amount` contém uma coluna. Ela nunca vai no sentido contrário, então `order amount` não ganha nada por ser parte de `revenue`.

### Teste cada tipo antes de escolhê-lo {: #test-each-type-before-you-pick-it }

Leia uma aresta com X como o termo de onde ela sai e Y como o termo para onde aponta. Depois aplique os testes nesta ordem e fique com o primeiro que passar.

| Tipo | Teste | Exemplo | O que ancora |
| --- | --- | --- | --- |
| **Tipo de** | Todo X é um Y. | Todo cliente corporativo é um cliente. | Y alcança os dados de X. |
| **Parte de** | Um Y tem um X. | Um pedido tem um valor do pedido. | Y alcança os dados de X. |
| **Valor válido de** | X é um dos valores permitidos de Y. | Platina é um nível de cliente. | Y alcança os dados de X. |
| **Sinônimo de** | X e Y significam a mesma coisa. | Cliente e freguês. | Cada um alcança os dados do outro. |
| **Relacionado a** | X e Y são assuntos distintos que andam juntos, e nenhum é um tipo, uma parte ou um valor do outro. | O bolo e as velas. | Nada. |

Derivado de, Substitui, Termo preferido para, Tradução de e Antônimo de registram, cada um, um fato sobre dois termos. Nenhum deles ancora.

Uma propriedade que descreve um conceito é parte dele. O início do turno descreve um turno, então é **Parte de** cobertura de turno. Nunca está apenas relacionado a ela.

Quando você adiciona um relacionamento ou muda o tipo dele, o seletor de tipo lista primeiro Tipo de, Parte de e Valor válido de, cada um com seu teste embaixo. Relacionado a vem por último, com seu teste e o lembrete de que não ancora. O link **Escolher uma relação que ancore** sob o seletor abre esta seção.

### "Relacionado a" guarda um lugar {: #related-to-holds-a-place }

Relacionado a é o último recurso. Como não ancora, um termo cujas únicas ligações são Relacionado a não alcança dados, por mais que tenha. Trate cada uma como um marcador de lugar para um par que você ainda não resolveu.

Para trabalhar os marcadores de lugar, marque **Relacionado a** acima da lista de termos, ao lado de **Obsoleto** e **Para revisão**. A lista se reduz aos termos com pelo menos uma ligação Relacionado a, em qualquer das pontas. O ícone de livro ao lado da caixa abre esta seção.

Um termo que não alcança dados e só tem ligações Relacionado a mostra uma nota na sua página. A nota diz que pode faltar um termo intermediário ao par e nomeia as formas que o resolveriam. É apenas uma orientação. Nada no termo fica bloqueado.

### Resolva um par com um termo novo {: #resolve-a-pair-with-one-new-term }

À maioria dos pares Relacionado a falta um termo intermediário. Um termo novo transforma o marcador de lugar em duas arestas que ancoram, e cinco formas fazem isso. Aqui estão elas para o bolo e as velas, da mais justa para a mais ampla:

| Forma | Termo novo | Arestas |
| --- | --- | --- |
| **Tipo de componente** | decoração de bolo | velas Tipo de decoração de bolo; decoração de bolo Parte de bolo |
| **Especialização** | bolo de aniversário | velas Parte de bolo de aniversário; bolo de aniversário Tipo de bolo |
| **Todo comum** | festa | bolo Parte de festa; velas Parte de festa |
| **Gênero comum** | artigos de festa | bolo Tipo de artigos de festa; velas Tipo de artigos de festa |
| **Enumeração comum** | tipo de item de festa | bolo Valor válido de tipo de item de festa; velas Valor válido de tipo de item de festa |

Siga a ancoragem para baixo em cada linha. Nas duas primeiras formas, bolo alcança os dados de velas pelo termo novo. Nas três últimas, o termo novo é a ponta mais ampla das duas arestas, então alcança os dados de ambos. Nem bolo nem velas alcança os do outro.

Às vezes o termo intermediário já existe, e basta mudar o tipo das ligações. A demo do abrigo incluída tem um conceito, `shift coverage`, definido como a janela e o local em que um funcionário está escalado para trabalhar. Suas partes `shift start`, `shift end` e `schedule location` contêm uma coluna cada. Ligado a elas por Relacionado a, shift coverage não alcançava dados. Com cada uma das três ligada como Parte de shift coverage, ele fica ancorado pelas três colunas. É um todo comum, e o todo estava lá o tempo todo.

### Prefira a forma mais justa {: #prefer-the-tightest-shape }

Quanto mais amplo o termo novo, menos a sua ancoragem diz a quem lê. Festa conecta o bolo e as velas com a mesma certeza que decoração de bolo. Mas um nó tão amplo também fica acima da lista de convidados, do local e da música, então a sua ancoragem já não diz quais dados respondem a uma pergunta sobre velas. Decoração de bolo dá a um agente um caminho curto até as colunas certas. Festa lhe dá um caminho longo até muitas.

Comece pelo tipo de componente e só desça na lista quando uma forma mais justa não se encaixar. Use um gênero comum ou uma enumeração comum quando o par não compartilhar nada mais estreito.

### Quando você escolhe "Relacionado a" {: #when-you-choose-related-to }

Escolher **Relacionado a** para uma ligação que você está adicionando abre uma caixa de diálogo, por exemplo "Ligar velas e bolo com "Relacionado a"?". Mudar o tipo de uma ligação existente para Relacionado a também a abre. A interrupção é proposital, porque este é o momento em que corrigir a ligação custa menos.

A caixa de diálogo primeiro explica por que Relacionado a é a aresta menos útil. Ela não ancora, então o termo continua sem poder alcançar dados por ela. Abaixo disso, desenha as cinco formas para os seus dois termos, da mais justa para a mais ampla. Cada forma mostra um nome para o termo novo, sugerido pelo modelo de IA da sua organização, por exemplo velas → decoração de bolo → bolo. Se o modelo não sugerir nomes, a caixa de diálogo diz por quê, e você digita o nome.

Então você escolhe uma de três ações.

- **Começar** ao lado de uma forma abre o formulário de termo novo. O nome sugerido vem preenchido, e **Ligações adicionadas com ele** lista as duas arestas. Renomeie o termo, adicione uma definição e seus domínios e clique em **Criar**. Criar adiciona o termo. A aresta que toca o termo que você está editando entra no seu formulário e é salva com ele. A outra aresta é gravada na hora.
- **Adicionar "Relacionado a" mesmo assim** adiciona a ligação com um clique. Ela se junta aos marcadores de lugar que o filtro **Relacionado a** lista.
- **Cancelar** não muda nada.

### Como o assistente de relacionamentos escolhe {: #how-the-relationship-assistant-chooses }

**Relações** no menu ✨ pede ao modelo de IA da sua organização relacionamentos entre todos os termos dos seus domínios. As instruções do modelo levam os mesmos testes. Ele prefere Tipo de, Parte de e Valor válido de sempre que um se encaixa, e liga uma propriedade ao conceito que ela descreve com Parte de. Só propõe Relacionado a quando dois termos claramente andam juntos e nenhum outro tipo se encaixa.

O assistente liga termos que já existem e nunca adiciona um termo. Para cada Relacionado a que adiciona, ele nomeia termos candidatos para cada forma que se encaixa. Quando termina, **Ligações "Relacionado a" para revisar** lista cada um desses pares com suas formas, da mais justa para a mais ampla, e os candidatos de cada uma. Clique em qualquer dos dois termos para abri-lo. Se um par precisa de um termo novo, e como chamá-lo, é você quem decide.

### Meça o glossário {: #measure-the-glossary }

O relatório **Ontology quality** mede o glossário a partir do modelo, por domínio e por steward. Abra-o no [visualizador de relatórios](admin.md#reports-viewer). Ele informa:

- a parcela dos termos em serviço que estão ancorados, contando à parte os totalmente ancorados e os pendentes de aprovação;
- a parcela dos aprovados e ancorados, que são os termos que um agente pode usar;
- os marcadores de lugar não resolvidos, ou seja, as ligações Relacionado a e os termos cujas únicas ligações são Relacionado a;
- a precisão da ancoragem, como os passos de cada termo até os dados e o número de caminhos que passam por um termo central amplo;
- os termos ancorados só por rascunhos, cada um com o termo cuja aprovação o ancoraria por completo;
- os termos sem definição em caminhos até os dados;
- a deriva, os termos que perderam ou ganharam ancoragem desde o período anterior.

Cada linha leva ao seu termo, e cada linha de marcador de lugar às ligações Relacionado a desse termo, para revisão. Como todo relatório, ele é lido pelo pipeline governado sob o seu papel, e você pode exportá-lo.

## Conceitos {: #concepts }

Um conceito não tem colunas nem tabelas próprias. Use um para uma ideia de negócio que abrange vários termos concretos, e depois ligue-o aos termos que contêm dados. `revenue`, por exemplo, poderia ser um conceito, com arestas `PART_OF` a partir de `order amount`, `adjustment amount` e `refund amount`.

Um conceito que não consegue alcançar nenhuma coluna ou tabela pelo grafo de relacionamentos não nomeia dados. Ele não aparece na busca de termos dos agentes nem na exportação de metadados, e **Para revisão** o lista como *sem dados*. Domínios se aplicam somente a um conceito. Todo outro termo herda os domínios das tabelas que ancora ou das colunas que contém.

## PII pelo significado {: #pii-on-meanings }

PII é uma propriedade do que uma coluna significa, então o Provisa permite marcá-la uma vez, no termo, e cobre toda coluna com esse significado, incluindo as registradas depois.

A tag `pii` vai em uma coluna ou em um termo de propriedade, nunca em uma fonte, uma tabela, uma classe ou um conceito. Um termo precisa estar definido e não retirado para recebê-la. Marcar um termo em rascunho é recusado com "Defina *termo* para marcá-lo", e você não pode apagar a definição de um termo marcado até desmarcá-lo.

Toda coluna de um termo marcado carrega `pii`, e `sensitive` junto, onde quer que tags sejam lidas: governança, mascaramento, sensibilidade, exportação e as views de tags. Nada é copiado para as colunas. Uma coluna movida para o termo carrega a tag na hora, e uma movida para fora não carrega mais. Cada coluna diz de onde vem a sua tag: definida na coluna, com o motivo, ou por meio de um termo, que ela nomeia.

Quando você marca uma coluna cujo termo é uma propriedade, a UI pergunta se deve marcar as outras colunas que significam a mesma coisa.

- **Sim** coloca a tag no termo.
- **Só esta** marca apenas a coluna, e pergunta por quê.

Uma coluna que é PII pelo seu termo não pode ser desmarcada sozinha. Desmarque o termo, ou mova a coluna para o termo que corresponde ao que ela contém.

### Movimentos que perdem tags {: #moves-that-lose-tags }

Mover uma coluna para outro termo pode tirar uma tag dela: mover `customers.email` de `customer email` (com a tag `pii`) para um termo sem tag deixaria a coluna de e-mail desprotegida. Todo movimento feito por uma pessoa é verificado antes. Isso inclui reivindicar ou mover uma coluna, retirar um termo, e salvar ou excluir um relacionamento que realoca colunas de chave.

Quando uma coluna deixaria de carregar `pii` por completo, o movimento é recusado até você decidir. A UI mostra um resumo do que cada coluna movida perde e ganha, perdas primeiro.

- **Mover** segue adiante, com cada tag perdida confirmada.
- **Mover e marcar** leva as tags perdidas para o termo para onde as colunas vão, na mesma alteração.
- **Cancelar** não move nada.

Uma coluna que mantém a tag pela própria marca não precisa de confirmação. Movimentos que o próprio modelo faz, como derivar de novo as colunas de um rascunho, nunca mudam uma tag herdada.

## Para revisão {: #to-review }

A ontologia informa o que precisa da sua atenção como achados. Um termo com qualquer achado está **Para revisão**, e a lista de termos filtra por isso. Um simples rascunho não é um achado; rascunhos são o estado comum da ontologia. Um termo que alcança dados só por rascunhos é um achado, porque uma única aprovação o colocaria diante dos agentes.

| Achado | O que significa | A correção |
| --- | --- | --- |
| Tabelas não vinculadas | Uma classe ancora várias tabelas que os relacionamentos de mesma entidade não unem todas. | Marque relacionamentos de mesma entidade entre elas. |
| Classe e atributo | Uma classe contém colunas que não são suas chaves, ou uma tabela com chave não tem classe porque o seu conceito é uma propriedade. | Mova as colunas para uma propriedade, ou ancore a tabela de novo. |
| Tag no tipo errado | Um termo que não é uma propriedade carrega uma tag que só uma propriedade transmite às colunas. | Remova a tag. |
| Retirada bloqueada | Um termo obsoleto passou da data de retirada planejada, mas ainda contém colunas ou tabelas. | Retire-o com um sucessor. |
| Sem dados | Um conceito em rascunho não alcança dados. | Relacione-o a um termo que contenha dados. |
| Aprovado, não ancorado | Um termo aprovado não alcança dados, então nenhum agente o vê. | Relacione-o a um termo que contenha dados. |
| Ancorado · caminho pendente de aprovação | Todo caminho até os dados passa por um rascunho. O achado nomeia o rascunho cuja aprovação ancora o termo. | Abra esse termo e aprove-o. |

Achados informam; não mudam nada até você agir.

## Exportação de metadados {: #metadata-export }

A ontologia é publicada em catálogos de dados externos como parte da exportação de metadados. Termos aprovados são publicados, cada um com a sua ancoragem: `grounded`, `grounded_pending` ou `not_grounded`. Um rascunho nunca sai do Provisa. A ancoragem para exportação é avaliada sobre as colunas e tabelas que de fato são publicadas. Um termo cujas colunas são todas retidas da exportação, porque suas tabelas não são produtos de dados ou porque filtros técnicos as excluem, é retido junto com elas.

Um termo aprovado que um curador depois marcou como obsoleto é publicado com status, motivo e data de retirada planejada, para que cada catálogo possa mapeá-lo para o seu próprio marcador de obsolescência.

Arestas de relacionamento só são publicadas quando os dois termos das pontas são publicados. Ativos de coluna são exportados de forma independente, então excluir um termo não esconde os dados subjacentes.

### Excluir um termo da exportação {: #excluding-a-term-from-export }

Algumas colunas carregam encanamento, como identificadores de lote de ETL, versões de linha e carimbos de data e hora de ingestão. Um termo derivado de uma coluna assim pode ter uma definição exata e ainda assim não ser vocabulário de negócio. O controle **Excluir da exportação de metadados** retém o termo, e qualquer aresta de relacionamento que termine nele, dos catálogos em que o Provisa publica. As colunas em si continuam sendo exportadas como ativos.

O que conta é se o negócio usa a palavra. Um identificador de lote de ETL tem um significado claro que pertence à ontologia para os engenheiros, mas não a um catálogo de negócio ao lado de `customer` e `revenue`.

## Trabalhar com a ontologia {: #working-with-the-ontology }

Abra **Ontologia** na barra de navegação. O painel esquerdo lista cada termo com seu papel, seu status quando não aprovado e seu chip de ancoragem. Os filtros **Status** e **Ancoragem** reduzem a lista por cada um, a caixa **Obsoleto** mostra ou oculta termos obsoletos, **Para revisão** a reduz a termos com achados, e **Relacionado a** a reduz a termos com uma ligação Relacionado a. Clique em um termo para abrir o painel dele.

O painel mostra lado a lado o papel, o status e a ancoragem do termo, e, para uma propriedade, a marca de PII com o número de colunas que ela cobre. Um termo ⏚ esmaecido nomeia os rascunhos cuja aprovação o ancoraria. Um termo obsoleto ou retirado mostra quem mudou o status e por quê. Um termo com achados abre com uma seção **Para revisão**: clique em um achado para ver o que foi encontrado e a ação que o corrige.

### Resumo de stewardship {: #stewardship-summary }

Sem nenhum termo aberto, o painel mostra o resumo de stewardship. Ele lista os termos que os agentes podem ver, os termos ⏚ esmaecidos que esperam cada um uma aprovação, com o termo a aprovar, e os termos aprovados que não alcançam dados. **Aprovar rascunhos definidos e ancorados** lista todo rascunho que tem definição e está ancorado; desmarque os que tiverem definição errada e aprove o restante de uma vez. O botão de área de transferência no cabeçalho traz o resumo de volta.

### Ver o impacto primeiro {: #seeing-the-impact-first }

Aprovar, retirar e vincular um termo mudam, cada um, o que alcança dados. Antes de qualquer um deles, a página mostra o que a ação mudaria: quantos termos ficam ancorados, ⏚ esmaecidos ou não ancorados, e para quantas tabelas os agentes ganham ou perdem um caminho. Por exemplo, "Aprovar region ancora 3 termos e dá aos agentes um caminho para mais 2 tabelas". **Aprovar** abre a prévia com seu botão de confirmação, **Retirar** a mostra sob o sucessor que você escolher, e ancorar uma tabela no formulário de edição a mostra sob a tabela antes de salvar.

**Editar** abre o termo inteiro como um formulário. Salvar aplica todas as alterações juntas.

- **Renomeie** o termo para mudar a redação sem mover as colunas.
- **Adicione uma definição** digitando-a, ou clique no botão de rascunho de IA para gerar um ponto de partida a partir do nome do termo, das colunas e dos relacionamentos. O rascunho só é salvo quando você salva o formulário.
- **Reivindique colunas** para mover colunas de outros termos para este, ou escolha **Mover para termo** ao lado de uma das suas próprias colunas. Cada coluna pertence a um termo. Colunas de chave seguem a sua classe e não podem ser movidas.
- **Ancore tabelas** em uma classe, ou remova-as.
- **Marque PII** em uma propriedade.
- **Adicione um relacionamento**, escolhendo o tipo. Mude o tipo de uma aresta existente no lugar, em vez de excluí-la e adicioná-la de novo.
- **Atribua especialistas** por ID de usuário, como `expert` ou `author`.

Ao lado de Editar, o painel oferece as ações de status que o termo permite: **Aprovar** um rascunho definido, **Marcar obsoleto** um termo aprovado, **Retirar** qualquer termo em serviço, e **Restaurar** um obsoleto ou retirado.

No menu ✨, **Definições** preenche toda definição em branco em uma passada e nunca sobrescreve o texto de uma pessoa. **Relações** adiciona arestas tipadas em toda a lista de termos; propostas que nomeiam termos desconhecidos, o mesmo termo duas vezes ou um tipo não reconhecido são descartadas. Cada Relacionado a que ele adiciona é listado para revisão, como descreve [Como o assistente de relacionamentos escolhe](#how-the-relationship-assistant-chooses).

O banner **Rascunho** em um termo diz se ele ainda precisa de uma definição (dê um alias à coluna ou adicione uma) ou só da sua aprovação. Um banner separado diz quando um termo não alcança dados: relacione-o a um termo que tenha dados.

## Consultar a ontologia {: #querying-the-ontology }

A ontologia é um esquema que você pode consultar, chamado `ontology`, ao lado do esquema `meta` que descreve a estrutura do modelo. Suas tabelas começam com sublinhado, então a tabela própria de uma classe nunca compartilha nome com uma delas.

| Tabela | Uma linha por |
|---|---|
| `_terms` | Termo, com seu status, sua ancoragem, sua definição e se é uma classe, uma propriedade ou ambos |
| `_term_refs` | Vinculação de uma propriedade a uma coluna, com a tabela e o domínio da coluna |
| `_term_edges` | Relacionamento entre dois termos |
| `_domains` | Domínio a que um termo pertence |
| `_experts` | Especialista ou autor atribuído a um termo |
| `_axioms` | Declaração da ontologia em RDFS, OWL e SKOS |
| `_closure` | Termo pareado com cada termo do qual é um tipo, em qualquer profundidade, e consigo mesmo |

Uma vinculação liga um termo a uma coluna no esquema `meta`, então você pode unir `ontology._term_refs` a `meta.table_columns` e `meta.registered_tables`. As tabelas da ontologia são somente leitura. Elas mudam conforme você faz a curadoria dos termos.

Você só lê um termo quando alcança um dos seus domínios. Um termo sem domínio é visível para todos, assim como um termo corporativo (`*`). Um relacionamento ou um axioma é visível quando os dois termos são.

### A ontologia em SPARQL {: #the-ontology-in-sparql }

O SPARQL responde a ontologia nos vocabulários padrão, então uma ferramenta RDF a lê como lê qualquer ontologia OWL:

- uma classe é uma `owl:Class`, e um relacionamento de tipo entre classes é `rdfs:subClassOf`;
- uma propriedade é uma `owl:DatatypeProperty`, ou uma `owl:ObjectProperty` quando toda coluna que ela vincula é uma chave para uma tabela com classe;
- o `rdfs:domain` de uma propriedade é a classe de cada tabela de que ela vincula uma coluna, e o seu `rdfs:range` é a classe a que as chaves levam ou o tipo XML Schema das colunas;
- um relacionamento de tipo entre propriedades é `rdfs:subPropertyOf`;
- todo termo tem seu nome como `skos:prefLabel` e sua definição como `skos:definition`.

Um termo é nomeado pela sua IRI de vocabulário, `<your Provisa address>/vocab/term/<name>`, a IRI que tipa as linhas das tabelas que ele ancora. Um termo retirado não declara nada.

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl:  <http://www.w3.org/2002/07/owl#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?class ?parent ?label WHERE {
  ?class a owl:Class ; rdfs:subClassOf ?parent .
  ?parent skos:prefLabel ?label .
}
```

Uma consulta pode unir a ontologia às linhas que você lê. As instâncias de uma classe incluem as de toda classe que é um tipo dela, em qualquer profundidade:

```sparql
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX term: <https://your-provisa/vocab/term/>

SELECT ?party ?class WHERE {
  ?party a ?class .
  ?class rdfs:subClassOf* term:Party .
}
```

Uma propriedade funciona do mesmo jeito: `?x ?p ?v . ?p rdfs:subPropertyOf* term:amount` lê os valores de toda propriedade que é um tipo de `amount`. Você obtém as instâncias das tabelas que lê, então uma classe cujas tabelas você não pode ler não acrescenta nenhuma.

As mesmas declarações são linhas de `ontology._axioms` em SQL, e os pares de tipo, fechados em qualquer profundidade, são linhas de `ontology._closure`. GraphQL, Cypher e as outras linguagens de consulta não oferecem essas duas tabelas.

## Veja também {: #see-also }

- [Exportação de metadados](metadata-export.md) explica como termos e relacionamentos são publicados em catálogos de dados externos.
- [Linhagem em nível de coluna](lineage.md) explica o explorador de linhagem e como uma coluna física informa os termos vinculados a ela.

## O esquema da ontologia {: #the-ontology-schema }

O esquema de sistema `ontology` torna o vocabulário consultável por SQL, GraphQL, Cypher, REST e MCP, as mesmas superfícies que você usa para os seus próprios dados. Ele fica ao lado de `meta`, que contém a estrutura do modelo; `ontology` contém o seu significado.

Duas partes compõem o esquema: views de definição e tabelas de classe.

### Views de definição {: #definition-views }

Cinco views expõem o conteúdo do glossário:

| View | Conteúdo |
| --- | --- |
| `_terms` | Todo termo: nome, papel (classe, propriedade, conceito), definição, status e ancoragem. |
| `_term_refs` | Toda vinculação de termo a coluna: o nome do termo, a fonte, a tabela e a coluna. |
| `_term_edges` | Toda aresta de relacionamento: termo de origem, tipo de aresta e termo de destino. |
| `_domains` | Pertencimento a domínios: o nome do termo e cada domínio a que pertence. |
| `_experts` | Atribuições de especialistas: o nome do termo, o ID de usuário e o papel (`expert` ou `author`). |

Uma sexta view, `_axioms`, contém toda tripla RDFS/OWL/SKOS que o esquema consegue expressar, uma linha por tripla, com as colunas `subject`, `predicate`, `object`, `object_iri` e `object_literal`. `object` nomeia o objeto quando ele é um termo; `object_iri` o contém quando é uma IRI, e `object_literal` quando é um literal. Uma das três contém o valor em cada linha. Ferramentas que preferem triplas a linhas tabulares leem daqui.

Uma **vinculação de termo**, isto é, um termo vinculado a uma coluna e registrado em `_term_refs`, é diferente de uma vinculação de comando. Uma vinculação de termo diz o que uma coluna significa; uma vinculação de comando liga uma função ao pipeline de consultas.

### Tabelas de classe {: #class-tables }

Todo termo de classe que não está retirado vira uma view no esquema `ontology` com o nome desse termo, com espaços trocados por sublinhados. Uma classe ancorada em uma tabela gera uma view; uma classe ancorada em várias tabelas gera a união dessas tabelas.

Em uma view de união, aparecem as linhas de toda tabela de origem. A coluna `_source_table` identifica de qual tabela veio cada linha: contém o nome qualificado da tabela, `domain.table`. Uma coluna de propriedade que uma tabela de origem não vincula é `NULL` para as linhas dela. Uma propriedade vinculada a várias colunas de uma mesma tabela vira uma coluna de array.

Toda coluna de uma view de classe corresponde a uma propriedade vinculada. A view é governada pelo mesmo pipeline que qualquer outra leitura.

**Exemplo.** Uma classe `customer` ancorada em `sales.customers` e `crm.contacts`, com uma propriedade `name` vinculada a `customers.name` e `contacts.full_name`, e uma propriedade `segment` vinculada só a `customers.segment`:

```sql
-- rows from both tables; segment is NULL for crm.contacts rows
SELECT _source_table, name, segment
FROM ontology.customer
WHERE segment = 'enterprise';
```

### Direitos de leitura {: #read-rights }

As mesmas regras de governança que valem para as suas próprias tabelas valem aqui.

Um termo só aparece nas views de definição se você alcança pelo menos um dos seus domínios. Um termo sem domínio é visível para todos. Uma linha de vinculação só aparece quando a coluna dela é servida a você. Uma aresta só aparece quando os dois termos são visíveis para você. Uma coluna de propriedade em uma view de classe só aparece quando as colunas subjacentes são servidas.

Nenhum acesso especial é necessário. Se você pode consultar uma tabela, pode consultar os termos de classe ancorados nela.

### SPARQL {: #sparql }

O endpoint SPARQL serve o esquema `ontology` como uma ontologia RDFS/OWL/SKOS padrão. Todo termo de classe vira uma `owl:Class`, toda aresta de tipo vira `rdfs:subClassOf`, todo termo de propriedade vira `owl:DatatypeProperty` ou `owl:ObjectProperty` com `rdfs:domain` e `rdfs:range`, arestas de subpropriedade viram `rdfs:subPropertyOf`, nomes viram `skos:prefLabel` e definições viram `skos:definition`. Os mesmos direitos de leitura valem.

A IRI base de todos os termos é derivada da configuração `server.public_url`, sob o caminho `/vocab/term/`. Um termo de classe chamado `customer` tem a IRI `{public_url}/vocab/term/customer`.

Três consultas que um ontologista escreveria:

**Encontrar todos os subtipos de Party, em qualquer profundidade:**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?c WHERE {
  ?c rdfs:subClassOf* :Party .
}
```

**Encontrar clientes com um valor de segmento, usando a IRI do termo:**

```sparql
PREFIX : <{public_url}/vocab/term/>

SELECT ?x ?s WHERE {
  ?x a :Customer ;
     :segment ?s .
}
```

**Listar todas as propriedades declaradas em Customer com seus intervalos:**

```sparql
PREFIX : <{public_url}/vocab/term/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?p ?range WHERE {
  ?p rdfs:domain :Customer ;
     rdfs:range ?range .
}
```

Substitua `{public_url}` pelo endereço público da sua implantação, conforme configurado em `server.public_url`.
