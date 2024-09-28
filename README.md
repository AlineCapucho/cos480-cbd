# Sobre este repositório

A finalidade deste repositório é conter o código desenvolvido para o Trabalho Prático 1 da disciplina COS480 - Construção de Banco de Dados da UFRJ. Foi desenvolvido implementações de 3 organizações primárias de arquivos de registros, e também uma bancada de teste para cada uma delas.

## A respeito da bancada de testes

A bancada de testes foi desenvolvida para o dataset Coffee Shop Sales, que simula transações em uma cafeteria. Para realizar os testes, primeiro é chamado a classe da organização primária de arquivos de registros, que para ser instanciada é necessário passar como parâmetro o tamanho dos blocos do arquivo de registros. Após ela ser instanciada, é necessário que ela leia o arquivo csv do dataset para construir o arquivo de registros da organização primária no formato txt ou tar.gzip (dependendo da organização primária). Em seguida, para realizar operações na base de dados lida a partir do csv é necessário passar o endereço do arquivo txt da base de dados. Isso também é necessário no caso da base de dados estar armazenada no formato tar.gzip, pois neste caso a base de dados é decomprimida para txt possuindo o mesmo nome mudando apenas a extensão. A seguir eu comentarei um pouco sobre os testes realizados em cada seção da bancada de testes.

### Insert Single Record

São realizados 3 testes, sendo um para verificar a verificação de integridade da base de dados ao tentar inserir um registro com chave primária duplicada, uma para verificar a inserção de um registro, e uma seleção por única chave primária para verificar se o registro inserido anteriormente foi mesmo inserido.

### Insert Multiple Records

São realizados 3 testes, sendo uma inserção de vários registros, uma verificação de que os registros foram inseridos corretamente por meio de uma seleção por única chave primária em cada chave primária dos registros inseridos, e uma selecção por múltiplas chaves primárias nas chaves primárias dos registros inseridos.

### Select By Single Primary Key

É realizado um teste que verifica se é possível selecionar um registro presente na base de dados por meio de uma única chave primária.

### Select By Multiple Primary Key

É realizado um teste que verifica se é possível selecionar múltiplos registros presentes na base de dados por meio de múltiplas chaves primárias.

### Select By Field Interval

São realizados 2 testes, ambos sendo seleção por intervalo de campo, onde no primeiro é um intervalo do campo de data, e o segundo é um intervalo do campo de hora. Após cada seleção é apresentado alguns dos registros selecionados e a quantidade de registros selecionados.

### Select By Single Field Value

É realizado um teste onde é feito uma seleção de registros por valor de um campo. Após a seleção é apresentado alguns dos registros selecionados e a quantidade de registros selecionados.

### Delete By Primary Key

São realizados 2 testes, o primeiro é uma deleção de um registro por meio da chave primária, e o segundo é uma seleção por única chave primária do registro deletado, que retorna uma exceção indicando que o registro foi mesmo deletado.

### Delete By Criterion

São realizados 4 testes, o primeiro é uma deleção de todos os registros que pediram um produto da categoria Coffee, seguido de uma seleção por valor de um campo usando o campo e valor da deleção anterior para mostrar que os registros foram mesmo deletados, o terceiro é uma deleção de todos os registros que possuem uma determinada data, seguido de uma seleção por valor de um campo usando o campo e valor da deleção anterior para mostrar que os registros foram mesmo deletados.