Projeto: Manipulação e Transformação de Dados com Apache Spark

Este projeto demonstra a aplicação do Apache Spark para leitura, transformação, estruturação e persistência de dados utilizando o Delta Lake.

Descrição do Projeto

O objetivo principal é manipular dados turísticos do Rio de Janeiro utilizando o Apache Spark, aplicando boas práticas de engenharia de dados, e persistir as informações no Delta Lake. O projeto envolve a leitura de arquivos JSON, transformação de dados, utilização de Spark SQL e DataFrame API, e armazenamento eficiente utilizando o formato Delta.

Tecnologias Utilizadas

- Apache Spark (PySpark)
- Delta Lake (para persistência de dados)
- Spark SQL (consultas SQL)
- DataFrame API (transformações de dados)
- DBFS (Databricks File System)

Estrutura do Projeto

1. Leitura de Arquivo JSON

O arquivo JSON contém dados com estrutura aninhada. A leitura foi feita com a opção `multiline` para lidar com múltiplas linhas em arquivos JSON:

df = spark.read.option("multiline", True).json("dbfs:/.../turismo_rio.json")

2. Criação de uma Visualização Temporária

Após a leitura, criamos uma visualização temporária chamada `turismo_rio` para facilitar o uso de consultas SQL diretamente no DataFrame:

df.createOrReplaceTempView("turismo_rio")

3. Consultas SQL e DataFrame API

Foram utilizadas diferentes abordagens para realizar a consulta e transformação dos dados:

Consultas com Spark SQL:

SELECT categoria, date_format(data_visita, 'yyyy-MM-dd') AS data_visita, nome_local
FROM turismo_rio

Consultas com DataFrame API:

df.select("categoria", date_format("data_visita", "yyyy-MM-dd"), "nome_local")

4. Transformação de Dados Estruturados (STRUCT)

Para normalizar os dados do tipo STRUCT (como o campo `endereco`), realizamos a transformação para colunas individuais:

enderecos_individuais = enderecos.select(
    col("id").alias("id"),
    col("endereco.bairro").alias("bairro"),
    col("endereco.cep").alias("cep"),
    col("endereco.cidade").alias("cidade"),
    col("endereco.estado").alias("estado"),
    col("endereco.rua").alias("rua")
)

5. Realização de JOINs

Foi realizado um JOIN entre os DataFrames `pontos_turisticos` e `enderecos_individuais` para enriquecer os dados com as informações de endereço:

df_result = pontos_turisticos.join(enderecos_individuais, on="id", how="left")

6. Escrita em Delta Lake

O DataFrame resultante foi salvo no Delta Lake, usando os modos `overwrite` e `append`:

df.write.format("delta").mode("overwrite").save("dbfs:/.../resultado/")
enderecos_individuais.write.format("delta").mode("append").save("dbfs:/.../resultado_end/")

7. Criação de Schema e Tabelas no Catálogo

Foi criado um schema turismo no catálogo do Spark e tabelas físicas para armazenar os dados de forma estruturada:

spark.sql("CREATE SCHEMA IF NOT EXISTS turismo")
pontos_turisticos.write.format("delta").mode("overwrite").saveAsTable("turismo.pontos_turisticos_rj")
enderecos_individuais.write.format("delta").mode("append").saveAsTable("turismo.enderecos_rj")

Objetivos do Projeto

- Demonstrar o uso do Apache Spark para manipulação de dados grandes.
- Aplicar boas práticas de engenharia de dados para transformação e persistência eficiente de dados em formato Delta Lake.
- Utilizar Spark SQL e DataFrame API para consultar e transformar dados.

Resultados Esperados

O resultado do projeto são tabelas físicas no catálogo do Spark, que podem ser consultadas, além de dados transformados e persistidos no Delta Lake, permitindo fácil integração com futuras consultas e análises.

Como Executar

Pré-requisitos

1. Apache Spark instalado.
2. Delta Lake configurado para seu ambiente.
3. O arquivo JSON `turismo_rio.json` presente no diretório especificado.

Passos para execução

1. Clone este repositório.
2. Coloque o arquivo JSON no caminho indicado no código (`dbfs:/.../turismo_rio.json`).
3. Execute os scripts para realizar as transformações e gravar os dados no Delta Lake.

Referências

- Apache Spark Documentation (https://spark.apache.org/docs/latest/)
- Delta Lake Documentation (https://docs.delta.io/latest/)
