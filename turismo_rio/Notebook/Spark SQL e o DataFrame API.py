# Databricks notebook source
# MAGIC %fs ls "dbfs:/FileStore/shared_uploads/"

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------
#Utilize o local onde foi feito o upload do arquivo
path = "dbfs:/FileStore/shared_uploads/"

# COMMAND ----------

# Leitura do arquivo Json e transformando em um DataFrame
#option("multiline", True) \   # Permite a leitura de arquivos JSON com várias linhas
#.option("header", "false") \  # Especifica que o arquivo JSON não possui cabeçalho
df = spark \
    .read \
    .option("multiline", True) \
    .option("header", "false") \
    .json(path+"turismo_rio.json")

# Exibindo as primeiras 20 linhas do DataFrame
display(df.head(20))
# Exibindo as últimas 20 linhas do DataFrame
display(df.tail(20))

# Exibindo o esquema do DataFrame
df.printSchema()
df.schema

# COMMAND ----------

# Criando ou substituindo uma visualização temporária chamada "turismo_rio"
df.createOrReplaceTempView("turismo_rio") 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Método: 1
# MAGIC -- Seleciona a categoria, formata a data da visita e exibe o nome do local
# MAGIC select categoria, date_format(data_visita,'yyyy-MM-dd HH:mm:ss') as data_visita, nome_local, endereco.*
# MAGIC from turismo_rio
# MAGIC where categoria is not null
# MAGIC order by categoria  ASC

# COMMAND ----------

# MAGIC %md
# MAGIC # Alguns Métodos do SparkSession
# MAGIC
# MAGIC | Método | Definição | Exemplo |
# MAGIC |--------|-----------|---------|
# MAGIC | `read` | Retorna um DataFrameReader que pode ser usado para ler dados em um DataFrame. | `df = spark.read.csv('caminho/para/arquivo.csv')` |
# MAGIC | `readStream` | Retorna um DataStreamReader que pode ser usado para ler dados de streaming em um DataFrame. | `df = spark.readStream.format('socket').option('host', 'localhost').option('port', 9999).load()` |
# MAGIC | `sql` | Executa uma consulta SQL e retorna o resultado como um DataFrame. | `df = spark.sql('SELECT * FROM tabela')` |
# MAGIC | `table` | Retorna o DataFrame associado a uma tabela. | `df = spark.table('nome_da_tabela')` |
# MAGIC | `createDataFrame` | Cria um DataFrame a partir de uma lista de listas, RDD ou pandas DataFrame. | `df = spark.createDataFrame([(1, 'a'), (2, 'b')], ['col1', 'col2'])` |
# MAGIC | `range` | Cria um DataFrame com uma única coluna contendo números inteiros dentro de um intervalo especificado. | `df = spark.range(1, 10)` |
# MAGIC | `catalog` | Retorna o catálogo de metadados do Spark. | `databases = spark.catalog.listDatabases()` |
# MAGIC | `udf` | Registra uma função definida pelo usuário (UDF) para ser usada em consultas SQL. | `spark.udf.register('soma', lambda x, y: x + y)` |
# MAGIC | `conf` | Retorna a configuração do Spark. | `config = spark.conf.get('spark.app.name')` |
# MAGIC | `newSession` | Cria uma nova sessão do Spark compartilhando a mesma configuração que a sessão atual. | `nova_sessao = spark.newSession()` |
# MAGIC | `stop` | Para a sessão do Spark. | `spark.stop()` |
# MAGIC | `version` | Retorna a versão do Spark. | `versao = spark.version` |
# MAGIC | `sparkContext` | Retorna o SparkContext associado à sessão do Spark. | `sc = spark.sparkContext` |
# MAGIC | `streams` | Retorna o StreamingQueryManager associado à sessão do Spark. | `queries = spark.streams.active` |
# MAGIC | `addFile` | Adiciona um arquivo ao SparkContext. | `spark.sparkContext.addFile('caminho/para/arquivo')` |
# MAGIC | `addPyFile` | Adiciona um arquivo Python ao SparkContext. | `spark.sparkContext.addPyFile('caminho/para/arquivo.py')` |
# MAGIC | `setCheckpointDir` | Define o diretório de checkpoint para o SparkContext. | `spark.sparkContext.setCheckpointDir('caminho/para/diretorio')` |

# COMMAND ----------

# Método: 2 Usando SparkSession "spark.read), DataFrame API (lazily evaluated), isto é, a query não será executada até que o método display(), show(), collect(), count() e etc, algum gatilho seja chamado
# Seleciona a categoria, formata a data da visita e exibe o nome do local
display(spark
      .table('turismo_rio')
      .select('categoria', 
              date_format('data_visita', 'yyyy-MM-dd HH:mm:ss').alias('data_visita'), 
              'nome_local',
              'endereco.*')
      .where("categoria is not null")
      .orderBy(asc('categoria'))
     )

# COMMAND ----------

# Método: 3 Usando SparkSession "spark.sql"
# Seleciona a categoria, formata a data da visita e exibe o nome do local

df_result = spark.sql("""
    SELECT categoria, 
           date_format(data_visita, 'yyyy-MM-dd HH:mm:ss') AS data_visita, 
           nome_local
      FROM turismo_rio
     WHERE categoria IS NOT NULL
  ORDER BY categoria ASC
""")
df_result.show() #"gatilho"

# COMMAND ----------

# Carregando a tabela temporária "turismo_rio" em um DataFrame
turismo_rio = spark.table('turismo_rio')
type(turismo_rio)

# COMMAND ----------

# Método: 4 Transformando em duas tabelas e realizando um left join
# Criando um DataFrame com os campos selecionados da tabela "turismo_rio"
pontos_turisticos = spark.sql("""select   ID, 
                                        avaliacao, 
                                        categoria, 
                                        data_visita, 
                                        nome_local, 
                                        numero_visitantes, 
                                        preco_medio 
                                from turismo_rio where id is not null""")

# Criando um DataFrame com os campos "id" e "endereco" da tabela "turismo_rio"
enderecos = spark.sql("select id, endereco from turismo_rio where id is not null")

# Selecionando campos individuais do STRUCT "endereco"
enderecos_individuais = enderecos.select(
                col("id").alias("id"),
                col("endereco.bairro").alias("bairro"),
                col("endereco.cep").alias("cep"),
                col("endereco.cidade").alias("cidade"),
                col("endereco.estado").alias("estado"),
                col("endereco.rua").alias("rua")
)

# Exibindo o DataFrame "pontos_turisticos"
display(pontos_turisticos)

# Exibindo o DataFrame "enderecos"
display(enderecos)

# Exibindo o DataFrame "enderecos_individuais"
display(enderecos_individuais)

# Realizando um join("LEFT JOIN") entre "pontos_turisticos" e "enderecos_individuais" no campo "id"
df_result = pontos_turisticos.join(enderecos_individuais, on="id", how='left')

# Exibindo o DataFrame resultante "df_result"
display(df_result)

# COMMAND ----------

#Criando arquivos no DBFS usando append e overwrite

# Reescreve o arquivo Delta com o DataFrame 
pontos_turisticos.write.format("delta").mode("overwrite").save(path+"resultado/")

# Adicione dados ao arquivo existente
enderecos_individuais.write.format("delta").mode("append").save(path+"resultado_end/")

# COMMAND ----------

display(dbutils.fs.ls(path+"resultado/"))
display(dbutils.fs.ls(path+"resultado_end/"))

# COMMAND ----------

#Criando o Schema Turismo
spark.sql("CREATE SCHEMA IF NOT EXISTS turismo")

# COMMAND ----------

# Criando tabelas físicas no catálogo
pontos_turisticos.write.format("delta").mode("overwrite").saveAsTable("turismo.pontos_turisticos_rj")
enderecos_individuais.write.format("delta").mode("append").saveAsTable("turismo.enderecos_rj")