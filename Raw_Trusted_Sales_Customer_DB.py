# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from dateutil.relativedelta import relativedelta
import os.path
import math
from datetime import date, datetime, timedelta
from pyspark.sql.functions import lit,input_file_name, monotonically_increasing_id, concat, col, count, trim, unix_timestamp, from_unixtime, to_date
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Conexão com Lake
# MAGIC %run /Repos/adb-reposturma01imersao/Imersao-Azure-Big-Data/databricks/Connection/Connection_Data_Lake

# COMMAND ----------

# DBTITLE 1,Conexão com DB
# MAGIC %run /Repos/adb-reposturma01imersao/Imersao-Azure-Big-Data/databricks/Connection/Connection_DB

# COMMAND ----------

# DBTITLE 1,Origem/Destino dos arquivos
#Diretorio de origem

#raw_path_address = "/mnt/raw/address/"
raw_path_customer = "/mnt/raw/Customer/"
raw_path_sales = "/mnt/raw/Sales/"

#Diretorio de destino com data atual

trusted_path_testePython = "/mnt/trusted/catalogo/vendas/Python/"
trusted_path_testeSQL = "/mnt/trusted/catalogo/vendas/SQL/"

#Setando os valores das variaveis no parâmetros para utilizar nas celulas em SQL
spark.conf.set('var.raw_path_sales', raw_path_sales)
spark.conf.set('var.raw_path_customer', raw_path_customer)
spark.conf.set('var.trusted_path_testePython', trusted_path_testePython)
spark.conf.set('var.trusted_path_testeSQL', trusted_path_testeSQL)
#spark.conf.set('var.raw_path_address', raw_path_address)

# COMMAND ----------

# DBTITLE 1,Leitura da tabela Sales (csv) - Python
df_read_raw_sales = spark.read.format('csv').load(raw_path_sales, sep=";", inferSchema="true", header="true")
df_read_raw_sales.createOrReplaceTempView ("tempView_vendasPython")

# COMMAND ----------

# DBTITLE 1,Leitura da tabela Sales (csv) - SQL
# MAGIC %sql
# MAGIC /*
# MAGIC Leitura da tabela Sales (parquet)
# MAGIC 
# MAGIC CREATE TEMPORARY VIEW parquetTable USING org.apache.spark.sql.parquet
# MAGIC OPTIONS (path "/mnt/raw/Sales/")
# MAGIC SELECT * FROM parquetTable
# MAGIC */
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_vendasSQL USING csv
# MAGIC OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'delimiter' = ';', path '${var.raw_path_sales}')

# COMMAND ----------

# DBTITLE 1,Exibir schema Python
df_read_raw_sales.printSchema()

# COMMAND ----------

# DBTITLE 1,Exibir schema da tabela - SQL
# MAGIC %sql
# MAGIC DESCRIBE TABLE tempView_vendasSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados no Dataframe - Python
df_read_raw_sales.show()
#ou
display(df_read_raw_sales)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - Python + SQL
dfTempView_vendasPython = spark.sql("""select * from tempView_vendasPython""")

display(dfTempView_vendasPython)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tabela temporária - SQL
# MAGIC %sql
# MAGIC SELECT * FROM tempView_vendasSQL

# COMMAND ----------

# DBTITLE 1,Leitura da tabela Customer - Python
df_read_raw_customer = spark.read.format('parquet').load(raw_path_customer)

# COMMAND ----------

# DBTITLE 1,Adicionando Campos no DF - Customer - Python
df_read_raw_customer = df_read_raw_customer.withColumn('Updated_Date', lit(''))
df_read_raw_customer = df_read_raw_customer.withColumn('Insert_Date', lit(''))

# COMMAND ----------

# DBTITLE 1,Removendo dados duplicados - Customer - Python
df_read_raw_customer_dup = df_read_raw_customer.dropDuplicates()
print('Em processo - leitura Raw e deleção de linhas duplicadas')

# COMMAND ----------

# DBTITLE 1,Criação tempview - Customer - Python
df_read_raw_customer_dup.createOrReplaceTempView ("tempView_customerPython")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from tempView_customerPython

# COMMAND ----------

# DBTITLE 1,Criação da Tempview Customer - SQL
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tempView_customerSQL USING org.apache.spark.sql.parquet
# MAGIC OPTIONS (path '${var.raw_path_customer}')

# COMMAND ----------

# DBTITLE 1,Adicionando Campos na TempView + Removendo dados duplicados - Customer - SQL
# MAGIC %sql
# MAGIC create or replace temp view tempView_customerSQL2 
# MAGIC as 
# MAGIC select distinct 
# MAGIC CustomerID
# MAGIC ,NameStyle
# MAGIC ,Title
# MAGIC ,FirstName
# MAGIC ,MiddleName
# MAGIC ,LastName
# MAGIC ,Suffix
# MAGIC ,CompanyName
# MAGIC ,SalesPerson
# MAGIC ,EmailAddress
# MAGIC ,Phone
# MAGIC ,PasswordHash
# MAGIC ,PasswordSalt
# MAGIC ,rowguid
# MAGIC ,ModifiedDate
# MAGIC , '' as Insert_Date
# MAGIC , '' as Updated_Date
# MAGIC from tempView_customerSQL

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  Python 
consulta_raw_customer = spark.sql("""select 
 CustomerID
,NameStyle
,Title
,FirstName
,MiddleName
,LastName
,Suffix
,CompanyName
,SalesPerson
,EmailAddress
,Phone
,PasswordHash
,PasswordSalt 
,Insert_Date
,Updated_Date
from tempView_customerPython""")
display(consulta_raw_customer)

# COMMAND ----------

# DBTITLE 1,Consultando dados da tempview -  SQL
# MAGIC %sql
# MAGIC select *
# MAGIC from tempView_customerSQL2

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - Python
display(consulta_raw_customer.limit(5))
#ou
consulta_raw_customer.limit(5).show()

# COMMAND ----------

# DBTITLE 1,Top N (Limit) - SQL
# MAGIC %sql
# MAGIC select *
# MAGIC from tempView_customerSQL2
# MAGIC limit 5

# COMMAND ----------

# DBTITLE 1,Filtro - Python
consulta_raw_customer.filter(consulta_raw_customer.Title == "Ms.").limit(5).show()

# COMMAND ----------

# DBTITLE 1,Filtro - SQL
# MAGIC %sql
# MAGIC select *
# MAGIC from tempView_customerSQL2
# MAGIC where Title = 'Ms.'
# MAGIC limit 5

# COMMAND ----------

# DBTITLE 1,Utilizando funções nativas - Python
consulta_raw_customer.withColumn('Title',regexp_replace('Title','Ms.','Sra.')).filter(consulta_raw_customer.CustomerID == "11").show()

# COMMAND ----------

# DBTITLE 1,Utilizando funções nativas - SQL
# MAGIC %sql
# MAGIC select 
# MAGIC   replace(Title, 'Ms.', 'Sra.') as funcaoReplace
# MAGIC   ,concat(FirstName, ' ', LastName) as funcaoConcat
# MAGIC   ,length(Phone) as funcaoLen
# MAGIC   ,DATE_FORMAT(ModifiedDate,'yMMdkkmm') as funcaoDateFormat
# MAGIC   ,cast(replace(Phone, '-', '') as bigint) as funcaoReplaceCast
# MAGIC   , *
# MAGIC from tempView_customerSQL2
# MAGIC where CustomerID = 11

# COMMAND ----------

# DBTITLE 1,Join - Python
dfJoinPython = consulta_raw_customer.join(dfTempView_vendasPython , consulta_raw_customer.CustomerID == dfTempView_vendasPython .CustomerID, "inner")

display(dfJoinPython)

# COMMAND ----------

# DBTITLE 1,Join Vendas + Customer - Python + SQL 
joinPythonSQL = spark.sql("""select 
csql.CustomerID
,concat (csql.FirstName, ' ', csql.LastName) as FullName
,vp.SalesOrderID
,vp.SalesOrderNumber
,vp.OrderDate
,vp.SubTotal
,vp.TaxAmt
,vp.Freight
,vp.TotalDue
,date_format(current_timestamp, 'yyyyMM') as Partition_Date
from tempView_customerSQL2 as csql
  join tempView_vendasPython as vp 
    on csql.CustomerID = vp.CustomerID""")

display(joinPythonSQL)

# COMMAND ----------

# DBTITLE 1,Join - SQL
# MAGIC %sql
# MAGIC select 
# MAGIC csql.CustomerID
# MAGIC ,concat (csql.FirstName, ' ', csql.LastName) as FullName
# MAGIC ,vp.SalesOrderID
# MAGIC ,vp.SalesOrderNumber
# MAGIC ,vp.OrderDate
# MAGIC ,vp.SubTotal
# MAGIC ,vp.TaxAmt
# MAGIC ,vp.Freight
# MAGIC ,vp.TotalDue
# MAGIC ,date_format(current_timestamp, 'yyyyMM') as Partition_Date
# MAGIC from tempView_customerSQL2 as csql
# MAGIC   join tempView_vendasPython as vp 
# MAGIC     on csql.CustomerID = vp.CustomerID

# COMMAND ----------

# DBTITLE 1,Agregação - Python
#Nesse exemplo utilizamos um sum na coluna Valor_total e agrupamos esses dados pelo cliente

df_read_raw_sales.groupby('CustomerID').agg(sum(col('TotalDue'))).show()

#Agregação com filtro cliente
#df_read_raw_sales.groupby('Cliente').agg(sum(col('Valor_Total'))).filter(df_read_raw_sales.Cliente == "BEMOLDADOS LTDA").show()

# COMMAND ----------

# DBTITLE 1,Agregação - SQL
# MAGIC %sql
# MAGIC select 
# MAGIC csql.CustomerID
# MAGIC ,concat (csql.FirstName, ' ', csql.LastName) as FullName
# MAGIC ,sum(vp.TotalDue) as ValorTotal
# MAGIC from tempView_customerSQL2 as csql
# MAGIC   join tempView_vendasPython as vp 
# MAGIC     on csql.CustomerID = vp.CustomerID
# MAGIC group by csql.CustomerID ,   concat (csql.FirstName, ' ', csql.LastName)

# COMMAND ----------

# DBTITLE 1,Agregação - PySpark
dfVendasAgregadas = spark.sql("""
select 
csql.CustomerID
,concat (csql.FirstName, ' ', csql.LastName) as FullName
,sum(vp.TotalDue) as ValorTotal
,date_format(current_timestamp, 'yyyyMM') as Partition_Date
from tempView_customerSQL2 as csql
  join tempView_vendasPython as vp 
    on csql.CustomerID = vp.CustomerID
group by csql.CustomerID ,   concat (csql.FirstName, ' ', csql.LastName)
""")

# COMMAND ----------

# DBTITLE 1,Exibir resultado da agregação - Pyspark
display(dfVendasAgregadas)

# COMMAND ----------

# DBTITLE 1,Adicionando campos de controle no dataframe de Vendas Agregadas - Python
dfVendasAgregadas = dfVendasAgregadas.withColumn('Updated_Date', lit(''));
dfVendasAgregadas = dfVendasAgregadas.withColumn('Insert_Date', lit(''));

# COMMAND ----------

# DBTITLE 1,Criando tempView Vendas Agregadas Python - Python
dfVendasAgregadas.createOrReplaceTempView ("tempView_vendasAgregadasPython")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tempView_vendasAgregadasPython

# COMMAND ----------

# DBTITLE 1,Criando tempView + Adicionando campos de controle no dataframe Vendas Agregadas SQL - SQL
# MAGIC %sql
# MAGIC create or replace temp view tempView_vendasAgregadasSQL
# MAGIC as
# MAGIC select 
# MAGIC csql.CustomerID
# MAGIC ,concat (csql.FirstName, ' ', csql.LastName) as FullName
# MAGIC ,sum(vp.TotalDue) as ValorTotal
# MAGIC , '' as Insert_Date
# MAGIC , '' as Updated_Date
# MAGIC ,date_format(current_timestamp, 'yyyyMM') as Partition_Date
# MAGIC from tempView_customerSQL2 as csql
# MAGIC   join tempView_vendasPython as vp 
# MAGIC     on csql.CustomerID = vp.CustomerID
# MAGIC group by csql.CustomerID ,   concat (csql.FirstName, ' ', csql.LastName)

# COMMAND ----------

# DBTITLE 1,Inserindo o arquivo na camada trusted - Python
dfVendasAgregadas.write.mode('overwrite').format('parquet').partitionBy('Partition_Date').save(trusted_path_testePython)

# COMMAND ----------

# DBTITLE 1,Inserindo o arquivo na camada trusted - SQL
# MAGIC %sql
# MAGIC 
# MAGIC /*
# MAGIC O comando gera o arquivo na camada trusted do data lake, porém, a tabela criada esta no formato delta (delta lake) armazenada no database default
# MAGIC */
# MAGIC 
# MAGIC CREATE external TABLE IF NOT EXISTS table_vendasAgregadasSQLTeste
# MAGIC PARTITIONED BY (Partition_Date)
# MAGIC LOCATION '${var.trusted_path_testeSQL}'
# MAGIC as
# MAGIC select *
# MAGIC from tempView_vendasAgregadasSQL

# COMMAND ----------

# DBTITLE 1,Criando Database Delta Lake - SQL
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS dbImersaoAzureBigData;

# COMMAND ----------

# DBTITLE 1,Exibindo schema da tempView - SQL
# MAGIC %sql
# MAGIC describe table tempView_vendasAgregadasSQL

# COMMAND ----------

# DBTITLE 1,Criando tabela DELTA TABLE - SQL
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbImersaoAzureBigData.table_vendasAgregadasSQL
# MAGIC (
# MAGIC CustomerID int
# MAGIC ,FullName varchar(100)
# MAGIC ,ValorTotal numeric(18,4)
# MAGIC ,Insert_Date varchar(20)
# MAGIC ,Updated_Date varchar(20)
# MAGIC ,Partition_Date varchar(6)
# MAGIC )
# MAGIC 
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Partition_Date)
# MAGIC LOCATION '/mnt/trusted/catalogo/bronze_delta_tables/vendasAgregadas'
# MAGIC COMMENT 'Tabela table_vendasAgregadasSQL' 
# MAGIC TBLPROPERTIES ('orc.compress'='SNAPPY', 
# MAGIC                'auto.purge'='true', 
# MAGIC                'delta.logRetentionDuration'='interval 1825 days', 
# MAGIC                'delta.deletedFileRetentionDuration'='interval 1825 days',
# MAGIC                'overwriteSchema' = 'true');

# COMMAND ----------

# DBTITLE 1,Inserindo Dados na Tabela Delta - SQL
# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE dbImersaoAzureBigData.table_vendasAgregadasSQL
# MAGIC select * from tempView_vendasAgregadasSQL

# COMMAND ----------

# DBTITLE 1,Consultando tabela Delta - SQL
# MAGIC %sql
# MAGIC select *
# MAGIC from dbImersaoAzureBigData.table_vendasAgregadasSQL

# COMMAND ----------

# DBTITLE 1,Consultando tabela direto do arquivo delta - SQL
# MAGIC %sql
# MAGIC select *
# MAGIC FROM
# MAGIC   delta.`/mnt/trusted/catalogo/bronze_delta_tables/vendasAgregadas/`
