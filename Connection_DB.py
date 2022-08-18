# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from pyspark.sql.functions import substring
from pyspark.sql.column import Column
from math import sin, cos, sqrt, atan2, radians
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from pyspark.sql import *
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import lit 
from datetime import datetime, date


# COMMAND ----------

# DBTITLE 1,Query para leitura de tabelas
query = "SELECT * FROM SalesLT.Address"

# COMMAND ----------

# DBTITLE 1,Variáveis e secrets
lakeAccountName = dbutils.secrets.get('keyvault2','blobAccountName')
lakeAccessKey   = dbutils.secrets.get('keyvault2','kvkeydatalake')
blobAccessKey   = lakeAccessKey
dwServerName    = dbutils.secrets.get('keyvault2','dwServerName') 
dwServerPort    = dbutils.secrets.get('keyvault2','dwServerPort') 
dwDatabaseName  = dbutils.secrets.get('keyvault2','dwDatabaseName')
dwUserName      = dbutils.secrets.get('keyvault2','dwUserDatabase')
dwPassword      = dbutils.secrets.get('keyvault2','dwPassDatabase')


# COMMAND ----------

# DBTITLE 1,Função para escrita (append) no banco de dados
def write_db_append(tbname,df):
  
  server_name = "jdbc:sqlserver://"+dwServerName+";database="+dwDatabaseName+";user="+dwUserName+";password="+dwPassword
 
  sql_conn = server_name;
 
  try:  
    df.write \
      .format("com.microsoft.sqlserver.jdbc.spark")\
      .option("url", sql_conn)\
      .option("dbtable", tbname)\
      .option("tableLock", "true")\
      .option("batchsize", "100000")\
      .mode("append")\
      .save()
  except ValueError as error :
      print("Conexão retornou o seguinte erro: ", error)

# COMMAND ----------

# DBTITLE 1,Função para escrita (overwrite) no banco de dados
def write_db(tbname,df):
  
  server_name = "jdbc:sqlserver://"+dwServerName+";database="+dwDatabaseName+";user="+dwUserName+";password="+dwPassword
 
  sql_conn = server_name;
 
  try:  
    df.write \
      .format("com.microsoft.sqlserver.jdbc.spark")\
      .option("url", sql_conn)\
      .option("dbtable", tbname)\
      .option("tableLock", "true")\
      .option("batchsize", "100000")\
      .mode("overwrite")\
      .save()
  except ValueError as error :
      print("Conexão retornou o seguinte erro: ", error)


# COMMAND ----------

# DBTITLE 1,Função para leitura de tabelas
def read_db(query):  
  
  server_name = "jdbc:sqlserver://"+dwServerName+";database="+dwDatabaseName+";user="+dwUserName+";password="+dwPassword
 
  sql_conn = server_name; 
 
  df_sel = spark.read\
    .format("com.microsoft.sqlserver.jdbc.spark")\
    .option("url", sql_conn)\
    .option("query", query)\
    .load()
  return df_sel

# COMMAND ----------

# DBTITLE 1,Realizando uma leitura no banco de dados
#read = read_db(query)

# COMMAND ----------

#display(read)
