# Databricks notebook source
# DBTITLE 1,Importa as bibliotecas
from dateutil.relativedelta import relativedelta
import os.path
import math
from datetime import date, datetime, timedelta
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Função para Criar um fluxo de stream
import random
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *
 

 # Function to start a streaming query with a stream of randomly generated load data and append to the parquet table
def generate_and_append_data_stream(table_format, table_name, schema_ok=False, type="batch"):
  
  stream_data = (spark.readStream.format("rate").option("rowsPerSecond", 500).load()
    .withColumn("loan_id", 10000 + col("value"))
    .withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer"))
    .withColumn("paid_amnt", col("funded_amnt") - (rand() * 2000))
    .withColumn("addr_state", random_state())
    .withColumn("type", lit(type)))
    
  if schema_ok:
    stream_data = stream_data.select("loan_id", "funded_amnt", "paid_amnt", "addr_state", "type", "timestamp")
      
  query = (stream_data.writeStream
    .format(table_format)
    .option("checkpointLocation", my_checkpoint_dir())
    .trigger(processingTime = "5 seconds")
    .table(table_name))
 
  return query

# COMMAND ----------

trusted_path_teste = "/mnt/trusted/catalogo/delta/vendas/"

# COMMAND ----------

# DBTITLE 1,Carregando arquivo delta
df = spark.read.format("delta").load(trusted_path_teste)

# COMMAND ----------

# DBTITLE 1,Criando uma tabela delta com merge no schema
df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable("sales_delta")

# COMMAND ----------

spark.sql("select count(1) from sales_delta").show()
spark.sql("select * from sales_delta").show(3)

# COMMAND ----------

# DBTITLE 1,Gerando o fluxo de stream
stream_A = generate_and_append_data_stream(table_format="delta", table_name="sales_delta", schema_ok=True, type='stream A')
stream_b = generate_and_append_data_stream(table_format="delta", table_name="sales_delta", schema_ok=True, type='stream B')

# COMMAND ----------

# DBTITLE 1,Lendo o fluxo de stream
display(spark.readStream.format("delta").table("sales_delta").groupBy("type").count().orderBy("type"))

# COMMAND ----------

# DBTITLE 1,Lendo o fluxo de stream
#Registros atualizados a cada 10 segundos (habilitar o grafico)
display(spark.readStream.format("delta").table("sales_delta").groupBy("type",window("timestamp","10 seconds")).count().orderBy("window"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select cliente, count(1) from sales_delta group by cliente

# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------

# DBTITLE 1,Visualizando os logs
# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales_delta
