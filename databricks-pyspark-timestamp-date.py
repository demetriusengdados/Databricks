# Databricks notebook source
# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
Adaptado por Afonso Feliciano
"""

# COMMAND ----------

# Criando sessão spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
          .appName('SparkByExamples.com') \
          .getOrCreate()

# COMMAND ----------

#Criando dataframe
df = spark.createDataFrame(
        data = [ ("1","2019-06-24 12:01:19.000")],
        schema = ["id","input_timestamp"])
df.printSchema()

# COMMAND ----------

#Importando funções spark sql
from pyspark.sql.functions import *

# COMMAND ----------

#Conversão utilizando o cast
df.withColumn('date_type', col('input_timestamp').cast('date')) \
       .show(truncate=False)

# COMMAND ----------

# Conversão utilizando to_timestamp com cast
df.withColumn('date_type', to_timestamp('input_timestamp').cast('date')) \
  .show(truncate=False)

# COMMAND ----------

#Conversão utilizando data e hora em formato customizado
df.select(to_date(lit('06-24-2019 12:01:19.000'),'MM-dd-yyyy HH:mm:ss.SSSS')) \
  .show()

# COMMAND ----------

#Timestamp String para Date Type
df.withColumn("date_type",to_date("input_timestamp")) \
  .show(truncate=False)



# COMMAND ----------

#Timestamp para DateType
df.withColumn("date_type",to_date(current_timestamp())) \
  .show(truncate=False) 

# COMMAND ----------

#Multiplas conversões, timestamp e datetype
df.withColumn("timestamp",to_timestamp(col("input_timestamp"))) \
  .withColumn("datetype",to_date(col("timestamp"))) \
  .show(truncate=False)


# COMMAND ----------

#SQL Timestamp para DateType
spark.sql("select to_date(current_timestamp) as date_type")


# COMMAND ----------

#SQL CAST Timestamp para DateType
spark.sql("select date(to_timestamp('2019-06-24 12:01:19.000')) as date_type")

# COMMAND ----------

#SQL CAST timestamp string para DateType
spark.sql("select date('2019-06-24 12:01:19.000') as date_type")

# COMMAND ----------

#SQL Timestamp String (formato default) para DateType
spark.sql("select to_date('2019-06-24 12:01:19.000') as date_type")


# COMMAND ----------

#SQL Formato customizado para DateType
spark.sql("select to_date('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as date_type")

# COMMAND ----------


