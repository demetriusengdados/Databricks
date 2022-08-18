# Databricks notebook source
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 10:20:19 2020

@author: prabha
Adaptado por Afonso Feliciano
"""

# COMMAND ----------

#Importando libs e funções para atualização de agregação
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import approx_count_distinct, collect_list
from pyspark.sql.functions import collect_set,sum,avg,max,countDistinct,count
from pyspark.sql.functions import first, last, kurtosis, min, mean, skewness 
from pyspark.sql.functions import stddev, stddev_samp, stddev_pop, sumDistinct
from pyspark.sql.functions import variance,var_samp,  var_pop

#Criando sessão
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# COMMAND ----------

#Criando amostra de dados
simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]

#Definindo nome das colunas
schema = ["funcionario", "departamento", "salario"]

# COMMAND ----------

#Criando dataframe
df = spark.createDataFrame(data=simpleData, schema = schema)

#Exibindo schema e dados
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

#Realizando contagem distinta
print("approx_count_distinct: " + str(df.select(approx_count_distinct("salario")).collect()[0][0]))


# COMMAND ----------

#Realizando média
print("avg: " + str(df.select(avg("salario")).collect()[0][0]))

# COMMAND ----------

#Transformando os salarios em lista de valores (sem distinção)
df.select(collect_list("salario")).show(truncate=False)

# COMMAND ----------

#Transformando os salarios em lista de valores (com distinção)
df.select(collect_set("salario")).show(truncate=False)

# COMMAND ----------

#Realizando contagem distinta de colunas combinadas
df2 = df.select(countDistinct("departamento", "salario"))

#Exibindo a contagem
df2.show(truncate=False)
print("Contagem distinta de departamento e salario: "+str(df2.collect()[0][0]))

# COMMAND ----------

#Realizando contagem
print("count: "+str(df.select(count("salario")).collect()[0]))

# COMMAND ----------

#Exibindo primeiro salario
df.select(first("salario")).show(truncate=False)

# COMMAND ----------

#Exibindo ultimo salario
df.select(last("salario")).show(truncate=False)

# COMMAND ----------

#Exibindo o achatamento da curva da função de distribuição de probabilidade (é uma medida estatistica)
df.select(kurtosis("salario")).show(truncate=False)

# COMMAND ----------

#Exibindo o maior salario
df.select(max("salario")).show(truncate=False)

# COMMAND ----------

#xibindo o menor salario
df.select(min("salario")).show(truncate=False)

# COMMAND ----------

#Exibindo a media
df.select(mean("salario")).show(truncate=False)

# COMMAND ----------

#Exibindo os tipos de desvio padrão
df.select(stddev("salario"), stddev_samp("salario"), stddev_pop("salario")).show(truncate=False)

# COMMAND ----------

#Exibindo a soma
df.select(sum("salario")).show(truncate=False)

# COMMAND ----------

#Exibindo uma soma distinta
df.select(sumDistinct("salario")).show(truncate=False)

# COMMAND ----------

#Exibindo a variancia
df.select(variance("salario"),var_samp("salario"),var_pop("salario")).show(truncate=False)

# COMMAND ----------


