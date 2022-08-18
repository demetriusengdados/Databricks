# Databricks notebook source
# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
Adaptado por Afonso Feliciano
"""

from pyspark.sql import SparkSession


# COMMAND ----------

#Cria uma sparksession para trabalhar com RDD
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Exemplo 1 

# COMMAND ----------

#Define os dados
data = [("James", "Smith", "USA", "CA"),("Michael", "Rose", "USA","NY"), \
    ("Robert","Williams","USA","CA"),("Maria","Jones","USA","FL") \
       ]

#Define as colunas
columns = ["firstname","lastname","country","state"]

#Cria o dataframe
df=spark.createDataFrame(data=data,schema=columns)

#Exibe o dataframe
df.show()
display(df)

# COMMAND ----------

#Utiliza lambda para transformar os valores da coluna state em uma lista
states1 = df.rdd.map(lambda x: x[3]).collect()
print(states1)

# COMMAND ----------

#No resultado acima os valores não eram únicos, visto que o CA era visível mais de uma vez
from collections import OrderedDict 

#Utiliza a fromkeys para retornar um novo dicionário com valores únicos e após isso, converte o dicionário para lista
res = list(OrderedDict.fromkeys(states1))

print(res)

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ## Exemplo 2 

# COMMAND ----------

#Utiliza lambda para transformar os valores da coluna state em uma lista
states2 = df.rdd.map(lambda x: x.state).collect()

print(states2)

# COMMAND ----------

#Utiliza o select e collect para exibir os dados a níveis de row
states3 = df.select(df.state).collect()

print(states3)

# COMMAND ----------

#Combinação de select com lambda
states4 = df.select(df.state).rdd.flatMap(lambda x: x).collect()

print(states4)

# COMMAND ----------

#Utilização do pandas

states5 = df.select(df.state).toPandas()['state']
states6 = list(states5)
print(states6)




# COMMAND ----------

#Utilização do pandas para conversão dos dados em lista

pandDF = df.select(df.state,df.firstname).toPandas()

print(list(pandDF['state']))
print(list(pandDF['firstname']))
