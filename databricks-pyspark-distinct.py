# Databricks notebook source
# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
Adaptado por Afonso Feliciano
"""

# COMMAND ----------

#Importação das funções
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# COMMAND ----------

#Criando sessão em spark
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# COMMAND ----------

#Criando os dados
data = [("James", "Sales", 3000), \
    ("Michael", "Sales", 4600), \
    ("Robert", "Sales", 4100), \
    ("Maria", "Finance", 3000), \
    ("James", "Sales", 3000), \
    ("Scott", "Finance", 3300), \
    ("Jen", "Finance", 3900), \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000), \
    ("Saif", "Sales", 4100) \
  ]

#Defindo as colunas
columns= ["employee_name", "department", "salary"]

#Criando o dataframe spark
df = spark.createDataFrame(data = data, schema = columns)

#Exibindo o schema e os dados
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

#Criando dataframe com distinct 
distinctDF = df.distinct()

#Exibindo a quantidade de valores distintos
print("Contagem distinta: " +str(distinctDF.count()))

#Exibindo o dataframe distinto
distinctDF.show(truncate=False)

# COMMAND ----------

#Criando dataframe utilizando o dropDuplicates()
df2 = df.dropDuplicates()

#Exibindo a quantidade de valores distintos
print("Contagem de valores distintos: "+str(df2.count()))

#Exibindo o dataframe
df2.show(truncate=False)

# COMMAND ----------

#Removendo valores duplicados de colunas específicas do dataframe
dropDisDF = df.dropDuplicates(["department","salary"])

#Realizando a contagem distinta das colunas específicas
print("Contagem distinta das colunas department e salary : "+str(dropDisDF.count()))

#Exibindo o dataframe
dropDisDF.show(truncate=False)
