# Databricks notebook source
# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
Adaptado por Afonso Feliciano
"""

# COMMAND ----------

#Importando bibliotecas
import pyspark
from pyspark.sql import SparkSession

# COMMAND ----------

#Criando spark session
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# COMMAND ----------

#Criando valores do departamento
dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]

#Criando as colunas
deptColumns = ["dept_name", "dept_id"]

#Criando dataframe
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)

#Exibindo o schema
deptDF.printSchema()

#Exibindo o dataframe
deptDF.display()

# COMMAND ----------

#Utilizando a função collect em todo o dataframe
dataCollect = deptDF.collect()

#Exibindo o resultado
print(dataCollect)

# COMMAND ----------

#Utilizando a função collect juntamente com a select para buscar apenas uma coluna do dataframe
dataCollect2 = deptDF.select("dept_name").collect()

#Exibindo o resultado
print(dataCollect2)

# COMMAND ----------

#Utilizando um laço de repetição para percorrer os dados presente na collect e exibir linha a linha
for row in dataCollect:
    print(row['dept_name'] + "," +str(row['dept_id']))
