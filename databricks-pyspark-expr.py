# Databricks notebook source
# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
Adaptado por Afonso Feliciano
"""

# COMMAND ----------

#Criando sessão em Spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# COMMAND ----------

#Importando a função expr
from pyspark.sql.functions import expr

# COMMAND ----------

#Criando dados e dataframe
dados = [("James","Bond"),("Scott","Varsa")] 
df = spark.createDataFrame(dados).toDF("col1","col2") 

#Concatenando colunas
df.withColumn("Nome",expr(" col1 ||','|| col2")).show()

# COMMAND ----------

#Utilizando CASE WHEN em SQL
#Definindo dados e colunas
data = [("James","M"),("Michael","F"),("Jen","")]
columns = ["nome","sexo"]

#Criando dataframe
df = spark.createDataFrame(data = data, schema = columns)

#Utilizando expr
df2 = df.withColumn("sexo", expr("CASE WHEN sexo = 'M' THEN 'Masculino' " + "WHEN sexo = 'F' THEN 'Feminino' ELSE 'Não Informado' END"))
df2.show()

# COMMAND ----------

#Adicionando meses em um valor de outra coluna
#Criando dados e dataframe
data = [("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)] 
df = spark.createDataFrame(data).toDF("data","incremento") 

#Realizando a soma de meses
df.select(df.data, df.incremento, expr("add_months(data,incremento)").alias("incremento_data")).show()

# COMMAND ----------

# Criando um alias utilizando as
df.select(df.data, df.incremento, expr("""add_months(data, incremento) as incremento_de_data""")).show()

# COMMAND ----------

# Realizando somas
df.select(df.data, df.incremento, expr("incremento + 5 as novo_incremento")).show()

# COMMAND ----------

#Realizando conversão e visualizando o schema
df.select("incremento", expr("cast(incremento as string) as string_incremento")).printSchema()

# COMMAND ----------

#Utilizando expr() para filtrar linhas
dados = [(100,2),(200,3000),(500,500)] 

#Criando dataframe
df = spark.createDataFrame(dados).toDF("col1","col2") 

#Realizando filtro
df.filter(expr("col1 == col2")).show()

# COMMAND ----------


