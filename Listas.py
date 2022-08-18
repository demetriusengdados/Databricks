# Databricks notebook source
# MAGIC %md
# MAGIC ### Listas

# COMMAND ----------

# Criando uma lista
listadomercado = ["ovos, farinha, leite, maças"]

# COMMAND ----------

# Imprimindo a lista
print(listadomercado)

# COMMAND ----------

# Criando outra lista
listadomercado2 = ["ovos", "farinha", "leite", "maças"]

# COMMAND ----------

# Imprimindo a lista
print(listadomercado2)

# COMMAND ----------

# Criando lista
lista3 = [12, 100, "Universidade"]

# COMMAND ----------

# Imprimindo
print(lista3)

# COMMAND ----------

lista3 = [12, 100, "Universidade"]

# COMMAND ----------

# Atribuindo cada valor da lista a uma variável.
item1 = lista3[0]
item2 = lista3[1]
item3 = lista3[2]

# COMMAND ----------

# Imprimindo as variáveis
print(item1, item2, item3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Atualizando um item da lista

# COMMAND ----------

# Imprimindo um item da lista
listadomercado2[2]

# COMMAND ----------

# Atualizando um item da lista
listadomercado2[2] = "chocolate"

# COMMAND ----------

# Imprimindo lista alterada
listadomercado2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deletando um item da lista

# COMMAND ----------

# Deletando um item específico da lista
del listadomercado2[3]

# COMMAND ----------

# Imprimindo o item com a lista alterada
listadomercado2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Listas de listas (Listas aninhadas)
# MAGIC Listas de listas são matrizes em Python

# COMMAND ----------

# Criando uma lista de listas
listas = [[1,2,3], [10,15,14], [10.1,8.7,2.3]]

# COMMAND ----------

# Imprimindo a lista
listas

# COMMAND ----------

# Atribuindo um item da lista a uma variável
a = listas[0]

# COMMAND ----------

a

# COMMAND ----------

b = a[0]

# COMMAND ----------

b

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concatenando listas

# COMMAND ----------

lista_s1 = [34, 32, 56]

# COMMAND ----------

lista_s1

# COMMAND ----------

lista_s2 = [21, 90, 51]

# COMMAND ----------

lista_s2

# COMMAND ----------

# Concatenando listas
lista_total = lista_s1 + lista_s2

# COMMAND ----------

lista_total

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operador in

# COMMAND ----------

# Criando uma lista
lista_teste_op = [100, 2, -5, 3.4]

# COMMAND ----------

# Verificando se o valor 10 pertence a lista
print(10 in lista_teste_op)

# COMMAND ----------

# Verificando se o valor 100 pertence a lista
print(100 in lista_teste_op)
