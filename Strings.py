# Databricks notebook source
# MAGIC %md
# MAGIC ## Strings

# COMMAND ----------

# Uma única palavra
'Oi'

# COMMAND ----------

# Uma frase
'Criando uma string em Python'

# COMMAND ----------

# Podemos usar aspas duplas
"Podemos usar aspas duplas ou simples para strings em Python"

# COMMAND ----------

# Você pode combinar aspas duplas e simples
"Testando strings em 'Python'"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imprimindo uma String

# COMMAND ----------

print ('Testando Strings em Python')

# COMMAND ----------

print ('Testando \nStrings \nem \nPython')

# COMMAND ----------

print ('\n')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexando Strings

# COMMAND ----------

# Atribuindo uma string
s = 'Data Science'

# COMMAND ----------

print(s)

# COMMAND ----------

# Primeiro elemento da string. 
s[0]

# COMMAND ----------

s[1]

# COMMAND ----------

s[2]

# COMMAND ----------

# Retorna todos os elementos da string, começando pela posição (lembre-se que Python começa a indexação pela posição 0),
# até o fim da string.
s[1:]

# COMMAND ----------

# A string original permanece inalterada
s

# COMMAND ----------

# Retorna tudo até a posição 3
s[:3]

# COMMAND ----------

s[:]

# COMMAND ----------

# Nós também podemos usar a indexação negativa e ler de trás para frente.
s[-1]

# COMMAND ----------

# Retornar tudo, exceto a última letra
s[:-1]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Propriedades de Strings

# COMMAND ----------

s

# COMMAND ----------

# Concatenando strings
s + ' é a melhor maneira de estar preparado para o mercado de trabalho em Ciência de Dados!'

# COMMAND ----------

s = s + ' é a melhor maneira de estar preparado para o mercado de trabalho em Ciência de Dados!'

# COMMAND ----------

print(s)

# COMMAND ----------

# Podemos usar o símbolo de multiplicação para criar repetição!
letra = 'w'

# COMMAND ----------

letra * 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funções Built-in de Strings

# COMMAND ----------

s

# COMMAND ----------

# Letras maiusculas 
s.upper()

# COMMAND ----------

# Letras minusculas
s.lower()

# COMMAND ----------

# Dividir uma string por espaços em branco (padrão)
s.split()

# COMMAND ----------

# Dividir uma string por um elemento específico
s.split('y')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Funções String

# COMMAND ----------

s = 'seja bem vindo ao universo de python'

# COMMAND ----------

s.capitalize()

# COMMAND ----------

s.find('p')

# COMMAND ----------

s.isspace()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparando Strings

# COMMAND ----------

print("Python" == "R")

# COMMAND ----------

print("Python" == "Python")
