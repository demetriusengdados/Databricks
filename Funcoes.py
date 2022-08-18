# Databricks notebook source
# MAGIC %md
# MAGIC ###Funções

# COMMAND ----------

# Definindo uma função
def primeiraFunc():
    print('Bem vindo')

# COMMAND ----------

primeiraFunc()

# COMMAND ----------

# Definindo uma função com parâmetro
def primeiraFunc(nome):
    print('Hello %s' %(nome))

# COMMAND ----------

primeiraFunc('Aluno')

# COMMAND ----------

def funcLeitura():
    for i in range(0, 5):
        print("Número " + str(i))

# COMMAND ----------

funcLeitura()

# COMMAND ----------

# Função para somar números
def addNum(firstnum, secondnum):
    print("Primeiro número: " + str(firstnum))
    print("Segundo número: " + str(secondnum))
    print("Soma: ", firstnum + secondnum)

# COMMAND ----------

# Chamando a função e passando parâmetros
addNum(45, 3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variáveis locais e globais

# COMMAND ----------

# Variável Global
var_global = 10  # Esta é uma variável global

def multiply(num1, num2):
    var_global = num1 * num2  # Esta é uma variável local
    print(var_global)

# COMMAND ----------

multiply(5, 25)

# COMMAND ----------

print(var_global)

# COMMAND ----------

list1 = [23, 23, 34, 45]

# COMMAND ----------

sum(list1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fazendo split dos dados

# COMMAND ----------

# Fazendo split dos dados
def split_string(text):
    return text.split(" ")

# COMMAND ----------

texto = 'teste de Fabio'

# COMMAND ----------

# Isso divide a string em uma lista.
print(split_string(texto))

# COMMAND ----------

# Podemos atribuir o output de uma função, para uma variável
token = split_string(texto)

# COMMAND ----------

token
