# Databricks notebook source
# MAGIC %md
# MAGIC ### Variáveis e Operadores 

# COMMAND ----------

# Atribuindo o valor 1 à variável var_teste
var_teste = 1
my_var = 32

# COMMAND ----------

# Imprimindo o valor da variável
var_teste

# COMMAND ----------

# Imprimindo o valor da variável
print(var_teste)

# COMMAND ----------

# Não podemos utilizar uma variável que não foi definida. Veja a mensagem de erro.
my_var

# COMMAND ----------

var_teste = 2

# COMMAND ----------

var_teste

# COMMAND ----------

type(var_teste)

# COMMAND ----------

var_teste = 9.5

# COMMAND ----------

type(var_teste)

# COMMAND ----------

x = 1

# COMMAND ----------

x

# COMMAND ----------

# MAGIC %md
# MAGIC ### Declaração Múltipla

# COMMAND ----------

pessoa1, pessoa2, pessoa3 = "Maria", "José", "Tobias"

# COMMAND ----------

pessoa1

# COMMAND ----------

pessoa2

# COMMAND ----------

pessoa3

# COMMAND ----------

fruta1 = fruta2 = fruta3 = "Laranja"

# COMMAND ----------

fruta1

# COMMAND ----------

fruta2

# COMMAND ----------

# Fique atento!!! Python é case-sensitive. Criamos a variável fruta2, mas não a variável Fruta2.
# Letras maiúsculas e minúsculas tem diferença no nome da variável.
Fruta2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pode-se usar letras, números e underline (mas não se pode começar com números)

# COMMAND ----------

x1 = 60

# COMMAND ----------

x1

# COMMAND ----------

# Mensagem de erro, pois o Python não permite nomes de variáveis que iniciem com números
1x = 50

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Não podemos usar palavras reservadas como nome de variável

# COMMAND ----------

#False      
#class      
#finally    
#is         
#return
#None       
#continue   
#for        
#lambda     
#try
#True       
#def        
#from       
#nonlocal   
#while
#and        
#del        
#global     
#not        
#with
#as         
#elif       
#if         
#or         
#yield
#assert     
#else       
#import     
#pass
#break      
#except     
#in         
#raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variáveis atribuídas a outras variáveis e ordem dos operadores

# COMMAND ----------

largura = 2

# COMMAND ----------

altura = 4

# COMMAND ----------

area = largura * altura

# COMMAND ----------

print(area)

# COMMAND ----------

perimetro = 2 * largura + 2 * altura + area

# COMMAND ----------

perimetro

# COMMAND ----------

# A ordem dos operadores é a mesma seguida na Matemática
perimetro = 2 * (largura + 2)  * altura

# COMMAND ----------

perimetro

# COMMAND ----------

# MAGIC %md
# MAGIC ### Operações com variáveis

# COMMAND ----------

idade1 = 25

# COMMAND ----------

idade2 = 35

# COMMAND ----------

idade1 + idade2

# COMMAND ----------

idade2 - idade1

# COMMAND ----------

idade2 * idade1

# COMMAND ----------

idade2 / idade1

# COMMAND ----------

idade2 % idade1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Concatenação de Variáveis

# COMMAND ----------

nome = "Steve"

# COMMAND ----------

sobrenome = "Jobs"

# COMMAND ----------

fullName = nome + " " + sobrenome

# COMMAND ----------

fullName
