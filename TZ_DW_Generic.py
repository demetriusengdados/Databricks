# Databricks notebook source
dbutils.widgets.text("tbname", "","")
tbname = dbutils.widgets.get("tbname")

dbutils.widgets.text("Schema", "","")
schema = dbutils.widgets.get("Schema")

dbutils.widgets.text("directory", "","")
directory = dbutils.widgets.get("directory")

#query = "select * from tbk.dim_contrato_teste"

#schema = 'tbk'
#tbname = "tbk.dim_produto"
#directory = "Dim_Produto_Financiado"

tbname = schema + '.' + tbname

# COMMAND ----------

# MAGIC %run /LIBS/Generic_Conn

# COMMAND ----------

# MAGIC %run /LIBS/Functions

# COMMAND ----------

# Data do dia formatada (yyyymmdd)
#data  = (datetime.now() + timedelta(hours=-3)).strftime('%Y%m%d%H%M%S')


#Diretorio de destino com data atual
transformedzone_path = "/mnt/stagingarea/Transformedzone/"+directory+"/"

print('transformedzone_path: ' + transformedzone_path)

# COMMAND ----------

df = spark.read.format("parquet").load(transformedzone_path)

# COMMAND ----------

write_db_append(tbname,df)

# COMMAND ----------

#read = read_db(query)
#display(read)
