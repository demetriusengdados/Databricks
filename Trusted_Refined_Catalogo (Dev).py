# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from dateutil.relativedelta import relativedelta
import os.path
import math
from datetime import date, datetime, timedelta
from pyspark.sql.functions import lit, input_file_name, monotonically_increasing_id, concat, col, count, trim, unix_timestamp, from_unixtime, to_date
import pandas as pd
import numpy as np

# COMMAND ----------

# DBTITLE 1,Conexão com o data lake
# MAGIC %run /Repos/adb-reposturma01imersao/Imersao-Azure-Big-Data/databricks/Connection/Connection_Data_Lake

# COMMAND ----------

# DBTITLE 1,Conexão com o banco de dados
# MAGIC %run /Repos/adb-reposturma01imersao/Imersao-Azure-Big-Data/databricks/Connection/Connection_DB

# COMMAND ----------

# DBTITLE 1,Parâmetros
#Parametros databricks

#dbutils.widgets.text("tbname", "","")
#tbname = dbutils.widgets.get("tbname")

#dbutils.widgets.text("Schema", "","")
#schema = dbutils.widgets.get("Schema")

#dbutils.widgets("directory", "","")
#directory = dbutils.widgets.get("directory")

#Query leitura

#query = "select * from SalesLT.Address"

#Diretório de origem

trusted_path_catalog = "/mnt/trusted/catalogo/vendas/Python"

#Diretório de destino com data atual

#schema = 'SalesLT'
tbname = "SalesLT.vendas"
#directory = "Dim_Produto_Financiado"

refined_path_teste = "/mnt/refined/catalogo/vendas/Python"

#tbname = schema + '.' + tbname

# COMMAND ----------

# DBTITLE 1,Leitura da tabela csv em python
#df_read_raw_address = spark.read.format('csv').load(raw_path, sep=";", inferSchema="true", header=true)
#df_read_raw_address.creatOrReplaceTempView("raw")

# COMMAND ----------

# DBTITLE 1,Leitura da tabela Catalogo em formato parquet
df_read_trusted_vendas = spark.read.format('parquet').load(trusted_path_catalog)

# COMMAND ----------

# DBTITLE 1,Consultando o dataframe
display(df_read_trusted_vendas)

# COMMAND ----------

# DBTITLE 1,Criando view temporária
df_read_trusted_vendas.createOrReplaceTempView('tmp_vendas')

# COMMAND ----------

# DBTITLE 1,Convertendo campos
df_read_trusted_vendas_conv = spark.sql("""

    SELECT 
    CAST(CUSTOMERID AS INT) AS CLIENTE, 
    CAST(FULLNAME AS VARCHAR(255)) AS FULLNAME,
    CAST(VALORTOTAL AS DECIMAL(20,4)) AS VALORTOTAL,
    CAST(PARTITION_DATE AS VARCHAR(6))
    
    FROM TMP_VENDAS

""")

# COMMAND ----------

# DBTITLE 1,Exibindo o schema
df_read_trusted_vendas_conv.printSchema()

# COMMAND ----------

df_read_trusted_vendas_conv.write.mode('overwrite').format('parquet').partitionBy('Partition_Date').save(refined_path_teste)

# COMMAND ----------

# DBTITLE 1,Escrevendo no banco de dados (append)
write_db_append(tbname, df_read_trusted_vendas_conv)

# COMMAND ----------

write_db(tbname, catalogconv)

# COMMAND ----------


