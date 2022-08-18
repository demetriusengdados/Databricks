# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook responsável por trazer os dados da Raw(Parquet) para a HistoryZone(Parquet)

# COMMAND ----------

from dateutil.relativedelta import relativedelta
import os.path
import math
from datetime import date, datetime, timedelta
from pyspark.sql.functions import lit,input_file_name, monotonically_increasing_id, concat, col, count, trim, unix_timestamp, from_unixtime, to_date

# COMMAND ----------

# MAGIC %run /LIBS/Generic_Conn

# COMMAND ----------

# Data do dia formatada (yyyymmdd)
#data = date.today().strftime('%Y%m%d')
#data = '20210310'
data  = (datetime.now() + timedelta(hours=-3)).strftime('%Y%m%d')

#data = '20210418'

#Diretorio de origem
raw_path = "/mnt/stagingarea/Raw/TB_Agentes/"+data+"/TB_Agentes.csv"

#Diretorio de destino com data atual
historyzone_path = "/mnt/stagingarea/Historyzone/TB_Agentes/"

print('raw_path: ' + raw_path)

print('historyzone_path: ' + historyzone_path)

# COMMAND ----------

raw_leitura = spark.read.format('csv').load(raw_path, sep=";", inferSchema="true", header="true")
raw_leitura = raw_leitura.withColumn('Updated_Date', lit(''))
raw_leitura = raw_leitura.withColumn('Insert_Date', lit(''))
raw_dup = raw_leitura.dropDuplicates()

print('Em processo - leitura Raw e deleção de linhas duplicadas')
raw_dup.createOrReplaceTempView ("raw")

# COMMAND ----------

raw_dup.createOrReplaceTempView ("raw")

# COMMAND ----------

# Leia a história anterior, se não existir, considere o novo conjunto de dados como um conjunto de dados antigo
try:
  historyzone = spark.read.format('parquet').load(historyzone_path)  
except:
  historyzone = spark.sql('select * from raw where 0 = 1')
  
  print('Except')

# COMMAND ----------

historyzone.createOrReplaceTempView ("history")

# COMMAND ----------

# Registros novos (preencher apenas coluna Insert_Date)
history_reg_novos = spark.sql("""select r.CdAgente,r.CdAgenteDetran,r.DsUF,r.NmAgente,r.NuCNPJ,r.DsAssinaturaEletronica,r.DtCriacao,r.DtAlteracao,r.IcAtivo,r.CdTipoAgente,r.DsRegime,r.DtOpcaoRegime,r.DsSituacaoCredenciado,r.DtCredenciamento,r.CdGrupoEconomico, r.Updated_Date, date_format(current_timestamp, 'yyyyMMddHHmmss') as Insert_Date, date_format(current_timestamp, 'yyyyMM') as Partition_Date
from raw r 
where 
	CdAgente not in (select h.CdAgente from history h where Updated_Date = '')
    """)
history_reg_novos.createOrReplaceTempView ("history_reg_novos") 

# COMMAND ----------

# Resultados atualizados (preencher coluna Insert_Date)
history_reg_atu_insert = spark.sql("""select r.CdAgente,r.CdAgenteDetran,r.DsUF,r.NmAgente,r.NuCNPJ,r.DsAssinaturaEletronica,r.DtCriacao,r.DtAlteracao,r.IcAtivo,r.CdTipoAgente,r.DsRegime,r.DtOpcaoRegime,r.DsSituacaoCredenciado,r.DtCredenciamento,r.CdGrupoEconomico, r.Updated_Date, date_format(current_timestamp, 'yyyyMMddHHmmss') as Insert_Date, date_format(current_timestamp, 'yyyyMM') as Partition_Date
from raw r
inner join history h on h.CdAgente = r.CdAgente
where
	h.Updated_Date = ''
	and (r.CdAgenteDetran <> h.CdAgenteDetran or r.DsUF <> h.DsUF or r.NmAgente <> h.NmAgente or r.NuCNPJ <> h.NuCNPJ or r.DsAssinaturaEletronica <> h.DsAssinaturaEletronica or r.DtCriacao <> h.DtCriacao or r.DtAlteracao <> h.DtAlteracao or r.IcAtivo <> h.IcAtivo or r.CdTipoAgente <> h.CdTipoAgente or r.DsRegime <> h.DsRegime or r.DtOpcaoRegime <> h.DtOpcaoRegime or r.DsSituacaoCredenciado <> h.DsSituacaoCredenciado or r.DtCredenciamento <> h.DtCredenciamento or r.CdGrupoEconomico <> h.CdGrupoEconomico)
    """)
history_reg_atu_insert.createOrReplaceTempView ("history_reg_atu_insert")

# COMMAND ----------

# Resultados a serem atualizados (preencher coluna Updated_Date)
history_reg_atu_update = spark.sql("""select h.CdAgente,h.CdAgenteDetran,h.DsUF,h.NmAgente,h.NuCNPJ,h.DsAssinaturaEletronica,h.DtCriacao,h.DtAlteracao,h.IcAtivo,h.CdTipoAgente,h.DsRegime,h.DtOpcaoRegime,h.DsSituacaoCredenciado,h.DtCredenciamento,h.CdGrupoEconomico, date_format(current_timestamp, 'yyyyMMddHHmmss') as Updated_Date, h.Insert_Date, date_format(current_timestamp, 'yyyyMM') as Partition_Date
from history h
inner join raw r on r.CdAgente = h.CdAgente
where
	h.Updated_Date = ''
	and (r.CdAgenteDetran <> h.CdAgenteDetran or r.DsUF <> h.DsUF or r.NmAgente <> h.NmAgente or r.NuCNPJ <> h.NuCNPJ or r.DsAssinaturaEletronica <> h.DsAssinaturaEletronica or r.DtCriacao <> h.DtCriacao or r.DtAlteracao <> h.DtAlteracao or r.IcAtivo <> h.IcAtivo or r.CdTipoAgente <> h.CdTipoAgente or r.DsRegime <> h.DsRegime or r.DtOpcaoRegime <> h.DtOpcaoRegime or r.DsSituacaoCredenciado <> h.DsSituacaoCredenciado or r.DtCredenciamento <> h.DtCredenciamento or r.CdGrupoEconomico <> h.CdGrupoEconomico)
    """)
history_reg_atu_update.createOrReplaceTempView ("history_reg_atu_update")



# COMMAND ----------

# Registros atualizados no passado (Juntar com os outros registros no union)
history_reg_atu_passado = spark.sql("""select CdAgente,CdAgenteDetran,DsUF,NmAgente,NuCNPJ,DsAssinaturaEletronica,DtCriacao,DtAlteracao,IcAtivo,CdTipoAgente,DsRegime,DtOpcaoRegime,DsSituacaoCredenciado,DtCredenciamento,CdGrupoEconomico, Updated_Date, Insert_Date, date_format(current_timestamp, 'yyyyMM') as Partition_Date
from history
where
  Updated_Date <> ''
  """)
history_reg_atu_passado.createOrReplaceTempView ("history_reg_atu_passado")

# COMMAND ----------

# Registros que não foram atualizados
history_reg_n_atu = spark.sql("""
select CdAgente,CdAgenteDetran,DsUF,NmAgente,NuCNPJ,DsAssinaturaEletronica,DtCriacao,DtAlteracao,IcAtivo,CdTipoAgente,DsRegime,DtOpcaoRegime,DsSituacaoCredenciado,DtCredenciamento,CdGrupoEconomico, Updated_Date, Insert_Date, date_format(current_timestamp, 'yyyyMM') as Partition_Date
from history
where
  Updated_Date = ''
  and CdAgente not in (
  select h.CdAgente
from history h
inner join raw r on r.CdAgente = h.CdAgente
where
	h.Updated_Date = ''
	and (r.CdAgenteDetran <> h.CdAgenteDetran or r.DsUF <> h.DsUF or r.NmAgente <> h.NmAgente or r.NuCNPJ <> h.NuCNPJ or r.DsAssinaturaEletronica <> h.DsAssinaturaEletronica or r.DtCriacao <> h.DtCriacao or r.DtAlteracao <> h.DtAlteracao or r.IcAtivo <> h.IcAtivo or r.CdTipoAgente <> h.CdTipoAgente or r.DsRegime <> h.DsRegime or r.DtOpcaoRegime <> h.DtOpcaoRegime or r.DsSituacaoCredenciado <> h.DsSituacaoCredenciado or r.DtCredenciamento <> h.DtCredenciamento or r.CdGrupoEconomico <> h.CdGrupoEconomico)
  )
""")
history_reg_n_atu.createOrReplaceTempView("history_reg_n_atu")

# COMMAND ----------

#União
history_union = spark.sql("""
select * from history_reg_novos
union
select * from history_reg_atu_insert
union
select * from history_reg_atu_update
union
select * from history_reg_atu_passado
union
select * from history_reg_n_atu
""")

# COMMAND ----------

history_union.createOrReplaceTempView ("history_union")

# COMMAND ----------

#%sql
#select * from history_union

# COMMAND ----------

history_union.write.mode('overwrite').format('parquet').partitionBy('Partition_Date').save(historyzone_path)

# COMMAND ----------

#dbutils.widgets.removeAll()
#raw_dup.printSchema()
