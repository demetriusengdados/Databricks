# Databricks notebook source
# MAGIC %md
# MAGIC #Notebook responsável por trazer os dados da Historyzone(Parquet) para a Transformedzone(Parquet)

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
data  = (datetime.now() + timedelta(hours=-3)).strftime('%Y%m%d%H%M%S')

#Diretorio de origem
historyzone_path = "/mnt/stagingarea/Historyzone/TB_Agentes/"

#Diretorio de destino com data atual
transformedzone_path = "/mnt/stagingarea/Transformedzone/Dim_Agente/"

print('historyzone_path: ' + historyzone_path)

print('transformedzone_path: ' + transformedzone_path)

# COMMAND ----------

history_leitura = spark.read.format("parquet").load(historyzone_path)

history_atu = history_leitura.where("updated_date = ''")

print('Em processo - leitura Historyzone')

history_atu.createOrReplaceTempView ("dim_agente_history_atu")

# COMMAND ----------

# Leia a história anterior, se não existir, considere o novo conjunto de dados como um conjunto de dados antigo
try:
  transformedzone = spark.read.format('parquet').load(transformedzone_path)  
except:
  transformedzone = spark.sql('select * from dim_agente_history_atu where 0 = 1')
  print('Except')

# COMMAND ----------

# Início da transformação

transf_1 = spark.sql("""select 
                                 
                                 cast(CdAgente as int)                             as cd_agente,                        
                                 cast(CdAgenteDetran as bigint)                    as cd_agente_detran,              
                                 cast(DsUF as char(2))                             as ds_uf,                  
                                 cast(NuCNPJ as varchar(14))                       as nu_cnpj,                     
                                 cast(NmAgente as varchar(120))                    as nm_agente,              
                                 cast(DsAssinaturaEletronica as varchar(350))      as ds_assinatura_eletronica,
                                 cast(DtCriacao as timestamp)                      as dt_criacao,              
                                 cast(DtAlteracao as timestamp)                    as dt_alteracao,              
                                 cast(IcAtivo as int)                              as ic_ativo,                       
                                 cast(CdTipoAgente as int)                         as cd_tipo_agente,                
                                 cast(DsRegime as varchar(350))                    as ds_regime,              
                                 cast(DtOpcaoRegime as timestamp)                  as dt_opcao_regime,         
                                 cast(DsSituacaoCredenciado as varchar(350))       as ds_situacao_credenciado,  
                                 cast(DtCredenciamento as timestamp)               as dt_credenciamento,          
                                 cast(CdGrupoEconomico as smallint)                as cd_grupo_economico,               
                                 cast(Insert_Date as bigint)                       as insert_date,               
                                 cast(Updated_Date as bigint)                      as updated_date,
                                 cast(Partition_Date as bigint)                    as partition_date
                                 FROM dim_agente_history_atu 
                                 """)
transf_1.createOrReplaceTempView ("transf_1") 

# COMMAND ----------

transf_2 = spark.sql("""select distinct
                                
                                 cast(0  as int)                 as cd_agente, 
                                 cast(0 as bigint)               as cd_agentedetran,
                                 null                            as ds_uf,
                                 cast(0 as bigint)               as nu_cnpj, 
                                 cast('Não informado'            as varchar(255)) as  nm_agente,
                                 null                            as ds_assinatura_eletronica,
                                 null                            as dt_criacao,              
                                 null                            as dt_alteracao,             
                                 null                            as ic_ativo,                 
                                 null                            as cd_tipo_agente,           
                                 null                            as ds_regime,              
                                 null                            as dt_opcao_regime,         
                                 null                            as ds_situacao_credenciado,  
                                 null                            as dt_credenciamento,        
                                 null                            as cd_grupo_economico,       
                                 cast(insert_date as bigint)     as insert_date,
                                 cast(updated_date as bigint)    as updated_date,
                                 cast(Partition_Date as bigint)  as partition_date
                                from dim_agente_history_atu
                             """)
transf_2.createOrReplaceTempView ("transf_2")

# COMMAND ----------

transformedzone = spark.sql("""
select * from transf_1
union
select * from transf_2
""")
transformedzone.createOrReplaceTempView ("transformedzone")

# COMMAND ----------

rv_linha_dup = transformedzone.dropDuplicates()

# COMMAND ----------

#Adicionando dados do Grupo
path_grupo = "/mnt/stagingarea/Temp/TB_Grupo/tb_grupo.csv"
df_grupo = spark.read.format('csv').load(path_grupo, sep=";", inferSchema="true", header="true")
df_grupo.createOrReplaceTempView("tb_grupo")

# COMMAND ----------

df_agente_grupo = spark.sql("""
select distinct  
ROW_NUMBER() OVER(ORDER BY cd_agente) as sk_agente,
ag.*, gr.ds_grupo from transformedzone ag
inner join tb_grupo gr on gr.nu_cnpj = ag.nu_cnpj 
""")

# COMMAND ----------

df_agente_grupo.write.mode('overwrite').format('parquet').partitionBy('Partition_Date').save(transformedzone_path)
