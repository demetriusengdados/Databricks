# Databricks notebook source
from dateutil.relativedelta import relativedelta
import os.path
import math
from datetime import date, datetime, timedelta
from delta.tables import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %run /LIBS/Generic_Conn

# COMMAND ----------

# Data do dia formatada (yyyymmdd)
#data = date.today().strftime('%Y%m%d')
data  = (datetime.now() + timedelta(hours=-3)).strftime('%Y%m%d')

#Diretorio de origem
#raw_path = "/mnt/stagingarea/Raw/TbFatoContrato.csv"

#Diretorio de destino com data atual
transformedzone_path = "/mnt/stagingarea/Transformedzone/Fato_Contrato/"

# COMMAND ----------

# MAGIC %sql
# MAGIC USE FatoContrato

# COMMAND ----------


###Definindo os paths dos arquivos a serem carregados
TB_HistoricoCore = "/mnt/stagingarea/Historyzone/TB_HistoricoCore/"
TB_HistoricoOperacao = "/mnt/stagingarea/Historyzone/TB_HistoricoOperacao/"
TB_HistoricoContrato = "/mnt/stagingarea/Historyzone/TB_HistoricoContrato/"
TB_Login = "/mnt/stagingarea/Historyzone/TB_Login/"
TB_Contratos = "/mnt/stagingarea/Historyzone/TB_Contratos/"
TB_ContratoGravame = "/mnt/stagingarea/Historyzone/TB_ContratoGravame/"
TB_Gravames = "/mnt/stagingarea/Historyzone/TB_Gravames/"
TB_Veiculos = "/mnt/stagingarea/Historyzone/TB_Veiculos/"
TB_VeiculoComplemento = "/mnt/stagingarea/Historyzone/TB_VeiculoComplemento/"
TB_HistoricoCoreComplemento = "/mnt/stagingarea/Historyzone/TB_HistoricoCoreComplemento/"
TB_ContratoDUDA = "/mnt/stagingarea/Historyzone/TB_ContratoDUDA/"
TB_HistoricoContratoArquivoImagem = "/mnt/stagingarea/Historyzone/TB_HistoricoContratoArquivoImagem/"
TB_ItemArquivoContrato = "/mnt/stagingarea/Historyzone/TB_ItemArquivoContrato/"
TB_HistoricoLote = "/mnt/stagingarea/Historyzone/TB_HistoricoLote/"
TB_Contrato_ArquivoContrato = "/mnt/stagingarea/Historyzone/TB_Contrato_ArquivoContrato/"

TB_Entidades = "/mnt/stagingarea/Historyzone/TB_Entidades/"
TB_Agentes = "/mnt/stagingarea/Historyzone/TB_Agentes/"
TB_Operacao = "/mnt/stagingarea/Historyzone/TB_Operacao/"

# COMMAND ----------

###Carregando os arquivos em dataframes
print("Iniciando a importação dos arquivos para os dataframes...")

df_HistoricoCore = spark.read.format("parquet").load(TB_HistoricoCore)
df_HistoricoOperacao = spark.read.format("parquet").load(TB_HistoricoOperacao)
df_HistoricoContrato = spark.read.format("parquet").load(TB_HistoricoContrato)
df_Login = spark.read.format("parquet").load(TB_Login)
df_Contratos = spark.read.format("parquet").load(TB_Contratos)
df_ContratoGravame = spark.read.format("parquet").load(TB_ContratoGravame)
df_Gravames = spark.read.format("parquet").load(TB_Gravames)
df_Veiculos = spark.read.format("parquet").load(TB_Veiculos)
df_VeiculoComplemento = spark.read.format("parquet").load(TB_VeiculoComplemento)
df_ContratoDUDA = spark.read.format("parquet").load(TB_ContratoDUDA)
df_HistoricoContratoArquivoImagem = spark.read.format("parquet").load(TB_HistoricoContratoArquivoImagem)
df_ItemArquivoContrato = spark.read.format("parquet").load(TB_ItemArquivoContrato)
df_HistoricoLote = spark.read.format("parquet").load(TB_HistoricoLote)
df_HistoricoCoreComplemento = spark.read.format("parquet").load(TB_HistoricoCoreComplemento)
df_Contrato_ArquivoContrato = spark.read.format("parquet").load(TB_Contrato_ArquivoContrato)

print("carregando os dataframes em visões...")

df_HistoricoCore.createOrReplaceTempView ("vw_HistoricoCore")
df_HistoricoOperacao.createOrReplaceTempView ("vw_HistoricoOperacao")
df_HistoricoContrato.createOrReplaceTempView ("vw_HistoricoContrato")
df_Login.createOrReplaceTempView ("vw_Login")
df_Contratos.createOrReplaceTempView ("vw_Contratos")
df_ContratoGravame.createOrReplaceTempView ("vw_ContratoGravame")
df_Gravames.createOrReplaceTempView ("vw_Gravames")
df_Veiculos.createOrReplaceTempView ("vw_Veiculos")
df_VeiculoComplemento.createOrReplaceTempView ("vw_VeiculoComplemento")
df_ContratoDUDA.createOrReplaceTempView ("vw_ContratoDUDA")
df_HistoricoContratoArquivoImagem.createOrReplaceTempView ("vw_HistoricoContratoArquivoImagem")
df_ItemArquivoContrato.createOrReplaceTempView ("vw_ItemArquivoContrato")
df_HistoricoLote.createOrReplaceTempView ("vw_HistoricoLote")
df_HistoricoCoreComplemento.createOrReplaceTempView ("vw_HistoricoCoreComplemento")
df_Contrato_ArquivoContrato.createOrReplaceTempView ("vw_Contrato_ArquivoContrato")

print("Carregamento encerrado")

# COMMAND ----------

df_Entidades = spark.read.format("parquet").load(TB_Entidades)
df_Entidades.createOrReplaceTempView ("vw_Entidades")

df_Agentes = spark.read.format("parquet").load(TB_Agentes)
df_Agentes.createOrReplaceTempView ("vw_Agentes")

df_Operacao = spark.read.format("parquet").load(TB_Operacao)
df_Operacao.createOrReplaceTempView ("vw_Operacao")

# COMMAND ----------

#Carregando dados das dimensões

print("Identificando diretórios...")

Dim_Agente_path = "/mnt/stagingarea/Transformedzone/Dim_Agente/"
Dim_CanalServico_path = "/mnt/stagingarea/Transformedzone/Dim_CanalServico/"
Dim_Contrato_path = "/mnt/stagingarea/Transformedzone/Dim_Contrato/"
Dim_Entidade_path = "/mnt/stagingarea/Transformedzone/Dim_Entidade/"
Dim_Gravames_path = "/mnt/stagingarea/Transformedzone/Dim_Gravames/"
Dim_Operacao_path = "/mnt/stagingarea/Transformedzone/Dim_Operacao/"
Dim_Produto_Financiado_path = "/mnt/stagingarea/Transformedzone/Dim_Produto_Financiado/"
Dim_Tipo_Restricao_Financeira_path = "/mnt/stagingarea/Transformedzone/Dim_Tipo_Restricao_Financeira/"
Dim_Retorno_B3_path = "/mnt/stagingarea/Transformedzone/Dim_Retorno_B3/"
Dim_Retorno_Transacao_path = "/mnt/stagingarea/Transformedzone/Dim_Retorno_Transacao/"
Dim_Status_Diario_Fechamento_path = "/mnt/stagingarea/Transformedzone/Dim_Status_Diario_Fechamento/"
Dim_Flag_Transacao_path = "/mnt/stagingarea/Transformedzone/Dim_Flag_Transacao/"
Dim_Veiculos_path = "/mnt/stagingarea/Transformedzone/Dim_Veiculos/"

print("Carregando dimensões...")

df_Agente = spark.read.format("parquet").load(Dim_Agente_path)
df_CanalServico = spark.read.format("parquet").load(Dim_CanalServico_path)
df_Contrato = spark.read.format("parquet").load(Dim_Contrato_path)
df_Entidade = spark.read.format("parquet").load(Dim_Entidade_path)
df_Gravames = spark.read.format("parquet").load(Dim_Gravames_path)
df_Operacao = spark.read.format("parquet").load(Dim_Operacao_path)
df_Produto_Financiado = spark.read.format("parquet").load(Dim_Produto_Financiado_path)
df_Tipo_Restricao_Financeira = spark.read.format("parquet").load(Dim_Tipo_Restricao_Financeira_path)
df_Retorno_B3 = spark.read.format("parquet").load(Dim_Retorno_B3_path)
df_Retorno_Transacao = spark.read.format("parquet").load(Dim_Retorno_Transacao_path)
df_Status_Diario_Fechamento = spark.read.format("parquet").load(Dim_Status_Diario_Fechamento_path)
#df_Flag_Transacao = spark.read.format("parquet").load(Dim_Flag_Transacao_path)
df_Veiculos = spark.read.format("parquet").load(Dim_Veiculos_path)

print("Criando visões...")

df_Agente.createOrReplaceTempView('Dim_Agente')
df_CanalServico.createOrReplaceTempView('Dim_CanalServico')
df_Contrato.createOrReplaceTempView('Dim_Contrato')
df_Entidade.createOrReplaceTempView('Dim_Entidade')
df_Gravames.createOrReplaceTempView('Dim_Gravames')
df_Operacao.createOrReplaceTempView('Dim_Operacao')
df_Produto_Financiado.createOrReplaceTempView('Dim_Produto_Financiado')
df_Tipo_Restricao_Financeira.createOrReplaceTempView('Dim_Tipo_Restricao_Financeira')
df_Retorno_B3.createOrReplaceTempView('Dim_Retorno_B3')
df_Retorno_Transacao.createOrReplaceTempView('Dim_Retorno_Transacao')
df_Status_Diario_Fechamento.createOrReplaceTempView('Dim_Status_Diario_Fechamento')
#df_Flag_Transacao.createOrReplaceTempView('Dim_Flag_Transacao')
df_Veiculos.createOrReplaceTempView('Dim_Veiculos')

print("Carregamento encerrado")

# COMMAND ----------

# MAGIC %sql
# MAGIC use fatocontrato

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_HistoricoCore
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_HistoricoCore

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_Agentes
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_Agentes

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_Entidades
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_Entidades

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_Operacao
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_Operacao

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE TB_HistoricoOperacao
# MAGIC USING Delta
# MAGIC SELECT --REPLACE(CdHistoricoOperacao, '-', '') AS CdHistoricoOperacao, 
# MAGIC CdHistoricoOperacao,
# MAGIC CdProcesso,  CdEntidade,
# MAGIC CdArea, CdPessoa, DtOperacao,  CdCanalServico, 
# MAGIC CdStatusDiarioFechamento, CdOperacao, CdProduto, 
# MAGIC DsChave, IcTravarChave, CdStatusDiarioRepasse, 
# MAGIC Updated_Date, Insert_Date
# MAGIC FROM vw_HistoricoOperacao

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_HistoricoCore
# MAGIC USING Delta
# MAGIC SELECT CdHistorico,CdEntidade, CdArea,
# MAGIC CdPessoa, CdEvento, DtEvento, 
# MAGIC DtCriacao, CdCanalServico, 
# MAGIC --REPLACE(CdHistoricoOperacao, '-', '') AS CdHistoricoOperacao, 
# MAGIC CdHistoricoOperacao,
# MAGIC Updated_Date, Insert_Date
# MAGIC FROM vw_HistoricoCore

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_HistoricoContrato
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_HistoricoContrato

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_Login
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_Login

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_Contratos
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_Contratos

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_ContratoGravame
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_ContratoGravame

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_Gravames
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_Gravames

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_Veiculos
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_Veiculos

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_VeiculoComplemento
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_VeiculoComplemento

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_HistoricoCoreComplemento
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_HistoricoCoreComplemento

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_ContratoDUDA
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_ContratoDUDA

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_HistoricoContratoArquivoImagem
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_HistoricoContratoArquivoImagem

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE TB_ItemArquivoContrato
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_ItemArquivoContrato

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE  TB_HistoricoLote
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_HistoricoLote

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE TB_Contrato_ArquivoContrato
# MAGIC USING Delta
# MAGIC SELECT * FROM vw_Contrato_ArquivoContrato

# COMMAND ----------

# MAGIC %sql truncate table fatocontrato.tbfatocontrato

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE CdHistoricoContratoCdContrato
# MAGIC USING DELTA 
# MAGIC as
# MAGIC select hcon.CdHistoricoContrato, hcon.CdContrato
# MAGIC 	from TB_HistoricoContrato as hcon 
# MAGIC 	inner join TB_HistoricoCore as hcore 
# MAGIC 		on hcore.CdHistorico = hcon.CdHistorico
# MAGIC 	inner join TB_HistoricoOperacao as hop 
# MAGIC 		on hcore.CdHistoricoOperacao = hop.CdHistoricoOperacao
# MAGIC 	where hop.CdOperacao in (1, 2, 3, 4, 5, 6)
# MAGIC 	and not exists (select fc.CdHistoricoContrato
# MAGIC 						from TbFatoContrato fc 
# MAGIC 						where fc.CdHistoricoContrato = hcon.CdHistoricoContrato);
# MAGIC 
# MAGIC 		

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into fatocontrato.tbfatocontrato
# MAGIC select 
# MAGIC 			hcon.CdHistoricoContrato, -- Plataforma.Sistema.TB_HistoricoContrato
# MAGIC 			hcon.CdHistorico, -- Plataforma.Sistema.TB_HistoricoContrato
# MAGIC 			hcon.CdGravame, -- Plataforma.Sistema.TB_HistoricoContrato
# MAGIC 			hcon.CdContrato, -- Plataforma.Sistema.TB_HistoricoContrato
# MAGIC 			hcon.IcFlagTransacao, -- Plataforma.Sistema.TB_HistoricoContrato
# MAGIC 			hcon.DsUF, -- Plataforma.Sistema.TB_HistoricoContrato
# MAGIC 			hcon.CdAgente,  -- Plataforma.Sistema.TB_HistoricoContrato
# MAGIC 			hcon.CdRetorno,  -- Plataforma.Sistema.TB_HistoricoContrato
# MAGIC 			hcon.CdRetornoDetran,  -- Plataforma.Sistema.TB_HistoricoContrato
# MAGIC 
# MAGIC 			hcore.CdEntidade, -- Plataforma.Sistema.TB_HistoricoCore
# MAGIC 			hcore.CdEvento, -- Plataforma.Sistema.TB_HistoricoCore
# MAGIC 			hcore.CdCanalServico, -- Plataforma.Sistema.TB_HistoricoCore
# MAGIC 
# MAGIC 			hop.CdHistoricoOperacao, -- Plataforma.Sistema.TB_HistoricoOperacao
# MAGIC 			hop.CdProcesso, -- Plataforma.Sistema.TB_HistoricoOperacao
# MAGIC 			hop.DtOperacao, -- Plataforma.Sistema.TB_HistoricoOperacao
# MAGIC 			hop.DtOperacao, -- Plataforma.Sistema.TB_HistoricoOperacao
# MAGIC 			hop.CdStatusDiarioFechamento, -- Plataforma.Sistema.TB_HistoricoOperacao 
# MAGIC 			hop.CdOperacao, -- Plataforma.Sistema.TB_HistoricoOperacao
# MAGIC 			hop.CdProduto, -- Plataforma.Sistema.TB_HistoricoOperacao
# MAGIC 			hop.DsChave, -- Plataforma.Sistema.TB_HistoricoOperacao
# MAGIC 			hop.IcTravarChave, -- Plataforma.Sistema.TB_HistoricoOperacao
# MAGIC 
# MAGIC 			lo.NmUsuario, -- Plataforma.Seguranca.TB_Login
# MAGIC 
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null,
# MAGIC 			null
# MAGIC 
# MAGIC 		from CdHistoricoContratoCdContrato as chc 
# MAGIC 		inner join TB_HistoricoContrato as hcon
# MAGIC 			on chc.CdHistoricoContrato = hcon.CdHistoricoContrato
# MAGIC 		inner join TB_HistoricoCore as hcore 
# MAGIC 			on hcore.CdHistorico = hcon.CdHistorico
# MAGIC 		inner join TB_HistoricoOperacao as hop 
# MAGIC 			on hcore.CdHistoricoOperacao = hop.CdHistoricoOperacao
# MAGIC 		inner join TB_Login as lo
# MAGIC 			on lo.CdPessoa = hop.CdPessoa; 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ContratoGravameVeiculo 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC select 	
# MAGIC 		con.CdContrato,
# MAGIC 		con.NuDocumentoDevedor,
# MAGIC 		con.NmDevedor,
# MAGIC 		con.VlTotalFinanciamento,
# MAGIC 		con.IcAtivo as IcAtivoContrato ,
# MAGIC 		gr.CdGravame,
# MAGIC 		gr.CdAgente,
# MAGIC 		gr.CdTipoRestricao,
# MAGIC 		gr.NuRestricao,
# MAGIC 		gr.NuContrato,	
# MAGIC 		cast(gr.DtContrato as timestamp) as DtContrato,
# MAGIC 		gr.DsUF as DsUFG,
# MAGIC 		gr.IcAtivo as IcAtivoG,
# MAGIC 		gr.NuQtdParcelas,
# MAGIC 		ve.DsPlaca,
# MAGIC 		ve.NuRenavam,
# MAGIC 		ve.DsAnoFabricacao,
# MAGIC 		ve.DsAnoModelo,
# MAGIC 		vc.NmProprietario,
# MAGIC 		vc.NuDocumento,
# MAGIC 		vc.IcRemarcacao,
# MAGIC 		vc.DsUF as DsUFVC,
# MAGIC 		vc.DsChassi
# MAGIC 	from fatocontrato.CdHistoricoContratoCdContrato as chc 
# MAGIC 	left join fatocontrato.TB_Contratos as con 
# MAGIC 		on chc.CdContrato = con.CdContrato
# MAGIC 	left join fatocontrato.TB_ContratoGravame as cg 
# MAGIC 		on con.CdContrato = cg.CdContrato
# MAGIC 	left join fatocontrato.TB_Gravames as gr 
# MAGIC 		on gr.CdGravame = cg.CdGravame
# MAGIC 	left join fatocontrato.TB_Veiculos as ve 
# MAGIC 		on ve.CdVeiculo = gr.CdVeiculo 
# MAGIC 	left join fatocontrato.TB_VeiculoComplemento as vc  
# MAGIC 		on vc.CdVeiculo = gr.CdVeiculo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tbGravameHistContrato
# MAGIC USING Delta
# MAGIC select distinct  
# MAGIC                  cgv.NuDocumentoDevedor,
# MAGIC                  cgv.NmDevedor,
# MAGIC 		         cgv.VlTotalFinanciamento,
# MAGIC 		         cgv.IcAtivoContrato,
# MAGIC 		         cgv.CdGravame,
# MAGIC 		         cgv.CdAgente,
# MAGIC 		         cgv.CdTipoRestricao,
# MAGIC 		         cgv.NuRestricao,
# MAGIC 		         cgv.NuContrato,	
# MAGIC 		         cgv.DtContrato,
# MAGIC 		         cgv.DsUFG,
# MAGIC 		         cgv.IcAtivoG,
# MAGIC 		         cgv.NuQtdParcelas,
# MAGIC 		         cgv.DsPlaca,
# MAGIC 		         cgv.NuRenavam,
# MAGIC 		         cgv.DsAnoFabricacao,
# MAGIC 		         cgv.DsAnoModelo,
# MAGIC 		         cgv.NmProprietario,
# MAGIC 		         cgv.NuDocumento,
# MAGIC 		         cgv.IcRemarcacao,
# MAGIC 		         cgv.DsUFVC,
# MAGIC                  fc.CdContrato, 
# MAGIC                  fc.CdHistoricoContrato,
# MAGIC                  dd.DUDA,
# MAGIC                  dd.CdHistoricoOperacao
# MAGIC from fatocontrato.CdHistoricoContratoCdContrato as chc 
# MAGIC 	inner join fatocontrato.TbFatoContrato as fc 
# MAGIC 		on chc.CdHistoricoContrato = fc.CdHistoricoContrato
# MAGIC 	inner join fatocontrato.ContratoGravameVeiculo as cgv 
# MAGIC 		on fc.CdContrato = cgv.CdContrato
# MAGIC 		and ltrim(rtrim(fc.DsChave)) = ltrim(rtrim(cgv.DsChassi)) 
# MAGIC 	left join fatocontrato.TB_ContratoDUDA as dd
# MAGIC 		on fc.CdHistoricoOperacao = dd.CdHistoricoOperacao
# MAGIC         

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fatocontrato.TbFatoContrato 
# MAGIC USING fatocontrato.tbGravameHistContrato as CdHistoricoContratoCdContrato_1
# MAGIC ON      1=1 --TbFatoContrato.CdHistoricoOperacao = CdHistoricoContratoCdContrato_1.CdHistoricoOperacao
# MAGIC AND     TbFatoContrato.CdHistoricoContrato = CdHistoricoContratoCdContrato_1.CdHistoricoContrato
# MAGIC         WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC         TbFatoContrato.NuDocumentoDevedor = CdHistoricoContratoCdContrato_1.NuDocumentoDevedor,
# MAGIC         TbFatoContrato.NmDevedor = CdHistoricoContratoCdContrato_1.NmDevedor,
# MAGIC 		TbFatoContrato.VlTotalFinanciamento = CdHistoricoContratoCdContrato_1.VlTotalFinanciamento,
# MAGIC 		TbFatoContrato.IcAtivoContrato = CdHistoricoContratoCdContrato_1.IcAtivoContrato,
# MAGIC 		TbFatoContrato.CdGravame = CdHistoricoContratoCdContrato_1.CdGravame,
# MAGIC 		TbFatoContrato.CdAgente = CdHistoricoContratoCdContrato_1.CdAgente,
# MAGIC 		TbFatoContrato.CdTipoRestricao = CdHistoricoContratoCdContrato_1.CdTipoRestricao,
# MAGIC 		TbFatoContrato.NuRestricao = CdHistoricoContratoCdContrato_1.NuRestricao,
# MAGIC 		TbFatoContrato.NuContrato = rtrim(ltrim(CdHistoricoContratoCdContrato_1.NuContrato)),	
# MAGIC 		TbFatoContrato.DtContrato = CdHistoricoContratoCdContrato_1.DtContrato,
# MAGIC 		TbFatoContrato.DsUFG = CdHistoricoContratoCdContrato_1.DsUFG,
# MAGIC 		TbFatoContrato.IcAtivoG = CdHistoricoContratoCdContrato_1.IcAtivoG,
# MAGIC 		TbFatoContrato.NuQtdParcelas = CdHistoricoContratoCdContrato_1.NuQtdParcelas,
# MAGIC 		TbFatoContrato.DsPlaca = CdHistoricoContratoCdContrato_1.DsPlaca,
# MAGIC 		TbFatoContrato.NuRenavam = CdHistoricoContratoCdContrato_1.NuRenavam,
# MAGIC 		TbFatoContrato.DsAnoFabricacao = CdHistoricoContratoCdContrato_1.DsAnoFabricacao,
# MAGIC 		TbFatoContrato.DsAnoModelo = CdHistoricoContratoCdContrato_1.DsAnoModelo,
# MAGIC 		TbFatoContrato.NmProprietario = CdHistoricoContratoCdContrato_1.NmProprietario,
# MAGIC 		TbFatoContrato.NuDocumento = CdHistoricoContratoCdContrato_1.NuDocumento,
# MAGIC 		TbFatoContrato.IcRemarcacao = CdHistoricoContratoCdContrato_1.IcRemarcacao,
# MAGIC 		TbFatoContrato.DsUFVC = CdHistoricoContratoCdContrato_1.DsUFVC,
# MAGIC 		TbFatoContrato.DUDA = CdHistoricoContratoCdContrato_1.DUDA
# MAGIC 
# MAGIC 		

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tbRetorno
# MAGIC USING Delta
# MAGIC SELECT distinct 
# MAGIC fc.CdHistoricoContrato, 
# MAGIC hcc.DsRetorno,
# MAGIC fc.CdHistoricoOperacao
# MAGIC from fatocontrato.CdHistoricoContratoCdContrato as chc 
# MAGIC inner join fatocontrato.TbFatoContrato as fc 
# MAGIC 	on chc.CdHistoricoContrato = fc.CdHistoricoContrato
# MAGIC left join fatocontrato.TB_HistoricoCoreComplemento as hcc
# MAGIC 	on fc.CdHistorico = hcc.CdHistorico
# MAGIC where fc.CdRetorno in (6022, 6023)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fatocontrato.TbFatoContrato as fato 
# MAGIC USING fatocontrato.tbRetorno as retorno
# MAGIC ON      fato.CdHistoricoContrato = retorno.CdHistoricoContrato
# MAGIC         WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC         fato.DsRetorno = retorno.DsRetorno

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE tbRetornoDetran
# MAGIC USING Delta
# MAGIC SELECT distinct 
# MAGIC    hc.CdRetorno, hc.CdRetornoDetran, fc.CdHistoricoContrato
# MAGIC from  fatocontrato.tbFatoContrato as fc 
# MAGIC inner join fatocontrato.TB_HistoricoContrato as hc 
# MAGIC 	on hc.CdHistoricoContrato = fc.CdHistoricoContrato
# MAGIC where  (fc.CdRetornoDetran <> hc.CdRetornoDetran 
# MAGIC 	or fc.CdRetornoDetran <> hc.CdRetornoDetran)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fatocontrato.TbFatoContrato as fato 
# MAGIC USING fatocontrato.tbRetornoDetran as retorno
# MAGIC ON      fato.CdHistoricoContrato = retorno.CdHistoricoContrato
# MAGIC         WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC         fato.CdRetorno = retorno.CdRetorno,
# MAGIC         fato.CdRetornoDetran = retorno.CdRetornoDetran

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tbContratoAtivo
# MAGIC USING Delta
# MAGIC SELECT  DISTINCT
# MAGIC con.IcAtivo,  con.CdContrato
# MAGIC from TbFatoContrato as fc  
# MAGIC 	inner join  TB_Contratos as con   
# MAGIC 		on con.CdContrato = fc.CdContrato
# MAGIC 	where fc.IcAtivoContrato <> con.IcAtivo

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fatocontrato.TbFatoContrato as fato 
# MAGIC USING fatocontrato.tbContratoAtivo as contrato
# MAGIC ON      fato.CdContrato = contrato.CdContrato
# MAGIC         WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC         fato.IcAtivoContrato = contrato.IcAtivo

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fatocontrato.TbFatoContrato as fato 
# MAGIC USING fatocontrato.TB_Gravames as gravames
# MAGIC ON      fato.CdGravame  = gravames.CdGravame
# MAGIC AND     fato.IcAtivoG  <> gravames.IcAtivo
# MAGIC         WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC         fato.IcAtivoG = gravames.IcAtivo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tbStatusDiarioFechamento
# MAGIC USING Delta
# MAGIC SELECT hop.IcTravarChave, hop.CdHistoricoOperacao, hop.CdStatusDiarioFechamento
# MAGIC from fatocontrato.TbFatoContrato as fc 
# MAGIC 	inner join fatocontrato.TB_HistoricoOperacao as hop 
# MAGIC 		on hop.CdHistoricoOperacao = fc.CdHistoricoOperacao
# MAGIC 	where fc.CdStatusDiarioFechamento <> hop.CdStatusDiarioFechamento
# MAGIC 		or fc.IcTravarChave <> hop.IcTravarChave

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fatocontrato.TbFatoContrato as fato 
# MAGIC USING fatocontrato.tbStatusDiarioFechamento as historico_operacao
# MAGIC ON      fato.CdHistoricoOperacao  = historico_operacao.CdHistoricoOperacao
# MAGIC         WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC         fato.CdStatusDiarioFechamento = historico_operacao.CdStatusDiarioFechamento,
# MAGIC         fato.IcTravarChave = historico_operacao.IcTravarChave

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tbArquivoImagem_1
# MAGIC USING Delta
# MAGIC select 
# MAGIC  fc.cdhistoricooperacao, ac.CdContrato,
# MAGIC ac.DtUpload,
# MAGIC case when ac.NmArquivo is null then 0 else 1 end as NmArquivo
# MAGIC from fatocontrato.TbFatoContrato as fc 
# MAGIC     left join (	select hcai.CdContrato, max(iac.NmArquivo) as NmArquivo, max(cac.DtCriacao) as DtUpload 
# MAGIC 				from TB_HistoricoContratoArquivoImagem as hcai 
# MAGIC 				left join fatocontrato.TB_Contrato_ArquivoContrato as cac 
# MAGIC 					on hcai.CdArquivoContrato = cac.CdArquivoContrato 
# MAGIC 					and cac.IcAtivo = 1
# MAGIC 				left join fatocontrato.TB_ItemArquivoContrato as iac 
# MAGIC 					on hcai.CdArquivoContrato = iac.CdArquivoContrato
# MAGIC 				where hcai.DsNomeArquivoImagem <> ''
# MAGIC 				group by hcai.CdContrato) as ac
# MAGIC 		on fc.CdContrato = ac.CdContrato

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE tbArquivoImagem
# MAGIC USING Delta
# MAGIC select max(nmarquivo) as nmarquivo, max(DtUpload) as DtUpload, CdContrato 
# MAGIC from tbArquivoImagem_1 
# MAGIC group by CdContrato

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fatocontrato.TbFatoContrato as fato 
# MAGIC USING fatocontrato.tbArquivoImagem as imagem
# MAGIC ON      fato.CdContrato  = imagem.CdContrato      
# MAGIC         WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC         fato.Imagem  = imagem.NmArquivo,
# MAGIC         fato.DtUpload = imagem.DtUpload

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fatocontrato.TbFatoContrato as fato 
# MAGIC USING fatocontrato.TB_HistoricoLote as historico_lote
# MAGIC ON      fato.CdHistorico  = historico_lote.CdHistorico
# MAGIC         WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC         fato.CdProcessamentoLote = historico_lote.CdProcessamentoLote

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC MERGE INTO fatocontrato.TbFatoContrato as fato 
# MAGIC USING fatocontrato.TB_Contratos as contrato
# MAGIC ON      fato.CdContrato  = contrato.CdContrato
# MAGIC AND     fato.VlTotalFinanciamento <> contrato.VlTotalFinanciamento 
# MAGIC         WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC         fato.VlTotalFinanciamento = contrato.VlTotalFinanciamento

# COMMAND ----------

df_fato = spark.sql("""
select 
            dag.sk_agente,
            den.sk_entidade , 
            dgv.sk_gravame,
            dpf.sk_produto,
            dop.sk_operacao,
            dsf.sk_status_diario_fechamento,
            dcn.sk_contrato, 
            drf.sk_tipo_restricao,
			drt.sk_retorno_transacao  ,
            dcs.sk_canalservico as sk_canal_servico,
            dvc.sk_veiculo,
            cast(t1.CdHistoricoContrato as bigint) as nu_historico_contrato, 
			cast(t1.CdHistorico as int) as nu_historico,
			cast(t1.CdRetornoDetran as int) as nu_retorno_detran,  
			cast(t1.CdHistoricoOperacao as int) as nu_historico_operacao, 
			cast(t1.CdProcesso as int) as nu_processo,
            cast(t1.DtOperacao as date) AS dt_operacao,
            cast(t1.DtOperacao as date) AS dt_contrato,
			case 
				when t1.VlTotalFinanciamento <= 10000 then '00-ATE 10.000'
				when t1.VlTotalFinanciamento <= 20000 then '01-10.001 20.000'
				when t1.VlTotalFinanciamento <= 30000 then '02-20.001 30.000'
				when t1.VlTotalFinanciamento <= 40000 then '03-30.001 40.000'
				when t1.VlTotalFinanciamento <= 50000 then '04-40.001 50.000'
				when t1.VlTotalFinanciamento >  50000 then '05-ACIMA 50.000'
			end as ds_faixa_financiamento

			,case 
				when t1.NuQtdParcelas <= 12 then '12'
				when t1.NuQtdParcelas <= 24 then '24'
				when t1.NuQtdParcelas <= 36 then '36'
				when t1.NuQtdParcelas <= 48 then '48'
				when t1.NuQtdParcelas <= 60 then '60'
				when t1.NuQtdParcelas <= 72 then '72'
				when t1.NuQtdParcelas >  72 then 'ACIMA 72'
			end as ds_qtd_parcelas
			,(year(t1.DataContrato)- t1.DsAnoFabricacao) as nu_idade_veiculo
			,case
				when (year(t1.DataContrato)- t1.DsAnoFabricacao) <=  0 then 'NOVOS'			    -- Até 2020
				when (year(t1.DataContrato)- t1.DsAnoFabricacao) <=  3 then 'ATE 03 ANOS'		-- Semi-novos
				when (year(t1.DataContrato)- t1.DsAnoFabricacao) <=  8 then 'DE 04 - 08'		-- Usados Jovens
				when (year(t1.DataContrato)- t1.DsAnoFabricacao) <= 11 then 'DE 09 - 11'		-- Usados Maduros
				when (year(t1.DataContrato)- t1.DsAnoFabricacao)  > 11 then 'MAIS DE 12 ANOS'	-- Velhinhos
                when t1.DataContrato is null then 'SEM INFORMACAO'
                when t1.DsAnoFabricacao is null then 'SEM INFORMACAO'
				else 'SEM INFORMACAO'
			end as ds_idade_veiculo
            ,case
				when t1.CdTipoRestricao = 0 then 'SEM INFORMACAO'
				when t1.CdTipoRestricao = 1 then 'LEASING'
				when t1.CdTipoRestricao = 2 then 'RESERVA DE DOMINIO'
				when t1.CdTipoRestricao = 3 then 'CDC'
				when t1.CdTipoRestricao = 4 then 'PENHOR'
				when t1.CdTipoRestricao = 9 then 'PENHOR'
				else 'SEM INFORMACAO'
			end as ds_produto_financiado,
            cast(ctr.VlTotalFinanciamento as decimal(10,2)) as vl_total_financiamento
from fatocontrato.TbFatoContrato t1
left join TB_Gravames grv on grv.CdGravame = t1.CdGravame
left join TB_Entidades ent on ent.CdEntidade = t1.CdEntidade
left join TB_Agentes agt on t1.CdAgente = agt.CdAgente
left join TB_Operacao opr on t1.CdOperacao = opr.CdOperacao
left join TB_Contratos ctr on t1.CdContrato = ctr.CdContrato
left join Dim_Agente dag on t1.CdAgente = dag.cd_agente
left join Dim_Entidade den on t1.CdEntidade = den.cd_entidade
left join Dim_CanalServico dcs on t1.CdCanalServico = dcs.cd_canalservico
left join Dim_Contrato dcn on t1.CdContrato = dcn.cd_contrato 
left join Dim_Gravames dgv on t1.CdGravame = dgv.cd_gravame 
left join Dim_Operacao dop on t1.CdOperacao = dop.cd_operacao
left join Dim_Produto_Financiado dpf on t1.CdProduto = dpf.cd_produto
left join Dim_Tipo_Restricao_Financeira drf on t1.CdTipoRestricao = drf.cd_tipo_restricao
left join Dim_Retorno_Transacao drt on t1.CdRetorno = drt.cd_retorno_transacao 
left join Dim_Status_Diario_Fechamento dsf on t1.CdStatusDiarioFechamento = dsf.cd_status_diario_fechamento 
left join Dim_Veiculos dvc on dgv.cd_veiculo = dvc.cd_veiculo 
""")


# COMMAND ----------

df_fato.createOrReplaceTempView("Fato")

# COMMAND ----------

df_fato.write.mode('overwrite').format('parquet').save(transformedzone_path)
