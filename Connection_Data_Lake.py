# Databricks notebook source
# DBTITLE 1,Criando conexão com o Data Lake
spark.conf.set(
    "fs.azure.account.key.dlsturma01imersaoafonso.dfs.core.windows.net", 
    dbutils.secrets.get(scope="keyvault2", key="kvkeydatalake"))

# COMMAND ----------

# DBTITLE 1,Criando variável de configuração para autenticação no Data Lake
configs = {"fs.azure.account.auth.type": "OAuth", 
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider", 
           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="keyvault2", key="applicationClientId"),
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="keyvault2", key="secret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+dbutils.secrets.get(scope="keyvault2", key="directoryTenantId")+"/oauth2/token"}

# COMMAND ----------

# DBTITLE 1,Criando ponto de montagem das camadas
zones = ["raw", "trusted", "refined"]
for zone in zones:

        dbutils.fs.mount(
            source = "abfss://"+zone+"@dlsturma01imersaoafonso.dfs.core.windows.net",
            mount_point = "/mnt/"+zone+"/",
            extra_configs = configs
        )

        

# COMMAND ----------

#%fs ls /mnt/raw/

# COMMAND ----------

#dbutils.fs.unmount("/mnt/")

# COMMAND ----------


