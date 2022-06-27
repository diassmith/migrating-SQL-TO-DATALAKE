# Databricks notebook source
# DBTITLE 1,Libraries
from delta.tables import *
from pyspark.sql.types import *
from pyspark.sql.functions import col
import pytz
from pyspark.sql.functions import lit
from pyspark.sql import Window
from datetime import date
from datetime import datetime
#from datetime import timedelta

# COMMAND ----------

# DBTITLE 1,Location variables and from database
spark.conf.set(
    "fs.azure.account.key.adlsmigratingfromsql.dfs.core.windows.net",
    dbutils.secrets.get(scope="adlskey",key="adlsKey"))


configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="adlskey",key="clientId"),
       "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="adlskey",key="secretkey"),
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+dbutils.secrets.get(scope="adlskey", key="tenantId")+"/oauth2/token"}


delta_path_svr = "/mnt/silver/"
delta_path_brz = "/mnt/bronze/"
db_svr = 'silver'
db_brz = 'bronze'
# variables to try and except
NOTEBOOK_RESULT = "{'result':{'status': {'statusCode': 'status_code', 'message': 'status_message' }}}"
message = NOTEBOOK_RESULT.replace("status_code","1").replace("status_message","SUCCESFUL")


# COMMAND ----------

# DBTITLE 1,Variables JDBC connections on Azure SQL using Azure Key Vault
jdbcServerName = dbutils.secrets.get(scope="adbkey", key="serverName")
jdbcPort     =  dbutils.secrets.get(scope="adbkey", key="jdbcport")
jdbcDatabase = dbutils.secrets.get(scope="adbkey", key="namedatabase")
jdbcUsername = dbutils.secrets.get(scope="adbkey", key="userdb") 
jdbcPassword = dbutils.secrets.get(scope="adbkey", key="passworddb")
jdbcUrl      = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcServerName, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

# COMMAND ----------

# DBTITLE 1,JDBC connection
try:
        
  # JDBC Connection
    jdbcUrl = f"jdbc:sqlserver://{jdbcServerName}:{jdbcPort};database={jdbcDatabase}"
    connectionProperties = {
                            "user"     : jdbcUsername,
                            "password" : jdbcPassword,
                            "driver"   : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                           }
except Exception as ex:
    print(f"ERRO => {ex}")    
    message = NOTEBOOK_RESULT.replace("status_code","0").replace("status_message",str(ex).replace("'","`"))

# COMMAND ----------

""""try:
    
  # checking the all tables and views from database
  tables_and_views = """ 
                       (select 
                            s.name + '.' + t.name Tables
                        from sys.tables t 
                         left join sys.schemas s 
                          on t.schema_id = s.schema_id
                          where s.name + '.' + t.name <> 'dbo.ErrorLog' AND s.name + '.' + t.name <> 'dbo.BuildVersion' 
                  
                                      union all 
                 
                        select 
                            s.name + '.' + v.name 
                        from sys.views v 
                         left join sys.schemas s 
                          on v.schema_id = s.schema_id 
                           where s.name + '.' + v.name <>'sys.database_firewall_rules' 
                           AND s.name + '.' + v.name <> 'sys.ipv6_database_firewall_rules') tbls """

  # storing the query in Dataframe
  df_stg = spark.read.jdbc(url=jdbcUrl, table=tables_and_views, properties=connectionProperties)
  
except Exception as ex:
    print(f"ERRO => {ex}")    
    message = NOTEBOOK_RESULT.replace("status_code","0").replace("status_message",str(ex).replace("'","`"))


# print all tables and views that 
#print(df_stg.collect())"""

# COMMAND ----------

#variable to recive Dataframe that contains the collection of the tables and Views
#list_tables_and_views = df_stg.collect()

#list_tables_and_views[0]

# COMMAND ----------

# DBTITLE 1,testing the modificacion with just 1 table

#variable to recive Dataframe that contains the collection of the tables and Views
# list_tables_and_views = df_stg.collect()*/

    
# doing a command FOR to return data just from a table(SalesLT.Customer) 
for i in list_tables_and_views[0]:       
        # entity name
        entity = i.replace('.','_')
        #print(entity)
        
        query = "(select * from {0}) query".format(i)  
        df_tv = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
        
        
        df_tv = df_tv.withColumn("MigrationCreationDate",lit(str(datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S.%f'))).cast("timestamp"))
        
        dt = df_tv.collect()
        print(dt)
        


# COMMAND ----------

# DBTITLE 1,Creating Tables
# DBTITLE 1,Criação de Tabelas Delta e DDL de Tabelas de Metadados
try:
    # Variable to store all datas from all tables and views from database
    #lst_tbls = df_stg.collect()
    list_tables_and_views = df_stg.collect()
    
        #------------------------------------------------------------------------------------------------------------------#
        
    #Loop to get all tables and views 
    for i in list_tables_and_views:
       
        # entity name
        entity = i["Tables"].replace('.','_')
        
        #------------------------------------------------------------------------------------------------------------------#
        
        # creating variables and dataframe to bring all datas from all tables and views
        query = "(select * from {0}) query".format(i["Tables"])        
        df_tv = spark.read.jdbc(url=jdbcUrl, table=query, properties=connectionProperties)
        
        
        #------------------------------------------------------------------------------------------------------------------#
                
        # New column to control of data migration from tables and views
        df_tv = df_tv.withColumn("MigrationCreationDate",lit(str(datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S.%f'))).cast("timestamp"))
                
        # Here I have persisted on delta tables doing overwrite on datas
        df_tv.write.format("delta").mode("overwrite").option("mergeSchema", "false").save(delta_path_svr + entity)
        
        # Droping tables if it ready theare are in the database
        drop_table = (f"drop table if exists {db_svr}.{entity}")
        print('=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n',drop_table)
        spark.sql(drop_table)
        
        # Criação das tabelas de metadados no db silver do Databricks
        #Creating the table of metadatas in the databricks silver database
        create_table= (f" create table {db_svr}.{entity} using delta location '{delta_path_svr}{entity}'")
        print(create_table,'\n Tabela Criada com Sucesso!')
        spark.sql(create_table)
        
        #------------------------------------------------------------------------------------------------------------------#
        
        #I've done the transfomation of all column in all tables to STRING
        #As the ingestion is in the bronze layer, we bring the raw datas as text 
        for i in df_tv.columns:
            df_tv = df_tv.withColumn(i,col(i).cast(StringType()))
        
        #------------------------------------------------------------------------------------------------------------------#
        
        # Novo campo para controle da data de migração das tabelas     
        df_tv = df_tv.withColumn("MigrationCreationDate",lit(str(datetime.now(pytz.timezone('America/Sao_Paulo')).strftime('%Y-%m-%d %H:%M:%S.%f'))).cast("string"))
                        
        # Aqui é feita a persistência da tabela delta dando overwrite nos dados
        df_tv.write.format("delta").mode("overwrite").option("mergeSchema", "false").save(delta_path_brz + entity)
                         
        print(' ')
        # Drop de tabelas caso as mesmas ja existam no db silver
        drop_table = (f" drop table if exists {db_brz}.{entity}")
        print(drop_table)
        spark.sql(drop_table)
        
        # Criação das tabelas de metadados no db silver do Databricks
        create_table = (f" create table {db_brz}.{entity} using delta location '{delta_path_brz}{entity}'")
        print(create_table,'\n Tabela Criada com Sucesso!')
        spark.sql(create_table)
        
       #------------------------------------------------------------------------------------------------------------------#
    print('=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=*MIGRAÇÃO CONCLUÍDA COM SUCESSO!*=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=')
    
except Exception as ex:
    print(f"ERRO => {ex}")    
    message = NOTEBOOK_RESULT.replace("status_code","0").replace("status_message",str(ex).replace("'","`"))

# COMMAND ----------

try:
  
  df_stg.unpersist()
  del df_stg
    
  df_fnl.unpersist()
  del df_fnl
  
except Exception as ex:
    print(f"Waring => {ex}")  
    pass

# COMMAND ----------

##### I'M NOT ABLE TO SAVE DATAS IN DATA LAKE #######

# COMMAND ----------

#dbutils.notebook.exit(message)

# COMMAND ----------

#display(df_stg)

# COMMAND ----------

#dbutils.secrets.listScopes()
