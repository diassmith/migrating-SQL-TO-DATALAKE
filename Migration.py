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
# Variáveis de location e de database
delta_path_svr = "/mnt/silver/db_migration/"
delta_path_brz = "/mnt/bronze/db_migration/"
db_svr = 'silver'
db_brz = 'bronze'
# Variáveis para try/except
NOTEBOOK_RESULT = "{'result':{'status': {'statusCode': 'status_code', 'message': 'status_message' }}}"
message = NOTEBOOK_RESULT.replace("status_code","1").replace("status_message","SUCCESFUL")

# COMMAND ----------

# DBTITLE 1,Variables JDBC connections
# Variáveis p/ conexão JDBC no Azure SQL. Em algumas delas foram usados secrets de Key Vault p/ boas práticas de segurança
jdbcHostname = "svrmigration.database.windows.net"
jdbcPort     =  1433
jdbcDatabase = "sql-sample-sql-sample-AdventureWorksLT"
jdbcUsername = dbutils.secrets.get(scope="adbkey", key="userdb") 
jdbcPassword = dbutils.secrets.get(scope="adbkey", key="passworddb")
jdbcUrl      = "jdbc:sqlserver://{0}:{1};database={2};user={3};password={4}".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

# COMMAND ----------

# DBTITLE 1,JDBC connection
try:
        
  # Conexão JDBC
    jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"
    connectionProperties = {
                            "user"     : jdbcUsername,
                            "password" : jdbcPassword,
                            "driver"   : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                           }
except Exception as ex:
    print(f"ERRO => {ex}")    
    message = NOTEBOOK_RESULT.replace("status_code","0").replace("status_message",str(ex).replace("'","`"))

# COMMAND ----------

try:
    
  # checking the all tables and views from database
  tables_and_views = """ 
                       (select 
                            s.name + '.' + t.name Tables
                        from sys.tables t 
                         left join sys.schemas s 
                          on t.schema_id = s.schema_id 
                  ------------------------------------------------      
                                      union all 
                  ------------------------------------------------
                        select 
                            s.name + '.' + v.name 
                        from sys.views v 
                         left join sys.schemas s 
                          on v.schema_id = s.schema_id) tbls """

  # storing the query in Dataframe
  df_stg = spark.read.jdbc(url=jdbcUrl, table=tables_and_views, properties=connectionProperties)
  
except Exception as ex:
    print(f"ERRO => {ex}")    
    message = NOTEBOOK_RESULT.replace("status_code","0").replace("status_message",str(ex).replace("'","`"))


# print all tables and views that 
#print(df_stg.collect())

# COMMAND ----------

print(df_stg.collect())

# COMMAND ----------

dbutils.secrets.listScopes()
