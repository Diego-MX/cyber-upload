# https://docs.microsoft.com/en-us/azure/databricks/data/metastores/external-hive-metastore

spark.sql.hive.metastore.version X.Y.Z
spark.sql.hive.metastore.jars builtin

# maybe save in .ini
javax.jdo.option.ConnectionURL {{secrets/scope/the-connection}}
javax.jdo.option.ConnectionUserName {{secrets/scope/the-username}}
javax.jdo.option.ConnectionPassword {{secrets/scope/the-password}}
javax.jdo.option.ConnectionDriverName com.microsoft.sqlserver.jdbc.SQLServerDriver

# Production
# hive.metastore.schema.verification true 

datanucleus.autoCreateSchema true
datanucleus.schema.autoCreateTables true