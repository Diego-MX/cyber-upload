-- Databricks notebook source
-- MAGIC %md ##Lista archivos

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("<RutaArchivos>")   # Ruta archivos Json

-- COMMAND ----------

-- MAGIC %md ##Loan Contract

-- COMMAND ----------

CREATE TEMPORARY VIEW <NombreVista> USING json OPTIONS (path= "<RutaDestino>");   /* Nombre vista y ruta archivos*/

DESCRIBE EXTENDED <NombreVista>;  --Nombre de la vista

-- COMMAND ----------

SELECT * FROM    /* Nombre vista*/

-- COMMAND ----------

CREATE OR REPLACE TABLE <NombreTabla> USING DELTA LOCATION '<RutaDestinoTabla>'   /* Nombre de la tabla y ruta destino tabla */
AS                  
(                   
  SELECT            
  d.*
  FROM
    <NombreVista>    /* Nombre vista */
);  

SELECT * FROM <NombreTabla>;   -- Nombre tabla

-- COMMAND ----------

-- MAGIC %md ##Loan Contract Payment Plan

-- COMMAND ----------

CREATE TEMPORARY VIEW <NombreVista> USING json OPTIONS (path= "<RutaArchivo>");   -- Nombre vista y ruta archivo

DESCRIBE EXTENDED <NombreVista>;   -- Nombre Vista

-- COMMAND ----------

SELECT * FROM <NombreVista>

-- COMMAND ----------

CREATE OR REPLACE TABLE <NombreTabla> USING DELTA LOCATION '<RutaTabla>'   -- Nombre tabla y ruta destino tabla
WITH ExplodeSource  
AS                  
(                   
  SELECT            
    EXPLODE (d.results)
  FROM
    <NombreVista>  -- Nombre Vista
)
SELECT             
  col.*
FROM               
  ExplodeSource;  
  
SELECT * FROM <NombreTabla>;   -- Nombre de la tabla

-- COMMAND ----------

-- MAGIC %md ##Loan Contract Balances

-- COMMAND ----------

CREATE TEMPORARY VIEW <NombreVista> USING json OPTIONS (path= "<RutaArchivo>");   -- Nombre vista y ruta archivo

DESCRIBE EXTENDED <NombreVista>;   -- Nombre Vista

-- COMMAND ----------

SELECT * FROM <NombreVista>

-- COMMAND ----------

CREATE OR REPLACE TABLE <NombreTabla> USING DELTA LOCATION '<RutaTabla>'  -- Nombre tabla y ruta destino tabla
WITH ExplodeSource  
AS                  
(                   
  SELECT            
    EXPLODE (d.results)
  FROM
    <NombreVista>   -- Nombre vista
)
SELECT             
  col.*
FROM               
  ExplodeSource;  
  
SELECT * FROM <NombreTabla>;   -- Nombre Tabla

-- COMMAND ----------

-- MAGIC %md ##Loan Contract Open Items

-- COMMAND ----------

CREATE TEMPORARY VIEW <NombreVista> USING json OPTIONS (path= "<RutaArchivo>");   -- Nombre Vista y ruta archivos

DESCRIBE EXTENDED <NombreVista>;

-- COMMAND ----------

SELECT * FROM <NombreVista>   -- Nombre Vista

-- COMMAND ----------

CREATE OR REPLACE TABLE <NombreTabla> USING DELTA LOCATION '<RutaTabla>'   -- Nombre tabla y ruta destino tabla
WITH ExplodeSource  
AS                  
(                   
  SELECT            
    EXPLODE (d.results)
  FROM
    <NombreVista>   -- Nombre vista
)
SELECT             
  col.*
FROM               
  ExplodeSource;  
  
SELECT * FROM <NombreTabla>;   -- Nombre Tabña

-- COMMAND ----------

-- MAGIC %md ##Loan Contract Smart Analyzer

-- COMMAND ----------

CREATE TEMPORARY VIEW <NombreVista> USING json OPTIONS (path= "<RutaArchivo>");   -- Nombre vista y ruta archivo

DESCRIBE EXTENDED <NombreVista>;

-- COMMAND ----------

SELECT * FROM <NombreVista>

-- COMMAND ----------

CREATE OR REPLACE TABLE <NombreTabla> USING DELTA LOCATION '<RutaTabla>'   -- Nombre tabla y ruta destino tabla
WITH ExplodeSource  
AS                  
(                   
  SELECT            
    EXPLODE (d.results)
  FROM
    <NombreVista>   -- Nombre Vista
)
SELECT             
  col.*
FROM               
  ExplodeSource;  
  
SELECT * FROM <NombreTabla>;   -- Nombre tabla

-- COMMAND ----------

-- MAGIC %md ##Loan Contract Transactions

-- COMMAND ----------

CREATE TEMPORARY VIEW <NombreVista> USING json OPTIONS (path= "<RutaArchivo>");   -- Nombre vista y ruta archivo

DESCRIBE EXTENDED <NombreVista>;   -- Nombre vista

-- COMMAND ----------

SELECT * FROM <NombreVista>

-- COMMAND ----------

CREATE OR REPLACE TABLE <NombreTabla> USING DELTA LOCATION '<RutaTabla>'   -- Nombre tabla y reuta destino tabla
WITH ExplodeSource  
AS                  
(                   
  SELECT            
    EXPLODE (d.results)
  FROM
    <NombreVista>   -- Nombre vista
)
SELECT             
  col.*
FROM               
  ExplodeSource;  
  
SELECT * FROM <NombreTabla>;   -- Nombre tabla
