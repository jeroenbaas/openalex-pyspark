# Databricks notebook source
from pyspark.sql import functions as func

# COMMAND ----------

# MAGIC %md ## schema

# COMMAND ----------

# MAGIC %run ./schema

# COMMAND ----------

# MAGIC %md ## config

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

# MAGIC %md ## get dataframes

# COMMAND ----------

# map all raw txt files to dataframes
df_raw_import={}
for tablename in openalex_import_tables:
  print(tablename)
  df_raw_import[tablename]=(
    spark
    .read
    .format('csv')
    .option('delimiter',"\t")
    .option('header',True)
    .schema(openalex_import_tables[tablename]['schema'])
    .load(f"{base_path}{openalex_import_tables[tablename]['subpath']}/{openalex_import_tables[tablename]['filename']}")
  )
