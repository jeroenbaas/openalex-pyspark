# Databricks notebook source
# MAGIC %md # Config
# MAGIC rename this file from config-example to config and set your variables accoring to your environment

# COMMAND ----------

# Where are the open-alex files?
base_path='dbfs:/path/to/your/mounted/dataset/openalex/data_dump_v1/2021-10-11/'

# COMMAND ----------

# sizes for the partitions of the out-tables
partition_sizes={
  'author':1000,
  'affiliation':10,
  'fieldofstudy':10,
  'paper':1000,
  'journal':10,
  'default':200
}
