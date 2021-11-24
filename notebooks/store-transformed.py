# Databricks notebook source
# MAGIC %md # Run transform
# MAGIC this will load the schema, the raw txt tables, then transform the dataframes to structured dataframes.

# COMMAND ----------

# MAGIC %run ./transform

# COMMAND ----------

# MAGIC %md # Store
# MAGIC write the transformed dataframes to our base-path

# COMMAND ----------

for table in df_openalex_c:
  target=f'{base_path}parquet/{table}'
  if table in partition_sizes:
    partitions=partition_sizes[table]
  else:
    partitions=partition_sizes['default']
  if file_exists(target):
    print(f'{target} already exists, skip')
  else:
    print(f'writing {target}')
    df_openalex_c[table].repartition(partitions).write.format('parquet').save(target)

