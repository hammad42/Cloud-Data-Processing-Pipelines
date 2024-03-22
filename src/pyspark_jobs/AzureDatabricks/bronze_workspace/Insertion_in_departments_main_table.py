# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT into bronze.main_tables.departments
# MAGIC (select *
# MAGIC FROM my_transactional_data.dbo.departments  )
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC FROM bronze.main_tables.departments version as of 0
