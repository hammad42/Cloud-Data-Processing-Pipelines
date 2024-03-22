# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE bronze.config_tables.configurations(
# MAGIC   table_name VARCHAR(255),
# MAGIC   checkmark INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze.config_tables.configurations(
# MAGIC   table_name ,
# MAGIC   checkmark
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC   'employees',0
# MAGIC ),
# MAGIC (
# MAGIC   'dept_emp',0
# MAGIC ),
# MAGIC (
# MAGIC   'dept_manager',0
# MAGIC ),
# MAGIC (
# MAGIC   'salaries',0
# MAGIC ),
# MAGIC (
# MAGIC   'titles',0
# MAGIC )
# MAGIC
