# Databricks notebook source
# MAGIC %sql
# MAGIC with tmp as
# MAGIC (select *,
# MAGIC row_number()  over (partition by emp_no,from_date,salary order by salary) as dup
# MAGIC
# MAGIC from silver.main_table.joined_data)
# MAGIC select * from tmp
# MAGIC where dup>1

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into bronze.main_tables.employees
# MAGIC (
# MAGIC   select * from my_transactional_data.dbo.employees
# MAGIC   where emp_no=10199
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.main_tables.employees
# MAGIC order by emp_no
