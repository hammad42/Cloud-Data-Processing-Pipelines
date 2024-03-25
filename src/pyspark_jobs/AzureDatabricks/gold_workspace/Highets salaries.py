# Databricks notebook source
# %sql
# SELECT dept_name,max(salary) as highest_salary

# FROM silver.main_table.joined_data
# group by dept_name

# COMMAND ----------

# %sql
# SELECT dept_name,max(salary) as highest_salary

# FROM silver.main_table.joined_data
# group by dept_name

# COMMAND ----------

# %sql
# SELECT dept_name,avg(salary) as average_salary

# FROM silver.main_table.joined_data
# group by dept_name


# COMMAND ----------

# %sql
# SELECT dept_name,avg(salary) as average_salary
# FROM silver.main_table.joined_data
# group by dept_name


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.main_tables.average_salaries as target
# MAGIC USING(
# MAGIC   SELECT dept_name,avg(salary) as average_salary
# MAGIC   FROM silver.main_table.joined_data
# MAGIC   group by dept_name
# MAGIC ) AS source
# MAGIC ON
# MAGIC source.dept_name=target.dept_name
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC target.dept_name=source.dept_name,
# MAGIC target.salary=source.average_salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT  (
# MAGIC   dept_name,
# MAGIC   salary
# MAGIC )
# MAGIC VALUES (
# MAGIC   dept_name,
# MAGIC   average_salary
# MAGIC )
