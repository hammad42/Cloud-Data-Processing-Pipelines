# Databricks notebook source
# %sql
# with tmp2 as
# (WITH tmp as 
# (SELECT first_name,last_name,e.emp_no,dept_name,s.from_date,salary FROM bronze.main_tables.employees as e
# INNER JOIN bronze.main_tables.dept_emp as d
# on e.emp_no=d.emp_no
# INNER JOIN bronze.main_tables.departments as dep
# on d.dept_no=dep.dept_no
# INNER JOIN bronze.main_tables.salaries as s
# on s.emp_no=e.emp_no)
# SELECT first_name,last_name,emp_no,dept_name,from_date,salary,
# row_number() OVER ( PARTITION BY emp_no,from_date,salary,dept_name ORDER BY salary) as dup
# FROM tmp)
# select * from tmp2
# where dup>1


# COMMAND ----------

# %sql
# SELECT first_name,last_name,e.emp_no,dept_name,s.from_date,salary FROM bronze.main_tables.employees as e
# INNER JOIN bronze.main_tables.dept_emp as d
# on e.emp_no=d.emp_no
# INNER JOIN bronze.main_tables.departments as dep
# on d.dept_no=dep.dept_no
# INNER JOIN bronze.main_tables.salaries as s
# on s.emp_no=e.emp_no

# COMMAND ----------

# %sql
# SELECT e.emp_no,s.from_date,salary,count(*) FROM bronze.main_tables.employees as e
# INNER JOIN bronze.main_tables.dept_emp as d
# on e.emp_no=d.emp_no
# INNER JOIN bronze.main_tables.departments as dep
# on d.dept_no=dep.dept_no
# INNER JOIN bronze.main_tables.salaries as s
# on s.emp_no=e.emp_no

# group by e.emp_no,s.from_date,salary,d.dept_no
# HAVING COUNT(*) > 1
# order by e.emp_no,s.salary

# COMMAND ----------

# %sql
# truncate table silver.main_table.joined_data

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.main_table.joined_data as target
# MAGIC USING(
# MAGIC SELECT first_name,last_name,e.emp_no,dept_name,s.from_date,salary FROM bronze.main_tables.employees as e
# MAGIC INNER JOIN bronze.main_tables.dept_emp as d
# MAGIC on e.emp_no=d.emp_no
# MAGIC INNER JOIN bronze.main_tables.departments as dep
# MAGIC on d.dept_no=dep.dept_no
# MAGIC INNER JOIN bronze.main_tables.salaries as s
# MAGIC on s.emp_no=e.emp_no
# MAGIC ) AS source
# MAGIC ON target.emp_no=source.emp_no
# MAGIC AND target.from_date=source.from_date
# MAGIC AND target.salary=source.salary
# MAGIC AND target.dept_name=source.dept_name
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET
# MAGIC target.first_name=source.first_name,
# MAGIC target.last_name=source.last_name,
# MAGIC target.emp_no=source.emp_no,
# MAGIC target.dept_name=source.dept_name,
# MAGIC target.from_date=source.from_date,
# MAGIC target.salary=source.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT(
# MAGIC   first_name,
# MAGIC   last_name,
# MAGIC   emp_no,
# MAGIC   dept_name,
# MAGIC   from_date,
# MAGIC   salary
# MAGIC )
# MAGIC VALUES
# MAGIC (
# MAGIC   source.first_name,
# MAGIC   source.last_name,
# MAGIC   source.emp_no,
# MAGIC   source.dept_name,
# MAGIC   source.from_date,
# MAGIC   source.salary
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## checking duplicates

# COMMAND ----------

# %sql
# select emp_no,dept_name,salary,from_date,count(*) from silver.main_table.joined_data
# group by emp_no,dept_name,salary,from_date
# having count(*)>1
