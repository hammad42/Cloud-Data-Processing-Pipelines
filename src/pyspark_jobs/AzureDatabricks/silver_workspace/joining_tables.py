# Databricks notebook source
# MAGIC %sql
# MAGIC with tmp2 as
# MAGIC (WITH tmp as 
# MAGIC (SELECT first_name,last_name,e.emp_no,dept_name,s.from_date,salary FROM bronze.main_tables.employees as e
# MAGIC INNER JOIN bronze.main_tables.dept_emp as d
# MAGIC on e.emp_no=d.emp_no
# MAGIC INNER JOIN bronze.main_tables.departments as dep
# MAGIC on d.dept_no=dep.dept_no
# MAGIC INNER JOIN bronze.main_tables.salaries as s
# MAGIC on s.emp_no=e.emp_no)
# MAGIC SELECT first_name,last_name,emp_no,dept_name,from_date,salary,
# MAGIC row_number() OVER ( PARTITION BY emp_no,from_date,salary ORDER BY salary) as dup
# MAGIC FROM tmp)
# MAGIC select * from tmp2
# MAGIC where dup>1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.main_table.joined_data as target
# MAGIC USING(
# MAGIC with tmp2 as
# MAGIC (WITH tmp as 
# MAGIC (SELECT first_name,last_name,e.emp_no,dept_name,s.from_date,salary FROM bronze.main_tables.employees as e
# MAGIC INNER JOIN bronze.main_tables.dept_emp as d
# MAGIC on e.emp_no=d.emp_no
# MAGIC INNER JOIN bronze.main_tables.departments as dep
# MAGIC on d.dept_no=dep.dept_no
# MAGIC INNER JOIN bronze.main_tables.salaries as s
# MAGIC on s.emp_no=e.emp_no)
# MAGIC SELECT first_name,last_name,emp_no,dept_name,from_date,salary,
# MAGIC row_number() OVER ( PARTITION BY emp_no,from_date,salary ORDER BY salary) as dup
# MAGIC FROM tmp)
# MAGIC select * from tmp2
# MAGIC where dup>1
# MAGIC ) AS source
# MAGIC ON target.emp_no=source.emp_no
# MAGIC AND target.from_date=source.from_date
# MAGIC AND target.salary=source.salary
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
