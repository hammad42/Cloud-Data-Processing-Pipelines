# Databricks notebook source
# MAGIC %md
# MAGIC ### DDL for Employee table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze.main_tables.employees
# MAGIC (
# MAGIC   emp_no INT NOT NULL,
# MAGIC   birth_date DATE,
# MAGIC   first_name VARCHAR(255),
# MAGIC   last_name VARCHAR(255),
# MAGIC   gender CHAR(1),
# MAGIC   hire_date DATE,
# MAGIC   CONSTRAINT pk_employees PRIMARY KEY (emp_no)
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (hire_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL for Departments table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze.main_tables.departments
# MAGIC (
# MAGIC   dept_no CHAR(4) NOT NULL,
# MAGIC   dept_name VARCHAR(255) NOT NULL,
# MAGIC   CONSTRAINT pk_department PRIMARY KEY (dept_no)
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL for dept_manager table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze.main_tables.dept_manager (
# MAGIC   emp_no INT NOT NULL,
# MAGIC   dept_no CHAR(4) NOT NULL,
# MAGIC   from_date DATE NOT NULL,
# MAGIC   to_date DATE NOT NULL,
# MAGIC   CONSTRAINT FK_dept_manager_employees FOREIGN KEY (emp_no) REFERENCES bronze.main_tables.employees (emp_no) ,
# MAGIC   CONSTRAINT FK_dept_manager_departments FOREIGN KEY (dept_no) REFERENCES bronze.main_tables.departments (dept_no) ,
# MAGIC   CONSTRAINT PK_dept_manager PRIMARY KEY (emp_no, dept_no) 
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL for dept_emp table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze.main_tables.dept_emp (
# MAGIC   emp_no      INT             NOT NULL,
# MAGIC   dept_no CHAR(4) NOT NULL,
# MAGIC   from_date   DATE            NOT NULL,
# MAGIC   to_date     DATE            NOT NULL,
# MAGIC   FOREIGN KEY (emp_no)  REFERENCES bronze.main_tables.employees,
# MAGIC   FOREIGN KEY (dept_no) REFERENCES bronze.main_tables.departments ,
# MAGIC   PRIMARY KEY (emp_no,dept_no)
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL for titles table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze.main_tables.titles (
# MAGIC   emp_no      INT             NOT NULL,
# MAGIC   title       VARCHAR(50)     NOT NULL,
# MAGIC   from_date   DATE            NOT NULL,
# MAGIC   to_date     DATE,
# MAGIC   FOREIGN KEY (emp_no) REFERENCES bronze.main_tables.employees (emp_no) ,
# MAGIC   PRIMARY KEY (emp_no,title, from_date)
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL for salaries table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze.main_tables.salaries (
# MAGIC     emp_no      INT             NOT NULL,
# MAGIC     salary      INT             NOT NULL,
# MAGIC     from_date   DATE            NOT NULL,
# MAGIC     to_date     DATE            NOT NULL,
# MAGIC     FOREIGN KEY (emp_no) REFERENCES bronze.main_tables.employees (emp_no),
# MAGIC     PRIMARY KEY (emp_no, from_date)
# MAGIC ) 
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dropping tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop table bronze.main_tables.departments;
# MAGIC -- Drop table bronze.main_tables.salaries;
# MAGIC -- Drop table bronze.main_tables.titles;
# MAGIC -- Drop table bronze.main_tables.dept_emp;
# MAGIC -- Drop table bronze.main_tables.dept_manager;
# MAGIC -- Drop table bronze.main_tables.employees;

# COMMAND ----------


