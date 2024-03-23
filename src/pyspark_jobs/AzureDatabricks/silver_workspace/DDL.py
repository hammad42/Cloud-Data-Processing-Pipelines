# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE silver.main_table.joined_data
# MAGIC (
# MAGIC   first_name VARCHAR(255) NOT NULL,
# MAGIC last_name VARCHAR(255) NOT NULL,
# MAGIC emp_no INT NOT NULL,
# MAGIC dept_name VARCHAR(255) NOT NULL,
# MAGIC from_date DATE NOT NULL,
# MAGIC salary INT NOT NULL
# MAGIC )
