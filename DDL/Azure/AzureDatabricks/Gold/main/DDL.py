# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE gold.main_tables.average_salaries(
# MAGIC dept_name VARCHAR(255) NOT NULL,
# MAGIC salary FLOAT NOT NULL
# MAGIC )
# MAGIC USING DELTA
