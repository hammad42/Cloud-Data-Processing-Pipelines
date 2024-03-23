# Databricks notebook source
# %sql
# -- INSERT INTO bronze.main_tables.employees
# -- (SELECT * FROM my_transactional_data.dbo.employees );
# -- DECLARE VARIABLE max_emp INT;
# -- SET VAR max_emp = (SELECT MAX(emp_no) FROM bronze.main_tables.employees);
# -- UPDATE bronze.config_tables.configurations
# -- SET checkmark=max_emp
# -- where table_name='employees';
# -- DROP TEMPORARY VARIABLE max_emp;



# COMMAND ----------

# %sql
# -- SELECT count(*) FROM bronze.main_tables.employees
# -- truncate table bronze.main_tables.employees


# COMMAND ----------

# check_mark=spark.sql("select checkmark from bronze.config_tables.configurations where table_name='employees'")
# check_mark=check_mark.collect()[0][0]
# df_transactional=spark.sql("select * from my_transactional_data.dbo.employees where emp_no>{}".format(check_mark))
# df_transactional.createOrReplaceTempView('tmp_employees')
# df_tmp=spark.sql("""
#     INSERT INTO bronze.main_tables.employees
#     (
#     SELECT * FROM tmp_employees
#     );
#     """)
# check_mark=spark.sql("""SELECT max(emp_no) as checkmark FROM tmp_employees""")
# update_conf="""
#     UPDATE bronze.config_tables.configurations
#     SET checkmark={}
#     where table_name='employees'
#     """.format(check_mark.collect()[0][0])
# spark.sql(update_conf)

# COMMAND ----------

import pyspark.sql.functions as F

def cdc_load(spark,table,config_table):
    """Updates employees data and configuration table.

    Args:
        spark (SparkSession): The active Spark session.
        transactional_db (str): Name of the transactional database.
        config_table (str): Name of the configuration table.
    """

    check_mark = spark.read.table(config_table) \
                          .filter(F.col("table_name") == table) \
                          .select("checkmark") \
                          .collect()[0][0]
    print(check_mark)

    df_transactional = spark.read.table(f"my_transactional_data.dbo.{table}") \
                                 .filter(F.col("emp_no") > check_mark) 
    df_transactional.createOrReplaceTempView(f"tmp_{table}")

    df_transactional.write.format("delta") \
                            .mode("append") \
                            .saveAsTable(f"bronze.main_tables.{table}")

    new_check_mark = spark.sql("SELECT max(emp_no) as checkmark FROM bronze.main_tables.{}".format(table)).collect()[0][0]
    if new_check_mark is None:
        new_check_mark=0

    spark.sql(f"""
        UPDATE bronze.config_tables.configurations
        SET checkmark = {new_check_mark}
        WHERE table_name = '{table}'
    """)

# Example usage (assuming you have a SparkSession called 'spark')

tables=("employees","dept_emp","dept_manager","salaries","titles")
for table in tables:
    cdc_load(spark, table, "bronze.config_tables.configurations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Code for upserts

# COMMAND ----------

# MERGE INTO customers AS target
# USING (
#   SELECT 102 AS customer_id, 'Alice' AS name, 'NY' AS state
#   UNION ALL
#   SELECT 105 AS customer_id, 'Bob' AS name, 'CA' as state 
# ) AS source
# ON target.customer_id = source.customer_id
# WHEN MATCHED THEN 
#   UPDATE SET name = source.name, state = source.state
# WHEN NOT MATCHED THEN 
#   INSERT (customer_id, name, state) 
#   VALUES (source.customer_id, source.name, source.state);


# COMMAND ----------

# from pyspark.sql.functions import col

# updates_df = spark.createDataFrame([
#     (102, 'Alice Updated', 'CA'),  # Existing customer - update
#     (200, 'Charlie', 'FL')          # New customer - insert
# ], schema=["customer_id", "name", "state"])

# # Option 1: Using overwrite if exists, filtering (less efficient for large tables):
# updates_df.write.format("delta") \
#             .mode("overwrite") \
#             .option("mergeSchema", "true") \
#             .saveAsTable("customers") 

# # Option 2: More performant approach, especially for larger tables
# customers_df = spark.read.table("customers")

# result_df = customers_df.alias("target") \
#                         .merge(updates_df.alias("source"), "target.customer_id = source.customer_id") \
#                         .whenMatchedUpdateAll() \
#                         .whenNotMatchedInsertAll() \
#                         .execute() 
