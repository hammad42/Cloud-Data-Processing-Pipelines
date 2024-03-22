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
cdc_load(spark, "employees", "bronze.config_tables.configurations") 

