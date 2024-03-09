import warnings
from google.cloud import storage
from pyspark.sql import SparkSession
import json
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Consider using environment variables (more secure )
config_table = 'planes.config_table'

def load_and_process_data(spark,config_data_fetch_str, config_data_fetch_dict,check_mark):
    """Fetches data from MySQL, processes it, and saves to BigQuery.

    Args:
        spark (SparkSession): The Spark session object.
        config_data_fetch (dict): Configuration data.
        bucket (str): Temporary GCS bucket name.
    """
    try:
    
    # JDBC connection details
        print("#####     STAGE3     #####")
        jdbc_hostname = config_data_fetch_dict["jdbc_hostname"]
        print(jdbc_hostname)
        jdbc_port = config_data_fetch_dict["jdbc_port"]
        print(jdbc_port)
        database = config_data_fetch_dict["database"]
        print(database)
        username = config_data_fetch_dict["username"]
        print(username)
        password = config_data_fetch_dict["password"]
        print(password)
        table = config_data_fetch_dict["table_"]
        print(table)
        bucket=config_data_fetch_dict["bucket"]
        print(bucket)

        jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{database}"

        print("#####     STAGE4     #####")

        # Read data from MySQL, update checkmark
        mysql_query = f"(SELECT * FROM {table} WHERE pri_key >'{check_mark}') AS subquery"
        print(mysql_query)
        spark_df = spark.read.format("jdbc") \
                        .option("driver", "com.mysql.cj.jdbc.Driver") \
                        .option("url", jdbc_url) \
                        .option("user", username) \
                        .option("password", password) \
                        .option("dbtable", mysql_query) \
                        .load()

        print("#####     STAGE5     #####")

        spark_df.createOrReplaceTempView(table)
        spark_df.write.format('bigquery') \
                        .option('table', f"{database}.{table}") \
                        .option('temporaryGcsBucket', bucket) \
                        .mode('append') \
                        .save()
        
        print("#####     STAGE6     #####")

        query2 = "(SELECT max(pri_key) as max_check_mark FROM {}  ) AS subquery".format(table)  # Replace with your query
        spark_df2 = spark.read.format("jdbc").option("driver", "com.mysql.cj.jdbc.Driver").option("url", jdbc_url).option("user", username).option("password", password).option("dbtable", query2).load()

        print("#####     STAGE7     #####")

        spark_df2.createOrReplaceTempView("check_mark")
        checkmark=spark.sql("select max_check_mark from check_mark")
        checkmark_=checkmark.select('max_check_mark').collect()[0][0]

        schema = StructType([
            StructField("check_mark", IntegerType(), True),  # Integer Column
            StructField("config_data", StringType(), True)   # String Column
        ])

        print("#####     STAGE8     #####")
        data = [(checkmark_, config_data_fetch_str)]

        sparkDF=spark.createDataFrame(data,schema) 

        sparkDF.write.format('bigquery').option('table', config_table).option('temporaryGcsBucket', bucket).mode('overwrite').save()

    except:
        print("error")



def main():

    print("#####     STAGE1     #####")
    spark = SparkSession.builder.appName('TransactionalData').config("spark.jars", "gs://data-bucket52/jars/mysql-connector-java-8.0.13.jar,gs://data-bucket52/jars/spark-2.4-bigquery-0.36.1.jar").getOrCreate()

    # Load configuration data
    print("#####     STAGE2     #####")
    df = spark.read.format("bigquery").option("table", "planes.config_table").load()
    df.createOrReplaceTempView("configuration")
    config_data=spark.sql("select * from configuration")
    print(config_data.show())
    config_data_fetch_str=config_data.select("config_data").collect()[0][0]
    print(config_data_fetch_str)
    check_mark=config_data.select("check_mark").collect()[0][0]
    print(check_mark)

    config_data_fetch_dict = json.loads(config_data_fetch_str)
    print(config_data_fetch_dict)

    load_and_process_data(spark,config_data_fetch_str, config_data_fetch_dict,check_mark)

    spark.stop()

if __name__ == "__main__":
    main()
