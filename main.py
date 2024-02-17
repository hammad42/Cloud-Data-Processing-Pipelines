from pyspark.sql import SparkSession
import warnings
from google.cloud import storage
def storage_to_bigquery():    
    


    warnings.filterwarnings("ignore")
    bucket = "data-bucket4200"
    storage_client = storage.Client() # global storage client
    spark = SparkSession.builder.appName("ReadParquetFromCloudStorage").config("spark.jars", "gs://data-bucket4200/gcs-connector-hadoop2-2.1.1.jar").getOrCreate()
    spark.conf.set('temporaryGcsBucket', bucket)
    blobs = list(storage_client.list_blobs(storage_client.get_bucket(bucket),prefix="processing_zone/data_sample"))
    print("count of blobs is {}".format(len(blobs)))
    blob=str(blobs[0]).split(',')[1]
    blob=blob.strip()
    dest_blob="processed_zone/"+blob.split('/')[-1]
    print(dest_blob)




    file="gs://"+bucket+"/"+blob.strip()
    print(file)
    header_present = True
    # Read the Parquet file into a DataFrame
    df = spark.read.format("parquet").option("header", header_present).load(file)
    # df = spark.read.parquet(file)
    df.createOrReplaceTempView("Sales") 
    print(df.show())
    result = spark.sql("Select * from sales;")


    result.write.format('bigquery').option('table', 'sales.sales_data_complete').option('temporaryGcsBucket', bucket).mode('append').save()
    

    copy_file(bucket,blob, bucket,dest_blob)
    spark.stop()

def copy_file(source_bucket,blob_name ,destination_bucket,destination_blob_name):
    # try:   
    storage_client = storage.Client() # global storage client  
    source_bucket = storage_client.bucket(source_bucket)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket)
    blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
    # source_bucket.delete_blob(source_blob)
    source_blob.delete()
storage_to_bigquery()