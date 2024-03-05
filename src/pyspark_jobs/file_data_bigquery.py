import warnings
from google.cloud import storage
from pyspark.sql import SparkSession

def copy_file(source_bucket,blob_name ,destination_bucket,destination_blob_name):
    # try:   
    storage_client = storage.Client() # global storage client  
    source_bucket = storage_client.bucket(source_bucket)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket)
    blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)
    # source_bucket.delete_blob(source_blob)
    source_blob.delete()


def storage_to_bigquery(bucket_name: str, processing_zone: str = "processing_zone",
                       data_sample: str = "data_sample",
                       processed_zone: str = "processed_zone",
                       sales_table_name: str = "sales_data_complete"):
    """
    Reads Parquet files from Cloud Storage, processes them, and writes the results
    to BigQuery.

    Args:
        bucket_name (str): Name of the Cloud Storage bucket.
        processing_zone (str, optional): Prefix for files to process. Defaults to "processing_zone".
        data_sample (str, optional): Sub-folder within processing_zone containing data samples. Defaults to "data_sample".
        processed_zone (str, optional): Prefix for processed files. Defaults to "processed_zone".
        sales_table_name (str, optional): Name of the BigQuery table to write to. Defaults to "sales_data_complete".
    """

    spark = SparkSession.builder.appName("ReadParquetFromCloudStorage").config("spark.jars",
                                                                              f"gs://{bucket_name}/files/gcs-connector-hadoop2-2.1.1.jar").getOrCreate()
    spark.conf.set('temporaryGcsBucket', bucket_name)

    # Get list of blobs in processing_zone/data_sample
    warnings.filterwarnings("ignore", category=UserWarning)  # Temporarily suppress specific warnings
    try:
        storage_client = storage.Client()
        blobs = list(storage_client.list_blobs(storage_client.get_bucket(bucket_name),
                                              prefix=f"{processing_zone}/{data_sample}"))
    except Exception as e:
        print(f"Error listing blobs: {e}")
        spark.stop()
        return

    if not blobs:
        print(f"No blobs found in {processing_zone}/{data_sample}")
        spark.stop()
        return

    # Process the first blob
    blob = str(blobs[0]).split(',')[1].strip()
    source_file = f"gs://{bucket_name}/{blob}"
    dest_blob = f"{processed_zone}/{blob.split('/')[-1]}"

    # Read Parquet file and create temporary view
    header_present = True
    df = spark.read.format("parquet").option("header", header_present).load(source_file)
    df.createOrReplaceTempView("Sales")

    # Perform any desired processing on the DataFrame

    # Write to BigQuery
    result = spark.sql("Select * from sales")
    result.write.format('bigquery').option('table', f"sales.{sales_table_name}").option('temporaryGcsBucket', bucket_name).mode('append').save()

    # Move the processed file to processed_zone
    try:
        copy_file(bucket_name,blob, bucket_name,dest_blob)
    except Exception as e:
        print(f"Error moving file: {e}")

    spark.stop()




if __name__ == "__main__":
    storage_to_bigquery("data-bucket522")  # Replace with your actual bucket name
