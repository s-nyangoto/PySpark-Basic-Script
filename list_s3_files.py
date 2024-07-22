from pyspark.sql import SparkSession

def list_s3_files(bucket_path):
    # Initialize Spark session
    spark = SparkSession.builder.appName("ListS3Files").getOrCreate()

    try:
        # List the files in the specified S3 bucket path
        files_df = spark.read.format("binaryFile").load(bucket_path + "/*")
        files = files_df.select("path").collect()

        print("Files in the bucket:")
        for file in files:
            print(file["path"])
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    bucket_path = "<path_to_s3_bucket>"
    list_s3_files(bucket_path)
