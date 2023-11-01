from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a Spark session
spark = SparkSession.builder.appName("CSV Processing").getOrCreate()

# Read the CSV file into a DataFrame
csv_file_path = "sample.csv"  # Replace with the path to your CSV file
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show the schema and first few rows of the DataFrame
df.printSchema()
df.show()

# Perform data transformations (e.g., filter and select columns)
filtered_df = df.filter(col("Age") >= 18).select("Name", "Age")

# Show the transformed DataFrame
filtered_df.show()

# Save the transformed DataFrame to a new CSV file
output_file_path = "filtered_data.csv"  # Replace with the desired output file path
filtered_df.write.csv(output_file_path, header=True, mode="overwrite")

# Stop the Spark session
spark.stop()
