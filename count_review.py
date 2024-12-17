import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

def main():
    # Start the timer
    start_time = time.time()

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("WordCountInReviews") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .getOrCreate()

    # Input and output paths
    input_dir = sys.argv[1]  # Input path in GCS
    output_dir = sys.argv[2]  # Output path in GCS

    # UDF to compute word count
    def word_count(text):
        return len(text.split()) if text else 0

    word_count_udf = udf(word_count, IntegerType())

    try:
        # Load all CSV files from the GCS input directory
        data = spark.read.csv(f"{input_dir}/*.csv", header=True, inferSchema=True)

        # Check for required columns: 'review_text' and 'review_score'
        if "review_text" in data.columns and "review_score" in data.columns:
            # Add word count column and select required columns
            processed_data = data.withColumn("word_count", word_count_udf(col("review_text"))) \
                                 .select("review_text", "review_score", "word_count","sentiment")

            # Write results to the output directory in GCS
            processed_data.write.csv(output_dir, header=True, mode="overwrite")
            print("Processed data successfully")
        else:
            print("Required columns 'review_text' or 'review_score' not found in the dataset")

    except Exception as e:
        print(f"Error processing data: {e}")

    # Stop Spark session
    spark.stop()

    # End the timer
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Total processing time: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
