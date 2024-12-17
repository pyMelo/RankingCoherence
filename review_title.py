import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

# Initialize a Spark session without PyArrow
spark = SparkSession.builder \
    .appName("AmazonBookReviewsFilteredReviews") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

# Paths to the input and output
input_path = sys.argv[1]  # Input path for CSV files
output_path = sys.argv[2]  # Output path to save results

# Load the dataset
data = spark.read.csv(input_path, header=True, inferSchema=True)

# Rename columns for easier access
data = data.withColumnRenamed("review/score", "review_score") \
           .withColumnRenamed("review/text", "review_text")

# Filter only review_text where review_score is between 1.0 and 5.0
filtered_data = data.select("review_text", "review_score") \
                    .filter((col("review_score") >= 1.0) & (col("review_score") <= 5.0)) \
                    .filter(col("review_text").isNotNull())

# Replace any "," in the review_text column with "-"
cleaned_data = filtered_data.withColumn("review_text", regexp_replace(col("review_text"), ",", "-"))

# Save the cleaned and filtered data as CSV to the output path
cleaned_data.write.csv(f"{output_path}/filtered_reviews", header=True, mode="overwrite")

print("Filtered and cleaned reviews with scores between 1.0 and 5.0 have been saved.")

# Stop the Spark session
spark.stop()
