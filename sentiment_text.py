import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from nltk.sentiment import SentimentIntensityAnalyzer
import nltk

def main():
    # Inizializza Spark
    spark = SparkSession.builder \
        .appName("SentimentAnalysisWithGCS") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()

    # Scarica il dizionario di NLTK
    nltk.download('vader_lexicon', download_dir='/home/antogre01/nltk_data')
    os.environ['NLTK_DATA'] = '/home/antogre01/nltk_data'


    # Percorsi su Google Cloud Storage
    input_gcs_path = "gs://esame-cloud/outputParaSentiment/filtered_reviews/*.csv"
    output_gcs_path = "gs://esame-cloud/output"

    # Inizializza il sentiment analyzer
    sia = SentimentIntensityAnalyzer()

    # UDF per il sentiment analysis
    def sentiment_analysis(text):
        if not text:
            return "neutral"
        try:
            score = sia.polarity_scores(str(text))['compound']
            return "positive" if score > 0.05 else "negative" if score < -0.05 else "neutral"
        except Exception as e:
            print(f"Error processing text: {e}")
            return "neutral"

    # Crea UDF per Spark
    sentiment_udf = udf(sentiment_analysis, StringType())

    try:
        # Leggi i dati direttamente da GCS
        data = spark.read.csv(input_gcs_path, header=True, inferSchema=True)

        # Identifica la colonna dei review
        review_columns = [c for c in data.columns if 'review' in c.lower() and 'text' in c.lower()]

        if review_columns:
            review_col = review_columns[0]

            # Aggiungi il sentiment
            data_with_sentiment = data.withColumn("sentiment", sentiment_udf(col(review_col)))

            # Scrivi i risultati direttamente su GCS
            data_with_sentiment.write.csv(output_gcs_path, header=True, mode="overwrite")
            print(f"Sentiment analysis completed successfully! Results saved to {output_gcs_path}")
        else:
            print("No 'review_text' column found in the input data.")

    except Exception as e:
        print(f"Error processing the data: {e}")

    # Termina Spark
    spark.stop()

if __name__ == "__main__":
    main()
