import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import gcsfs
import sys

def main():
    input_dir = sys.argv[1]  # GCS directory
    output_plot_dir = sys.argv[2]  # Local directory for saving plots

    # Initialize GCS FileSystem
    fs = gcsfs.GCSFileSystem()

    # List all files in the GCS directory
    all_data = []
    files = fs.glob(f"{input_dir}/*.csv")
    
    for file_path in files:
        try:
            with fs.open(file_path, 'r') as f:
                df = pd.read_csv(f)
                
                # Validate and ensure columns
                required_columns = {"review_score", "sentiment", "word_count"}
                if not required_columns.issubset(df.columns):
                    print(f"Skipping {file_path}: Missing required columns")
                    continue

                # Ensure 'review_score' is numeric and filter valid rows
                df = df[pd.to_numeric(df["review_score"], errors="coerce").notna()]
                df["review_score"] = df["review_score"].astype(float)

                # Filter for valid review scores
                df = df[df["review_score"].isin([1.0, 2.0, 3.0, 4.0, 5.0])]

                # Append to the list
                all_data.append(df)
                print(f"Processed file: {file_path}")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    # Combine all data into a single DataFrame
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)

        # Separate positive and negative sentiments
        positive_df = combined_df[combined_df["sentiment"] == "positive"]
        negative_df = combined_df[combined_df["sentiment"] == "negative"]

        # Review scores to analyze
        review_scores = [1.0, 2.0, 3.0, 4.0, 5.0]

        # Plotting word count distribution for positive reviews
        plt.figure(figsize=(10, 6))
        boxes_pos = [positive_df[positive_df["review_score"] == score]["word_count"] for score in review_scores]
        plt.boxplot(boxes_pos, labels=review_scores)
        plt.title("Word Count Distribution for Positive Reviews")
        plt.xlabel("Review Score")
        plt.ylabel("Number of Words")
        plt.savefig(f"{output_plot_dir}/boxplot_positive_reviews.png")
        plt.close()

        # Plotting word count distribution for negative reviews
        plt.figure(figsize=(10, 6))
        boxes_neg = [negative_df[negative_df["review_score"] == score]["word_count"] for score in review_scores]
        plt.boxplot(boxes_neg, labels=review_scores)
        plt.title("Word Count Distribution for Negative Reviews")
        plt.xlabel("Review Score")
        plt.ylabel("Number of Words")
        plt.savefig(f"{output_plot_dir}/boxplot_negative_reviews.png")
        plt.close()

        print(f"Boxplots saved to: {output_plot_dir}")
    else:
        print("No valid CSV files found.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script_name.py gs://<bucket>/<path> <output_plot_directory>")
        sys.exit(1)
    main()