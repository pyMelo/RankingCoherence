import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import gcsfs
import sys

def main():
    input_dir = sys.argv[1]  # GCS directory
    output_plot = sys.argv[2]  # Local path for saving plot

    # Initialize GCS FileSystem
    fs = gcsfs.GCSFileSystem()

    # List all files in the GCS directory
    all_data = []
    files = fs.glob(f"{input_dir}/*.csv")
    for file_path in files:
        try:
            with fs.open(file_path, 'r') as f:
                df = pd.read_csv(f)
                df = df[pd.to_numeric(df["review_score"], errors="coerce").notna()]
                df["review_score"] = df["review_score"].astype(float)
                all_data.append(df[["review_score", "sentiment"]])
                print(f"Processed file: {file_path}")
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    # Combine and plot as per the original logic
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        filtered_df = combined_df[combined_df["sentiment"].isin(["positive", "negative"])]
        review_scores = [1.0, 2.0, 3.0, 4.0, 5.0]
        positive_counts = [filtered_df[(filtered_df["review_score"] == score) & (filtered_df["sentiment"] == "positive")].shape[0] for score in review_scores]
        negative_counts = [filtered_df[(filtered_df["review_score"] == score) & (filtered_df["sentiment"] == "negative")].shape[0] for score in review_scores]

        # Plot the histogram
        x = np.arange(len(review_scores))
        width = 0.35
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(x - width/2, positive_counts, width, label="Positive", color="green", alpha=0.7)
        ax.bar(x + width/2, negative_counts, width, label="Negative", color="red", alpha=0.7)
        ax.set_xlabel("Review Score")
        ax.set_ylabel("Number of Reviews")
        ax.set_title("Number of Positive and Negative Reviews by Review Score")
        ax.set_xticks(x)
        ax.set_xticklabels(review_scores)
        ax.legend()
        plt.tight_layout()
        plt.savefig(output_plot)
        plt.show()
        print(f"Histogram saved to: {output_plot}")
    else:
        print("No valid CSV files found.")

if __name__ == "__main__":
    main()