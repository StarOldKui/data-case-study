import os

import boto3

# S3 bucket name
BUCKET_NAME = "data-case-study-raw-data"
# Local directory containing the CSV files
DATA_DIRECTORY = "../../data/raw"


def upload_files_to_s3(directory: str, bucket_name: str) -> None:
    # Initialize S3 client
    s3_client = boto3.client("s3")

    # Traverse through files in the specified directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                # S3 key is based on the file name to create individual folder paths
                folder_name = os.path.splitext(file)[0]  # Use file name without extension as folder
                s3_key = f"{folder_name}/{file}"  # Path structure in S3

                try:
                    print(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}")
                    s3_client.upload_file(file_path, bucket_name, s3_key)
                    print("Done")
                except Exception as e:
                    print(f"Failed to upload {file_path}. Error: {e}")


if __name__ == "__main__":
    upload_files_to_s3(DATA_DIRECTORY, BUCKET_NAME)
