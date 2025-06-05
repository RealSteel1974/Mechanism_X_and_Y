import pandas as pd
import requests
import io
import boto3
from datetime import datetime
import time

# Configurations
GDRIVE_FILE_ID = "1AGXVlDhbMbhoGXDJG0IThnqz86Qy3hqb"
S3_BUCKET = "onkarpawar-take-home-assg"
S3_PREFIX = "transactions_chunks_databricks"
CHUNK_SIZE = 10000
REGION = "us-east-1"

# Google Drive direct download URL
GDRIVE_URL = f"https://drive.google.com/uc?export=download&id={GDRIVE_FILE_ID}"

# Use boto3 with access keys 
# NOT THE BEST PRACTISE BUT WORKS FOR DEVELOPMENT
s3 = boto3.client(
    's3',
    aws_access_key_id='ACCESS_KEY',
    aws_secret_access_key='SECRET_ACCESS_KEY',
    region_name='us-east-1'
)

def upload_chunk_to_s3(chunk_df, chunk_index):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{S3_PREFIX}/chunk_{timestamp}_{chunk_index}.csv"
    csv_buffer = io.StringIO()
    chunk_df.to_csv(csv_buffer, index=False)
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=file_name,
        Body=csv_buffer.getvalue()
    )
    print(f"Uploaded {file_name} to S3")

def main():
    print("Downloading CSV from Google Drive...")
    response = requests.get(GDRIVE_URL)
    response.raise_for_status()

    csv_stream = io.StringIO(response.text)
    chunk_iter = pd.read_csv(csv_stream, chunksize=CHUNK_SIZE)

    for i, chunk in enumerate(chunk_iter):
        upload_chunk_to_s3(chunk, i)
        time.sleep(1)

    print("All chunks uploaded.")

if __name__ == "__main__":
    main()
