import boto3
import time
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

bucket_name = f'sph-yt-data-bucket'
raw_prefix = f'channel-data/raw'
processed_prefix = f'channel-data/processed'


"""Method to upload data to S3 bucket"""


def upload_to_s3(df, part):
    retry_count = 0
    delay = 1  # Initial delay in seconds
    file_name = f'{part}.json.gz'
    max_retries = 3
    df.to_json(file_name, orient='records', lines=True, compression='gzip')
    s3_key = f'{raw_prefix}/{file_name}'

    # Upload the compressed file to S3
    while retry_count < max_retries:
        try:
            s3.upload_file(file_name, bucket_name, s3_key)
            break
        except ClientError as e:
            print(f"Attempt {retry_count + 1} failed: {e}")
            retry_count += 1
            if retry_count < max_retries:
                sleep_time = delay * (2 ** (retry_count - 1))  # Exponential backoff
                print(f"Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                print(f"Failed to upload file {file_name} after {max_retries} attempts.")
                break

    print(f'File uploaded to s3://{bucket_name}/{s3_key}')
