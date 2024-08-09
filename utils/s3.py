import boto3

s3 = boto3.client('s3')

bucket_name = f'sph-yt-data-bucket'
raw_prefix = f'channel-data/raw'
processed_prefix = f'channel-data/processed'


def upload_to_s3(df, part):
    # retry with exponential backoff?
    file_name = f'{part}.json.gz'
    df.to_json(file_name, orient='records', lines=True, compression='gzip')
    s3_key = f'{raw_prefix}/{file_name}'

    # Upload the compressed file to S3
    s3.upload_file(file_name, bucket_name, s3_key)

    print(f'File uploaded to s3://{bucket_name}/{s3_key}')
