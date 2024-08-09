import boto3
import gzip

s3 = boto3.client('s3')

bucket_name = f'sph-yt-data-bucket'
raw_prefix = f'channel-data/raw'
processed_prefix = f'channel-data/processed'


def upload_to_s3(df, part):
    # retry with exponential backoff?
    # file_name = f'{part}.csv'
    # file_name = f'{part}.json'
    file_name = f'{part}.json.gz'
    # df.to_csv(file_name, mode='w', encoding='utf-8', index=False, lineterminator='\n')
    df.to_json(file_name, orient='records', lines=True, compression='gzip')

    # file_obj = f'{file_name}.gz'
    # with open(file_name, 'rb') as f_in:
    #     with gzip.open(file_obj, 'wb') as f_out:
    #         f_out.writelines(f_in)

    # Define the S3 key (path) where the file will be stored
    # s3_key = f'{raw_prefix}/{file_obj}'
    s3_key = f'{raw_prefix}/{file_name}'

    # Upload the compressed file to S3
    # s3.upload_file(file_obj, bucket_name, s3_key)
    s3.upload_file(file_name, bucket_name, s3_key)

    print(f'File uploaded to s3://{bucket_name}/{s3_key}')
