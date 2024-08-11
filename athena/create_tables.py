import boto3
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

glue_client = boto3.client(
        'glue',
        aws_access_key_id='my-key',
        aws_secret_access_key='my-secret'
    )


def trigger_glue_crawler(glue_crawler_name):
    try:
        response = glue_client.start_crawler(Name=glue_crawler_name)
        print(f"Crawler '{glue_crawler_name}' has been triggered successfully.")
        print(f"Crawler response: {response}")
    except Exception as e:
        print(f"Failed to start crawler '{glue_crawler_name}': {str(e)}")


if __name__ == "__main__":
    crawler_name = 'sph_yt_crawler'
    trigger_glue_crawler(crawler_name)
