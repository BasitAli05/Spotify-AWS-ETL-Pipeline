import boto3
import os

# Set up AWS S3 client with explicit credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id='----',
    aws_secret_access_key='----',
    region_name='eu-north-1'
)

def download_bucket(bucket_name, download_path):
    """Download all objects from a specified S3 bucket to a local directory."""
    if not os.path.exists(download_path):
        os.makedirs(download_path)
    
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                local_file_path = os.path.join(download_path, key)
                
                # Create local directories if needed
                local_dir = os.path.dirname(local_file_path)
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)
                
                # Download the object
                s3_client.download_file(bucket_name, key, local_file_path)
                print(f"Downloaded {key} to {local_file_path}")

def main():
    # List all buckets
    response = s3_client.list_buckets()
    buckets = response['Buckets']

    # Download each bucket
    for bucket in buckets:
        bucket_name = bucket['Name']
        download_path = os.path.join('E:/s3', bucket_name)
        print(f"Downloading bucket: {bucket_name} to {download_path}")
        download_bucket(bucket_name, download_path)

if __name__ == "__main__":
    main()
