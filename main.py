import boto3
from botocore.client import Config
import os

ACCESS_KEY_ID = "KEY_ID"
SECRET_ACCESS_KEY = "ACCESS_KEY"
ENDPOINT_URL = "ENDPOINT_URL"
BUCKET_NAME = "TARGET_BUCKET"

s3_client = boto3.client(
    's3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    config=Config(signature_version='s3v4'),
    region_name='auto'
)

def find_mp4_file():
    for file in os.listdir('.'):
        if file.endswith('.mp4'):
            return file
    return None

def upload_large_file(file_path, bucket_name, key_name):
    response = s3_client.create_multipart_upload(Bucket=bucket_name, Key=key_name)
    upload_id = response['UploadId']
    print(f"Multipart upload initiated. Upload ID: {upload_id}")

    part_number = 1
    parts = []

    try:
        with open(file_path, 'rb') as file:
            while True:
                data = file.read(5 * 1024 * 1024)
                if not data:
                    break

                part_response = s3_client.upload_part(
                    Bucket=bucket_name,
                    Key=key_name,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )

                parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
                print(f"Uploaded part {part_number}")
                part_number += 1

        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key_name,
            UploadId=upload_id,
            MultipartUpload={'Parts': parts}
        )
        print(f"File uploaded successfully: {key_name}")

    except Exception as e:
        print(f"An error occurred: {e}")
        s3_client.abort_multipart_upload(Bucket=bucket_name, Key=key_name, UploadId=upload_id)
        print("Multipart upload aborted.")

mp4_file = find_mp4_file()

if mp4_file:
    print(f"Found file: {mp4_file}")
    upload_large_file(mp4_file, BUCKET_NAME, mp4_file)
else:
    print("No .mp4 file found in the current directory.")
