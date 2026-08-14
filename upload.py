import boto3, glob

s3 = boto3.client('s3', region_name='us-east-2')
bucket = 'sentinel-raw-data'

for path in glob.glob('raw/*/*.csv'):
    key = path.replace('raw/', '')
    s3.upload_file(path, bucket, key)
    print(f"Uploaded {key}")
    