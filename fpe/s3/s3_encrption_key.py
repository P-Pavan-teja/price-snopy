# use_key_encrypt.py
from function import * 
import boto3

s3 = boto3.client("s3")
bucket = "fpe-keys-storage-bucket"
s3_key = "testing_data/custom_project_key.bin"

# --------------------------------------------------------------
# Define your project key (must be bytes)
# --------------------------------------------------------------
key = b'pavanteja'  # ✅ 16 bytes

# --------------------------------------------------------------
# Create encryptor with that key
# --------------------------------------------------------------
encryptor = DataEncryptor(key=key)

# --------------------------------------------------------------
# Save it once for reuse
# --------------------------------------------------------------
s3.put_object(Bucket=bucket, Key=s3_key, Body=encryptor.key)
print(f"Uploaded to s3://{bucket}/{s3_key}")
