# setup_minio_block.py
import os
import traceback
from prefect_aws import S3Bucket, MinIOCredentials
from prefect_aws.credentials import AwsCredentials
from prefect_aws.client_parameters import AwsClientParameters
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
import subprocess

load_dotenv()

MINIO_ENDPOINT_URL = os.getenv("MINIO_ENDPOINT_URL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("MINIO_BUCKET")
os.environ['PREFECT_API_URL'] = os.getenv("PREFECT_API_URL")

client = Minio(
    MINIO_ENDPOINT_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False, 
)

try:
    # Check if the bucket exists
    if client.bucket_exists(BUCKET_NAME):
        print(f"✅ Minio Bucket '{BUCKET_NAME}' already exists.")
    else:
        # Create the bucket
        client.make_bucket(BUCKET_NAME)
        print(f"✅ Minio Bucket '{BUCKET_NAME}' created successfully!")

except S3Error as e:
    print(f"❌ Error creating Minio bucket: {e}")

try:
    print("🔧 Creating MinIO credentials block...")
    minio_credentials_block = MinIOCredentials(
        minio_root_user=os.getenv("MINIO_ACCESS_KEY"),
        minio_root_password=os.getenv("MINIO_SECRET_KEY"),
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        secure=False,
        aws_client_parameters={
            "endpoint_url": os.getenv("MINIO_ENDPOINT"),
            "use_ssl": False
        }
    )
    minio_credentials_block.save("minio-stock-bucket", overwrite=True)
    print("✅ Prefect MinIO block created successfully")

except Exception as e:
    print("❌ Error creating MinIOCredentials block:")
    traceback.print_exc()

try:
    print("\n🔧 Creating AWS credentials block...")
    creds = AwsCredentials(
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
        aws_client_parameters={
            "endpoint_url": os.getenv("MINIO_ENDPOINT"),
            "use_ssl": False
        }
    )
    creds.save("minio-stock-bucket", overwrite=True)
    print("✅ Prefect AWS block created successfully")

except Exception as e:
    print("❌ Error creating AwsCredentials block:")
    traceback.print_exc()

try:
    print("\n🔧 Creating S3Bucket block...")
    s3_block = S3Bucket(
        bucket_name="minio-stock-bucket",
        credentials=minio_credentials_block,
        aws_client_parameters={
            "endpoint_url": os.getenv("MINIO_ENDPOINT"),
            "use_ssl": False
        },
        secure=False
    )
    s3_block.save("minio-stock-bucket", overwrite=True)
    print("✅ Prefect S3Bucket block created successfully")

except Exception as e:
    print("❌ Error creating S3Bucket block:")
    traceback.print_exc()


command = "prefect worker start --pool default"
print(f"Starting Prefect worker for pool 'default'...")
try:
    subprocess.run(command, shell=True, check=True) 
    print("✅ Prefect Worker (default) Started successfully.")
except subprocess.CalledProcessError as e:
    print(f"Error starting worker: {e}")
except KeyboardInterrupt:
    print("\nWorker process interrupted.")