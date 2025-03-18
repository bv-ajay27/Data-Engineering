import boto3
import botocore

try:
    # Create an S3 client
    s3_client = boto3.client("s3")

    # Test the connection by listing buckets
    response = s3_client.list_buckets()

    print("✅ AWS Credentials are working!")
    print("Your S3 Buckets:")
    for bucket in response["Buckets"]:
        print(f" - {bucket['Name']}")

except botocore.exceptions.NoCredentialsError:
    print("❌ No AWS credentials found. Check your ~/.aws/credentials file.")
except botocore.exceptions.PartialCredentialsError:
    print("❌ Incomplete AWS credentials. Ensure both access_key and secret_key are set.")
except botocore.exceptions.ClientError as e:
    print(f"❌ AWS Client Error: {e}")
