"""
S3 Upload Script for IFS Parsed Data
Uploads parsed_data and options files to S3
"""

import boto3
import os
from botocore.exceptions import ClientError

ACCESS_KEY = '<ACCESS_KEY>'
SECRET_ACCESS_KEY = '<SECRET_ACCESS_KEY>'
REGION = '<REGION>'
BUCKET_NAME = 'ngage-ifs-api-ai-data'
BASE_URL = f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com"


def get_s3_client():
    """Create and return an S3 client."""
    return boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_ACCESS_KEY,
        region_name=REGION
    )


def upload_file_to_s3(s3_client, local_file_path: str, s3_key: str) -> bool:
    """
    Upload a file to S3 bucket.

    Args:
        s3_client: boto3 S3 client
        local_file_path: Path to the local file
        s3_key: S3 object key (path in bucket)

    Returns:
        True if successful, False otherwise
    """
    try:
        s3_client.upload_file(
            local_file_path,
            BUCKET_NAME,
            s3_key,
            ExtraArgs={'ContentType': 'application/json'}
        )
        return True
    except ClientError as e:
        print(f"Error uploading {local_file_path}: {e}")
        return False
    except FileNotFoundError:
        print(f"File not found: {local_file_path}")
        return False


def upload_directory(s3_client, local_dir: str, s3_prefix: str) -> tuple:
    """
    Upload all JSON files from a local directory to S3.

    Args:
        s3_client: boto3 S3 client
        local_dir: Local directory path
        s3_prefix: S3 prefix (folder) for uploads

    Returns:
        Tuple of (success_count, total_count)
    """
    success_count = 0
    total_count = 0

    if not os.path.exists(local_dir):
        print(f"Directory not found: {local_dir}")
        return 0, 0

    for file_name in os.listdir(local_dir):
        if file_name.endswith('.json') and not file_name.startswith('.'):
            total_count += 1
            local_path = os.path.join(local_dir, file_name)
            s3_key = f"{s3_prefix}/{file_name}"
            s3_url = f"{BASE_URL}/{s3_key}"

            print(f"\nUploading: {file_name}")
            print(f"  -> {s3_url}")

            if upload_file_to_s3(s3_client, local_path, s3_key):
                print(f"  ✓ Success")
                success_count += 1
            else:
                print(f"  ✗ Failed")

    return success_count, total_count


def main():
    """Main function to upload all parsed and options files to S3."""
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parsed_data_dir = os.path.join(script_dir, 'parsed_data')
    options_dir = os.path.join(script_dir, 'options')

    # Create S3 client
    s3_client = get_s3_client()

    total_success = 0
    total_files = 0

    # Upload parsed_data files
    print("=" * 60)
    print("Uploading parsed_data files to S3")
    print("=" * 60)
    success, count = upload_directory(s3_client, parsed_data_dir, 'parsed_data')
    total_success += success
    total_files += count

    # Upload options files
    print("\n" + "=" * 60)
    print("Uploading options files to S3")
    print("=" * 60)
    success, count = upload_directory(s3_client, options_dir, 'options')
    total_success += success
    total_files += count

    # Summary
    print("\n" + "=" * 60)
    print("UPLOAD SUMMARY")
    print("=" * 60)
    print(f"Total files uploaded: {total_success}/{total_files}")
    print(f"Status: {'✓ All successful' if total_success == total_files else '✗ Some failed'}")
    print("=" * 60)

    return total_success == total_files


if __name__ == '__main__':
    main()