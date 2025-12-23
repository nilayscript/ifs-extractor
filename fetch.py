"""
S3 Data Fetcher for IFS API - Creates API registry JSON from S3 buckets.
"""

import boto3
import json
import uuid
from datetime import datetime, timezone
from botocore.exceptions import ClientError, NoCredentialsError
from concurrent.futures import ThreadPoolExecutor, as_completed

# AWS Credentials
AWS_ACCESS_KEY_ID = '<ACCESS_KEY>'
AWS_SECRET_ACCESS_KEY = '<SECRET_ACCESS_KEY>'
AWS_REGION = '<REGION>'

S3_BUCKET = "ngage-ifs-api-ai-data"
S3_BASE_URL = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com"
PARSED_DATA_PREFIX = "parsed_data/"
OPTIONS_PREFIX = "options/"

OUTPUT_FILE = "api_registry.json"
BATCH_SIZE = 100


def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )


def list_s3_files(s3_client, prefix):
    """List all files in S3 bucket with given prefix."""
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('/'):
                    files.append(key)
    return files


def fetch_options_file(s3_client, key):
    """Fetch and parse a single options JSON file from S3."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        return data.get('api', None), None
    except Exception as e:
        return None, str(e)


def build_api_entry(s3_client, options_key):
    """Build a single API registry entry from an options file key."""
    api_name, error = fetch_options_file(s3_client, options_key)

    if error:
        return None, f"Error fetching {options_key}: {error}"

    if not api_name:
        return None, f"No 'api' field in {options_key}"

    filename = options_key.split('/')[-1]
    base_name = filename.replace('-options.json', '')

    options_url = f"{S3_BASE_URL}/{options_key}"
    parsed_data_url = f"{S3_BASE_URL}/parsed_data/{base_name}.json"

    entry = {
        "uuid": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "name": api_name,
        "options_url": options_url,
        "parsed_data_url": parsed_data_url
    }

    return entry, None


def process_batch(s3_client, batch_keys):
    """Process a batch of options keys."""
    results = []
    errors = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(build_api_entry, s3_client, key): key for key in batch_keys}

        for future in as_completed(futures):
            entry, error = future.result()
            if entry:
                results.append(entry)
            if error:
                errors.append(error)

    return results, errors


def main():
    print("=== Building API Registry from S3 ===\n")

    try:
        s3 = get_s3_client()
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f"Connected to S3 bucket: {S3_BUCKET}")
    except NoCredentialsError:
        print("ERROR: AWS credentials not found")
        return
    except ClientError as e:
        print(f"ERROR: {e}")
        return

    # List all options files
    print("Listing options files...")
    options_keys = list_s3_files(s3, OPTIONS_PREFIX)
    total_files = len(options_keys)
    print(f"Found {total_files} options files\n")

    # Process in batches
    registry = []
    all_errors = []
    processed = 0

    for i in range(0, total_files, BATCH_SIZE):
        batch = options_keys[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (total_files + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"Processing batch {batch_num}/{total_batches} ({len(batch)} files)...")

        results, errors = process_batch(s3, batch)
        registry.extend(results)
        all_errors.extend(errors)
        processed += len(batch)

        print(f"  Batch complete. Total processed: {processed}/{total_files}")

    # Sort by name
    registry.sort(key=lambda x: x['name'].lower())

    # Save to file
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(registry, f, indent=2)

    print(f"\n{'='*50}")
    print(f"=== COMPLETE ===")
    print(f"{'='*50}")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Total APIs processed: {processed}")
    print(f"Total APIs in registry: {len(registry)}")
    if all_errors:
        print(f"Total errors: {len(all_errors)}")
        print("\nFirst 5 errors:")
        for err in all_errors[:5]:
            print(f"  - {err}")


if __name__ == "__main__":
    main()
