"""
IFS API Data Generator

Complete pipeline to:
1. Parse OpenAPI JSON files from data/ folder
2. Generate options files for UI

Usage: python generate.py
"""

import json
import os
import glob
# import boto3
# from botocore.exceptions import ClientError
from parse_openapi1 import parse_openapi_spec, generate_simplified_output


# Directories
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, 'data')
PARSED_DATA_DIR = os.path.join(SCRIPT_DIR, 'parsed_data')
OPTIONS_DIR = os.path.join(SCRIPT_DIR, 'options')


def ensure_directories():
    """Ensure output directories exist."""
    os.makedirs(PARSED_DATA_DIR, exist_ok=True)
    os.makedirs(OPTIONS_DIR, exist_ok=True)


def get_output_filename(input_filename: str) -> str:
    """Convert input filename to lowercase output filename."""
    base_name = os.path.splitext(os.path.basename(input_filename))[0]
    return base_name.lower() + '.json'


def process_options(input_file: str, output_file: str) -> dict:
    """Process a parsed JSON file and generate options file."""
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    id_counter = 1
    options = {
        "api": data["api_info"]["title"],
        "entities": []
    }

    def process_array(arr, http_method):
        """Add ID and method to objects in an array and collect entity info."""
        nonlocal id_counter

        if not arr or not isinstance(arr, list):
            return []

        entity_info = []

        for item in arr:
            if isinstance(item, dict):
                item["id"] = id_counter
                item["method"] = http_method
                id_counter += 1

                entity_data = {
                    "name": item.get("name", ""),
                    "id": item["id"]
                }

                if "nested_entities" in item:
                    entity_data["nested_entities"] = []

                    for nested_method in ["GET", "POST", "PATCH", "PUT", "DELETE"]:
                        if item["nested_entities"].get(nested_method) and len(item["nested_entities"][nested_method]) > 0:
                            nested_info = process_array(item["nested_entities"][nested_method], nested_method)
                            for nested in nested_info:
                                entity_data["nested_entities"].append({
                                    "method": nested_method,
                                    "name": nested["name"],
                                    "id": nested["id"]
                                })

                    if len(entity_data["nested_entities"]) == 0:
                        del entity_data["nested_entities"]

                entity_info.append(entity_data)

        return entity_info

    # Process each HTTP method at root level
    for method in ["GET", "POST", "PATCH", "PUT", "DELETE"]:
        if data.get(method) and len(data[method]) > 0:
            method_entities = process_array(data[method], method)
            options["entities"].append({
                "method": method,
                "items": method_entities
            })

    # Write the modified data JSON back (with IDs added)
    with open(input_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

    # Write the options JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(options, f, indent=2)

    return {
        'total_ids': id_counter - 1,
        'api_name': options['api']
    }


# def get_s3_client():
#     """Create and return an S3 client."""
#     return boto3.client(
#         's3',
#         aws_access_key_id=S3_ACCESS_KEY,
#         aws_secret_access_key=S3_SECRET_KEY,
#         region_name=S3_REGION
#     )


# def upload_file_to_s3(s3_client, local_path: str, s3_key: str) -> bool:
#     """Upload a file to S3 bucket."""
#     try:
#         s3_client.upload_file(
#             local_path,
#             S3_BUCKET,
#             s3_key,
#             ExtraArgs={'ContentType': 'application/json'}
#         )
#         return True
#     except (ClientError, FileNotFoundError) as e:
#         print(f"  Error uploading {local_path}: {e}")
#         return False


# def upload_directory_to_s3(s3_client, local_dir: str, s3_prefix: str) -> tuple:
#     """Upload all JSON files from a local directory to S3."""
#     success_count = 0
#     total_count = 0

#     if not os.path.exists(local_dir):
#         print(f"  Directory not found: {local_dir}")
#         return 0, 0

#     for file_name in sorted(os.listdir(local_dir)):
#         if file_name.endswith('.json') and not file_name.startswith('.'):
#             total_count += 1
#             local_path = os.path.join(local_dir, file_name)
#             s3_key = f"{s3_prefix}/{file_name}"
#             s3_url = f"{S3_BASE_URL}/{s3_key}"

#             if upload_file_to_s3(s3_client, local_path, s3_key):
#                 print(f"  ✓ {file_name} -> {s3_url}")
#                 success_count += 1
#             else:
#                 print(f"  ✗ {file_name} FAILED")

#     return success_count, total_count


def step1_parse_data():
    """Step 1: Parse all JSON files in data directory."""
    print("\n" + "=" * 60)
    print("STEP 1: Parsing OpenAPI JSON files")
    print("=" * 60)

    ensure_directories()

    json_files = glob.glob(os.path.join(DATA_DIR, '*.json'))

    if not json_files:
        print(f"No JSON files found in {DATA_DIR}/")
        return []

    print(f"Found {len(json_files)} file(s) to parse\n")

    parsed_files = []

    for input_file in sorted(json_files):
        filename = os.path.basename(input_file)
        output_filename = get_output_filename(filename)
        output_file = os.path.join(PARSED_DATA_DIR, output_filename)

        print(f"Parsing: {filename}")

        try:
            # Parse the OpenAPI spec
            parsed_data = parse_openapi_spec(input_file)

            # Generate simplified output
            simplified_data = generate_simplified_output(parsed_data)

            # Save simplified data to parsed_data folder
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(simplified_data, f, indent=2, ensure_ascii=False)

            summary = parsed_data.get('summary', {})
            print(f"  -> {output_filename}")
            print(f"     Endpoints: {summary.get('total_endpoints', 0)}, "
                  f"Entities: {summary.get('total_entities', 0)}, "
                  f"Nested: {summary.get('total_nested_entities', 0)}")

            parsed_files.append({
                'input': input_file,
                'output': output_file,
                'filename': output_filename
            })

        except Exception as e:
            print(f"  ERROR: {e}")
            continue

    print(f"\nParsed {len(parsed_files)} file(s) to {PARSED_DATA_DIR}/")
    return parsed_files


def step2_generate_options():
    """Step 2: Generate options files for all parsed JSON files."""
    print("\n" + "=" * 60)
    print("STEP 2: Generating options files")
    print("=" * 60)

    json_files = glob.glob(os.path.join(PARSED_DATA_DIR, '*.json'))

    if not json_files:
        print(f"No JSON files found in {PARSED_DATA_DIR}/")
        return []

    print(f"Found {len(json_files)} file(s) to process\n")

    options_files = []

    for input_file in sorted(json_files):
        filename = os.path.basename(input_file)
        base_name = os.path.splitext(filename)[0]
        output_file = os.path.join(OPTIONS_DIR, f"{base_name}-options.json")

        print(f"Processing: {filename}")

        try:
            stats = process_options(input_file, output_file)
            print(f"  -> {base_name}-options.json (IDs: {stats['total_ids']})")

            options_files.append({
                'input': input_file,
                'output': output_file,
                'total_ids': stats['total_ids']
            })

        except Exception as e:
            print(f"  ERROR: {e}")
            continue

    print(f"\nGenerated {len(options_files)} options file(s) to {OPTIONS_DIR}/")
    return options_files


# def step3_upload_to_s3():
#     """Step 3: Upload parsed_data and options to S3."""
#     print("\n" + "=" * 60)
#     print("STEP 3: Uploading to S3")
#     print("=" * 60)

#     try:
#         s3_client = get_s3_client()
#     except Exception as e:
#         print(f"Failed to create S3 client: {e}")
#         return False

#     total_success = 0
#     total_files = 0

#     # Upload parsed_data files
#     print(f"\nUploading parsed_data/")
#     success, count = upload_directory_to_s3(s3_client, PARSED_DATA_DIR, 'parsed_data')
#     total_success += success
#     total_files += count

#     # Upload options files
#     print(f"\nUploading options/")
#     success, count = upload_directory_to_s3(s3_client, OPTIONS_DIR, 'options')
#     total_success += success
#     total_files += count

#     print(f"\nUploaded {total_success}/{total_files} file(s) to S3")
#     return total_success == total_files


def main():
    """Main entry point."""
    print("\n" + "#" * 60)
    print("#" + " " * 18 + "IFS API GENERATOR" + " " * 19 + "#")
    print("#" * 60)

    # Step 1: Parse all files
    parsed_files = step1_parse_data()
    if not parsed_files:
        print("\nNo files were parsed. Exiting.")
        return

    # Step 2: Generate options
    options_files = step2_generate_options()
    if not options_files:
        print("\nNo options files were generated. Exiting.")
        return

    # Summary
    print("\n" + "=" * 60)
    print("GENERATION COMPLETE")
    print("=" * 60)
    print(f"Parsed data:  {PARSED_DATA_DIR}/ ({len(parsed_files)} files)")
    print(f"Options:      {OPTIONS_DIR}/ ({len(options_files)} files)")
    print("=" * 60 + "\n")


if __name__ == '__main__':
    main()
