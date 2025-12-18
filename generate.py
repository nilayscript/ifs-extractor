"""
Batch processor for OpenAPI JSON files.
Parses all JSON files in data/ folder and generates options files.

Usage: python generate.py
"""

import json
import os
import glob
from parse_openapi import parse_openapi_spec, generate_simplified_output


# Directories
DATA_DIR = 'data'
PARSED_DATA_DIR = 'parsed_data'
OPTIONS_DIR = 'options'


def ensure_directories():
    """Ensure output directories exist."""
    os.makedirs(PARSED_DATA_DIR, exist_ok=True)
    os.makedirs(OPTIONS_DIR, exist_ok=True)


def get_output_filename(input_filename: str) -> str:
    """Convert input filename to lowercase output filename.

    Example: CustomerOrderHandling.json -> customerorderhandling.json
    """
    base_name = os.path.splitext(os.path.basename(input_filename))[0]
    return base_name.lower() + '.json'


def print_parsing_stats(filename: str, parsed_data: dict, simplified_data: dict):
    """Print statistics after parsing a file."""
    summary = parsed_data.get('summary', {})

    print(f"\n{'='*60}")
    print(f"PARSED: {filename}")
    print(f"{'='*60}")
    print(f"API Title: {parsed_data['api_info'].get('title', 'N/A')}")
    print(f"\nMethods & API Count:")
    methods_count = summary.get('methods_count', {})
    for method, count in methods_count.items():
        print(f"  {method}: {count} APIs")

    print(f"\nEntities:")
    print(f"  Main Entities: {summary.get('total_entities', 0)}")
    print(f"  Nested Entities: {summary.get('total_nested_entities', 0)}")
    print(f"  Reference Entities: {summary.get('total_reference_entities', 0)}")
    print(f"  Actions: {summary.get('total_actions', 0)}")
    print(f"  Functions: {summary.get('total_functions', 0)}")
    print(f"  Total Endpoints: {summary.get('total_endpoints', 0)}")
    print(f"{'='*60}\n")


def process_options(input_file: str, output_file: str) -> dict:
    """Process a parsed JSON file and generate options file.

    Returns stats about the processing.
    """
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


def parse_all_files():
    """Parse all JSON files in data directory."""
    ensure_directories()

    # Find all JSON files in data directory
    json_files = glob.glob(os.path.join(DATA_DIR, '*.json'))

    if not json_files:
        print(f"No JSON files found in {DATA_DIR}/")
        return []

    print(f"\nFound {len(json_files)} JSON file(s) to parse\n")

    parsed_files = []

    for input_file in json_files:
        filename = os.path.basename(input_file)
        output_filename = get_output_filename(filename)
        output_file = os.path.join(PARSED_DATA_DIR, output_filename)

        print(f"Parsing: {filename}...")

        try:
            # Parse the OpenAPI spec
            parsed_data = parse_openapi_spec(input_file)

            # Generate simplified output
            simplified_data = generate_simplified_output(parsed_data)

            # Save simplified data to parsed_data folder
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(simplified_data, f, indent=2)

            print(f"  Saved to: {output_file}")

            # Print stats
            print_parsing_stats(filename, parsed_data, simplified_data)

            parsed_files.append({
                'input': input_file,
                'output': output_file,
                'filename': output_filename
            })

        except Exception as e:
            print(f"  ERROR parsing {filename}: {e}")
            continue

    return parsed_files


def generate_all_options():
    """Generate options files for all parsed JSON files."""
    # Find all JSON files in parsed_data directory
    json_files = glob.glob(os.path.join(PARSED_DATA_DIR, '*.json'))

    if not json_files:
        print(f"No JSON files found in {PARSED_DATA_DIR}/")
        return

    print(f"\n{'#'*60}")
    print("GENERATING OPTIONS FILES")
    print(f"{'#'*60}\n")

    for input_file in json_files:
        filename = os.path.basename(input_file)
        base_name = os.path.splitext(filename)[0]
        output_file = os.path.join(OPTIONS_DIR, f"{base_name}-options.json")

        print(f"Processing: {filename}...")

        try:
            stats = process_options(input_file, output_file)
            print(f"  Created: {output_file}")
            print(f"  API: {stats['api_name']}")
            print(f"  Total IDs assigned: {stats['total_ids']}\n")

        except Exception as e:
            print(f"  ERROR processing {filename}: {e}\n")
            continue


def main():
    """Main entry point."""
    print("\n" + "="*60)
    print("IFS API JSON GENERATOR")
    print("="*60)

    # Step 1: Parse all files in data/
    print("\nSTEP 1: Parsing OpenAPI JSON files from data/")
    print("-"*60)
    parsed_files = parse_all_files()

    if not parsed_files:
        print("No files were parsed. Exiting.")
        return

    # Step 2: Generate options for all parsed files
    print("\nSTEP 2: Generating options files")
    print("-"*60)
    generate_all_options()

    # Summary
    print("\n" + "="*60)
    print("GENERATION COMPLETE")
    print("="*60)
    print(f"Parsed files saved to: {PARSED_DATA_DIR}/")
    print(f"Options files saved to: {OPTIONS_DIR}/")
    print("\nYou can now run main.py to start the FastAPI server.")
    print("="*60 + "\n")


if __name__ == '__main__':
    main()
