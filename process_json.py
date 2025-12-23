"""
Process parsed JSON files and generate options files.
Reads from parsed_data/ and outputs to options/
"""

import json
import sys
import os


def process_array(arr, http_method, id_counter, parent_entity_name=None):
    """Add ID and method to objects in an array and collect entity info"""
    if not arr or not isinstance(arr, list):
        return [], id_counter

    entity_info = []

    for item in arr:
        if isinstance(item, dict):
            # Add id and method to the object
            item["id"] = id_counter
            item["method"] = http_method
            id_counter += 1

            entity_data = {
                "name": item.get("name", ""),
                "id": item["id"]
            }

            # Process nested_entities if they exist
            if "nested_entities" in item:
                entity_data["nested_entities"] = []

                for nested_method in ["GET", "POST", "PATCH", "PUT", "DELETE"]:
                    if item["nested_entities"].get(nested_method) and len(item["nested_entities"][nested_method]) > 0:
                        nested_info, id_counter = process_array(
                            item["nested_entities"][nested_method],
                            nested_method,
                            id_counter,
                            item.get("name")
                        )
                        for nested in nested_info:
                            entity_data["nested_entities"].append({
                                "method": nested_method,
                                "name": nested["name"],
                                "id": nested["id"]
                            })

                # Only keep nested_entities if there are any
                if len(entity_data["nested_entities"]) == 0:
                    del entity_data["nested_entities"]

            entity_info.append(entity_data)

    return entity_info, id_counter


def process_file(input_file, output_file):
    """Process a single parsed JSON file and generate options file."""
    # Read the original JSON
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    id_counter = 1
    options = {
        "api": data["api_info"]["title"],
        "entities": []
    }

    # Process each HTTP method at root level
    for method in ["GET", "POST", "PATCH", "PUT", "DELETE"]:
        if data.get(method) and len(data[method]) > 0:
            method_entities, id_counter = process_array(data[method], method, id_counter)
            options["entities"].append({
                "method": method,
                "items": method_entities
            })

    # Write the modified JSON back (with IDs added)
    with open(input_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)

    # Write the options JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(options, f, indent=2)

    return id_counter - 1


def main():
    """Main entry point."""
    # Get script directory for relative paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parsed_data_dir = os.path.join(script_dir, 'parsed_data')
    options_dir = os.path.join(script_dir, 'options')

    # Ensure options directory exists
    os.makedirs(options_dir, exist_ok=True)

    # If specific file provided, process only that file
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        if not os.path.exists(input_file):
            print(f"Error: File '{input_file}' not found")
            sys.exit(1)
        files_to_process = [input_file]
    else:
        # Process all JSON files in parsed_data directory
        if not os.path.exists(parsed_data_dir):
            print(f"Error: parsed_data directory not found: {parsed_data_dir}")
            sys.exit(1)
        files_to_process = [
            os.path.join(parsed_data_dir, f) for f in os.listdir(parsed_data_dir)
            if f.endswith('.json') and not f.startswith('.')
        ]

    if not files_to_process:
        print("No JSON files found to process.")
        sys.exit(1)

    print(f"Found {len(files_to_process)} file(s) to process\n")

    for input_file in files_to_process:
        # Generate output filename: options/<name>-options.json
        file_name = os.path.basename(input_file)
        base_name = os.path.splitext(file_name)[0]
        output_file = os.path.join(options_dir, f"{base_name}-options.json")

        print(f"Processing: {input_file}")
        total_ids = process_file(input_file, output_file)
        print(f"  Updated: {input_file}")
        print(f"  Created: {output_file}")
        print(f"  Total IDs: {total_ids}\n")

    print("=" * 60)
    print(f"Processing complete. {len(files_to_process)} file(s) processed.")
    print("=" * 60)


if __name__ == '__main__':
    main()
