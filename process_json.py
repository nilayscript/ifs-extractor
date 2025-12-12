import json

# Read the original JSON
with open('customerorder.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

id_counter = 1
options = {
    "api": data["api_info"]["title"],
    "entities": []
}

def process_array(arr, http_method, parent_entity_name=None):
    """Add ID and method to objects in an array and collect entity info"""
    global id_counter

    if not arr or not isinstance(arr, list):
        return []

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
                        nested_info = process_array(item["nested_entities"][nested_method], nested_method, item.get("name"))
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

    return entity_info

# Process each HTTP method at root level
for method in ["GET", "POST", "PATCH", "PUT", "DELETE"]:
    if data.get(method) and len(data[method]) > 0:
        method_entities = process_array(data[method], method)
        options["entities"].append({
            "method": method,
            "items": method_entities
        })

# Write the modified JSON back
with open('customerorder.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, indent=2)
print('Updated customerorder.json with IDs')

# Write the options JSON
with open('customerorder-options.json', 'w', encoding='utf-8') as f:
    json.dump(options, f, indent=2)
print('Created customerorder-options.json')

print(f'Total IDs assigned: {id_counter - 1}')
