"""
OpenAPI Parser for IFS CustomerOrderHandling API
Extracts APIs, entities, nested entities, and their metadata
"""

import json
from collections import defaultdict
from typing import Any


def load_openapi_spec(file_path: str) -> dict:
    """Load the OpenAPI specification from JSON file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def resolve_ref(spec: dict, ref: str) -> dict:
    """Resolve a $ref pointer to its actual schema."""
    if not ref or not ref.startswith('#/'):
        return {}

    parts = ref[2:].split('/')
    result = spec.get('openapi_spec', spec)

    for part in parts:
        if isinstance(result, dict) and part in result:
            result = result[part]
        else:
            return {}

    return result if isinstance(result, dict) else {}


def extract_schema_properties(spec: dict, schema: dict) -> dict:
    """Extract properties from a schema, resolving $refs."""
    if '$ref' in schema:
        schema = resolve_ref(spec, schema['$ref'])

    properties = {}
    if 'properties' in schema:
        for prop_name, prop_def in schema['properties'].items():
            prop_info = {
                'type': prop_def.get('type', 'unknown'),
                'description': prop_def.get('description', ''),
                'maxLength': prop_def.get('maxLength'),
                'format': prop_def.get('format'),
                'example': prop_def.get('example'),
            }

            # Handle $ref in property
            if '$ref' in prop_def:
                prop_info['$ref'] = prop_def['$ref']
                prop_info['type'] = 'enum/reference'

            # Remove None values
            properties[prop_name] = {k: v for k, v in prop_info.items() if v is not None}

    return {
        'required': schema.get('required', []),
        'properties': properties
    }


def get_payload_schema(spec: dict, request_body: dict) -> dict:
    """Extract payload schema from requestBody."""
    if not request_body:
        return None

    # Handle $ref
    if '$ref' in request_body:
        ref_path = request_body['$ref']
        request_body = resolve_ref(spec, ref_path)
        if not request_body:
            return {'$ref': ref_path}

    content = request_body.get('content', {})
    json_content = content.get('application/json', {})
    schema = json_content.get('schema', {})

    if '$ref' in schema:
        schema_ref = schema['$ref']
        resolved = resolve_ref(spec, schema_ref)
        return {
            'schema_ref': schema_ref,
            'schema_name': schema_ref.split('/')[-1] if schema_ref else None,
            **extract_schema_properties(spec, resolved)
        }

    return extract_schema_properties(spec, schema)


def get_response_schema(spec: dict, responses: dict, include_properties: bool = True) -> dict:
    """Extract response schema from responses with full property details."""
    result = {}

    for status_code, response in responses.items():
        response_info = {
            'description': '',
            'schema_ref': None,
            'schema_name': None,
        }

        # Handle $ref in response
        if '$ref' in response:
            ref_path = response['$ref']
            response_info['response_ref'] = ref_path
            response = resolve_ref(spec, ref_path)

        response_info['description'] = response.get('description', '')

        # Get schema from content
        content = response.get('content', {})
        json_content = content.get('application/json', {})
        schema = json_content.get('schema', {})

        if schema:
            # Handle $ref in schema
            if '$ref' in schema:
                schema_ref = schema['$ref']
                response_info['schema_ref'] = schema_ref
                response_info['schema_name'] = schema_ref.split('/')[-1] if schema_ref else None

                if include_properties:
                    resolved_schema = resolve_ref(spec, schema_ref)
                    if resolved_schema:
                        props = extract_schema_properties(spec, resolved_schema)
                        response_info['properties'] = props.get('properties', {})

            # Handle inline schema (for array responses)
            elif 'properties' in schema:
                if 'value' in schema['properties']:
                    value_schema = schema['properties']['value']
                    if 'items' in value_schema and '$ref' in value_schema['items']:
                        items_ref = value_schema['items']['$ref']
                        response_info['schema_ref'] = items_ref
                        response_info['schema_name'] = items_ref.split('/')[-1]
                        response_info['is_array'] = True

                        if include_properties:
                            resolved_schema = resolve_ref(spec, items_ref)
                            if resolved_schema:
                                props = extract_schema_properties(spec, resolved_schema)
                                response_info['properties'] = props.get('properties', {})

        result[status_code] = response_info

    return result


def extract_parameters(parameters: list) -> dict:
    """Extract and categorize parameters."""
    result = {
        'path_params': [],
        'query_params': [],
        'header_params': [],
        'filters': []
    }

    for param in parameters:
        param_info = {
            'name': param.get('name'),
            'description': param.get('description', ''),
            'required': param.get('required', False),
            'type': param.get('schema', {}).get('type', 'string'),
        }

        # Add enum values if present
        schema = param.get('schema', {})
        if 'enum' in schema:
            param_info['enum'] = schema['enum'][:10]  # Limit enum values
            param_info['enum_count'] = len(schema['enum'])

        if 'items' in schema and 'enum' in schema['items']:
            param_info['enum'] = schema['items']['enum'][:10]
            param_info['enum_count'] = len(schema['items']['enum'])

        location = param.get('in', 'query')

        if location == 'path':
            result['path_params'].append(param_info)
        elif location == 'header':
            result['header_params'].append(param_info)
        elif location == 'query':
            result['query_params'].append(param_info)
            # Identify OData filter params
            if param_info['name'] in ['$filter', '$select', '$orderby', '$top', '$skip', '$count']:
                result['filters'].append(param_info)

    return result


def identify_entity_type(path: str) -> dict:
    """Identify if path is entity, nested entity, action, or function."""
    result = {
        'is_reference': path.startswith('/Reference_'),
        'is_action': False,
        'is_function': False,
        'is_nested': False,
        'parent_entity': None,
        'entity_name': None
    }

    # Check for actions (contain entity method calls)
    if '/IfsApp.' in path and not path.endswith(')'):
        result['is_action'] = True
        # Extract action name
        action_part = path.split('/IfsApp.')[-1]
        result['action_name'] = action_part

    # Check for functions (end with parentheses with parameters)
    if '(' in path and path.endswith(')') and '/IfsApp.' in path:
        result['is_function'] = True

    # Check for nested entities
    parts = path.split('/')
    if len(parts) > 2:
        # Check if it's accessing a child collection
        for i, part in enumerate(parts[1:], 1):
            if '(' in part and i < len(parts) - 1:
                result['is_nested'] = True
                result['parent_entity'] = parts[1].split('(')[0]
                result['entity_name'] = parts[-1].split('(')[0]
                break

    # Extract main entity name
    if not result['entity_name']:
        main_part = parts[1] if len(parts) > 1 else ''
        result['entity_name'] = main_part.split('(')[0]

    return result


def parse_openapi_spec(file_path: str) -> dict:
    """Main function to parse the OpenAPI spec and extract structured data."""

    data = load_openapi_spec(file_path)
    spec = data.get('openapi_spec', data)
    basic_info = data.get('basic_info', {})

    result = {
        'api_info': {
            'title': spec.get('info', {}).get('title', ''),
            'description': basic_info.get('description', spec.get('info', {}).get('description', '')),
            'version': spec.get('info', {}).get('version', ''),
            'base_url': spec.get('servers', [{}])[0].get('url', ''),
            'component': basic_info.get('component', ''),
            'api_type': basic_info.get('api_type', ''),
        },
        'authentication': [],
        'entities': {},
        'nested_entities': {},
        'reference_entities': {},
        'actions': {},
        'functions': {},
        'summary': {
            'total_endpoints': 0,
            'total_entities': 0,
            'total_nested_entities': 0,
            'total_reference_entities': 0,
            'total_actions': 0,
            'total_functions': 0,
            'methods_count': defaultdict(int)
        }
    }

    # Extract authentication
    security = spec.get('security', [])
    for sec in security:
        result['authentication'].extend(list(sec.keys()))

    # Process paths
    paths = spec.get('paths', {})

    for path, path_item in paths.items():
        entity_info = identify_entity_type(path)

        # Get path-level parameters
        path_params = path_item.get('parameters', [])

        # Process each HTTP method
        for method in ['get', 'post', 'put', 'patch', 'delete']:
            if method not in path_item:
                continue

            operation = path_item[method]
            result['summary']['total_endpoints'] += 1
            result['summary']['methods_count'][method.upper()] += 1

            # Build API entry
            api_entry = {
                'path': path,
                'method': method.upper(),
                'summary': operation.get('summary', ''),
                'description': operation.get('description', ''),
                'tags': operation.get('tags', []),
                'parameters': extract_parameters(path_params + operation.get('parameters', [])),
                'request_body': get_payload_schema(spec, operation.get('requestBody')),
                'responses': get_response_schema(spec, operation.get('responses', {})),
            }

            # Create unique key for API
            api_key = f"{method.upper()} {path}"

            # Categorize the API
            if entity_info['is_action']:
                if entity_info['entity_name'] not in result['actions']:
                    result['actions'][entity_info['entity_name']] = []
                result['actions'][entity_info['entity_name']].append(api_entry)
                result['summary']['total_actions'] += 1

            elif entity_info['is_function']:
                if entity_info['entity_name'] not in result['functions']:
                    result['functions'][entity_info['entity_name']] = []
                result['functions'][entity_info['entity_name']].append(api_entry)
                result['summary']['total_functions'] += 1

            elif entity_info['is_reference']:
                entity_name = entity_info['entity_name']
                if entity_name not in result['reference_entities']:
                    result['reference_entities'][entity_name] = {
                        'name': entity_name,
                        'description': '',
                        'apis': []
                    }
                result['reference_entities'][entity_name]['apis'].append(api_entry)

            elif entity_info['is_nested']:
                parent = entity_info['parent_entity']
                child = entity_info['entity_name']
                key = f"{parent}/{child}"

                if key not in result['nested_entities']:
                    result['nested_entities'][key] = {
                        'parent_entity': parent,
                        'nested_entity': child,
                        'full_path_pattern': path,
                        'apis': []
                    }
                result['nested_entities'][key]['apis'].append(api_entry)

            else:
                entity_name = entity_info['entity_name']
                if entity_name not in result['entities']:
                    result['entities'][entity_name] = {
                        'name': entity_name,
                        'description': '',
                        'is_reference': entity_info['is_reference'],
                        'apis': []
                    }
                result['entities'][entity_name]['apis'].append(api_entry)

    # Update summary counts
    result['summary']['total_entities'] = len(result['entities'])
    result['summary']['total_nested_entities'] = len(result['nested_entities'])
    result['summary']['total_reference_entities'] = len(result['reference_entities'])
    result['summary']['methods_count'] = dict(result['summary']['methods_count'])

    # Extract all schemas
    result['schemas'] = extract_all_schemas(data)
    result['summary']['total_schemas'] = len(result['schemas'])

    return result


def extract_all_schemas(spec: dict) -> dict:
    """Extract all schemas from the OpenAPI spec."""
    schemas = {}
    openapi_spec = spec.get('openapi_spec', spec)
    components = openapi_spec.get('components', {})
    schema_defs = components.get('schemas', {})

    for schema_name, schema_def in schema_defs.items():
        # Skip enum types and simple types
        if schema_def.get('type') == 'string' and 'enum' in schema_def:
            schemas[schema_name] = {
                'type': 'enum',
                'values': schema_def.get('enum', [])
            }
        elif 'properties' in schema_def:
            props = {}
            for prop_name, prop_def in schema_def.get('properties', {}).items():
                prop_info = {
                    'type': prop_def.get('type', 'unknown'),
                }
                if 'maxLength' in prop_def:
                    prop_info['maxLength'] = prop_def['maxLength']
                if 'format' in prop_def:
                    prop_info['format'] = prop_def['format']
                if 'maximum' in prop_def:
                    prop_info['maximum'] = prop_def['maximum']
                if '$ref' in prop_def:
                    prop_info['$ref'] = prop_def['$ref']
                    prop_info['type'] = 'reference'
                if 'description' in prop_def and prop_def['description']:
                    prop_info['description'] = prop_def['description']

                props[prop_name] = prop_info

            schemas[schema_name] = {
                'type': 'object',
                'required': schema_def.get('required', []),
                'properties': props
            }

    return schemas


def generate_simplified_output(parsed_data: dict) -> dict:
    """Generate a simplified, form-builder-friendly output organized by HTTP method."""

    base_url = parsed_data['api_info'].get('base_url', '')
    schemas = parsed_data.get('schemas', {})

    simplified = {
        'api_info': {
            'title': parsed_data['api_info'].get('title', ''),
            'description': parsed_data['api_info'].get('description', ''),
            'base_url': base_url,
        },
        'GET': [],
        'POST': [],
        'PATCH': [],
        'PUT': [],
        'DELETE': [],
    }

    def get_schema_fields(schema_name: str) -> list:
        """Get list of field names from a schema."""
        if not schema_name or schema_name not in schemas:
            return []
        schema = schemas.get(schema_name, {})
        props = schema.get('properties', {})
        return list(props.keys())

    def get_schema_fields_with_types(schema_name: str) -> dict:
        """Get field properties including types from a schema."""
        if not schema_name or schema_name not in schemas:
            return {}
        schema = schemas.get(schema_name, {})
        return schema.get('properties', {})

    def get_schema_fields_with_required(schema_name: str) -> list:
        """Get list of fields with required info from a schema."""
        if not schema_name or schema_name not in schemas:
            return []
        schema = schemas.get(schema_name, {})
        props = schema.get('properties', {})
        required_fields = set(schema.get('required', []))

        result = []
        for field_name in props.keys():
            result.append({
                'key': field_name,
                'required': field_name in required_fields
            })
        return result

    def get_response_fields(api: dict) -> list:
        """Extract response fields with data types from API responses."""
        if not api.get('responses'):
            return []
        for code in ['200', '201']:
            if code in api['responses']:
                resp = api['responses'][code]
                if resp.get('properties'):
                    # Properties already have type info
                    props = resp['properties']
                    result = []
                    for field_name, field_info in props.items():
                        field_entry = {'key': field_name}
                        if isinstance(field_info, dict):
                            if 'type' in field_info:
                                field_entry['type'] = field_info['type']
                            if 'maxLength' in field_info:
                                field_entry['maxLength'] = field_info['maxLength']
                            if 'format' in field_info:
                                field_entry['format'] = field_info['format']
                            if 'maximum' in field_info:
                                field_entry['maximum'] = field_info['maximum']
                        result.append(field_entry)
                    return result
                # Try to get from schema name
                schema_name = resp.get('schema_name')
                if schema_name:
                    props = get_schema_fields_with_types(schema_name)
                    result = []
                    for field_name, field_info in props.items():
                        field_entry = {'key': field_name}
                        if isinstance(field_info, dict):
                            if 'type' in field_info:
                                field_entry['type'] = field_info['type']
                            if 'maxLength' in field_info:
                                field_entry['maxLength'] = field_info['maxLength']
                            if 'format' in field_info:
                                field_entry['format'] = field_info['format']
                            if 'maximum' in field_info:
                                field_entry['maximum'] = field_info['maximum']
                        result.append(field_entry)
                    return result
        return []

    def get_payload_fields(api: dict) -> list:
        """Extract payload fields with required info and data types from API request body."""
        if not api.get('request_body'):
            return []
        req = api['request_body']
        schema_name = req.get('schema_name')

        # Get required fields from schema
        required_fields = set()
        if schema_name and schema_name in schemas:
            required_fields = set(schemas[schema_name].get('required', []))

        # Get properties with type info
        props_dict = {}
        if req.get('properties'):
            props_dict = req['properties']
        elif schema_name:
            props_dict = get_schema_fields_with_types(schema_name)

        # Build result with required info and data types
        result = []
        for field_name, field_info in props_dict.items():
            field_entry = {
                'key': field_name,
                'required': field_name in required_fields
            }
            if isinstance(field_info, dict):
                if 'type' in field_info:
                    field_entry['type'] = field_info['type']
                if 'maxLength' in field_info:
                    field_entry['maxLength'] = field_info['maxLength']
                if 'format' in field_info:
                    field_entry['format'] = field_info['format']
                if 'maximum' in field_info:
                    field_entry['maximum'] = field_info['maximum']
            result.append(field_entry)
        return result

    def camel_to_readable(name: str) -> str:
        """Convert camelCase or PascalCase to readable text.

        Examples:
            CalculateOrderDiscount -> Calculate Order Discount
            SetCancelled -> Set Cancelled
            CustomerOrderSet -> Customer Order Set
        """
        import re
        # Insert space before uppercase letters (but not at start)
        result = re.sub(r'(?<!^)(?=[A-Z])', ' ', name)
        return result

    def extract_action_name_from_path(path: str) -> str:
        """Extract action/function name from IfsApp path.

        Pattern: /EntitySet(...)/IfsApp.Module.Entity_ActionName
        Returns: ActionName (e.g., CalculateOrderDiscount, SetCancelled)
        Returns None if not an action/function URL.
        """
        if '/IfsApp.' not in path:
            return None

        # Get the last part after IfsApp.
        # e.g., "CustomerOrderHandling.CustomerOrder_CalculateOrderDiscount"
        action_part = path.split('/IfsApp.')[-1]

        # Remove trailing parentheses for functions like _Default()
        if '(' in action_part:
            action_part = action_part.split('(')[0]

        # Extract action name after the last underscore
        # Pattern: CustomerOrderHandling.CustomerOrder_CalculateOrderDiscount -> CalculateOrderDiscount
        if '_' in action_part:
            action_name = action_part.split('_')[-1]
            return action_name

        return None

    def extract_entity_name_from_path(path: str) -> str:
        """Extract entity name from URL path (without any suffix)."""
        # Remove leading slash
        path = path.lstrip('/')

        # Get the first segment (entity name)
        if '(' in path:
            # Has parameters like /CustomerOrderSet(OrderNo='{OrderNo}')
            entity_name = path.split('(')[0]
        elif '/' in path:
            # Nested path like /CustomerOrderSet/OrderLinesArray
            entity_name = path.split('/')[0]
        else:
            entity_name = path

        return entity_name

    def extract_nested_resource_from_path(path: str) -> str:
        """Extract nested resource name from URL path if present.

        Detects patterns like:
        - /EntitySet(params)/NestedResource -> returns 'NestedResource'
        - /EntitySet(params)/IfsApp.Module.Action -> returns None (action, not nested resource)
        - /EntitySet(params) -> returns None (no nested resource)
        """
        # Remove leading slash
        path = path.lstrip('/')

        # Check if there's content after the closing parenthesis
        if '(' in path and ')' in path:
            after_params = path.split(')')[-1]
            if after_params.startswith('/'):
                nested_part = after_params[1:]  # Remove leading slash
                # Skip if it's an IfsApp action/function
                if nested_part.startswith('IfsApp.'):
                    return None
                # Return the nested resource name (before any further path segments)
                if '/' in nested_part:
                    nested_part = nested_part.split('/')[0]
                if nested_part:
                    return nested_part
        return None

    def get_primary_key(path_params: list) -> str:
        """Get the primary key from path parameters.

        Returns the first path param as primary key.
        Common primary keys: OrderNo, Objkey, Contract, etc.
        """
        if not path_params:
            return None
        return path_params[0].get('key', path_params[0].get('name', ''))

    def generate_api_name(method: str, entity_name: str, path_params: list, action_name: str = None, nested_resource: str = None) -> str:
        """Generate a readable API name based on method, entity, and params.

        Naming convention:
        - GET (no params): List {EntityName}
        - GET (with params): Get {EntityName} by {PrimaryKey}
        - GET (with nested resource): Get {EntityName} {NestedResource} by {PrimaryKey}
        - POST (no params): Create {EntityName}
        - POST (with params - action): {ActionName} (readable)
        - PATCH (with params): Update {EntityName} by {PrimaryKey}
        - DELETE (with params): Delete {EntityName} by {PrimaryKey}
        - Functions/Actions: Convert to readable text, add "by {Key}" if has params
        """
        has_params = path_params and len(path_params) > 0
        primary_key = get_primary_key(path_params) if has_params else None
        readable_entity = camel_to_readable(entity_name)

        # Add nested resource to entity name if present
        if nested_resource:
            readable_nested = camel_to_readable(nested_resource)
            readable_entity = f"{readable_entity} {readable_nested}"

        # If it's an action/function, use the action name
        if action_name:
            readable_action = camel_to_readable(action_name)
            # Add "by {PrimaryKey}" to distinguish item-level from collection-level actions
            if has_params:
                return f"{readable_action} by {primary_key}"
            return readable_action

        # Generate name based on method
        if method == 'GET':
            if has_params:
                return f"Get {readable_entity} by {primary_key}"
            else:
                return f"List {readable_entity}"
        elif method == 'POST':
            if has_params:
                # POST with params but no action name - unusual, use entity name
                return f"Create {readable_entity}"
            else:
                return f"Create {readable_entity}"
        elif method == 'PATCH':
            if has_params:
                return f"Update {readable_entity} by {primary_key}"
            else:
                return f"Update {readable_entity}"
        elif method == 'PUT':
            if has_params:
                return f"Replace {readable_entity} by {primary_key}"
            else:
                return f"Replace {readable_entity}"
        elif method == 'DELETE':
            if has_params:
                return f"Delete {readable_entity} by {primary_key}"
            else:
                return f"Delete {readable_entity}"

        # Fallback
        return readable_entity

    def process_api(api: dict, include_nested: bool = False, nested_list: list = None, entity_name_override: str = None) -> dict:
        """Process a single API into simplified format."""
        # Build full URL
        path = api['path']
        full_url = f"{base_url}{path}"

        # Get filters
        filters = [p['name'] for p in api['parameters'].get('filters', [])]

        # Extract path parameters with data types
        path_params_list = api['parameters'].get('path_params', [])
        path_params_with_types = None
        if path_params_list:
            path_params_with_types = []
            for p in path_params_list:
                param_entry = {
                    'key': p['name'],
                    'type': p.get('type', 'string')
                }
                if 'enum' in p:
                    param_entry['enum'] = p['enum']
                path_params_with_types.append(param_entry)

        # Get payload and response fields
        payload_fields = get_payload_fields(api)
        response_fields = get_response_fields(api)

        # Extract name using new naming convention
        if entity_name_override:
            name = entity_name_override
        else:
            # Check if this is an action/function URL (contains /IfsApp.)
            action_name = extract_action_name_from_path(path)
            entity_name = extract_entity_name_from_path(path)
            nested_resource = extract_nested_resource_from_path(path)
            method = api['method']

            # Generate readable name based on method, entity, params, action, and nested resource
            name = generate_api_name(method, entity_name, path_params_with_types, action_name, nested_resource)

        result = {
            'id': None,  # Will be assigned after sorting
            'name': name,
            'method': api['method'],  # HTTP method (GET, POST, etc.)
            'description': api['summary'],  # Move summary to description
            'url': full_url,
            'path_params': path_params_with_types,  # Path parameters with data types
            'filters': filters if filters else None,
            'payload_fields': payload_fields if payload_fields else None,
            'response_fields': response_fields if response_fields else None,
        }

        # Add nested entities if provided
        if include_nested and nested_list:
            result['nested_entities'] = nested_list

        return result

    # Build parent -> nested entities mapping organized by HTTP method
    nested_entities_data = parsed_data.get('nested_entities', {})
    parent_to_nested = {}  # parent_entity -> {GET: [], POST: [], ...}

    for key, nested_data in nested_entities_data.items():
        parent = nested_data.get('parent_entity', '')
        if parent not in parent_to_nested:
            parent_to_nested[parent] = {
                'GET': [],
                'POST': [],
                'PATCH': [],
                'PUT': [],
                'DELETE': []
            }

        # Process each nested entity API
        for api in nested_data.get('apis', []):
            method = api['method']
            nested_entity_name = nested_data.get('nested_entity', '')
            api_path = api['path']

            # Extract path parameters with data types
            path_params_list = api['parameters'].get('path_params', [])
            path_params_with_types = None
            if path_params_list:
                path_params_with_types = []
                for p in path_params_list:
                    param_entry = {
                        'key': p['name'],
                        'type': p.get('type', 'string')
                    }
                    if 'enum' in p:
                        param_entry['enum'] = p['enum']
                    path_params_with_types.append(param_entry)

            # Generate name using new naming convention
            action_name = extract_action_name_from_path(api_path)
            nested_resource = extract_nested_resource_from_path(api_path)
            name = generate_api_name(method, nested_entity_name, path_params_with_types, action_name, nested_resource)

            nested_api_info = {
                'id': None,  # Will be assigned after sorting
                'name': name,
                'method': method,  # HTTP method (GET, POST, etc.)
                'description': api['summary'],  # Move summary to description
                'url': f"{base_url}{api_path}",
                'path_params': path_params_with_types,  # Path parameters with data types
                'filters': [p['name'] for p in api['parameters'].get('filters', [])] or None,
                'payload_fields': get_payload_fields(api) or None,
                'response_fields': get_response_fields(api) or None,
            }
            if method in parent_to_nested[parent]:
                parent_to_nested[parent][method].append(nested_api_info)

    # Process main entities
    entities_data = parsed_data.get('entities', {})
    for entity_name, entity_data in entities_data.items():
        apis = entity_data.get('apis', [])
        # Get nested entities for this parent (organized by method)
        nested_for_entity = parent_to_nested.get(entity_name, None)

        # Only include nested_entities if there's at least one API in any method
        if nested_for_entity:
            has_nested = any(len(v) > 0 for v in nested_for_entity.values())
            if not has_nested:
                nested_for_entity = None

        for api in apis:
            method = api['method']
            if method in simplified:
                simplified[method].append(
                    process_api(api, include_nested=True, nested_list=nested_for_entity)
                )

    # Process reference entities (no nested entities for these)
    reference_entities_data = parsed_data.get('reference_entities', {})
    for entity_name, entity_data in reference_entities_data.items():
        apis = entity_data.get('apis', [])
        for api in apis:
            method = api['method']
            if method in simplified:
                simplified[method].append(process_api(api))

    # Process actions and functions (list structure)
    action_sources = [
        parsed_data.get('actions', {}),
        parsed_data.get('functions', {}),
    ]

    for source in action_sources:
        for action_list in source.values():
            # action_list is a list of APIs
            if isinstance(action_list, list):
                for api in action_list:
                    method = api['method']
                    if method in simplified:
                        simplified[method].append(process_api(api))

    def resolve_duplicate_names(apis: list) -> list:
        """Resolve duplicate names by adding distinguishing info from URL path."""
        from collections import Counter
        import re

        # Count occurrences of each name
        name_counts = Counter(api['name'] for api in apis)
        duplicates = {name for name, count in name_counts.items() if count > 1}

        if not duplicates:
            return apis

        # First pass: try to resolve with entity prefix
        for api in apis:
            if api['name'] in duplicates:
                url = api.get('url', '')
                # Extract entity prefix from URL path
                # Pattern: .../EntitySet(...)/IfsApp... or .../EntitySet/...
                path_part = url.split('.svc/')[-1] if '.svc/' in url else url

                # Get just the first entity name (before any parentheses or /IfsApp)
                entity_prefix = re.split(r'[(/]', path_part)[0]

                # Remove 'Set' suffix if present for cleaner names
                if entity_prefix.endswith('Set'):
                    entity_prefix = entity_prefix[:-3]

                # Convert entity prefix to readable format
                readable_prefix = camel_to_readable(entity_prefix)

                # Create unique name: ReadablePrefix - ActionName
                api['name'] = f"{readable_prefix} - {api['name']}"

        # Second pass: check if duplicates remain (same entity, different params)
        name_counts = Counter(api['name'] for api in apis)
        remaining_duplicates = {name for name, count in name_counts.items() if count > 1}

        if remaining_duplicates:
            for api in apis:
                if api['name'] in remaining_duplicates:
                    # Use second path param to distinguish if available
                    path_params = api.get('path_params', [])
                    if path_params and len(path_params) > 1:
                        second_key = path_params[1].get('key', '')
                        if second_key:
                            api['name'] = f"{api['name']}, {second_key}"

        return apis

    # Resolve duplicate names within each HTTP method
    for method in ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']:
        simplified[method] = resolve_duplicate_names(simplified[method])

    # Sort all method arrays alphabetically by name for binary search
    for method in ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']:
        simplified[method] = sorted(simplified[method], key=lambda x: x['name'].lower())

        # Also sort nested_entities arrays within each API
        for api in simplified[method]:
            if api.get('nested_entities'):
                for nested_method in ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']:
                    if api['nested_entities'].get(nested_method):
                        # Resolve duplicates in nested entities too
                        api['nested_entities'][nested_method] = resolve_duplicate_names(
                            api['nested_entities'][nested_method]
                        )
                        api['nested_entities'][nested_method] = sorted(
                            api['nested_entities'][nested_method],
                            key=lambda x: x['name'].lower()
                        )

    # Assign sequential IDs to all APIs (global counter across all methods)
    global_id = 1
    for method in ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']:
        for api in simplified[method]:
            api['id'] = global_id
            global_id += 1

            # Also assign IDs to nested entities
            if api.get('nested_entities'):
                for nested_method in ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']:
                    if api['nested_entities'].get(nested_method):
                        for nested_api in api['nested_entities'][nested_method]:
                            nested_api['id'] = global_id
                            global_id += 1

    return simplified


def main():
    """Main entry point."""
    import sys
    import os

    # Default file path
    file_path = '/Applications/digisigns/ifs-data/CustomerOrderHandling.json'

    if len(sys.argv) > 1:
        file_path = sys.argv[1]

    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        sys.exit(1)

    print(f"Parsing OpenAPI spec: {file_path}")
    print("This may take a moment for large files...")

    # Parse the spec
    parsed_data = parse_openapi_spec(file_path)

    # Generate simplified output
    simplified_data = generate_simplified_output(parsed_data)

    # Output file paths
    base_name = os.path.splitext(file_path)[0]
    full_output_path = f"{base_name}_parsed_full.json"
    simplified_output_path = f"{base_name}_parsed_summary.json"

    # Write full output
    with open(full_output_path, 'w', encoding='utf-8') as f:
        json.dump(parsed_data, f, indent=2, ensure_ascii=False)
    print(f"Full parsed output written to: {full_output_path}")

    # Write simplified output
    with open(simplified_output_path, 'w', encoding='utf-8') as f:
        json.dump(simplified_data, f, indent=2, ensure_ascii=False)
    print(f"Simplified output written to: {simplified_output_path}")

    # Print summary
    print("\n" + "="*60)
    print("PARSING SUMMARY")
    print("="*60)
    print(f"API Title: {parsed_data['api_info']['title']}")
    print(f"Base URL: {parsed_data['api_info']['base_url']}")
    print(f"\nTotal Endpoints: {parsed_data['summary']['total_endpoints']}")
    print(f"Main Entities: {parsed_data['summary']['total_entities']}")
    print(f"Nested Entities: {parsed_data['summary']['total_nested_entities']}")
    print(f"Reference Entities: {parsed_data['summary']['total_reference_entities']}")
    print(f"Actions: {parsed_data['summary']['total_actions']}")
    print(f"Functions: {parsed_data['summary']['total_functions']}")
    print(f"\nMethods breakdown:")
    for method, count in parsed_data['summary']['methods_count'].items():
        print(f"  {method}: {count}")

    return parsed_data, simplified_data


if __name__ == '__main__':
    main()
