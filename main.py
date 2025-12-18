from fastapi import FastAPI, Query, HTTPException, APIRouter, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, Any
import json
import httpx
import glob
import os

app = FastAPI(title="IFS API", description="API to fetch IFS entities")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Directories
OPTIONS_DIR = 'options'
PARSED_DATA_DIR = 'parsed_data'


def load_api_registry():
    """Dynamically load API registry from options folder."""
    registry = {}

    # Find all options files
    options_files = glob.glob(os.path.join(OPTIONS_DIR, '*-options.json'))

    for idx, options_file in enumerate(sorted(options_files), start=1):
        filename = os.path.basename(options_file)
        # Extract base name: customerorder-options.json -> customerorder
        base_name = filename.replace('-options.json', '')
        data_file = os.path.join(PARSED_DATA_DIR, f"{base_name}.json")

        # Check if corresponding data file exists
        if not os.path.exists(data_file):
            print(f"Warning: Data file not found for {filename}, skipping...")
            continue

        # Load options to get API name
        with open(options_file, 'r', encoding='utf-8') as f:
            options_data = json.load(f)

        registry[idx] = {
            "id": idx,
            "name": options_data.get('api', base_name),
            "options_file": options_file,
            "data_file": data_file
        }

    return registry


# Dynamically build API registry from options folder
api_registry = load_api_registry()

# Load all API data
api_data = {}
for api_id, api_info in api_registry.items():
    with open(api_info['options_file'], 'r', encoding='utf-8') as f:
        options_data = json.load(f)
    with open(api_info['data_file'], 'r', encoding='utf-8') as f:
        full_data = json.load(f)

    entity_by_id = {}

    def build_entity_lookup(arr, lookup_dict):
        if not arr:
            return
        for item in arr:
            if isinstance(item, dict) and 'id' in item:
                lookup_dict[item['id']] = item
                if 'nested_entities' in item:
                    for method in ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']:
                        if item['nested_entities'].get(method):
                            build_entity_lookup(item['nested_entities'][method], lookup_dict)

    for method in ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']:
        if full_data.get(method):
            build_entity_lookup(full_data[method], entity_by_id)

    api_data[api_id] = {
        "options": options_data,
        "full": full_data,
        "entity_by_id": entity_by_id
    }



@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}


@app.get("/api/list")
def get_api_list():
    """Get list of all available APIs"""
    return {
        "apis": [
            {"id": info["id"], "name": info["name"]}
            for info in api_registry.values()
        ]
    }


@app.get("/api/{api_id}/methods")
def get_methods_by_api(api_id: int):
    """Get all available HTTP methods for a specific API"""
    if api_id not in api_data:
        raise HTTPException(status_code=404, detail=f'API with ID {api_id} not found')

    api_options = api_data[api_id]["options"]
    methods = [entity['method'] for entity in api_options['entities']]
    return {
        'api_id': api_id,
        'api': api_options['api'],
        'methods': methods
    }


@app.get("/api/{api_id}/entities")
def get_all_entities_by_api(api_id: int):
    """Get all entities for a specific API"""
    if api_id not in api_data:
        raise HTTPException(status_code=404, detail=f'API with ID {api_id} not found')

    api_options = api_data[api_id]["options"]
    return {
        'api_id': api_id,
        'api': api_options['api'],
        'entities': api_options['entities']
    }


@app.get("/api/{api_id}/entities/search")
def search_entities(
    api_id: int,
    method: str = Query(..., description="HTTP method (GET, POST, PUT, PATCH, DELETE)"),
    q: Optional[str] = Query(None, description="Search query for entity name"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Results per page")
):
    """Search entities with pagination for a specific API"""
    if api_id not in api_data:
        raise HTTPException(status_code=404, detail=f'API with ID {api_id} not found')

    options_data = api_data[api_id]["options"]

    # Trim whitespace and handle None
    query = q.strip().lower() if q and q.strip() else ""

    method_filter = method.upper()

    all_results = []

    for entity_group in options_data['entities']:
        if entity_group['method'] != method_filter:
            continue

        for item in entity_group['items']:
            if not query or query in item['name'].lower():
                all_results.append({
                    'method': entity_group['method'],
                    'name': item['name'],
                    'id': item['id'],
                    'has_nested': 'nested_entities' in item
                })

    total_count = len(all_results)
    total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
    start_index = (page - 1) * limit
    end_index = start_index + limit
    paginated_results = all_results[start_index:end_index]

    return {
        'api_id': api_id,
        'api': options_data['api'],
        'method': method_filter,
        'query': q,
        'results': paginated_results,
        'pagination': {
            'page': page,
            'limit': limit,
            'total_count': total_count,
            'total_pages': total_pages,
            'has_more': page < total_pages,
            'has_previous': page > 1
        }
    }


@app.get("/api/{api_id}/entities/{method}")
def get_entities_by_api_and_method(api_id: int, method: str):
    """Get all entities for a specific API and HTTP method"""
    if api_id not in api_data:
        raise HTTPException(status_code=404, detail=f'API with ID {api_id} not found')

    method = method.upper()
    api_options = api_data[api_id]["options"]

    for entity_group in api_options['entities']:
        if entity_group['method'] == method:
            return {
                'api_id': api_id,
                'api': api_options['api'],
                'method': method,
                'entities': entity_group['items']
            }

    raise HTTPException(status_code=404, detail=f'Method {method} not found in API {api_id}')


@app.get("/api/{api_id}/entity/{entity_id}")
def get_entity_by_api_and_id(api_id: int, entity_id: int):
    """Get entity details by API ID and entity ID"""
    if api_id not in api_data:
        raise HTTPException(status_code=404, detail=f'API with ID {api_id} not found')

    entity_lookup = api_data[api_id]["entity_by_id"]
    if entity_id not in entity_lookup:
        raise HTTPException(status_code=404, detail=f'Entity with ID {entity_id} not found in API {api_id}')

    return entity_lookup[entity_id]


@app.get("/api/{api_id}/entity/by-name")
def get_entity_by_name(
    api_id: int,
    name: str = Query(..., description="Exact entity name"),
    method: Optional[str] = Query(None, description="HTTP method")
):
    """Get entity by name for a specific API"""
    if api_id not in api_data:
        raise HTTPException(status_code=404, detail=f'API with ID {api_id} not found')

    full_data = api_data[api_id]["full"]

    method_filter = method.upper() if method else None
    methods_to_search = [method_filter] if method_filter else ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']

    for m in methods_to_search:
        if full_data.get(m):
            for item in full_data[m]:
                if item.get('name') == name:
                    return {
                        'api_id': api_id,
                        'method': m,
                        'entity': item
                    }

    raise HTTPException(status_code=404, detail=f'Entity "{name}" not found in API {api_id}')


# IFS API Proxy Routes
IFS_BASE_URL = "https://ifsgcsc2-d02.demo.ifs.cloud/main/ifsapplications/projection/v1/CustomerOrderHandling.svc"

ifs_router = APIRouter(prefix="/ifs-apis", tags=["IFS APIs"])


@ifs_router.get("/customer-order/{order_no}")
def get_customer_order(order_no: str, token: str = Query(..., description="Bearer auth token")):
    """Get customer order by order number from IFS API"""
    url = f"{IFS_BASE_URL}/CustomerOrderSet(OrderNo='{order_no}')"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        with httpx.Client() as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Request failed: {str(e)}")


@ifs_router.patch("/customer-order/{order_no}")
def update_customer_order(order_no: str, payload: Any = Body(...)):
    """Update customer order by order number in IFS API"""
    url = f"{IFS_BASE_URL}/CustomerOrderSet(OrderNo='{order_no}')"

    # Extract token from payload
    token = payload.pop("token", None)
    if not token:
        raise HTTPException(status_code=400, detail="token is required in payload")

    headers = {"Authorization": f"Bearer {token}"}

    try:
        with httpx.Client() as client:
            response = client.patch(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(status_code=e.response.status_code, detail=str(e))
    except httpx.RequestError as e:
        raise HTTPException(status_code=500, detail=f"Request failed: {str(e)}")


app.include_router(ifs_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
