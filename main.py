from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import json

app = FastAPI(title="CustomerOrderHandling API", description="API to fetch IFS CustomerOrder entities")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

with open('customerorder-options.json', 'r', encoding='utf-8') as f:
    options_data = json.load(f)

with open('customerorder.json', 'r', encoding='utf-8') as f:
    full_data = json.load(f)

entity_by_id = {}

def build_entity_lookup(arr):
    if not arr:
        return
    for item in arr:
        if isinstance(item, dict) and 'id' in item:
            entity_by_id[item['id']] = item
            if 'nested_entities' in item:
                for method in ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']:
                    if item['nested_entities'].get(method):
                        build_entity_lookup(item['nested_entities'][method])

# Build lookup from full data
for method in ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']:
    if full_data.get(method):
        build_entity_lookup(full_data[method])


@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}


@app.get("/api/methods")
def get_methods():
    """Get all available HTTP methods"""
    methods = [entity['method'] for entity in options_data['entities']]
    return {
        'api': options_data['api'],
        'methods': methods
    }


@app.get("/api/entities/search")
def search_entities(
    method: str = Query(..., description="HTTP method (GET, POST, PUT, PATCH, DELETE)"),
    q: Optional[str] = Query(None, description="Search query for entity name"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(10, ge=1, le=100, description="Results per page")
):
    query = q.lower() if q else ""
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


@app.get("/api/entities/{method}")
def get_entities_by_method(method: str):
    """Get all entities for a specific HTTP method (GET, POST, PUT, PATCH, DELETE)"""
    method = method.upper()

    for entity_group in options_data['entities']:
        if entity_group['method'] == method:
            return {
                'method': method,
                'entities': entity_group['items']
            }

    raise HTTPException(status_code=404, detail=f'Method {method} not found')


@app.get("/api/entity/by-name")
def get_entity_by_name(
    name: str = Query(..., description="Exact entity name"),
    method: Optional[str] = Query(None, description="HTTP method")
):
    method_filter = method.upper() if method else None
    methods_to_search = [method_filter] if method_filter else ['GET', 'POST', 'PATCH', 'PUT', 'DELETE']

    for m in methods_to_search:
        if full_data.get(m):
            for item in full_data[m]:
                if item.get('name') == name:
                    return {
                        'method': m,
                        'entity': item
                    }

    raise HTTPException(status_code=404, detail=f'Entity "{name}" not found')


@app.get("/api/entity/{entity_id}")
def get_entity_by_id(entity_id: int):
    if entity_id not in entity_by_id:
        raise HTTPException(status_code=404, detail=f'Entity with ID {entity_id} not found')

    return entity_by_id[entity_id]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
