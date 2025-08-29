import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json

# Create test data with nested structure
data = [
    {
        "id": 1,
        "name": "John Doe",
        "address": {
            "street": "123 Main St",
            "city": "Seattle",
            "zipcode": "98101",
            "coordinates": {
                "lat": 47.6062,
                "lng": -122.3321
            }
        },
        "tags": ["engineer", "python"]
    },
    {
        "id": 2,
        "name": "Jane Smith",
        "address": {
            "street": "456 Oak Ave",
            "city": "Portland",
            "zipcode": "97201",
            "coordinates": {
                "lat": 45.5152,
                "lng": -122.6784
            }
        },
        "tags": ["designer", "ui/ux"]
    }
]

# Convert to PyArrow table with nested schema
table = pa.Table.from_pylist(data)

# Write to parquet
pq.write_table(table, 'test_nested.parquet')
print("Created test_nested.parquet with nested structure")
