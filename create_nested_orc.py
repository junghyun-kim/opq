#!/usr/bin/env python3
"""
중첩된 구조를 가진 ORC 샘플 파일을 생성하는 스크립트
"""

import pandas as pd
import pyarrow as pa
import pyarrow.orc as orc
from pyarrow import struct

# 중첩된 구조의 샘플 데이터 생성
def create_nested_orc_sample():
    # 샘플 데이터 정의
    data = {
        'user_id': [1, 2, 3],
        'profile': [
            {
                'name': 'Alice',
                'contact': {
                    'email': 'alice@example.com',
                    'phone': '123-456-7890',
                    'address': {
                        'street': '123 Main St',
                        'city': 'New York',
                        'coordinates': {
                            'lat': 40.7128,
                            'lng': -74.0060
                        }
                    }
                },
                'preferences': {
                    'theme': 'dark',
                    'language': 'en',
                    'notifications': {
                        'email': True,
                        'push': False
                    }
                }
            },
            {
                'name': 'Bob',
                'contact': {
                    'email': 'bob@example.com',
                    'phone': '987-654-3210',
                    'address': {
                        'street': '456 Oak Ave',
                        'city': 'Los Angeles',
                        'coordinates': {
                            'lat': 34.0522,
                            'lng': -118.2437
                        }
                    }
                },
                'preferences': {
                    'theme': 'light',
                    'language': 'es',
                    'notifications': {
                        'email': False,
                        'push': True
                    }
                }
            },
            {
                'name': 'Charlie',
                'contact': {
                    'email': 'charlie@example.com',
                    'phone': '555-123-4567',
                    'address': {
                        'street': '789 Pine Rd',
                        'city': 'Chicago',
                        'coordinates': {
                            'lat': 41.8781,
                            'lng': -87.6298
                        }
                    }
                },
                'preferences': {
                    'theme': 'auto',
                    'language': 'fr',
                    'notifications': {
                        'email': True,
                        'push': True
                    }
                }
            }
        ],
        'metadata': [
            {
                'created_at': '2023-01-15',
                'last_active': '2024-08-29',
                'stats': {
                    'login_count': 150,
                    'messages_sent': 2340
                }
            },
            {
                'created_at': '2023-03-20',
                'last_active': '2024-08-28',
                'stats': {
                    'login_count': 89,
                    'messages_sent': 1205
                }
            },
            {
                'created_at': '2023-06-10',
                'last_active': '2024-08-29',
                'stats': {
                    'login_count': 234,
                    'messages_sent': 3456
                }
            }
        ]
    }
    
    # PyArrow 스키마 정의 (중첩 구조)
    coordinates_schema = pa.struct([
        pa.field('lat', pa.float64()),
        pa.field('lng', pa.float64())
    ])
    
    address_schema = pa.struct([
        pa.field('street', pa.string()),
        pa.field('city', pa.string()),
        pa.field('coordinates', coordinates_schema)
    ])
    
    notifications_schema = pa.struct([
        pa.field('email', pa.bool_()),
        pa.field('push', pa.bool_())
    ])
    
    preferences_schema = pa.struct([
        pa.field('theme', pa.string()),
        pa.field('language', pa.string()),
        pa.field('notifications', notifications_schema)
    ])
    
    contact_schema = pa.struct([
        pa.field('email', pa.string()),
        pa.field('phone', pa.string()),
        pa.field('address', address_schema)
    ])
    
    profile_schema = pa.struct([
        pa.field('name', pa.string()),
        pa.field('contact', contact_schema),
        pa.field('preferences', preferences_schema)
    ])
    
    stats_schema = pa.struct([
        pa.field('login_count', pa.int64()),
        pa.field('messages_sent', pa.int64())
    ])
    
    metadata_schema = pa.struct([
        pa.field('created_at', pa.string()),
        pa.field('last_active', pa.string()),
        pa.field('stats', stats_schema)
    ])
    
    schema = pa.schema([
        pa.field('user_id', pa.int64()),
        pa.field('profile', profile_schema),
        pa.field('metadata', metadata_schema)
    ])
    
    # 데이터를 PyArrow 배열로 변환
    user_ids = pa.array(data['user_id'])
    
    # profile 데이터 변환
    profiles = []
    for profile in data['profile']:
        contact = profile['contact']
        address = contact['address']
        coordinates = address['coordinates']
        preferences = profile['preferences']
        notifications = preferences['notifications']
        
        profile_struct = {
            'name': profile['name'],
            'contact': {
                'email': contact['email'],
                'phone': contact['phone'],
                'address': {
                    'street': address['street'],
                    'city': address['city'],
                    'coordinates': {
                        'lat': coordinates['lat'],
                        'lng': coordinates['lng']
                    }
                }
            },
            'preferences': {
                'theme': preferences['theme'],
                'language': preferences['language'],
                'notifications': {
                    'email': notifications['email'],
                    'push': notifications['push']
                }
            }
        }
        profiles.append(profile_struct)
    
    profile_array = pa.array(profiles, type=profile_schema)
    
    # metadata 데이터 변환
    metadatas = []
    for metadata in data['metadata']:
        stats = metadata['stats']
        metadata_struct = {
            'created_at': metadata['created_at'],
            'last_active': metadata['last_active'],
            'stats': {
                'login_count': stats['login_count'],
                'messages_sent': stats['messages_sent']
            }
        }
        metadatas.append(metadata_struct)
    
    metadata_array = pa.array(metadatas, type=metadata_schema)
    
    # PyArrow Table 생성
    table = pa.table([user_ids, profile_array, metadata_array], schema=schema)
    
    # ORC 파일로 저장
    output_path = 'samples/nested_sample.orc'
    orc.write_table(table, output_path)
    print(f"중첩된 ORC 파일이 생성되었습니다: {output_path}")
    
    return table

if __name__ == "__main__":
    table = create_nested_orc_sample()
    print("생성된 스키마:")
    print(table.schema)
