use std::sync::Arc;
use arrow::datatypes::{DataType, Field, Schema};

fn main() {
    // 중첩된 구조체를 가진 스키마 생성
    let coordinates_fields = vec![
        Arc::new(Field::new("lat", DataType::Float64, false)),
        Arc::new(Field::new("lng", DataType::Float64, false)),
    ];
    let coordinates_type = DataType::Struct(coordinates_fields.into());

    let address_fields = vec![
        Arc::new(Field::new("street", DataType::Utf8, false)),
        Arc::new(Field::new("city", DataType::Utf8, false)),
        Arc::new(Field::new("coordinates", coordinates_type, false)),
    ];
    let address_type = DataType::Struct(address_fields.into());

    let preferences_fields = vec![
        Arc::new(Field::new("theme", DataType::Utf8, false)),
        Arc::new(Field::new("language", DataType::Utf8, false)),
    ];
    let preferences_type = DataType::Struct(preferences_fields.into());

    let profile_fields = vec![
        Arc::new(Field::new("name", DataType::Utf8, false)),
        Arc::new(Field::new("email", DataType::Utf8, false)),
        Arc::new(Field::new("address", address_type, false)),
        Arc::new(Field::new("preferences", preferences_type, false)),
    ];
    let profile_type = DataType::Struct(profile_fields.into());

    let schema = Schema::new(vec![
        Arc::new(Field::new("user_id", DataType::Int64, false)),
        Arc::new(Field::new("profile", profile_type, false)),
        Arc::new(Field::new("active", DataType::Boolean, false)),
    ]);

    println!("중첩된 ORC 스키마 시뮬레이션:");
    println!("{:#?}", schema);
}
