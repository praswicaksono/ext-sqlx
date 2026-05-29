#[macro_export]
macro_rules! impl_row_to_values {
    ($func_name:ident, $row_type:ty) => {
        pub fn $func_name(row: &$row_type) -> $crate::types::SqlxResult<Vec<$crate::types::QueryParam>> {
            use sqlx::{Row, ValueRef, TypeInfo};
            let mut map = Vec::new();
            for i in 0..row.len() {
                let raw_val = row.try_get_raw(i).map_err(|e| $crate::types::SqlxError(e.to_string()))?;
                if raw_val.is_null() {
                    map.push($crate::types::QueryParam::Null);
                    continue;
                }
                
                let type_info = raw_val.type_info();
                let type_name = type_info.name();
                match type_name {
                    "INTEGER" | "INT" | "INT4" | "INT8" | "TINYINT" | "SMALLINT" | "BIGINT" | "MEDIUMINT" => {
                        if let Ok(v) = row.try_get::<i64, _>(i) {
                            map.push($crate::types::QueryParam::Int(v));
                            continue;
                        }
                    },
                    "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "NUMERIC" | "DECIMAL" => {
                        if let Ok(v) = row.try_get::<f64, _>(i) {
                            map.push($crate::types::QueryParam::Float(v));
                            continue;
                        }
                    },
                    "BOOLEAN" | "BOOL" => {
                        if let Ok(v) = row.try_get::<bool, _>(i) {
                            map.push($crate::types::QueryParam::Bool(v));
                            continue;
                        }
                    },
                    "TEXT" | "VARCHAR" | "CHAR" | "JSON" | "JSONB" | "UUID" => {
                        if let Ok(v) = row.try_get::<String, _>(i) {
                            map.push($crate::types::QueryParam::String(v));
                            continue;
                        }
                    },
                    "DATETIME" | "TIMESTAMP" => {
                        if let Ok(v) = row.try_get::<chrono::NaiveDateTime, _>(i) {
                            map.push($crate::types::QueryParam::String(v.to_string()));
                            continue;
                        }
                    },
                    "TIMESTAMPTZ" => {
                        if let Ok(v) = row.try_get::<chrono::DateTime<chrono::Utc>, _>(i) {
                            map.push($crate::types::QueryParam::String(v.to_string()));
                            continue;
                        }
                    },
                    "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => {
                        if let Ok(v) = row.try_get::<Vec<u8>, _>(i) {
                            map.push($crate::types::QueryParam::String(String::from_utf8_lossy(&v).into_owned()));
                            continue;
                        }
                    },
                    _ => {}
                }
                
                // Fallback (e.g. SQLite generic columns or complex types)
                if let Ok(v) = row.try_get::<String, _>(i) {
                    map.push($crate::types::QueryParam::String(v));
                } else if let Ok(v) = row.try_get::<i64, _>(i) {
                    map.push($crate::types::QueryParam::Int(v));
                } else if let Ok(v) = row.try_get::<f64, _>(i) {
                    map.push($crate::types::QueryParam::Float(v));
                } else if let Ok(v) = row.try_get::<bool, _>(i) {
                    map.push($crate::types::QueryParam::Bool(v));
                } else if let Ok(v) = row.try_get::<chrono::NaiveDateTime, _>(i) {
                    map.push($crate::types::QueryParam::String(v.to_string()));
                } else {
                    map.push($crate::types::QueryParam::Null);
                }
            }
            Ok(map)
        }
    }
}

#[macro_export]
macro_rules! bind_params {
    ($q:expr, $params:expr) => {
        for p in $params {
            match p {
                $crate::types::QueryParam::String(s) => $q = $q.bind(s),
                $crate::types::QueryParam::Int(i) => $q = $q.bind(i),
                $crate::types::QueryParam::Float(f) => $q = $q.bind(f),
                $crate::types::QueryParam::Bool(b) => $q = $q.bind(b),
                $crate::types::QueryParam::Null => $q = $q.bind(Option::<String>::None),
            }
        }
    }
}
