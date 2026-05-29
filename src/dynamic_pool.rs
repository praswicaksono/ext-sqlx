use ext_php_rs::prelude::*;
use ext_php_rs::types::{Zval, ZendHashTable};
use ext_php_rs::convert::IntoZval;
use php_tokio::EventLoop;
use sqlx::{Row, Column, TypeInfo};

pub enum QueryParam {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
}

pub enum DynamicPool {
    Sqlite(sqlx::SqlitePool),
    MySql(sqlx::MySqlPool),
    Postgres(sqlx::PgPool),
}

pub enum DynamicTransaction {
    Sqlite(sqlx::Transaction<'static, sqlx::Sqlite>),
    MySql(sqlx::Transaction<'static, sqlx::MySql>),
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
}

// Helpers to bind params
macro_rules! bind_params {
    ($q:expr, $params:expr) => {
        for p in $params {
            match p {
                QueryParam::String(s) => $q = $q.bind(s),
                QueryParam::Int(i) => $q = $q.bind(i),
                QueryParam::Float(f) => $q = $q.bind(f),
                QueryParam::Bool(b) => $q = $q.bind(b),
                QueryParam::Null => $q = $q.bind(Option::<String>::None),
            }
        }
    }
}

// ... we will need more logic here
