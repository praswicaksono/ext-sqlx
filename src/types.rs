use ext_php_rs::prelude::*;
use ext_php_rs::types::{Zval, ZendHashTable};
use ext_php_rs::convert::IntoZval;
use ext_php_rs::exception::PhpException;

#[php_class]
#[php(name = "Sqlx\\Exception", extends(ce = ext_php_rs::zend::ce::exception, stub = "\\Exception"))]
pub struct SqlxException;

#[derive(Debug)]
pub struct SqlxError(pub String);

impl std::fmt::Display for SqlxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for SqlxError {}

impl From<SqlxError> for PhpException {
    fn from(err: SqlxError) -> PhpException {
        PhpException::from_class::<SqlxException>(err.0)
    }
}

impl From<anyhow::Error> for SqlxError {
    fn from(err: anyhow::Error) -> SqlxError {
        SqlxError(err.to_string())
    }
}

impl From<sqlx::Error> for SqlxError {
    fn from(err: sqlx::Error) -> SqlxError {
        SqlxError(err.to_string())
    }
}

pub type SqlxResult<T> = std::result::Result<T, SqlxError>;
pub type SharedColumns = std::sync::Arc<Vec<String>>;
pub type RowValues = Vec<QueryParam>;
pub type StreamRow = SqlxResult<(SharedColumns, RowValues)>;

#[derive(Clone)]
pub enum QueryParam {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Null,
}

pub fn convert_zvals(params: Option<&Zval>) -> SqlxResult<Vec<QueryParam>> {
    let mut q_params = Vec::new();
    if let Some(p) = params {
        if p.is_array() {
            if let Some(ht) = p.array() {
                for (_k, v) in ht.iter() {
                    if let Some(s) = v.string() {
                        q_params.push(QueryParam::String(s));
                    } else if let Some(i) = v.long() {
                        q_params.push(QueryParam::Int(i));
                    } else if let Some(f) = v.double() {
                        q_params.push(QueryParam::Float(f));
                    } else if let Some(b) = v.bool() {
                        q_params.push(QueryParam::Bool(b));
                    } else if v.is_null() {
                        q_params.push(QueryParam::Null);
                    } else {
                        return Err(SqlxError("Unsupported parameter type".to_string()));
                    }
                }
            }
        }
    }
    Ok(q_params)
}

pub fn build_zval_from_values(columns: std::sync::Arc<Vec<String>>, values: Vec<QueryParam>) -> SqlxResult<Zval> {
    let mut row_arr = ZendHashTable::new();
    for (i, v) in values.into_iter().enumerate() {
        let k = &columns[i];
        match v {
            QueryParam::String(s) => { row_arr.insert(k.as_str(), s).map_err(|e| anyhow::anyhow!(e.to_string()))?; },
            QueryParam::Int(i) => { row_arr.insert(k.as_str(), i).map_err(|e| anyhow::anyhow!(e.to_string()))?; },
            QueryParam::Float(f) => { row_arr.insert(k.as_str(), f).map_err(|e| anyhow::anyhow!(e.to_string()))?; },
            QueryParam::Bool(b) => { row_arr.insert(k.as_str(), b).map_err(|e| anyhow::anyhow!(e.to_string()))?; },
            QueryParam::Null => {
                let mut null_zval = Zval::new();
                null_zval.set_null();
                row_arr.insert(k.as_str(), null_zval).map_err(|e| anyhow::anyhow!(e.to_string()))?;
            },
        }
    }
    Ok(row_arr.into_zval(false).unwrap())
}
