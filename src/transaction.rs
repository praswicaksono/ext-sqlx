use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;
use php_tokio::EventLoop;
use crate::types::{SqlxError, SqlxResult, SharedColumns, RowValues, convert_zvals, build_zval_from_values};

pub enum DynamicTransaction {
    Sqlite(sqlx::Transaction<'static, sqlx::Sqlite>),
    MySql(sqlx::Transaction<'static, sqlx::MySql>),
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
}

#[php_class]
#[php(name = "Sqlx\\Transaction")]
pub struct PhpTransaction {
    pub tx: std::sync::Arc<tokio::sync::Mutex<Option<DynamicTransaction>>>,
}

#[php_impl]
impl PhpTransaction {
    pub fn commit(&self) -> SqlxResult<()> {
        let tx_arc = self.tx.clone();
        EventLoop::suspend_on(async move {
            let mut guard = tx_arc.lock().await;
            if let Some(tx) = guard.take() {
                match tx {
                    DynamicTransaction::Sqlite(t) => { t.commit().await.map_err(|e| SqlxError(e.to_string()))?; },
                    DynamicTransaction::MySql(t) => { t.commit().await.map_err(|e| SqlxError(e.to_string()))?; },
                    DynamicTransaction::Postgres(t) => { t.commit().await.map_err(|e| SqlxError(e.to_string()))?; },
                }
            }
            Ok(())
        })
    }

    pub fn rollback(&self) -> SqlxResult<()> {
        let tx_arc = self.tx.clone();
        EventLoop::suspend_on(async move {
            let mut guard = tx_arc.lock().await;
            if let Some(tx) = guard.take() {
                match tx {
                    DynamicTransaction::Sqlite(t) => { t.rollback().await.map_err(|e| SqlxError(e.to_string()))?; },
                    DynamicTransaction::MySql(t) => { t.rollback().await.map_err(|e| SqlxError(e.to_string()))?; },
                    DynamicTransaction::Postgres(t) => { t.rollback().await.map_err(|e| SqlxError(e.to_string()))?; },
                }
            }
            Ok(())
        })
    }

    pub fn execute(&self, query: String, params: Option<&Zval>) -> SqlxResult<i64> {
        let q_params = convert_zvals(params)?;
        let tx_arc = self.tx.clone();
        EventLoop::suspend_on(async move {
            let mut guard = tx_arc.lock().await;
            let tx = match guard.as_mut() {
                Some(tx) => tx,
                None => return Err(SqlxError("Transaction already committed or rolled back".to_string())),
            };
            match tx {
                DynamicTransaction::Sqlite(t) => {
                    let mut q = sqlx::query::<sqlx::Sqlite>(&query);
                    bind_params!(q, q_params);
                    let result = q.execute(&mut **t).await.map_err(|e| SqlxError(e.to_string()))?;
                    Ok(result.rows_affected() as i64)
                },
                DynamicTransaction::MySql(t) => {
                    let mut q = sqlx::query::<sqlx::MySql>(&query);
                    bind_params!(q, q_params);
                    let result = q.execute(&mut **t).await.map_err(|e| SqlxError(e.to_string()))?;
                    Ok(result.rows_affected() as i64)
                },
                DynamicTransaction::Postgres(t) => {
                    let mut q = sqlx::query::<sqlx::Postgres>(&query);
                    bind_params!(q, q_params);
                    let result = q.execute(&mut **t).await.map_err(|e| SqlxError(e.to_string()))?;
                    Ok(result.rows_affected() as i64)
                }
            }
        })
    }

    #[allow(non_snake_case)]
    pub fn fetchOne(&self, query: String, params: Option<&Zval>) -> SqlxResult<Zval> {
        let q_params = convert_zvals(params)?;
        let tx_arc = self.tx.clone();
        let result: SqlxResult<Option<(SharedColumns, RowValues)>> = EventLoop::suspend_on(async move {
            let mut guard = tx_arc.lock().await;
            let tx = match guard.as_mut() {
                Some(tx) => tx,
                None => return Err(SqlxError("Transaction already committed or rolled back".to_string())),
            };
            match tx {
                DynamicTransaction::Sqlite(t) => {
                    let mut q = sqlx::query::<sqlx::Sqlite>(&query);
                    bind_params!(q, q_params);
                    let row = q.fetch_optional(&mut **t).await.map_err(|e| SqlxError(e.to_string()))?;
                    if let Some(r) = row {
                        use sqlx::Row;
                        use sqlx::Column;
                        let cols = std::sync::Arc::new(r.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>());
                        Ok(Some((cols, crate::sqlite_row_to_values(&r)?)))
                    } else { Ok(None) }
                },
                DynamicTransaction::MySql(t) => {
                    let mut q = sqlx::query::<sqlx::MySql>(&query);
                    bind_params!(q, q_params);
                    let row = q.fetch_optional(&mut **t).await.map_err(|e| SqlxError(e.to_string()))?;
                    if let Some(r) = row {
                        use sqlx::Row;
                        use sqlx::Column;
                        let cols = std::sync::Arc::new(r.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>());
                        Ok(Some((cols, crate::mysql_row_to_values(&r)?)))
                    } else { Ok(None) }
                },
                DynamicTransaction::Postgres(t) => {
                    let mut q = sqlx::query::<sqlx::Postgres>(&query);
                    bind_params!(q, q_params);
                    let row = q.fetch_optional(&mut **t).await.map_err(|e| SqlxError(e.to_string()))?;
                    if let Some(r) = row {
                        use sqlx::Row;
                        use sqlx::Column;
                        let cols = std::sync::Arc::new(r.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>());
                        Ok(Some((cols, crate::pg_row_to_values(&r)?)))
                    } else { Ok(None) }
                }
            }
        });

        match result {
            Ok(Some(row_map)) => Ok(build_zval_from_values(row_map.0, row_map.1)?),
            Ok(None) => {
                let mut null_zval = Zval::new();
                null_zval.set_null();
                Ok(null_zval)
            },
            Err(e) => Err(e),
        }
    }
}
