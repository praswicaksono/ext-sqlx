use ext_php_rs::prelude::*;
use ext_php_rs::types::{Zval, ZendHashTable};
use ext_php_rs::exception::PhpResult;
use php_tokio::EventLoop;
use futures_util::StreamExt;
use crate::types::{SqlxError, SqlxResult, SharedColumns, RowValues, convert_zvals, build_zval_from_values};
use crate::iterator::QueryIterator;
use crate::transaction::{PhpTransaction, DynamicTransaction};
use crate::{sqlite_row_to_values, mysql_row_to_values, pg_row_to_values};

#[derive(Clone)]
pub enum DynamicPool {
    Sqlite(sqlx::SqlitePool),
    MySql(sqlx::MySqlPool),
    Postgres(sqlx::PgPool),
}

#[php_class]
#[php(name = "Sqlx\\Connection")]
pub struct Connection {
    pub pool: DynamicPool,
}

#[php_impl]
impl Connection {
    pub fn init() -> PhpResult<u64> {
        EventLoop::init()
    }
    
    pub fn wakeup() -> PhpResult<()> {
        EventLoop::wakeup()
    }
    
    pub fn __construct(dsn: String, options: Option<&Zval>) -> PhpResult<Self> {
        let mut max_connections = 10;
        let mut min_connections = 0;
        let mut acquire_timeout = 30;
        let mut idle_timeout = 600;
        let mut max_lifetime = 1800;

        if let Some(opts) = options {
            if let Some(arr) = opts.array() {
                if let Some(val) = arr.get("max_connections") { max_connections = val.long().unwrap_or(10) as u32; }
                if let Some(val) = arr.get("min_connections") { min_connections = val.long().unwrap_or(0) as u32; }
                if let Some(val) = arr.get("acquire_timeout") { acquire_timeout = val.long().unwrap_or(30) as u64; }
                if let Some(val) = arr.get("idle_timeout") { idle_timeout = val.long().unwrap_or(600) as u64; }
                if let Some(val) = arr.get("max_lifetime") { max_lifetime = val.long().unwrap_or(1800) as u64; }
            }
        }

        let pool = EventLoop::suspend_on(async move {
            if dsn.starts_with("sqlite:") {
                let mut opts = sqlx::sqlite::SqlitePoolOptions::new()
                    .max_connections(max_connections)
                    .min_connections(min_connections)
                    .acquire_timeout(std::time::Duration::from_secs(acquire_timeout));
                if idle_timeout > 0 { opts = opts.idle_timeout(std::time::Duration::from_secs(idle_timeout)); } else { opts = opts.idle_timeout(None); }
                if max_lifetime > 0 { opts = opts.max_lifetime(std::time::Duration::from_secs(max_lifetime)); } else { opts = opts.max_lifetime(None); }
                Ok(DynamicPool::Sqlite(opts.connect(&dsn).await?))
            } else if dsn.starts_with("mysql:") || dsn.starts_with("mariadb:") {
                let mut opts = sqlx::mysql::MySqlPoolOptions::new()
                    .max_connections(max_connections)
                    .min_connections(min_connections)
                    .acquire_timeout(std::time::Duration::from_secs(acquire_timeout));
                if idle_timeout > 0 { opts = opts.idle_timeout(std::time::Duration::from_secs(idle_timeout)); } else { opts = opts.idle_timeout(None); }
                if max_lifetime > 0 { opts = opts.max_lifetime(std::time::Duration::from_secs(max_lifetime)); } else { opts = opts.max_lifetime(None); }
                Ok(DynamicPool::MySql(opts.connect(&dsn).await?))
            } else if dsn.starts_with("postgres:") || dsn.starts_with("postgresql:") {
                let mut opts = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(max_connections)
                    .min_connections(min_connections)
                    .acquire_timeout(std::time::Duration::from_secs(acquire_timeout));
                if idle_timeout > 0 { opts = opts.idle_timeout(std::time::Duration::from_secs(idle_timeout)); } else { opts = opts.idle_timeout(None); }
                if max_lifetime > 0 { opts = opts.max_lifetime(std::time::Duration::from_secs(max_lifetime)); } else { opts = opts.max_lifetime(None); }
                Ok(DynamicPool::Postgres(opts.connect(&dsn).await?))
            } else {
                Err(SqlxError("Unsupported database driver".to_string()))
            }
        })?;

        Ok(Connection { pool })
    }

    pub fn begin_transaction(&self) -> SqlxResult<PhpTransaction> {
        let pool = self.pool.clone();
        let tx = EventLoop::suspend_on(async move {
            match pool {
                DynamicPool::Sqlite(p) => Ok::<DynamicTransaction, anyhow::Error>(DynamicTransaction::Sqlite(p.begin().await.map_err(|e| SqlxError(e.to_string()))?)),
                DynamicPool::MySql(p) => Ok::<DynamicTransaction, anyhow::Error>(DynamicTransaction::MySql(p.begin().await.map_err(|e| SqlxError(e.to_string()))?)),
                DynamicPool::Postgres(p) => Ok::<DynamicTransaction, anyhow::Error>(DynamicTransaction::Postgres(p.begin().await.map_err(|e| SqlxError(e.to_string()))?)),
            }
        })?;
        Ok(PhpTransaction {
            tx: std::sync::Arc::new(tokio::sync::Mutex::new(Some(tx)))
        })
    }

    pub fn close(&self) -> SqlxResult<()> {
        let pool = self.pool.clone();
        EventLoop::suspend_on(async move {
            match pool {
                DynamicPool::Sqlite(p) => p.close().await,
                DynamicPool::MySql(p) => p.close().await,
                DynamicPool::Postgres(p) => p.close().await,
            }
            Ok(())
        })
    }

    pub fn fetch_all(&self, query: String, params: Option<&Zval>) -> SqlxResult<Zval> {
        let q_params = convert_zvals(params)?;
        let pool = self.pool.clone();
        
        let result: SqlxResult<(SharedColumns, Vec<RowValues>)> = EventLoop::suspend_on(async move {
            match pool {
                DynamicPool::Sqlite(p) => {
                    let mut q = sqlx::query::<sqlx::Sqlite>(&query);
                    bind_params!(q, q_params);
                    let rows = q.fetch_all(&p).await?;
                    if rows.is_empty() { return Ok((std::sync::Arc::new(Vec::new()), Vec::new())); }
                    use sqlx::{Row, Column};
                    let cols = std::sync::Arc::new(rows[0].columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>());
                    let mut mapped_rows = Vec::new();
                    for row in rows { mapped_rows.push(sqlite_row_to_values(&row)?); }
                    Ok((cols, mapped_rows))
                },
                DynamicPool::MySql(p) => {
                    let mut q = sqlx::query::<sqlx::MySql>(&query);
                    bind_params!(q, q_params);
                    let rows = q.fetch_all(&p).await?;
                    if rows.is_empty() { return Ok((std::sync::Arc::new(Vec::new()), Vec::new())); }
                    use sqlx::{Row, Column};
                    let cols = std::sync::Arc::new(rows[0].columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>());
                    let mut mapped_rows = Vec::new();
                    for row in rows { mapped_rows.push(mysql_row_to_values(&row)?); }
                    Ok((cols, mapped_rows))
                },
                DynamicPool::Postgres(p) => {
                    let mut q = sqlx::query::<sqlx::Postgres>(&query);
                    bind_params!(q, q_params);
                    let rows = q.fetch_all(&p).await?;
                    if rows.is_empty() { return Ok((std::sync::Arc::new(Vec::new()), Vec::new())); }
                    use sqlx::{Row, Column};
                    let cols = std::sync::Arc::new(rows[0].columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>());
                    let mut mapped_rows = Vec::new();
                    for row in rows { mapped_rows.push(pg_row_to_values(&row)?); }
                    Ok((cols, mapped_rows))
                }
            }
        });
        
        let (cols, rows) = result?;
        let mut ht = ZendHashTable::new();
        for row in rows {
            let row_zval = build_zval_from_values(cols.clone(), row)?;
            ht.push(row_zval).map_err(|_| anyhow::anyhow!("Failed to push into array"))?;
        }
        
        let mut result_zval = Zval::new();
        result_zval.set_hashtable(ht);
        Ok(result_zval)
    }

    pub fn fetch_all_stream(&self, query: String, params: Option<&Zval>) -> SqlxResult<QueryIterator> {
        let q_params = convert_zvals(params)?;
        let pool = self.pool.clone();
        
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        
        EventLoop::suspend_on(async move {
            tokio::spawn(async move {
                let query = query;
                match pool {
                    DynamicPool::Sqlite(p) => {
                        let mut q = sqlx::query::<sqlx::Sqlite>(&query);
                        bind_params!(q, q_params);
                        let mut stream = q.fetch(&p);
                        let mut cols: Option<std::sync::Arc<Vec<String>>> = None;
                        while let Some(row_res) = stream.next().await {
                            match row_res {
                                Ok(row) => {
                                    if cols.is_none() {
                                        use sqlx::{Row, Column};
                                        cols = Some(std::sync::Arc::new(row.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>()));
                                    }
                                    match sqlite_row_to_values(&row) {
                                        Ok(map) => if tx.send(Ok((cols.clone().unwrap(), map))).await.is_err() { break; },
                                        Err(e) => { let _ = tx.send(Err(e)).await; break; }
                                    }
                                },
                                Err(e) => { let _ = tx.send(Err(e.into())).await; break; }
                            }
                        }
                    },
                    DynamicPool::MySql(p) => {
                        let mut q = sqlx::query::<sqlx::MySql>(&query);
                        bind_params!(q, q_params);
                        let mut stream = q.fetch(&p);
                        let mut cols: Option<std::sync::Arc<Vec<String>>> = None;
                        while let Some(row_res) = stream.next().await {
                            match row_res {
                                Ok(row) => {
                                    if cols.is_none() {
                                        use sqlx::{Row, Column};
                                        cols = Some(std::sync::Arc::new(row.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>()));
                                    }
                                    match mysql_row_to_values(&row) {
                                        Ok(map) => if tx.send(Ok((cols.clone().unwrap(), map))).await.is_err() { break; },
                                        Err(e) => { let _ = tx.send(Err(e)).await; break; }
                                    }
                                },
                                Err(e) => { let _ = tx.send(Err(e.into())).await; break; }
                            }
                        }
                    },
                    DynamicPool::Postgres(p) => {
                        let mut q = sqlx::query::<sqlx::Postgres>(&query);
                        bind_params!(q, q_params);
                        let mut stream = q.fetch(&p);
                        let mut cols: Option<std::sync::Arc<Vec<String>>> = None;
                        while let Some(row_res) = stream.next().await {
                            match row_res {
                                Ok(row) => {
                                    if cols.is_none() {
                                        use sqlx::{Row, Column};
                                        cols = Some(std::sync::Arc::new(row.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>()));
                                    }
                                    match pg_row_to_values(&row) {
                                        Ok(map) => if tx.send(Ok((cols.clone().unwrap(), map))).await.is_err() { break; },
                                        Err(e) => { let _ = tx.send(Err(e)).await; break; }
                                    }
                                },
                                Err(e) => { let _ = tx.send(Err(e.into())).await; break; }
                            }
                        }
                    }
                }
            });
        });
        
        Ok(QueryIterator {
            receiver: Some(rx),
            current_key: -1,
            current_val: None,
            is_valid: false,
        })
    }

    pub fn fetch_one(&self, query: String, params: Option<&Zval>) -> SqlxResult<Zval> {
        let q_params = convert_zvals(params)?;
        let pool = self.pool.clone();
        
        let result: SqlxResult<Option<(SharedColumns, RowValues)>> = EventLoop::suspend_on(async move {
            match pool {
                DynamicPool::Sqlite(p) => {
                    let mut q = sqlx::query::<sqlx::Sqlite>(&query);
                    bind_params!(q, q_params);
                    let row = q.fetch_optional(&p).await?;
                    if let Some(r) = row {
                        use sqlx::{Row, Column};
                        let cols = std::sync::Arc::new(r.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>());
                        Ok(Some((cols, sqlite_row_to_values(&r)?)))
                    } else { Ok(None) }
                },
                DynamicPool::MySql(p) => {
                    let mut q = sqlx::query::<sqlx::MySql>(&query);
                    bind_params!(q, q_params);
                    let row = q.fetch_optional(&p).await?;
                    if let Some(r) = row {
                        use sqlx::{Row, Column};
                        let cols = std::sync::Arc::new(r.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>());
                        Ok(Some((cols, mysql_row_to_values(&r)?)))
                    } else { Ok(None) }
                },
                DynamicPool::Postgres(p) => {
                    let mut q = sqlx::query::<sqlx::Postgres>(&query);
                    bind_params!(q, q_params);
                    let row = q.fetch_optional(&p).await?;
                    if let Some(r) = row {
                        use sqlx::{Row, Column};
                        let cols = std::sync::Arc::new(r.columns().iter().map(|c| c.name().to_string()).collect::<Vec<_>>());
                        Ok(Some((cols, pg_row_to_values(&r)?)))
                    } else { Ok(None) }
                }
            }
        });
        
        match result {
            Ok(Some(row_map)) => Ok(build_zval_from_values(row_map.0, row_map.1)?),
            Ok(None) => {
                let mut z = Zval::new();
                z.set_null();
                Ok(z)
            },
            Err(e) => Err(e),
        }
    }

    pub fn execute(&self, query: String, params: Option<&Zval>) -> SqlxResult<i64> {
        let q_params = convert_zvals(params)?;
        let pool = self.pool.clone();
        
        EventLoop::suspend_on(async move {
            match pool {
                DynamicPool::Sqlite(p) => {
                    let mut q = sqlx::query::<sqlx::Sqlite>(&query);
                    bind_params!(q, q_params);
                    let result = q.execute(&p).await?;
                    Ok(result.rows_affected() as i64)
                },
                DynamicPool::MySql(p) => {
                    let mut q = sqlx::query::<sqlx::MySql>(&query);
                    bind_params!(q, q_params);
                    let result = q.execute(&p).await?;
                    Ok(result.rows_affected() as i64)
                },
                DynamicPool::Postgres(p) => {
                    let mut q = sqlx::query::<sqlx::Postgres>(&query);
                    bind_params!(q, q_params);
                    let result = q.execute(&p).await?;
                    Ok(result.rows_affected() as i64)
                }
            }
        })
    }
}
