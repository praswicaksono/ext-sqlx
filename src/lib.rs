use ext_php_rs::prelude::*;
use php_tokio::EventLoop;

#[macro_use] pub mod macros;
pub mod types;
pub mod iterator;
pub mod transaction;
pub mod connection;

use types::SqlxException;
use iterator::QueryIterator;
use transaction::PhpTransaction;
use connection::Connection;

impl_row_to_values!(sqlite_row_to_values, sqlx::sqlite::SqliteRow);
impl_row_to_values!(mysql_row_to_values, sqlx::mysql::MySqlRow);
impl_row_to_values!(pg_row_to_values, sqlx::postgres::PgRow);

pub extern "C" fn request_shutdown(_type: i32, _module_number: i32) -> i32 {
    EventLoop::shutdown();
    0
}

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .request_shutdown_function(request_shutdown)
        .class::<SqlxException>()
        .class::<Connection>()
        .class::<QueryIterator>()
        .class::<PhpTransaction>()
}
