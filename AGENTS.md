# ext-sqlx

PHP extension providing async SQL database access via the Rust `sqlx` library. Bridges PHP's event loop (Revolt/AMPHP) with Rust's Tokio runtime.

## Build & development

```sh
# Release build
cargo build --release    # produces target/release/libext_sqlx.dylib

# Debug build
cargo build             # produces target/debug/libext_sqlx.dylib

# PHP deps (required for script/test usage):
composer install        # pulls amphp/amp ^3.1 + revolt/event-loop
```

No CI, no lint/formatter config, no test harness. This is an early-stage project.

## Architecture

- **Rust side** (`src/`): A `cdylib` crate using `ext-php-rs` (v0.15) + `php-tokio` to expose Rust async SQL to PHP.
- **PHP side**: Classes live in the `Sqlx\` namespace. PHP scripts use `amphp/amp` for async concurrency.
- **Event loop bridge**: PHP must call `Connection::init()` once, then register the returned fd with Revolt's `onReadable`. `Connection::wakeup()` processes pending Tokio work.
- **Connection pooling**: `Connection` wraps a `DynamicPool` (Sqlite/MySql/Postgres). Only one pool per `Connection` object.

### PHP classes (namespace `Sqlx\`)

| Class | Purpose |
|---|---|
| `Connection` | Init, connect, query, execute, stream, begin transaction |
| `Transaction` | Commit, rollback, execute within tx, fetchOne |
| `QueryIterator` | Streaming iterator over query results (implements Iterator) |
| `Exception` | Extends `\Exception` |

### PHP naming: ext-php-rs converts snake_case → camelCase

| Rust method | PHP method |
|---|---|
| `fetch_one` | `fetchOne` |
| `query_stream` | `queryStream` |
| `begin_transaction` | `beginTransaction` |

Suppress the Rust `non_snake_case` lint with `#[allow(non_snake_case)]` on any method whose PHP-visible name must stay camelCase.

### PHP usage pattern

```php
$fd = Connection::init();
$f = fopen("php://fd/" . $fd, 'r+');
stream_set_blocking($f, false);
Revolt\EventLoop::onReadable($f, fn() => Connection::wakeup());

$db = new Connection("mysql://root@127.0.0.1/db", ["max_connections" => 10]);
$result = $db->fetchOne("SELECT 1");
$rows   = $db->query("SELECT * FROM t");          // returns array of associative arrays
$stream = $db->queryStream("SELECT * FROM t");    // returns QueryIterator (foreach-compatible)
$affected = $db->execute("INSERT INTO t VALUES (?)", [42]); // returns row count
```

### Database DSNs

- `sqlite://path/to/db.sqlite` — SQLite
- `mysql://user:pass@host/db` or `mariadb://...` — MySQL / MariaDB
- `postgres://user:pass@host/db` or `postgresql://...` — PostgreSQL

Connection options (passed as PHP array): `max_connections` (10), `min_connections` (0), `acquire_timeout` (30s), `idle_timeout` (600s), `max_lifetime` (1800s). Set any to 0 for unlimited.

## macOS build note

`.cargo/config.toml` sets `-undefined dynamic_lookup` for `aarch64-apple-darwin` and `x86_64-apple-darwin`. This is required for PHP extension `cdylib` linking on macOS — do not remove.

## Source structure

| File | Role |
|---|---|
| `src/lib.rs` | Module builder, registers all PHP classes, shutdown hook |
| `src/connection.rs` | `Connection` PHP class — pool creation, query, execute, stream, transactions |
| `src/transaction.rs` | `Transaction` PHP class — commit, rollback, tx-scoped queries |
| `src/iterator.rs` | `QueryIterator` PHP class — streaming row-by-row via Tokio mpsc channel |
| `src/types.rs` | `SqlxException`, type conversion (`Zval` ↔ Rust types) |
| `src/macros.rs` | `impl_row_to_values!` and `bind_params!` — row deserialization, param binding |
| `src/test_generic.rs` | Experimental sketch — not part of the lib module tree |
| `src/test_tx.rs` | Experimental sketch — `unimplemented!()`, not part of the lib |
| `benchmark.php` | Async-vs-PDO benchmark comparing ext-sqlx to PDO |

## Rust gotchas

- **`#[php_class]` for iterators**: A class with `current`/`key`/`next`/`rewind`/`valid` methods must explicitly declare `implements(ce = ext_php_rs::zend::ce::iterator, stub = "\\Iterator")` — otherwise `foreach` will not work (the class will appear to have zero elements).
- **`tokio::spawn` inside `EventLoop::suspend_on`**: Remember to `let query = query;` inside the spawned task to capture the query string by value, or it will be a dangling reference when the outer async block completes.
- **`build_zval_from_values`** needs `anyhow::Error` in scope for `.map_err()` calls.

## Testing

There is no test harness. The files `src/test_generic.rs` and `src/test_tx.rs` are **not** `#[cfg(test)]` modules and are not included in `lib.rs` — they are dead code. To verify a change, run:

```sh
cargo build
# then load the .dylib in a PHP script (see benchmark.php for the pattern)
```

## Key dependencies

- `ext-php-rs` 0.15 — Rust↔PHP FFI
- `php-tokio` — Tokio event loop bridge for PHP
- `sqlx` 0.8 — with features: `runtime-tokio-rustls`, `any`, `mysql`, `postgres`, `sqlite`, `chrono`, `time`, `json`
- `amphp/amp` ^3.1 (PHP-side, Composer)
