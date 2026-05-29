# sqlx-php

An asynchronous SQL database extension for PHP, powered by the incredible Rust [`sqlx`](https://github.com/launchbadge/sqlx) library and bridged to PHP's `Revolt` event loop via `ext-php-rs`. 

Enjoy non-blocking database queries in PHP using Tokio's ultra-fast multithreaded runtime.

### Features
* **Truly Async**: Your PHP event loop keeps ticking while heavy database queries run on Rust threads.
* **Multi-Database**: Built-in support for SQLite, MySQL/MariaDB, and PostgreSQL.
* **Streaming**: Stream massive query results efficiently back to PHP memory using Tokio MPSC channels.
* **Connection Pooling**: Automatic, configurable connection pools under the hood.

---

## Installation

This extension is distributed as a pre-compiled binary via **PIE (PHP Installer for Extensions)**. 
You do not need Rust installed on your server to use this extension.

```bash
# Ensure you have pie installed, then run:
pie install prsw/sqlx-php
```

You must also require the PHP-side event loop dependencies in your project:
```bash
composer require amphp/amp revolt/event-loop
```

---

## Quick Start

You must initialize the Tokio bridge and register it with `Revolt` before queueing your database logic.

```php
<?php
require 'vendor/autoload.php';

use Revolt\EventLoop;
use Sqlx\Connection;

// 1. Initialize the Tokio <-> Revolt bridge
$fd = Connection::init();
$f = fopen("php://fd/" . $fd, 'r+');
stream_set_blocking($f, false);
$watcherId = EventLoop::onReadable($f, fn() => Connection::wakeup());

// 2. Queue your async application logic
EventLoop::queue(function() use ($watcherId) {
    try {
        // Connect via DSN (sqlite://, mysql://, postgres://)
        $db = new Connection("sqlite://example.sqlite", [
            "max_connections" => 5
        ]);
        
        $db->execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)");
        
        // Execute queries with parameterized bindings
        $db->execute("INSERT INTO users (name) VALUES (?)", ["Alice"]);
        $db->execute("INSERT INTO users (name) VALUES (?)", ["Bob"]);
        
        // Fetch all rows instantly into an array
        $rows = $db->fetchAll("SELECT * FROM users", []);
        
        // OR stream large datasets asynchronously row-by-row
        $stream = $db->fetchAllStream("SELECT * FROM users", []);
        foreach ($stream as $row) {
            echo "User: {$row['name']}\n";
        }

    } finally {
        EventLoop::unreference($watcherId);
    }
});

// 3. Start the Event Loop
EventLoop::run();
```

> **Note:** For a more comprehensive example including transactions, check out `example/basic_usage.php`!
