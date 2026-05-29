<?php
require __DIR__ . '/../vendor/autoload.php';

use Revolt\EventLoop;
use Sqlx\Connection;
use Sqlx\Exception;

// 1. Initialize the Async Bridge between Rust Tokio and PHP Revolt
$fd = Connection::init();
$f = fopen("php://fd/" . $fd, 'r+');
stream_set_blocking($f, false);
$watcherId = EventLoop::onReadable($f, fn() => Connection::wakeup());

// Ensure the SQLite file exists before connecting
touch(__DIR__ . '/example.sqlite');

// 2. Queue your async application logic
EventLoop::queue(function() use ($watcherId) {
    try {
        echo "Connecting to database...\n";

        // Connect to a database using standard DSN strings.
        // Supported schemes: sqlite://, mysql://, postgres://
        $db = new Connection("sqlite://" . __DIR__ . "/example.sqlite", [
            "max_connections" => 5,
            "acquire_timeout" => 30 // seconds
        ]);

        echo "Setting up schema...\n";
        $db->execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)");
        $db->execute("DELETE FROM users", []);

        echo "Inserting records...\n";
        // Use standard ? bindings to safely insert data
        $db->execute("INSERT INTO users (name, age) VALUES (?, ?)", ["Alice", 28]);
        $db->execute("INSERT INTO users (name, age) VALUES (?, ?)", ["Bob", 35]);
        $db->execute("INSERT INTO users (name, age) VALUES (?, ?)", ["Charlie", 42]);

        echo "\nFetching all rows into memory at once:\n";
        $allRows = $db->fetchAll("SELECT * FROM users", []);
        echo "Found " . count($allRows) . " users via fetchAll!\n";

        echo "\nFetching single row:\n";
        $countRow = $db->fetchOne("SELECT COUNT(*) as total FROM users", []);
        echo "Total Users: {$countRow['total']}\n";

        echo "\nStreaming all rows asynchronously:\n";
        // fetchAllStream returns a generator that fetches rows via Tokio channels
        $stream = $db->fetchAllStream("SELECT * FROM users ORDER BY age DESC", []);
        foreach ($stream as $row) {
            echo "- User #{$row['id']}: {$row['name']} (Age: {$row['age']})\n";
        }

        echo "\nTesting Transactions:\n";
        $tx = $db->beginTransaction();
        echo "- Transaction started.\n";
        $tx->execute("INSERT INTO users (name, age) VALUES (?, ?)", ["David", 20]);
        // Data is visible inside the transaction
        $david = $tx->fetchOne("SELECT * FROM users WHERE name = ?", ["David"]);
        echo "- Inserted inside TX: {$david['name']}\n";

        // Rollback instead of commit to discard changes
        $tx->rollback();
        echo "- Transaction rolled back.\n";

        $missing = $db->fetchOne("SELECT * FROM users WHERE name = ?", ["David"]);
        if ($missing === null) {
            echo "- Verified: David was rolled back and is not in the database.\n";
        }

        echo "\nFinished successfully!\n";
    } catch (Exception $e) {
        echo "Sqlx Database Error: " . $e->getMessage() . "\n";
    } finally {
        EventLoop::unreference($watcherId);
    }
});

// 3. Start the Event Loop
EventLoop::run();
