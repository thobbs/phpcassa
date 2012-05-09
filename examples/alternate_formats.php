<?php

/*
 * Examples of how to use the alternate data formats for inserting
 * and retrieving data.
 *
 *
 * If the cli version of PHP is installed, it may be run directly:
 *
 *    $ php basic.php'
 *
 * If you're using Linux, verify that 'php5-cli' or a similar package has
 * been installed.
 *
 */

require_once(__DIR__.'/../lib/autoload.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\ColumnSlice;
use phpcassa\SystemManager;
use phpcassa\Schema\StrategyClass;
use phpcassa\UUID;

// Create a new keyspace and column family
$sys = new SystemManager('127.0.0.1');
$sys->create_keyspace('Keyspace1', array(
    "strategy_class" => StrategyClass::SIMPLE_STRATEGY,
    "strategy_options" => array('replication_factor' => '1')));

// We'll use TimeUUIDs for column names and UUIDs for row keys
$sys->create_column_family('Keyspace1', 'UserLogs', array(
    "comparator_type" => "TimeUUIDType",
    "key_validation_class" => "LexicalUUIDType"
));

// Start a connection pool, create our ColumnFamily instance
$pool = new ConnectionPool('Keyspace1', array('127.0.0.1'));
$user_logs = new ColumnFamily($pool, 'UserLogs');

// Don't use dictionary-style arrays for data
$user_logs->insert_format = ColumnFamily::ARRAY_FORMAT;
$user_logs->return_format = ColumnFamily::ARRAY_FORMAT;

// Make a couple of user IDs with non-Time UUIDs
$user1_id = UUID::uuid4();
$user2_id = UUID::uuid4();

// Insert some log messages
$user1_logs = array();
$user2_logs = array();
for($i = 0; $i < 5; $i++) {
    $user1_logs[] = array(UUID::uuid1(), "message$i");
    $user2_logs[] = array(UUID::uuid1(), "message$i");
}
$user_logs->batch_insert(array(
    array($user1_id, $user1_logs),
    array($user2_id, $user2_logs),
));

echo "Using ColumnFamily::ARRAY_FORMAT\n";
echo "================================\n\n";

// Pull the first two logs back out
$slice = new ColumnSlice('', '', $count=2);
$logs = $user_logs->get($user1_id, $slice);

echo "First two logs for user1: \n";

list($uuid, $message) = $logs[0];
echo "    ".$uuid->time.", ".$message."\n";

list($uuid, $message) = $logs[1];
echo "    ".$uuid->time.", ".$message."\n\n";

// Fetch the last log for both users at once
$slice = new ColumnSlice('', '', $count=1, $reversed=true);
$rows = $user_logs->multiget(array($user1_id, $user2_id), $slice);
foreach($rows as $row) {
    list($user_id, $logs) = $row;
    $log = $logs[0];
    echo "Most recent log for $user_id:\n";
    echo "    ".$log[0]->time.": ".$log[1]."\n";
}
echo "\n";

// Fetch the first column for each row in the CF:
$slice = new ColumnSlice('', '', $count=1);
foreach($user_logs->get_range('', '', 10, $slice) as $row) {
    list($user_id, $logs) = $row;
    $log = $logs[0];
    echo "First log for $user_id:\n";
    echo "    ".$log[0]->time.": ".$log[1]."\n";
}


echo "\n\n\n";
echo "Using ColumnFamily::OBJECT_FORMAT\n";
echo "================================\n\n";
$user_logs->return_format = ColumnFamily::OBJECT_FORMAT;

// Pull the first two logs back out
$slice = new ColumnSlice('', '', $count=2);
$logs = $user_logs->get($user1_id, $slice);

echo "First two logs for user1: \n";

$uuid = $logs[0]->name;
$message = $logs[0]->value;
echo "    ".$uuid->time.", ".$message."\n";

$uuid = $logs[1]->name;
$message = $logs[1]->value;
echo "    ".$uuid->time.", ".$message."\n\n";

// Fetch the last log for both users at once
$slice = new ColumnSlice('', '', $count=1, $reversed=true);
$rows = $user_logs->multiget(array($user1_id, $user2_id), $slice);
foreach($rows as $row) {
    list($user_id, $logs) = $row;

    $log = $logs[0];
    $uuid = $log->name;
    $message = $log->value;
    $timestamp = $log->timestamp;
    $ttl = $log->ttl;

    echo "Most recent log for $user_id:\n";
    echo "    ".$uuid->time.": ".$message."\n";
    echo "    timestamp: $timestamp, ttl: $ttl\n";
}
echo "\n";

// Fetch the first column for each row in the CF:
$slice = new ColumnSlice('', '', $count=1);
foreach($user_logs->get_range('', '', 10, $slice) as $row) {
    list($user_id, $logs) = $row;

    $log = $logs[0];
    $uuid = $log->name;
    $message = $log->value;
    $timestamp = $log->timestamp;
    $ttl = $log->ttl;

    echo "First log for $user_id:\n";
    echo "    ".$uuid->time.": ".$message."\n";
    echo "    timestamp: $timestamp, ttl: $ttl\n";

}
echo "\n";


// Clear out the column family
$user_logs->truncate();

// Destroy our schema
$sys->drop_keyspace("Keyspace1");

// Close our connections
$pool->close();
$sys->close();

?>
