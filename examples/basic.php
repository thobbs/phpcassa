<?php

/*
 * This file demonstrates some of the basic operations when working with
 * phpcassa and Cassandra.
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
use phpcassa\SystemManager;
use phpcassa\Schema\StrategyClass;

// Create a new keyspace and column family
$sys = new SystemManager('127.0.0.1');
$sys->create_keyspace('Keyspace1', array(
    "strategy_class" => StrategyClass::SIMPLE_STRATEGY,
    "strategy_options" => array('replication_factor' => '1')));
$sys->create_column_family('Keyspace1', 'Users');

// Start a connection pool, create our ColumnFamily instance
$pool = new ConnectionPool('Keyspace1', array('127.0.0.1'));
$users = new ColumnFamily($pool, 'Users');

// Insert a few records
$users->insert('user0', array("name" => "joe", "state" => "TX"));
$users->insert('user1', array("name" => "bob", "state" => "CA"));

// Fetch a user record
$user = $users->get('user0');
$name = $user["name"];
echo "Fetched user $name\n";

// Fetch both at once
$both = $users->multiget(array('user0', 'user1'));
foreach($both as $user_id => $columns) {
    echo "$user_id state: ".$columns["state"]."\n";
}

// Only fetch the name of user1
$columns = $users->get('user1', $column_slice=null, $column_names=array("name"));
echo "Name is ".$columns["name"]."\n";

// Insert two more records at once
$users->batch_insert(array("user3" => array("name" => "kat"),
                           "user4" => array("name" => "tom")));

// Remove the last row
$users->remove("user4");

// Clear out the column family
$users->truncate();

// Destroy our schema
$sys->drop_keyspace("Keyspace1");

// Close our connections
$pool->close();
$sys->close();

?>
