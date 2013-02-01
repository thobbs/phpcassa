<?php

/*
 * This file demonstrates the basics of working with counters
 * through phpcassa.
 *
 *
 * If the cli version of PHP is installed, it may be run directly:
 *
 *    $ php counters.php'
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
$sys->create_column_family('Keyspace1', 'Counts', array(
    "default_validation_class" => 'CounterColumnType'));

// Start a connection pool, create our ColumnFamily instance
$pool = new ConnectionPool('Keyspace1', array('127.0.0.1'));
$count_cf = new ColumnFamily($pool, 'Counts');

// ColumnFamily::add() is the easiest way to increment a counter
$count_cf->add("key1", "col1");
$results = $count_cf->get("key1");
$current_count = $results["col1"];
echo "After one add(): $current_count\n";

// You can specify a value other than 1 to adjust the counter by
$count_cf->add("key1", "col1", 10);
$results = $count_cf->get("key1");
$current_count = $results["col1"];
echo "After add(10): $current_count\n";

// ColumnFamily::insert() will also increment values, but you can
// adjust multiple columns at the same time
$count_cf->insert("key1", array("col1" => 3, "col2" => -1));
$results = $count_cf->get("key1");
$current_count = $results["col1"];
echo "After insert(3): $current_count\n\n";

// And ColumnFamily::batch_insert() lets you increment counters
// in multiple rows at the same time
$count_cf->batch_insert(array("key1" => array("col1" => 1,
                                              "col2" => -1),
                              "key2" => array("col1" => 16)));

echo "After batch_insert:\n";
print_r($count_cf->multiget(array("key1", "key2")));

// ColumnFamily::remove_counter() should basically only be used when you
// will *never* use a counter again
$count_cf->remove_counter("key1", "col1");
echo "\nRow key1 after remove_counter(key1, col1):\n";
print_r($count_cf->get("key1"));

// Destroy our schema
$sys->drop_keyspace("Keyspace1");

// Close our connections
$pool->close();
$sys->close();

?>
