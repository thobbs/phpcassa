<?php

/*
 * A demonstration of fetching a slice of columns from a row.
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

// Create a new keyspace and column family
$sys = new SystemManager('127.0.0.1');
$sys->create_keyspace('Keyspace1', array(
    "strategy_class" => StrategyClass::SIMPLE_STRATEGY,
    "strategy_options" => array('replication_factor' => '1')
));

$sys->create_column_family('Keyspace1', 'Letters', array(
    "column_type" => "Standard",
    "comparator_type" => "LongType",
    "key_validation_class" => "UTF8Type",
    "default_validation_class" => "UTF8Type"
));

// Start a connection pool, create our ColumnFamily instance
$pool = new ConnectionPool('Keyspace1', array('127.0.0.1'));
$letters = new ColumnFamily($pool, 'Letters');

// Insert a few records
$columns = array(1 => "a", 2 => "b", 3 => "c", 4 => "d", 5 => "e");
$letters->insert('key', $columns);

function print_slice($columns) {
    echo "(";
    foreach($columns as $number => $letter) {
        echo "$number => $letter, ";
    }
    echo ")\n";
}

// Fetch everything >= 2
$slice = new ColumnSlice(2);
print_slice($letters->get('key', $slice));

// Fetch everything between 2 and 4, inclusive
$slice = new ColumnSlice(2, 4);
print_slice($letters->get('key', $slice));

// Fetch the first three columns in the row
$slice = new ColumnSlice('', '', $count=3);
print_slice($letters->get('key', $slice));

// Fetch the last three columns in the row
$slice = new ColumnSlice('', '', $count=3, $reversed=true);
print_slice($letters->get('key', $slice));

// Fetch two columns before 4
$slice = new ColumnSlice(4, '', $count=2, $reversed=true);
print_slice($letters->get('key', $slice));

// Destroy our schema
$sys->drop_keyspace("Keyspace1");

// Close our connections
$pool->close();
$sys->close();

?>
