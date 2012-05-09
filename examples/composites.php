<?php

/*
 * Examples of working with composite types.
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

// Use composites for column names and row keys
$sys->create_column_family('Keyspace1', 'Composites', array(
    "comparator_type" => "CompositeType(LongType, AsciiType)",
    "key_validation_class" => "CompositeType(AsciiType, LongType)"
));

// Start a connection pool, create our ColumnFamily instance
$pool = new ConnectionPool('Keyspace1', array('127.0.0.1'));
$cf = new ColumnFamily($pool, 'Composites');

// Make it easier to work with non-scalar types
$cf->insert_format = ColumnFamily::ARRAY_FORMAT;
$cf->return_format = ColumnFamily::ARRAY_FORMAT;

// Insert a few records
$key1 = array("key", 1);
$key2 = array("key", 2);

$columns = array(
    array(array(0, "a"), "val0a"),

    array(array(1, "a"), "val1a"),
    array(array(1, "b"), "val1b"),
    array(array(1, "c"), "val1c"),

    array(array(2, "a"), "val2a"),

    array(array(3, "a"), "val3a")
);

$cf->insert($key1, $columns);
$cf->insert($key2, $columns);

// Fetch a user record
$row = $cf->get($key1);
$col1 = $row[0];
list($name, $value) = $col1;
echo "Column name: ";
print_r($name);
echo "Column value: ";
print_r($value);
echo "\n\n";

// Fetch columns with a first component of 1
$slice = new ColumnSlice(array(1), array(1));
$columns = $cf->get($key1, $slice);
foreach($columns as $column) {
    list($name, $value) = $column;
    echo "$value, ";
}
echo "\n\n";

// Fetch everything before (1, c), exclusive
$inclusive = False;
$slice = new ColumnSlice('', array(1, array("c", $inclusive)));
$columns = $cf->get($key1, $slice);
foreach($columns as $column) {
    list($name, $value) = $column;
    echo "$value, ";
}
echo "\n\n";

// Fetch everything between 0 and 2, exclusive on both ends
$slice = new ColumnSlice(
    $start = array(array(0, False)),
    $end   = array(array(2, False))
);
$columns = $cf->get($key1, $slice);
foreach($columns as $column) {
    list($name, $value) = $column;
    echo "$value, ";
}
echo "\n\n";

// Do the same thing in reverse
$slice = new ColumnSlice(
    $start = array(array(2, False)),
    $end   = array(array(0, False)),
    $count = 10,
    $reversed = True
);
$columns = $cf->get($key1, $slice);
foreach($columns as $column) {
    list($name, $value) = $column;
    echo "$value, ";
}
echo "\n\n";

// Clear out the column family
$cf->truncate();

// Destroy our schema
$sys->drop_keyspace("Keyspace1");

// Close our connections
$pool->close();
$sys->close();

?>
