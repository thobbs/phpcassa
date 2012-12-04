<?php

/*
 * This file demonstrates how to use get_range_by_token() iterator with
 * phpcassa and Cassandra 1.1.7.
 * 
 * 
 * Note, because of bug with Cassandra Thrift, 
 * this example DOES NOT work before Cassandra 1.1.7
 * 
 *
 * If the cli version of PHP is installed, it may be run directly:
 *
 *    $ php range_test.php'
 *
 * If you're using Linux, verify that 'php5-cli' or a similar package has
 * been installed.
 * 
 * 
 * Possible usages of get_range_by_token() :
 *   1. You can iterate part of the ring.
 *      This helps to start several processes,
 *      that scans the ring in parallel in fashion similar to Hadoop.
 *      Then full ring scan will take only 1 / <number of processes>
 * 
 *   2. You can iterate "local" token range for each Cassandra node.
 *      You can start one process on each cassandra node,
 *      that iterates only on token range for this node.
 *      In this case you minimize the network traffic between nodes.
 * 
 *   3. Combinations of the above.
 * 
 * 
 * Unlike other examples, you will need to run this on existing column family.
 * Here an example how to create example column family:
 * 
 * create column family a
 *  with column_type = 'Standard'
 *   and comparator = 'AsciiType'
 *   and default_validation_class = 'UTF8Type'
 *   and key_validation_class = 'AsciiType';
 * 
 * set a[0][id] = 0;
 * set a[1][id] = 1;
 * set a[2][id] = 2;
 * set a[3][id] = 3;
 * set a[4][id] = 4;
 * set a[5][id] = 5;
 * set a[6][id] = 6;
 * set a[7][id] = 7;
 * set a[8][id] = 8;
 * set a[9][id] = 9;
 *
 */
 
require_once(__DIR__.'/../lib/autoload.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;

/*
 * Instead to do some crazy token calculation,
 * we can crease an array with tokens that are interested for us.
 */

$tokens = array(
/* Start of the ring */	0 =>                                       "0",
			1 =>  "21267647932558653966460912964485513216",
/* 1/4 of the ring */	2 =>  "42535295865117307932921825928971026432",
			3 =>  "63802943797675961899382738893456539648",
/* 1/2 of the ring */	4 =>  "85070591730234615865843651857942052864",
			5 => "106338239662793269832304564822427566080",
/* 3/4 of the ring */	6 => "127605887595351923798765477786913079296",
			7 => "148873535527910577765226390751398592512",
/* End of the ring */	8 => "170141183460469231731687303715884105727",	// this is called Lucas prime number from 1876
);

/*
 * instead of Lucas number, you can specify token outside the ring,
 * however it will not work if you specify 0 (zero) as end of the ring.
 * 			8 => "200000000000000000000000000000000000000",
 */

// Connect to Cassandra and create an instance of the CF.
$pool = new ConnectionPool('test', array('127.0.0.1'));
$cf = new ColumnFamily($pool, 'a');
$column_to_show = "id";

// Collect the begin and end token from command line...
$begin_token = @$argv[1] ? $argv[1] : 0;
$end_token =   @$argv[2] ? $argv[2] : 8;

// Collect the begin and end token from query_string...
if (@$_REQUEST["begin_token"])
	$begin_token = $_REQUEST["begin_token"];

if (@$_REQUEST["end_token"])
	$end_token = $_REQUEST["end_token"];

// Use <pre> it for web output
echo "<pre>\n\n";

// Use the iterator...
$br = 0;
foreach($cf->get_range_by_token($tokens[ $begin_token ], $tokens[ $end_token ], 1000) as $x){
	printf("No. %10d : Data: %s\n", ++$br, @$x[ $column_to_show ]);
}


