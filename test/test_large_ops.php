<?php
require_once('simpletest/autorun.php');
require_once('../lib/autoload.php');

use phpcassa\ColumnFamily;
use phpcassa\Connection\ConnectionPool;

class TestLargeOps extends UnitTestCase {

    private static $VALS = array('val1', 'val2', 'val3');
    private static $KEYS = array('key1', 'key2', 'key3');

    private $client;
    private $cf;

    public function setUp() {
        $this->client = new ConnectionPool('Keyspace1');

        $this->cf = new ColumnFamily($this->client, 'Standard1');
    }

    public function tearDown() {
        $this->cf->truncate();
    }

    public function test_large_ops() {
        $str = '';
        foreach(range(0,255) as $i) {
            # each addition is 64 bytes
            $str .= 'aaaaaaa aaaaaaa aaaaaaa aaaaaa aaaaaaa aaaaaaa aaaaaaa aaaaaaa ';
        }

        foreach(range(0, 99) as $i)
            $this->cf->insert("key$i", array($str => $str));

        foreach(range(0, 99) as $i) {
            $res = $this->cf->get("key$i");
            self::assertEqual($res[$str], $str);
        }
    }
}