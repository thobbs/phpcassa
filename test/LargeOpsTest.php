<?php

use phpcassa\SystemManager;
use phpcassa\ColumnFamily;
use phpcassa\Connection\ConnectionPool;

class LargeOpsTest extends PHPUnit_Framework_TestCase {

    private static $VALS = array('val1', 'val2', 'val3');
    private static $KEYS = array('key1', 'key2', 'key3');
    private static $KS = "TestLargeOps";
    private static $CF = "Standard1";

    private $pool;
    private $cf;

    public static function setUpBeforeClass() {
        try {
            $sys = new SystemManager();

            $ksdefs = $sys->describe_keyspaces();
            $exists = False;
            foreach ($ksdefs as $ksdef)
                $exists = $exists || $ksdef->name == self::$KS;

            if ($exists)
                $sys->drop_keyspace(self::$KS);

            $sys->create_keyspace(self::$KS, array());

            $cfattrs = array("column_type" => "Standard");
            $sys->create_column_family(self::$KS, self::$CF, $cfattrs);

        } catch (Exception $e) {
            print($e);
            throw $e;
        }
    }

    public static function tearDownAfterClass() {
        $sys = new SystemManager();
        $sys->drop_keyspace(self::$KS);
        $sys->close();
    }

    public function setUp() {
        $this->pool = new ConnectionPool(self::$KS);
        $this->cf = new ColumnFamily($this->pool, self::$CF);
    }

    public function tearDown() {
        $this->pool->dispose();
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
            $this->assertEquals($res[$str], $str);
        }
    }
}
