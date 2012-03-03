<?php

use phpcassa\Connection\ConnectionPool;
use phpcassa\SystemManager;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;

class TestSuperCounterColumnFamily extends PHPUnit_Framework_TestCase {

    private $pool;
    private $cf;
    private $sys;

    private static $KS = "TestSuperCounterColumnFamily";

    public static function setUpBeforeClass() {
        try {
            $sys = new SystemManager();

            $ksdefs = $sys->describe_keyspaces();
            $exists = False;
            foreach ($ksdefs as $ksdef)
                $exists = $exists || $ksdef->name == self::$KS;

            if (!$exists) {
                $sys->create_keyspace(self::$KS, array());

                $cfattrs = array();
                $cfattrs["column_type"] = "Super";
                $cfattrs["default_validation_class"] = "CounterColumnType";
                $sys->create_column_family(self::$KS, 'SuperCounter1', $cfattrs);
            }
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
        $this->cf = new ColumnFamily($this->pool, 'SuperCounter1');
    }

    public function tearDown() {
        $this->pool->dispose();
    }

    public function test_add() {
        $key = "test_add";
        $this->cf->add($key, "col", 1, "supercol");
        $result = $this->cf->get($key, array("supercol"));
        $this->assertEquals($result, array("supercol" => array("col" => 1)));

        $this->cf->add($key, "col", 2, "supercol");
        $result = $this->cf->get($key, array("supercol"));
        $this->assertEquals($result, array("supercol" => array("col" => 3)));

        $this->cf->add($key, "col2", 5, "supercol");
        $result = $this->cf->get($key);
        $this->assertEquals($result, array("supercol" => array("col" => 3, "col2" => 5)));
        $result = $this->cf->get($key, null, "", "", False, 10, "supercol");
        $this->assertEquals($result, array("col" => 3, "col2" => 5));
    }

    public function test_remove_counter() {
        $key = "test_remove_counter";
        $this->cf->add($key, "col1", 1, "supercol");
        $this->cf->add($key, "col2", 1, "supercol");
        $result = $this->cf->get($key, array("supercol"));
        $this->assertEquals($result, array("supercol" => array("col1" => 1,
                                                             "col2" => 1)));

        $this->cf->remove_counter($key, "col1", "supercol");
        $result = $this->cf->get($key, array("supercol"));
        $this->assertEquals($result, array("supercol" => array("col2" => 1)));

        $this->cf->remove_counter($key, null, "supercol");
        try {
            $result = $this->cf->get($key, array("supercol"));
            assert(false);
        } catch (cassandra_NotFoundException $e) { }
    }

}


?>
