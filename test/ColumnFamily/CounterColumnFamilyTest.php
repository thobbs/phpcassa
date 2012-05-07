<?php

use phpcassa\Connection\ConnectionPool;
use phpcassa\SystemManager;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;

use cassandra\NotFoundException;

class TestCounterColumnFamily extends PHPUnit_Framework_TestCase {

    private $pool;
    private $cf;
    private $sys;

    private static $KS = "TestCounterColumnFamily";

    public static function setUpBeforeClass() {
        try {
            $sys = new SystemManager();

            $ksdefs = $sys->describe_keyspaces();
            $exists = False;
            foreach ($ksdefs as $ksdef)
                $exists = $exists || $ksdef->name == self::$KS;

            if (!$exists) {
                $sys->create_keyspace(self::$KS, array());

                $cfattrs = array("default_validation_class" => "CounterColumnType");
                $sys->create_column_family(self::$KS, 'Counter1', $cfattrs);
            }

        } catch (Exception $e) {
            print($e);
            throw $e;
        }
    }

    public static function tearDownAfterClass() {
        $sys = new SystemManager();
        $sys->drop_keyspace(self::$KS);
    }

    public function setUp() {
        $this->pool = new ConnectionPool(self::$KS);
        $this->cf = new ColumnFamily($this->pool, 'Counter1');
    }

    public function tearDown() {
        $this->pool->dispose();
    }

    public function test_add() {
        $key = "test_add";
        $this->cf->add($key, "col");
        $result = $this->cf->get($key, null, array("col"));
        $this->assertEquals($result, array("col" => 1));

        $this->cf->add($key, "col", 2);
        $result = $this->cf->get($key, null, array("col"));
        $this->assertEquals($result, array("col" => 3));

        $this->cf->add($key, "col2", 5);
        $result = $this->cf->get($key);
        $this->assertEquals($result, array("col" => 3, "col2" => 5));
    }

    public function test_remove_counter() {
        $key = "test_remove_counter";
        $this->cf->add($key, "col");
        $result = $this->cf->get($key, null, array("col"));
        $this->assertEquals($result, array("col" => 1));

        $this->cf->remove_counter($key, "col");
        $this->setExpectedException('\cassandra\NotFoundException');
        $result = $this->cf->get($key, null, array("col"));
    }
}

