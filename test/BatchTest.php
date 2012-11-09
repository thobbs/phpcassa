<?php

use phpcassa\SystemManager;
use phpcassa\ColumnFamily;
use phpcassa\SuperColumnFamily;
use phpcassa\Batch\Mutator;
use phpcassa\Connection\ConnectionPool;

class BatchTest extends PHPUnit_Framework_TestCase {

    private static $KS = "TestBatch";
    private static $CF = "Standard1";
    private static $COUNTER_CF = "Counter1";
    private static $SUPER_CF = "Super1";
    private static $COUNTER_SUPER_CF = "CounterSuper1";

    private $pool;
    private $cf;

    public static function setUpBeforeClass() {
        try {
            $sys = new SystemManager();

            $ksdefs = $sys->describe_keyspaces();
            $exists = false;
            foreach ($ksdefs as $ksdef)
                $exists = $exists || $ksdef->name == self::$KS;

            if ($exists)
                $sys->drop_keyspace(self::$KS);

            $sys->create_keyspace(self::$KS, array());

            $cfattrs = array("column_type" => "Standard");
            $sys->create_column_family(self::$KS, self::$CF, $cfattrs);

            $cfattrs = array(
                "column_type" => "Standard",
                "default_validation_class" => "CounterColumnType");
            $sys->create_column_family(self::$KS, self::$COUNTER_CF, $cfattrs);

            $cfattrs = array("column_type" => "Super");
            $sys->create_column_family(self::$KS, self::$SUPER_CF, $cfattrs);

            $cfattrs = array(
                "column_type" => "Super",
                "default_validation_class" => "CounterColumnType");
            $sys->create_column_family(self::$KS, self::$COUNTER_SUPER_CF, $cfattrs);

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
        $this->counter_cf = new ColumnFamily($this->pool, self::$COUNTER_CF);
        $this->super_cf = new SuperColumnFamily($this->pool, self::$SUPER_CF);
        $this->counter_super_cf = new SuperColumnFamily($this->pool, self::$COUNTER_SUPER_CF);
    }

    public function tearDown() {
        $this->pool->dispose();
    }

    public function test_cf_mutator() {
        $this->setExpectedException('\cassandra\NotFoundException');

        $batch = $this->cf->batch();
        $batch->insert("key1", array("col1" => "val1"));
        $batch->insert("key1", array("col2" => "val2"));
        $batch->insert("key2", array("col1" => "val1"));
        $batch->send();

        $this->assertEquals(array("col1" => "val1", "col2" => "val2"),
                            $this->cf->get("key1"));
        $this->assertEquals(array("col1" => "val1"),
                            $this->cf->get("key2"));

        $batch->remove("key1", array("col2"));
        $batch->insert("key2", array("col2" => "val2"));
        $batch->send();

        $this->assertEquals(array("col1" => "val1"),
                            $this->cf->get("key1"));
        $this->assertEquals(array("col1" => "val1", "col2" => "val2"),
                            $this->cf->get("key2"));

        $batch->remove("key1");
        $batch->remove("key2");
        $batch->send();

        $this->cf->get("key1");
    }

    public function test_super_cf_mutator() {
        $this->setExpectedException('\cassandra\NotFoundException');

        $batch = $this->super_cf->batch();
        $batch->insert("key1", array("super1" => array("col1" => "val1")));
        $batch->insert("key1", array("super1" => array("col2" => "val2")));
        $batch->insert("key1", array("super2" => array("col1" => "val1")));
        $batch->insert("key2", array("super1" => array("col1" => "val1")));
        $batch->send();

        $this->assertEquals(array("super1" => array("col1" => "val1", "col2" => "val2"),
                                  "super2" => array("col1" => "val1")),
                            $this->super_cf->get("key1"));
        $this->assertEquals(array("super1" => array("col1" => "val1")),
                            $this->super_cf->get("key2"));

        $batch->remove("key1", array("col2"), "super1");
        $batch->remove("key1", array("super2"));
        $batch->insert("key2", array("super1" => array("col2" => "val2")));
        $batch->send();

        $this->assertEquals(array("super1" => array("col1" => "val1")),
                            $this->super_cf->get("key1"));
        $this->assertEquals(array("super1" => array("col1" => "val1", "col2" => "val2")),
                            $this->super_cf->get("key2"));

        $batch->remove("key1");
        $batch->remove("key2");
        $batch->send();

        $this->super_cf->get("key1");
    }

    public function test_counter_cf_mutator() {
        $this->setExpectedException('\cassandra\NotFoundException');

        $batch = $this->counter_cf->batch();
        $batch->insert("key1", array("col1" => 1));
        $batch->insert("key1", array("col2" => 1));
        $batch->insert("key2", array("col1" => 1));
        $batch->send();

        $this->assertEquals(array("col1" => 1, "col2" => 1),
                            $this->counter_cf->get("key1"));
        $this->assertEquals(array("col1" => 1),
                            $this->counter_cf->get("key2"));

        $batch->insert("key1", array("col1" => 1));
        $batch->remove("key1", array("col2"));
        $batch->insert("key2", array("col2" => 1));
        $batch->send();

        $this->assertEquals(array("col1" => 2),
                            $this->counter_cf->get("key1"));
        $this->assertEquals(array("col1" => 1, "col2" => 1),
                            $this->counter_cf->get("key2"));

        $batch->remove("key1");
        $batch->remove("key2");
        $batch->send();

        $this->counter_cf->get("key1");
    }

    public function test_counter_super_cf_mutator() {
        $this->setExpectedException('\cassandra\NotFoundException');

        $batch = $this->counter_super_cf->batch();
        $batch->insert("key1", array("super1" => array("col1" => 1)));
        $batch->insert("key1", array("super1" => array("col2" => 1)));
        $batch->insert("key1", array("super2" => array("col1" => 1)));
        $batch->insert("key2", array("super1" => array("col1" => 1)));
        $batch->send();

        $this->assertEquals(array("super1" => array("col1" => 1, "col2" => 1),
                                  "super2" => array("col1" => 1)),
                            $this->counter_super_cf->get("key1"));
        $this->assertEquals(array("super1" => array("col1" => 1)),
                            $this->counter_super_cf->get("key2"));

        $batch->insert("key1", array("super1" => array("col1" => 1)));
        $batch->remove("key1", array("col2"), "super1");
        $batch->remove("key1", array("super2"));
        $batch->insert("key2", array("super1" => array("col2" => 1)));
        $batch->send();

        $this->assertEquals(array("super1" => array("col1" => 2)),
                            $this->counter_super_cf->get("key1"));
        $this->assertEquals(array("super1" => array("col1" => 1, "col2" => 1)),
                            $this->counter_super_cf->get("key2"));

        $batch->remove("key1");
        $batch->remove("key2");
        $batch->send();

        $this->counter_super_cf->get("key1");
    }

}

