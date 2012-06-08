<?php

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

abstract class AutopackBase extends PHPUnit_Framework_TestCase {

    protected $SERIALIZED = false;

    public static $have64Bit;

    protected static $VALS = array('val1', 'val2', 'val3');
    protected static $KEYS = array('key1', 'key2', 'key3');
    protected static $KS = "TestAutopacking";

    protected $client;
    protected $cf;

    public static function setUpBeforeClass() {
        $sys = new SystemManager();

        $ksdefs = $sys->describe_keyspaces();
        $exists = False;
        foreach ($ksdefs as $ksdef)
            $exists = $exists || $ksdef->name == self::$KS;

        if ($exists)
            $sys->drop_keyspace(self::$KS);

        $sys->create_keyspace(self::$KS, array());
    }

    public static function tearDownAfterClass() {
        $sys = new SystemManager();
        $sys->drop_keyspace(self::$KS);
        $sys->close();
    }

    public function setUp() {}

    public function tearDown() {
        foreach($this->cfs as $cf) {
            foreach(self::$KEYS as $key)
                $cf->remove($key);
        }
    }
}

AutopackBase::$have64Bit = (PHP_INT_MAX !== 2147483647);
