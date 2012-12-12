<?php

use phpcassa\ColumnSlice;
use phpcassa\Connection\ConnectionPool;
use phpcassa\SystemManager;
use phpcassa\SuperColumnFamily;
use phpcassa\Schema\DataType;

use cassandra\NotFoundException;

class TestSuperColumnFamily extends PHPUnit_Framework_TestCase {

    private $pool;
    private $cf;
    private $sys;

    private static $KEYS = array('key1', 'key2', 'key3');
    private static $KS = "TestSuperColumnFamily";

    public static function setUpBeforeClass() {
        $sys = new SystemManager();

        $ksdefs = $sys->describe_keyspaces();
        $exists = False;
        foreach ($ksdefs as $ksdef)
            $exists = $exists || $ksdef->name == self::$KS;

        if (!$exists) {
            $sys->create_keyspace(self::$KS, array());

            $cfattrs = array(
                "column_type" => "Super",
                "comparator_type" => "Int32Type"
            );
            $sys->create_column_family(self::$KS, 'Super1', $cfattrs);
        }
        $sys->close();
    }

    public static function tearDownAfterClass() {
        $sys = new SystemManager();
        $sys->drop_keyspace(self::$KS);
        $sys->close();
    }

    public function setUp() {
        $this->pool = new ConnectionPool(self::$KS);
        $this->cf = new SuperColumnFamily($this->pool, 'Super1');
    }

    public function tearDown() {
        foreach(self::$KEYS as $key) {
            $this->cf->remove($key);
        }
        $this->pool->dispose();
    }

    public function test_get() {
        $columns = array(1 => array('sub1' => 'val1', 'sub2' => 'val2'),
                         2 => array('sub3' => 'val3', 'sub3' => 'val3'));

        $this->setExpectedException('\cassandra\NotFoundException');
        $this->cf->get(self::$KEYS[0]);

        $this->cf->insert(self::$KEYS[0], $columns);
        $this->assertEquals($this->cf->get(self::$KEYS[0]), null, $columns);
        $this->assertEquals(
            $this->cf->multiget(array(self::$KEYS[0])), null,
            array(self::$KEYS[0] => $columns));
        $response = $this->cf->get_range($start_key=self::$KEYS[0],
                                         $finish_key=self::$KEYS[0]);
        foreach($response as $key => $cols) {
            #should only be one row
            $this->assertEquals($key, self::$KEYS[0]);
            $this->assertEquals($cols, $columns);
        }
    }

    private function insert_supers() {
        $sub12 = array('sub1' => 'val1', 'sub2' => 'val2');
        $sub34 = array('sub3' => 'val3', 'sub4' => 'val4');
        $cols = array(1 => $sub12, 2 => $sub34);
        $this->cf->insert(self::$KEYS[0], $cols);
    }

    public function test_get_super_column() {
        $this->insert_supers();
        $sub12 = array('sub1' => 'val1', 'sub2' => 'val2');

        $this->assertEquals(
            $this->cf->get_super_column(self::$KEYS[0], 1),
            $sub12);

        // specify a slice of subcolumns to fetch
        $slice = new ColumnSlice('sub2', 'sub2');
        $this->assertEquals(
            $this->cf->get_super_column(self::$KEYS[0], 1, $slice),
            array('sub2' => 'val2'));

        // specify a set of subcolumn names to fetch
        $subcols = array('sub2');
        $this->assertEquals(
            $this->cf->get_super_column(self::$KEYS[0], 1, null, $subcols),
            array('sub2' => 'val2'));

        $this->setExpectedException('\cassandra\NotFoundException');
        $this->cf->get_super_column(self::$KEYS[0], 3);
    }

    public function test_multiget_super_column() {
        $this->insert_supers();
        $sub12 = array('sub1' => 'val1', 'sub2' => 'val2');

        $this->assertEquals(
            $this->cf->multiget_super_column(array(self::$KEYS[0]), 1),
            array(self::$KEYS[0] => $sub12));

        // specify a slice of subcolumns to fetch
        $slice = new ColumnSlice('sub2', 'sub2');
        $this->assertEquals(
            $this->cf->multiget_super_column(array(self::$KEYS[0]), 1, $slice),
            array(self::$KEYS[0] => array('sub2' => 'val2')));

        // specify a set of subcolumn names to fetch
        $subcols = array('sub2');
        $this->assertEquals(
            $this->cf->multiget_super_column(array(self::$KEYS[0]), 1, null, $subcols),
            array(self::$KEYS[0] => array('sub2' => 'val2')));
    }

    public function test_get_super_column_range() {
        $this->insert_supers();
        $sub12 = array('sub1' => 'val1', 'sub2' => 'val2');

        $key = self::$KEYS[0];
        $response = $this->cf->get_super_column_range(1, $key, $key);
        foreach($response as $res_key => $cols) {
            #should only be one row
            $this->assertEquals($res_key, $key);
            $this->assertEquals($cols, $sub12);
        }

        // specify a slice of subcolumns to fetch
        $slice = new ColumnSlice('sub2', 'sub2');
        $response = $this->cf->get_super_column_range(1, $key, $key, 10, $slice);
        foreach($response as $res_key => $cols) {
            $this->assertEquals($res_key, $key);
            $this->assertEquals($cols, array('sub2' => 'val2'));
        }

        // specify a set of subcolumns names to fetch
        $subcols = array('sub2');
        $response = $this->cf->get_super_column_range(1, $key, $key, 10, null, $subcols);
        foreach($response as $res_key => $cols) {
            $this->assertEquals($res_key, $key);
            $this->assertEquals($cols, array('sub2' => 'val2'));
        }
    }

    public function test_get_subcolumn_count_and_remove() {
        $this->insert_supers();
        $key = self::$KEYS[0];

        $this->assertEquals(2, $this->cf->get_count($key));
        $this->cf->remove_super_column($key, 1);
        $this->assertEquals(1, $this->cf->get_count($key));
        $this->cf->remove_super_column($key, 2, array('sub3'));
        $this->assertEquals($this->cf->get_count($key), 1);
        $this->assertEquals($this->cf->get($key), array(2 => array('sub4' => 'val4')));
    }
}
