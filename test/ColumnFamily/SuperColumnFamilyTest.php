<?php

use phpcassa\Connection\ConnectionPool;
use phpcassa\SystemManager;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;

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

            $cfattrs = array("column_type" => "Super");
            $sys->create_column_family(self::$KS, 'Super1', $cfattrs);
        }
    }

    public static function tearDownAfterClass() {
        $sys = new SystemManager();
        $sys->drop_keyspace(self::$KS);
        $sys->close();
    }

    public function setUp() {
        $this->pool = new ConnectionPool(self::$KS);
        $this->cf = new ColumnFamily($this->pool, 'Super1');
    }

    public function tearDown() {
        foreach(self::$KEYS as $key) {
            $this->cf->remove($key);
        }
        $this->pool->dispose();
    }

    public function test_super() {
        $columns = array('1' => array('sub1' => 'val1', 'sub2' => 'val2'),
                         '2' => array('sub3' => 'val3', 'sub3' => 'val3'));
        try {
            $this->cf->get(self::$KEYS[0]);
            assert(false);
        } catch (cassandra_NotFoundException $e) {
        }

        $this->cf->insert(self::$KEYS[0], $columns);
        $this->assertEquals($this->cf->get(self::$KEYS[0]), $columns);
        $this->assertEquals($this->cf->multiget(array(self::$KEYS[0])), array(self::$KEYS[0] => $columns));
        $response = $this->cf->get_range($start_key=self::$KEYS[0],
                                         $finish_key=self::$KEYS[0]);
        foreach($response as $key => $cols) {
            #should only be one row
            $this->assertEquals($key, self::$KEYS[0]);
            $this->assertEquals($cols, $columns);
        }
    }

    public function test_super_column_argument() {
        $key = self::$KEYS[0];
        $sub12 = array('sub1' => 'val1', 'sub2' => 'val2');
        $sub34 = array('sub3' => 'val3', 'sub4' => 'val4');
        $cols = array('1' => $sub12, '2' => $sub34);
        $this->cf->insert($key, $cols);
        $this->assertEquals($this->cf->get($key, null, '', '', false, 100, $super_column='1'), $sub12);
        try {
            $this->cf->get($key, null, '', '', false, 100, $super_column='3');
            assert(false);
        } catch (cassandra_NotFoundException $e) {
        }

        $this->assertEquals($this->cf->multiget(array($key), null, '', '', false, 100, $super_column='1'),
                          array($key => $sub12));

        $response = $this->cf->get_range($start_key=$key, $end_key=$key, 100, null, '',
                                               '', false, 100, $super_column='1');
        foreach($response as $res_key => $cols) {
            #should only be one row
            $this->assertEquals($res_key, $key);
            $this->assertEquals($cols, $sub12);
        }

        $this->assertEquals($this->cf->get_count($key), 2);
        $this->cf->remove($key, null, '1');
        $this->assertEquals($this->cf->get_count($key), 1);
        $this->cf->remove($key, array('sub3'), '2');
        $this->assertEquals($this->cf->get_count($key), 1);
        $this->assertEquals($this->cf->get($key), array('2' => array('sub4' => 'val4')));
    }
}
