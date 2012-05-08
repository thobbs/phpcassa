<?php

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

use phpcassa\Index\IndexExpression;
use phpcassa\Index\IndexClause;

class ArrayFormatCFTest extends PHPUnit_Framework_TestCase {

    private static $KEYS = array('key1', 'key2', 'key3');
    private static $KS = "TestColumnFamily";

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
            $sys->create_column_family(self::$KS, 'Standard1', $cfattrs);

            $sys->create_column_family(self::$KS, 'Indexed1', $cfattrs);
            $sys->create_index(self::$KS, 'Indexed1', 'birthdate',
                                     DataType::LONG_TYPE, 'birthday_index');
            $sys->close();

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
        $this->cf = new ColumnFamily($this->pool, 'Standard1');
        $this->cf->insert_format = ColumnFamily::ARRAY_FORMAT;
        $this->cf->return_format = ColumnFamily::ARRAY_FORMAT;
    }

    public function tearDown() {
        if ($this->cf) {
            foreach(self::$KEYS as $key)
                $this->cf->remove($key);
        }
        $this->pool->dispose();
    }

    public function test_get() {
        $cols = array(array('col', 'val'), array('col2', 'val2'));
        $this->cf->insert(self::$KEYS[0], $cols);
        $res = $this->cf->get(self::$KEYS[0]);
        $this->assertEquals($cols, $res);
    }

    public function test_multiget() {
        $cols = array(array('col', 'val'), array('col2', 'val2'));
        $this->cf->insert(self::$KEYS[0], $cols);
        $this->cf->insert(self::$KEYS[1], $cols);
        $res = $this->cf->multiget(array(self::$KEYS[0], self::$KEYS[1]));

        $expected = array(array(self::$KEYS[0], $cols),
                          array(self::$KEYS[1], $cols));
        $this->assertEquals(sort($expected), sort($res));
    }

    public function test_get_range() {
        $cols = array(array('col', 'val'), array('col2', 'val2'));
        $rows = array(array(self::$KEYS[0], $cols),
                      array(self::$KEYS[1], $cols),
                      array(self::$KEYS[2], $cols));
        $this->cf->batch_insert($rows);

        $result = iterator_to_array($this->cf->get_range());
        $this->assertEquals(sort($rows), sort($result));
    }

    public function test_get_indexed_slices() {
        $cf = new ColumnFamily($this->pool, 'Indexed1');
        $cf->insert_format = ColumnFamily::ARRAY_FORMAT;
        $cf->return_format = ColumnFamily::ARRAY_FORMAT;

        $cols = array(array('col', 'val'), array('birthdate', 1));
        $rows = array(array(self::$KEYS[0], $cols),
                      array(self::$KEYS[1], $cols),
                      array(self::$KEYS[2], $cols));
        $cf->batch_insert($rows);

        $expr = new IndexExpression('birthdate', 1);
        $clause = new IndexClause(array($expr));
        $result = iterator_to_array($cf->get_indexed_slices($clause));
        $this->assertEquals(sort($rows), sort($result));
    }
}
