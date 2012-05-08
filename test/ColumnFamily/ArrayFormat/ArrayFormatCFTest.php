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
    protected static $CF = "Standard1";

    protected static $cfattrs = array("column_type" => "Standard");
    protected $cols = array(array('col1', 'val1'), array('col2', 'val2'));

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

            $sys->create_column_family(self::$KS, self::$CF, self::$cfattrs);

            $sys->create_column_family(self::$KS, 'Indexed1');
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
        $this->cf = new ColumnFamily($this->pool, self::$CF);
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
        $this->cf->insert(self::$KEYS[0], $this->cols);
        $res = $this->cf->get(self::$KEYS[0]);
        $this->assertEquals($this->cols, $res);
    }

    public function test_multiget() {
        $this->cf->insert(self::$KEYS[0], $this->cols);
        $this->cf->insert(self::$KEYS[1], $this->cols);
        $res = $this->cf->multiget(array(self::$KEYS[0], self::$KEYS[1]));

        $expected = array(array(self::$KEYS[0], $this->cols),
                          array(self::$KEYS[1], $this->cols));
        $this->assertEquals(sort($expected), sort($res));
    }

    public function test_get_range() {
        $rows = array(array(self::$KEYS[0], $this->cols),
                      array(self::$KEYS[1], $this->cols),
                      array(self::$KEYS[2], $this->cols));
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
