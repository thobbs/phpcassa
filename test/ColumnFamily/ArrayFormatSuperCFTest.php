<?php

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\SuperColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

use phpcassa\Index\IndexExpression;
use phpcassa\Index\IndexClause;

function sort_rows($a, $b) {
    if ($a[0] === $b[0])
        return 0;
    return $a[0] < $b[0] ? -1 : 1;
}

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

            if (!$exists) {
                $sys->create_keyspace(self::$KS, array());

                $cfattrs = array("column_type" => "Super");
                $sys->create_column_family(self::$KS, 'Super1', $cfattrs);
            }
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
        $this->cf = new SuperColumnFamily($this->pool, 'Super1');
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
        $cols = array(array('super1', array(array('col', 'val'), array('col2', 'val2'))),
                      array('super2', array(array('col', 'val'), array('col2', 'val2'))));
        $this->cf->insert(self::$KEYS[0], $cols);
        $res = $this->cf->get(self::$KEYS[0]);

        $this->assertEquals($cols, $res);
    }

    public function test_get_super_column() {
        $subcols = array(array('col', 'val'), array('col2', 'val2'));
        $cols = array(array('super1', $subcols));
        $this->cf->insert(self::$KEYS[0], $cols);
        $res = $this->cf->get_super_column(self::$KEYS[0], 'super1');

        $this->assertEquals($subcols, $res);
    }

    public function test_multiget() {
        $cols = array(array('super1', array(array('col', 'val'), array('col2', 'val2'))),
                      array('super2', array(array('col', 'val'), array('col2', 'val2'))));
        $this->cf->insert(self::$KEYS[0], $cols);
        $this->cf->insert(self::$KEYS[1], $cols);
        $result = $this->cf->multiget(array(self::$KEYS[0], self::$KEYS[1]));

        $expected = array(array(self::$KEYS[0], $cols),
                          array(self::$KEYS[1], $cols));

        usort($expected, "sort_rows");
        usort($result, "sort_rows");
        $this->assertEquals($expected, $result);
    }

    public function test_multiget_super_column() {
        $subcols = array(array('col', 'val'), array('col2', 'val2'));
        $cols = array(array('super1', $subcols));
        $this->cf->insert(self::$KEYS[0], $cols);
        $this->cf->insert(self::$KEYS[1], $cols);

        $keys = array(self::$KEYS[0], self::$KEYS[1]);
        $result = $this->cf->multiget_super_column($keys, 'super1');

        $expected = array(array(self::$KEYS[0], $subcols),
                          array(self::$KEYS[1], $subcols));

        usort($expected, "sort_rows");
        usort($result, "sort_rows");
        $this->assertEquals($expected, $result);
    }

    public function test_get_range() {
        $cols = array(array('super1', array(array('col', 'val'), array('col2', 'val2'))),
                      array('super2', array(array('col', 'val'), array('col2', 'val2'))));
        $rows = array(array(self::$KEYS[0], $cols),
                      array(self::$KEYS[1], $cols),
                      array(self::$KEYS[2], $cols));
        $this->cf->batch_insert($rows);

        $result = iterator_to_array($this->cf->get_range());
        usort($rows, "sort_rows");
        usort($result, "sort_rows");
        $this->assertEquals($rows, $result);
    }

    public function test_get_super_column_range() {
        $subcols = array(array('col', 'val'), array('col2', 'val2'));
        $cols = array(array('super1', $subcols));
        $rows = array(array(self::$KEYS[0], $cols),
                      array(self::$KEYS[1], $cols),
                      array(self::$KEYS[2], $cols));
        $this->cf->batch_insert($rows);

        $result = $this->cf->get_super_column_range('super1');
        $result = iterator_to_array($result);

        $expected = array(array(self::$KEYS[0], $subcols),
                          array(self::$KEYS[1], $subcols),
                          array(self::$KEYS[2], $subcols));
        usort($expected, "sort_rows");
        usort($result, "sort_rows");
        $this->assertEquals($expected, $result);
    }

}
