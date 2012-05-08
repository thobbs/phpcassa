<?php

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\SuperColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

use phpcassa\Index\IndexExpression;
use phpcassa\Index\IndexClause;

use phpcassa\UUID;

class ArrayFormatSuperCFTest extends PHPUnit_Framework_TestCase {

    private static $KEYS = array(0.25, 0.5, 0.75);
    private static $KS = "TestColumnFamily";
    protected static $CF = "Super1";

    protected static $cfattrs = array(
        "column_type" => "Super",
        "key_validation_class" => "FloatType",
        "subcomparator_type" => "TimeUUIDType"
    );

    public static function setUpBeforeClass() {
        try {
            $sys = new SystemManager();

            $ksdefs = $sys->describe_keyspaces();
            $exists = False;
            foreach ($ksdefs as $ksdef)
                $exists = $exists || $ksdef->name == self::$KS;

            if (!$exists) {
                $sys->create_keyspace(self::$KS, array());
                $sys->create_column_family(self::$KS, self::$CF, self::$cfattrs);
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
        $this->subcols = array(array(UUID::uuid1(), 'val1'),
                               array(UUID::uuid1(), 'val2'));

        $this->pool = new ConnectionPool(self::$KS);
        $this->cf = new SuperColumnFamily($this->pool, self::$CF);
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

    public function sort_rows($a, $b) {
        if ($a[0] === $b[0])
            return 0;
        return $a[0] < $b[0] ? -1 : 1;
    }

    public function test_get() {
        $cols = array(array('super1', $this->subcols),
                      array('super2', $this->subcols));
        $this->cf->insert(self::$KEYS[0], $cols);
        $res = $this->cf->get(self::$KEYS[0]);

        $this->assertEquals($cols, $res);
    }

    public function test_get_super_column() {
        $cols = array(array('super1', $this->subcols));
        $this->cf->insert(self::$KEYS[0], $cols);
        $res = $this->cf->get_super_column(self::$KEYS[0], 'super1');

        $this->assertEquals($this->subcols, $res);
    }

    public function test_multiget() {
        $cols = array(array('super1', $this->subcols),
                      array('super2', $this->subcols));
        $this->cf->insert(self::$KEYS[0], $cols);
        $this->cf->insert(self::$KEYS[1], $cols);
        $result = $this->cf->multiget(array(self::$KEYS[0], self::$KEYS[1]));

        $expected = array(array(self::$KEYS[0], $cols),
                          array(self::$KEYS[1], $cols));

        usort($expected, array("ArrayFormatSuperCFTest", "sort_rows"));
        usort($result, array("ArrayFormatSuperCFTest", "sort_rows"));
        $this->assertEquals($expected, $result);
    }

    public function test_multiget_count() {
        $cols = array(array('super1', $this->subcols),
                      array('super2', $this->subcols));
        $this->cf->insert(self::$KEYS[0], $cols);
        $this->cf->insert(self::$KEYS[1], $cols);
        $result = $this->cf->multiget_count(array(self::$KEYS[0], self::$KEYS[1]));
        usort($result, array("ArrayFormatSuperCFTest", "sort_rows"));

        $expected = array(array(self::$KEYS[0], 2),
                          array(self::$KEYS[1], 2));

        $this->assertEquals($expected, $result);
    }

    public function test_multiget_super_column() {
        $cols = array(array('super1', $this->subcols));
        $this->cf->insert(self::$KEYS[0], $cols);
        $this->cf->insert(self::$KEYS[1], $cols);

        $keys = array(self::$KEYS[0], self::$KEYS[1]);
        $result = $this->cf->multiget_super_column($keys, 'super1');

        $expected = array(array(self::$KEYS[0], $this->subcols),
                          array(self::$KEYS[1], $this->subcols));

        usort($expected, array("ArrayFormatSuperCFTest", "sort_rows"));
        usort($result, array("ArrayFormatSuperCFTest", "sort_rows"));
        $this->assertEquals($expected, $result);
    }

    public function test_multiget_subcolumn_count() {
        $cols = array(array('super1', $this->subcols));
        $this->cf->insert(self::$KEYS[0], $cols);
        $this->cf->insert(self::$KEYS[1], $cols);

        $keys = array(self::$KEYS[0], self::$KEYS[1]);
        $result = $this->cf->multiget_subcolumn_count($keys, 'super1');
        usort($result, array("ArrayFormatSuperCFTest", "sort_rows"));

        $expected = array(array(self::$KEYS[0], 2),
                          array(self::$KEYS[1], 2));

        $this->assertEquals($expected, $result);
    }

    public function test_get_range() {
        $cols = array(array('super1', $this->subcols),
                      array('super2', $this->subcols));
        $rows = array(array(self::$KEYS[0], $cols),
                      array(self::$KEYS[1], $cols),
                      array(self::$KEYS[2], $cols));
        $this->cf->batch_insert($rows);

        $result = iterator_to_array($this->cf->get_range());
        usort($rows, array("ArrayFormatSuperCFTest", "sort_rows"));
        usort($result, array("ArrayFormatSuperCFTest", "sort_rows"));
        $this->assertEquals($rows, $result);
    }

    public function test_get_super_column_range() {
        $cols = array(array('super1', $this->subcols));
        $rows = array(array(self::$KEYS[0], $cols),
                      array(self::$KEYS[1], $cols),
                      array(self::$KEYS[2], $cols));
        $this->cf->batch_insert($rows);

        $result = $this->cf->get_super_column_range('super1');
        $result = iterator_to_array($result);

        $expected = array(array(self::$KEYS[0], $this->subcols),
                          array(self::$KEYS[1], $this->subcols),
                          array(self::$KEYS[2], $this->subcols));
        usort($expected, array("ArrayFormatSuperCFTest", "sort_rows"));
        usort($result, array("ArrayFormatSuperCFTest", "sort_rows"));
        $this->assertEquals($expected, $result);
    }

}
