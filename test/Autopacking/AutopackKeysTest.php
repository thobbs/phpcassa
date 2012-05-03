<?php
require_once(__DIR__.'/AutopackBase.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

use phpcassa\UUID;

class AutopackKeysTest extends AutopackBase {

    public static function setUpBeforeClass() {
        parent::setUpBeforeClass();
        $sys = new SystemManager();
        $cfattrs = array("column_type" => "Standard");

        $cfattrs["key_validation_class"] = DataType::LONG_TYPE;
        $sys->create_column_family(self::$KS, 'LongKeys', $cfattrs);
        $sys->create_index(self::$KS, 'LongKeys', 'subcol',
            DataType::TIME_UUID_TYPE, NULL, NULL);
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);
        $this->cf = new ColumnFamily($this->client, 'LongKeys');
    }

    public function tearDown() { }

    public function test_get() {
        $this->cf->insert(123, array("foo" => "bar"));
        $this->assertEquals(array("foo" => "bar"), $this->cf->get(123));
    }

    public function test_multiget() {
        $this->cf->insert(1, array("a" => "a"));
        $this->cf->insert(2, array("b" => "b"));
        $res = $this->cf->multiget(array(1, 2));
        $this->assertEquals(array(1 => array("a" => "a"),
                                  2 => array("b" => "b")),
                            $res);
    }

    /**
     * @expectedException cassandra_NotFoundException
     */
    public function test_remove() {
        $this->cf->insert(123, array("foo" => "bar"));
        $this->assertEquals(array("foo" => "bar"), $this->cf->get(123));
        $this->cf->remove(123);
        $this->cf->get(123);
    }

    public function test_get_range() {
        $this->cf = new ColumnFamily($this->client, 'LongKeys');
        $this->cf->truncate();
        $this->cf->insert(1, array("a" => "a"));
        $this->cf->insert(2, array("b" => "b"));
        $res_array = array();
        $res = iterator_to_array($this->cf->get_range());
        $this->assertEquals(array("1" => array("a" => "a"),
                                  "2" => array("b" => "b")),
                            $res);
    }
}
