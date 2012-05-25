<?php
require_once(__DIR__.'/AutopackBase.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\Index\IndexClause;
use phpcassa\Index\IndexExpression;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

use phpcassa\UUID;

class AutopackKeysTest extends AutopackBase {

    public static function setUpBeforeClass() {
        parent::setUpBeforeClass();
        $sys = new SystemManager();

        $cfattrs = array(
            "column_type" => "Standard",
            "key_validation_class" => DataType::LONG_TYPE
        );
        $sys->create_column_family(self::$KS, 'LongKeys', $cfattrs);
        $sys->create_index(self::$KS, 'LongKeys', 'subcol', DataType::LONG_TYPE);

        $cfattrs = array(
            "column_type" => "Standard",
            "key_validation_class" => DataType::UUID_TYPE
        );
        $sys->create_column_family(self::$KS, 'UUIDKeys', $cfattrs);
        $sys->create_index(self::$KS, 'UUIDKeys', 'subcol', DataType::LONG_TYPE);
        $sys->create_index(self::$KS, 'UUIDKeys', 'subcol', DataType::LONG_TYPE);
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);
        $this->cf = new ColumnFamily($this->client, 'LongKeys');
        $this->uuid_cf = new ColumnFamily($this->client, 'UUIDKeys');
    }

    public function tearDown() {
        $this->client->close();
    }

    public function test_get() {
        $this->cf->insert(123, array("foo" => "bar"));
        $this->assertEquals(array("foo" => "bar"), $this->cf->get(123));
    }

    public function test_get_serialized() {
        $uuid = UUID::uuid1();
        $this->uuid_cf->insert($uuid, array("foo" => "bar"));
        $this->assertEquals(array("foo" => "bar"), $this->uuid_cf->get($uuid));
    }

    public function test_multiget() {
        $this->cf->insert(1, array("a" => "a"));
        $this->cf->insert(2, array("b" => "b"));
        $res = $this->cf->multiget(array(1, 2));
        $this->assertEquals(array(1 => array("a" => "a"),
                                  2 => array("b" => "b")),
                            $res);
    }

    public function test_multiget_serialized() {
        $uuid1 = UUID::uuid1();
        $uuid2 = UUID::uuid1();
        $this->uuid_cf->insert($uuid1, array("a" => "a"));
        $this->uuid_cf->insert($uuid2, array("b" => "b"));
        $res = $this->uuid_cf->multiget(array($uuid1, $uuid2));
        $this->assertEquals(array(serialize($uuid1) => array("a" => "a"),
                                  serialize($uuid2) => array("b" => "b")),
                            $res);
    }

    public function test_remove() {
        $this->cf->insert(123, array("foo" => "bar"));
        $this->assertEquals(array("foo" => "bar"), $this->cf->get(123));
        $this->cf->remove(123);
        $this->setExpectedException('\cassandra\NotFoundException');
        $this->cf->get(123);
    }

    public function test_remove_serialized() {
        $uuid = UUID::uuid1();
        $this->uuid_cf->insert($uuid, array("foo" => "bar"));
        $this->assertEquals(array("foo" => "bar"), $this->uuid_cf->get($uuid));
        $this->uuid_cf->remove($uuid);
        $this->setExpectedException('\cassandra\NotFoundException');
        $this->uuid_cf->get($uuid);
    }

    public function test_get_range() {
        $this->cf->truncate();
        $this->cf->insert(0, array("a" => "a"));
        $this->cf->insert(1, array("b" => "b"));
        $this->cf->insert(2, array("c" => "c"));

        $this->cf->buffer_size = 2;

        $res = iterator_to_array($this->cf->get_range());
        $this->assertEquals(array(0 => array("a" => "a"),
                                  1 => array("b" => "b"),
                                  2 => array("c" => "c")),
                            $res);
    }

    public function test_get_range_serialized() {
        $this->uuid_cf->truncate();
        $uuid1 = UUID::uuid1();
        $uuid2 = UUID::uuid1();
        $uuid3 = UUID::uuid1();
        $this->uuid_cf->insert($uuid1, array("a" => "a"));
        $this->uuid_cf->insert($uuid2, array("b" => "b"));
        $this->uuid_cf->insert($uuid3, array("c" => "c"));

        $this->uuid_cf->buffer_size = 2;
        $res = iterator_to_array($this->uuid_cf->get_range());
        $this->assertEquals(array(serialize($uuid1) => array("a" => "a"),
                                  serialize($uuid2) => array("b" => "b"),
                                  serialize($uuid3) => array("c" => "c")),
                            $res);
    }

    public function test_get_indexed_slices() {
        $this->cf->truncate();
        $this->cf->insert(0, array("subcol" => 0));
        $this->cf->insert(1, array("subcol" => 1));
        $this->cf->insert(2, array("subcol" => 1));
        $this->cf->insert(3, array("subcol" => 1));

        $this->cf->buffer_size = 2;

        $expr = new IndexExpression("subcol", 1);
        $clause = new IndexClause(array($expr));
        $res = iterator_to_array($this->cf->get_indexed_slices($clause));
        $this->assertEquals(array(1 => array("subcol" => 1),
                                  2 => array("subcol" => 1),
                                  3 => array("subcol" => 1)),
                            $res);

        $expr = new IndexExpression("subcol", 0);
        $clause = new IndexClause(array($expr));
        $res = iterator_to_array($this->cf->get_indexed_slices($clause));
        $this->assertEquals(array(0 => array("subcol" => 0)), $res);
    }

    public function test_get_indexed_slices_serialized() {
        $this->uuid_cf->truncate();

        $uuid1 = UUID::uuid1();
        $uuid2 = UUID::uuid1();
        $uuid3 = UUID::uuid1();
        $this->uuid_cf->insert($uuid1, array("subcol" => 0));
        $this->uuid_cf->insert($uuid2, array("subcol" => 1));
        $this->uuid_cf->insert($uuid3, array("subcol" => 1));

        $this->uuid_cf->buffer_size = 2;

        $expr = new IndexExpression("subcol", 1);
        $clause = new IndexClause(array($expr));
        $res = iterator_to_array($this->uuid_cf->get_indexed_slices($clause));
        $this->assertEquals(array(serialize($uuid2) => array("subcol" => 1),
                                  serialize($uuid3) => array("subcol" => 1)),
                            $res);

        $expr = new IndexExpression("subcol", 0);
        $clause = new IndexClause(array($expr));
        $res = iterator_to_array($this->uuid_cf->get_indexed_slices($clause));
        $this->assertEquals(array(serialize($uuid1) => array("subcol" => 0)), $res);
    }

}
