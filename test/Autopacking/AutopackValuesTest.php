<?php
require_once(__DIR__.'/AutopackBase.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\Schema\DataType\IntegerType;
use phpcassa\SystemManager;

use phpcassa\UUID;

class AutopackValuesTest extends AutopackBase {

    public static function setUpBeforeClass() {
        parent::setUpBeforeClass();
        $sys = new SystemManager();
        $cfattrs = array("column_type" => "Standard");

        $cfattrs["default_validation_class"] = DataType::LONG_TYPE;
        $sys->create_column_family(self::$KS, 'ValidatorLong', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::TIME_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'ValidatorTime', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::BYTES_TYPE;
        $sys->create_column_family(self::$KS, 'ValidatorBytes', $cfattrs);

        $cfattrs["default_validation_class"] = "CompositeType(LongType, AsciiType)";
        $sys->create_column_family(self::$KS, 'ValidatorComposite', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::LONG_TYPE;
        $sys->create_column_family(self::$KS, 'DefaultValidator', $cfattrs);
        $sys->alter_column(self::$KS, 'DefaultValidator', 'subcol', 'TimeUUIDType');

        $cfattrs = array(
            "comparator_type" => "CompositeType(Int32Type, AsciiType)",
            "default_validation_class" => "Int32Type"
        );
        $sys->create_column_family(self::$KS, 'CompositeNames', $cfattrs);
        $sys->alter_column(self::$KS, 'CompositeNames', array(0, 'meta'), 'AsciiType');
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);
    }

    public function tearDown() { }

    public function test_longs() {
        $this->cf_valid_long = new ColumnFamily($this->client, 'ValidatorLong');
        $col = array('subcol' => 222222222222);
        $this->cf_valid_long->insert(self::$KEYS[0], $col);
        $this->assertEquals($col, $this->cf_valid_long->get(self::$KEYS[0]));
    }

    public function test_time_uuids() {
        $time = UUID::mint();
        $this->cf_valid_time = new ColumnFamily($this->client, 'ValidatorTime');
        $col = array('subcol' => $time);
        $this->cf_valid_time->insert(self::$KEYS[0], $col);
        $this->assertEquals($col, $this->cf_valid_time->get(self::$KEYS[0]));
    }

    public function test_bytes() {
        $this->cf_valid_bytes = new ColumnFamily($this->client, 'ValidatorBytes');
        $col = array('subcol' => 'aaa123');
        $this->cf_valid_bytes->insert(self::$KEYS[0], $col);
        $this->assertEquals($col, $this->cf_valid_bytes->get(self::$KEYS[0]));
    }

    public function test_composite() {
        $this->cf_valid_composite = new ColumnFamily($this->client, 'ValidatorComposite');
        $cols = array('subcol' => array(1, 'a'));
        $this->cf_valid_composite->insert(self::$KEYS[0], $cols);
        $this->assertEquals($cols,
            $this->cf_valid_composite->get(self::$KEYS[0]));
    }

    public function test_default_validated_columns() {
        $time = UUID::mint();
        $this->cf_def_valid = new ColumnFamily($this->client, 'DefaultValidator');
        $col_cf = array('aaaaaa' => 222222222222);
        $col_cm = array('subcol' => $time);

        # Both of these inserts work, as cf allows
        # longs and cm for 'subcol' allows TimeUUIDs
        $this->cf_def_valid->insert(self::$KEYS[0], $col_cf);
        $this->cf_def_valid->insert(self::$KEYS[0], $col_cm);
        $this->assertEquals($this->cf_def_valid->get(self::$KEYS[0]),
                          array('aaaaaa' => 222222222222, 'subcol' => $time));
    }

    public function test_serialized_col_name_validators() {
        $cf = new ColumnFamily($this->client, 'CompositeNames');

        $columns = array(
            serialize(array(0, 'meta')) => 'foo',
            serialize(array(1, 'blah')) => 10
        );

        $cf->insert('key', $columns);
        $this->assertEquals($columns, $cf->get('key'));
    }

    public function test_integer_type() {
        $i = new IntegerType();
        $to_test = array(
            0, 1, 2, 127, 128, 129, 255, 256, 257, 65535, 65536, 65537,
            -1, -2, -127, -128, -129, -255, -256, -257, -65535, -65536, -65537);
        foreach ($to_test as $test_val) {
            $this->assertEquals($test_val, $i->unpack($i->pack($test_val)));
        }
    }
}
