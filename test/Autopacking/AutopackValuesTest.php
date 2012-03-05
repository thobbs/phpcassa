<?php

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

use phpcassa\UUID;
use phpcassa\UUID\UUIDGen;

use phpcassa\UUID\DataType\LongType;
use phpcassa\UUID\DataType\IntegerType;
use phpcassa\UUID\DataType\BytesType;
use phpcassa\UUID\DataType\AsciiType;
use phpcassa\UUID\DataType\UTF8Type;
use phpcassa\UUID\DataType\LexicalUUIDType;
use phpcassa\UUID\DataType\TimeUUIDType;

class AutopackValuesTest extends PHPUnit_Framework_TestCase {

    private static $VALS = array('val1', 'val2', 'val3');
    private static $KEYS = array('key1', 'key2', 'key3');
    private static $KS = "TestAutopacking";

    private $client;
    private $cf;

    public static function setUpBeforeClass() {
        $sys = new SystemManager();

        $ksdefs = $sys->describe_keyspaces();
        $exists = False;
        foreach ($ksdefs as $ksdef)
            $exists = $exists || $ksdef->name == self::$KS;

        if ($exists)
            $sys->drop_keyspace(self::$KS);

        $sys->create_keyspace(self::$KS, array());

        $cfattrs = array("column_type" => "Standard");

        $cfattrs["default_validation_class"] = DataType::LONG_TYPE;
        $sys->create_column_family(self::$KS, 'ValidatorLong', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::INTEGER_TYPE;
        $sys->create_column_family(self::$KS, 'ValidatorInt', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::TIME_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'ValidatorTime', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::LEXICAL_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'ValidatorLex', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::ASCII_TYPE;
        $sys->create_column_family(self::$KS, 'ValidatorAscii', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::UTF8_TYPE;
        $sys->create_column_family(self::$KS, 'ValidatorUTF8', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::BYTES_TYPE;
        $sys->create_column_family(self::$KS, 'ValidatorBytes', $cfattrs);

        $cfattrs["default_validation_class"] = "CompositeType(LongType, AsciiType)";
        $sys->create_column_family(self::$KS, 'ValidatorComposite', $cfattrs);


        $cfattrs["default_validation_class"] = DataType::LONG_TYPE;
        $sys->create_column_family(self::$KS, 'DefaultValidator', $cfattrs);
        // Quick way to create a TimeUUIDType validator to subcol
        $sys->create_index(self::$KS, 'DefaultValidator', 'subcol',
            DataType::TIME_UUID_TYPE, NULL, NULL);
    }

    public static function tearDownAfterClass() {
        $sys = new SystemManager();
        $sys->drop_keyspace(self::$KS);
        $sys->close();
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);

        $this->TIME2 = UUIDGen::uuid1();
        $this->TIME3 = UUIDGen::uuid1();

        $this->LEX2 = UUID::import('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')->bytes;
        $this->LEX3 = UUID::import('cccccccccccccccccccccccccccccccc')->bytes;
    }

    public function tearDown() { }

    public function test_longs() {
        $this->cf_valid_long = new ColumnFamily($this->client, 'ValidatorLong');
        $col = array('subcol' => 222222222222);
        $this->cf_valid_long->insert(self::$KEYS[0], $col);
        $this->assertEquals($col, $this->cf_valid_long->get(self::$KEYS[0]));
    }

    public function test_integers() {
        $this->cf_valid_int = new ColumnFamily($this->client, 'ValidatorInt');
        $col = array('subcol' => 2);
        $this->cf_valid_int->insert(self::$KEYS[0], $col);
        $this->assertEquals($col, $this->cf_valid_int->get(self::$KEYS[0]));
    }

    public function test_time_uuids() {
        $this->TIME = UUIDGen::uuid1();
        $this->cf_valid_time = new ColumnFamily($this->client, 'ValidatorTime');
        $col = array('subcol' => $this->TIME);
        $this->cf_valid_time->insert(self::$KEYS[0], $col);
        $this->assertEquals($col, $this->cf_valid_time->get(self::$KEYS[0]));
    }

    public function test_lexical_uuids() {
        $this->LEX = UUID::import('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')->bytes;
        $this->cf_valid_lex = new ColumnFamily($this->client, 'ValidatorLex');
        $col = array('subcol' => $this->LEX);
        $this->cf_valid_lex->insert(self::$KEYS[0], $col);
        $this->assertEquals($col, $this->cf_valid_lex->get(self::$KEYS[0]));
    }

    public function test_ascii() {
        $this->cf_valid_ascii = new ColumnFamily($this->client, 'ValidatorAscii');
        $col = array('subcol' => 'aaa');
        $this->cf_valid_ascii->insert(self::$KEYS[0], $col);
        $this->assertEquals($col, $this->cf_valid_ascii->get(self::$KEYS[0]));
    }

    public function test_utf8() {
        $this->cf_valid_utf8 = new ColumnFamily($this->client, 'ValidatorUTF8');
        $col = array('subcol' => "a&#1047;");
        $this->cf_valid_utf8->insert(self::$KEYS[0], $col);
        $this->assertEquals($col, $this->cf_valid_utf8->get(self::$KEYS[0]));
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
        $this->TIME = UUIDGen::uuid1();
        $this->LEX = UUID::import('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')->bytes;
        $this->cf_def_valid = new ColumnFamily($this->client, 'DefaultValidator');
        $col_cf = array('aaaaaa' => 222222222222);
        $col_cm = array('subcol' => $this->TIME);

        # Both of these inserts work, as cf allows
        # longs and cm for 'subcol' allows TimeUUIDs
        $this->cf_def_valid->insert(self::$KEYS[0], $col_cf);
        $this->cf_def_valid->insert(self::$KEYS[0], $col_cm);
        $this->assertEquals($this->cf_def_valid->get(self::$KEYS[0]),
                          array('aaaaaa' => 222222222222, 'subcol' => $this->TIME));
    }
}
