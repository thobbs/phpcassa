<?php

require_once(__DIR__.'/StandardBase.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

class AutopackStandardTest extends StandardBase {

    public static function setUpBeforeClass() {
        parent::setUpBeforeClass();

        $sys = new SystemManager();

        if (self::$have64Bit) {
            $cfattrs = array("comparator_type" => DataType::LONG_TYPE);
            $sys->create_column_family(self::$KS, 'StdLong', $cfattrs);
        }

        $cfattrs = array("comparator_type" => DataType::INTEGER_TYPE);
        $sys->create_column_family(self::$KS, 'StdInteger', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::INT32_TYPE);
        $sys->create_column_family(self::$KS, 'StdInt32', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::ASCII_TYPE);
        $sys->create_column_family(self::$KS, 'StdAscii', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::UTF8_TYPE);
        $sys->create_column_family(self::$KS, 'StdUTF8', $cfattrs);
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);

        if (self::$have64Bit) {
            $this->cf_long  = new ColumnFamily($this->client, 'StdLong');
        }
        $this->cf_int   = new ColumnFamily($this->client, 'StdInteger');
        $this->cf_int32  = new ColumnFamily($this->client, 'StdInt32');
        $this->cf_ascii = new ColumnFamily($this->client, 'StdAscii');
        $this->cf_utf8  = new ColumnFamily($this->client, 'StdUTF8');

        $this->cfs = array($this->cf_int, $this->cf_ascii, $this->cf_utf8);
        if (self::$have64Bit) {
            $this->cfs[] = $this->cf_long;
        }
    }

    public function test_false_colnames() {
        $this->cf_int->insert(self::$KEYS[0], array(0 => "foo"));
        $this->assertEquals($this->cf_int->get(self::$KEYS[0]), array(0 => "foo"));
        $this->cf_int->remove(self::$KEYS[0]);
        $this->setExpectedException('UnexpectedValueException');
        $this->cf_int->insert(self::$KEYS[0], array(null => "foo"));
    }

    protected function make_type_groups() {
        $type_groups = array();

        if (self::$have64Bit) {
            $long_cols = array(111111111111,
                               222222222222,
                               333333333333);
            $type_groups[] = $this->make_group($this->cf_long, $long_cols);
        }

        $int_cols = array(1, 2, 3);
        $type_groups[] = $this->make_group($this->cf_int, $int_cols);

        $int32_cols = array(-123456789, 0, 123456789);
        $type_groups[] = $this->make_group($this->cf_int32, $int32_cols);

        $ascii_cols = array('aaaa', 'bbbb', 'cccc');
        $type_groups[] = $this->make_group($this->cf_ascii, $ascii_cols);

        $utf8_cols = array("a&#1047;", "b&#1048;", "c&#1049;"); 
        $type_groups[] = $this->make_group($this->cf_utf8, $utf8_cols);

        return $type_groups;
    }
}
