<?php
require_once(__DIR__.'/SuperBase.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\SuperColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

class AutopackSuperColumnsTest extends SuperBase {

    public static function setUpBeforeClass() {
        parent::setUpBeforeClass();

        $sys = new SystemManager();
        $cfattrs = array("column_type" => "Super");

        if (self::$have64Bit) {
            $cfattrs["comparator_type"] = DataType::LONG_TYPE;
            $sys->create_column_family(self::$KS, 'SuperLong', $cfattrs);
        }

        $cfattrs["comparator_type"] = DataType::INTEGER_TYPE;
        $sys->create_column_family(self::$KS, 'SuperInt', $cfattrs);

        $cfattrs["comparator_type"] = DataType::ASCII_TYPE;
        $sys->create_column_family(self::$KS, 'SuperAscii', $cfattrs);

        $cfattrs["comparator_type"] = DataType::UTF8_TYPE;
        $sys->create_column_family(self::$KS, 'SuperUTF8', $cfattrs);
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);

        if (self::$have64Bit) {
            $this->cf_suplong  = new SuperColumnFamily($this->client, 'SuperLong');
        }
        $this->cf_supint   = new SuperColumnFamily($this->client, 'SuperInt');
        $this->cf_supascii = new SuperColumnFamily($this->client, 'SuperAscii');
        $this->cf_suputf8  = new SuperColumnFamily($this->client, 'SuperUTF8');

        $this->cfs = array($this->cf_supint,
                           $this->cf_supascii, $this->cf_suputf8);
        if (self::$have64Bit) {
            $this->cfs[] = $this->cf_suplong;
        }
    }

    protected function make_type_groups() {
        $type_groups = array();

        if (self::$have64Bit) {
            $long_cols = array(111111111111,
                               222222222222,
                               333333333333);
            $type_groups[] = self::make_super_group($this->cf_suplong, $long_cols);
        }

        $int_cols = array(1, 2, 3);
        $type_groups[] = self::make_super_group($this->cf_supint, $int_cols);

        $ascii_cols = array('aaaa', 'bbbb', 'cccc');
        $type_groups[] = self::make_super_group($this->cf_supascii, $ascii_cols);

        $utf8_cols = array("a&#1047;", "b&#1048;", "c&#1049;"); 
        $type_groups[] = self::make_super_group($this->cf_suputf8, $utf8_cols);

        return $type_groups;
    }
}
