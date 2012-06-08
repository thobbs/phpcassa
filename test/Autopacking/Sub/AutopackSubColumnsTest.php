<?php
require_once(__DIR__.'/SubBase.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\SuperColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

class AutopackSubColumnsTest extends SubBase {

    protected $SERIALIZED = false;

    public static function setUpBeforeClass() {
        parent::setUpBeforeClass();

        $sys = new SystemManager();
        $cfattrs = array("column_type" => "Super", "comparator_type" => "Int32Type");

        if (self::$have64Bit) {
            $cfattrs["subcomparator_type"] = DataType::LONG_TYPE;
            $sys->create_column_family(self::$KS, 'SuperLongSubLong', $cfattrs);
        }

        $cfattrs["subcomparator_type"] = DataType::INTEGER_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubInt', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::ASCII_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubAscii', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::UTF8_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubUTF8', $cfattrs);
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);

        if (self::$have64Bit) {
            $this->cf_suplong_sublong  = new SuperColumnFamily(
                $this->client, 'SuperLongSubLong');
        }
        $this->cf_suplong_subint   = new SuperColumnFamily(
            $this->client, 'SuperLongSubInt');
        $this->cf_suplong_subascii = new SuperColumnFamily(
            $this->client, 'SuperLongSubAscii');
        $this->cf_suplong_subutf8  = new SuperColumnFamily(
            $this->client, 'SuperLongSubUTF8');

        $this->cfs = array($this->cf_suplong_subint,
                           $this->cf_suplong_subascii, $this->cf_suplong_subutf8);
        if (self::$have64Bit) {
            $this->cfs[] = $this->cf_suplong_sublong;
        }
    }

    public function make_type_groups() {
        $type_groups = array();

        if (self::$have64Bit) {
            $long_cols = array(111111111111,
                               222222222222,
                               333333333333);
            $type_groups[] = self::make_sub_group($this->cf_suplong_sublong, $long_cols);
        }

        $int_cols = array(1, 2, 3);
        $type_groups[] = self::make_sub_group($this->cf_suplong_subint, $int_cols);

        $ascii_cols = array('aaaa', 'bbbb', 'cccc');
        $type_groups[] = self::make_sub_group($this->cf_suplong_subascii, $ascii_cols);

        $utf8_cols = array("a&#1047;", "b&#1048;", "c&#1049;"); 
        $type_groups[] = self::make_sub_group($this->cf_suplong_subutf8, $utf8_cols);

        return $type_groups;
    }
}
