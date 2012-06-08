<?php
require_once(__DIR__.'/SubBase.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\SuperColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

use phpcassa\UUID;

class AutopackSerializedSubColumnsTest extends SubBase {

    protected $SERIALIZED = true;

    public static function setUpBeforeClass() {
        parent::setUpBeforeClass();

        $sys = new SystemManager();
        $cfattrs = array("column_type" => "Super", "comparator_type" => "Int32Type");

        $cfattrs["subcomparator_type"] = DataType::FLOAT_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubFloat', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::DOUBLE_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubDouble', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::TIME_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubTime', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::LEXICAL_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubLex', $cfattrs);

        $cfattrs["subcomparator_type"] = "CompositeType(Int32Type, AsciiType)";
        $sys->create_column_family(self::$KS, 'SuperLongSubComposite', $cfattrs);
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);

        $this->cf_suplong_subfloat     = new SuperColumnFamily($this->client, 'SuperLongSubFloat');
        $this->cf_suplong_subdouble    = new SuperColumnFamily($this->client, 'SuperLongSubDouble');
        $this->cf_suplong_subtime      = new SuperColumnFamily($this->client, 'SuperLongSubTime');
        $this->cf_suplong_sublex       = new SuperColumnFamily($this->client, 'SuperLongSubLex');
        $this->cf_suplong_subcomposite = new SuperColumnFamily($this->client, 'SuperLongSubComposite');

        $this->cfs = array($this->cf_suplong_subfloat, $this->cf_suplong_subdouble,
                           $this->cf_suplong_subtime, $this->cf_suplong_sublex,
                           $this->cf_suplong_subcomposite);

        $this->TIME1 = UUID::mint();
        $this->TIME2 = UUID::mint();
        $this->TIME3 = UUID::mint();

        $this->LEX1 = UUID::import('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
        $this->LEX2 = UUID::import('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');
        $this->LEX3 = UUID::import('cccccccccccccccccccccccccccccccc');
    }

    public function make_type_groups() {
        $type_groups = array();

        $float_cols = array(1.25, 1.5, 1.75);
        $type_groups[] = self::make_sub_group($this->cf_suplong_subfloat, $float_cols);

        $double_cols = array(1.25, 1.5, 1.75);
        $type_groups[] = self::make_sub_group($this->cf_suplong_subdouble, $double_cols);

        $time_cols = array($this->TIME1, $this->TIME2, $this->TIME3);
        $type_groups[] = self::make_sub_group($this->cf_suplong_subtime, $time_cols);

        $lex_cols = array($this->LEX1, $this->LEX2, $this->LEX3);
        $type_groups[] = self::make_sub_group($this->cf_suplong_sublex, $lex_cols);

        $composite_cols = array(array(1, 'a'), array(2, 'b'), array(3, 'c'));
        $type_groups[] = self::make_sub_group($this->cf_suplong_subcomposite, $composite_cols);

        return $type_groups;
    }
}
