<?php
require_once(__DIR__.'/SuperBase.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

use phpcassa\UUID;
use phpcassa\UUID\UUIDGen;

class AutopackSerializedSupersTest extends SuperBase {

    protected $SERIALIZED = true;

    public static function setUpBeforeClass() {
        parent::setUpBeforeClass();

        $sys = new SystemManager();
        $cfattrs = array("column_type" => "Super");

        $cfattrs["comparator_type"] = DataType::FLOAT_TYPE;
        $sys->create_column_family(self::$KS, 'SuperFloat', $cfattrs);

        $cfattrs["comparator_type"] = DataType::DOUBLE_TYPE;
        $sys->create_column_family(self::$KS, 'SuperDouble', $cfattrs);

        $cfattrs["comparator_type"] = DataType::TIME_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'SuperTime', $cfattrs);

        $cfattrs["comparator_type"] = DataType::LEXICAL_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLex', $cfattrs);

        $cfattrs["comparator_type"] = "CompositeType(LongType, AsciiType)";
        $sys->create_column_family(self::$KS, 'SuperComposite', $cfattrs);
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);

        $this->cf_supfloat     = new ColumnFamily($this->client, 'SuperFloat');
        $this->cf_supdouble    = new ColumnFamily($this->client, 'SuperDouble');
        $this->cf_suptime      = new ColumnFamily($this->client, 'SuperTime');
        $this->cf_suplex       = new ColumnFamily($this->client, 'SuperLex');
        $this->cf_supcomposite = new ColumnFamily($this->client, 'SuperComposite');

        $this->cfs = array($this->cf_supfloat, $this->cf_supdouble, $this->cf_suptime,
                           $this->cf_suplex, $this->cf_supcomposite);

        $this->TIME1 = UUID::mint();
        $this->TIME2 = UUID::mint();
        $this->TIME3 = UUID::mint();

        $this->LEX1 = UUID::import('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
        $this->LEX2 = UUID::import('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb');
        $this->LEX3 = UUID::import('cccccccccccccccccccccccccccccccc');
    }

    protected function make_type_groups() {
        $type_groups = array();

        $float_cols = array(1.25, 1.5, 1.75);
        $type_groups[] = self::make_super_group($this->cf_supfloat, $float_cols);

        $double_cols = array(1.25, 1.5, 1.75);
        $type_groups[] = self::make_super_group($this->cf_supdouble, $double_cols);

        $time_cols = array($this->TIME1, $this->TIME2, $this->TIME3);
        $type_groups[] = self::make_super_group($this->cf_suptime, $time_cols);

        $lex_cols = array($this->LEX1, $this->LEX2, $this->LEX3);
        $type_groups[] = self::make_super_group($this->cf_suplex, $lex_cols);

        $composite_cols = array(array(1, 'a'), array(2, 'b'), array(3, 'c'));
        $type_groups[] = self::make_super_group($this->cf_supcomposite, $composite_cols);

        return $type_groups;
    }
}
