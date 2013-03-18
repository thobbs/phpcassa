<?php

require_once(__DIR__.'/StandardBase.php');

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\Schema\DataType\DateType;
use phpcassa\SystemManager;
use phpcassa\Util\Clock;

use phpcassa\UUID;

class AutopackStandardSerializedTest extends StandardBase {

    protected $SERIALIZED = true;

    public static function setUpBeforeClass() {
        parent::setUpBeforeClass();

        $sys = new SystemManager();

        $cfattrs = array("comparator_type" => DataType::FLOAT_TYPE);
        $sys->create_column_family(self::$KS, 'StdFloat', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::DOUBLE_TYPE);
        $sys->create_column_family(self::$KS, 'StdDouble', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::TIME_UUID_TYPE);
        $sys->create_column_family(self::$KS, 'StdTimeUUID', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::LEXICAL_UUID_TYPE);
        $sys->create_column_family(self::$KS, 'StdLexicalUUID', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::DATE_TYPE);
        $sys->create_column_family(self::$KS, 'StdDate', $cfattrs);

        $cfattrs = array("comparator_type" => 'CompositeType(Int32Type, AsciiType)');
        $sys->create_column_family(self::$KS, 'StdComposite', $cfattrs);

        $cfattrs = array("comparator_type" => 'CompositeType(LexicalUUIDType, DateType)');
        $sys->create_column_family(self::$KS, 'StdNonScalarComposite', $cfattrs);
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);

        $this->cf_float                = new ColumnFamily($this->client, 'StdFloat');
        $this->cf_double               = new ColumnFamily($this->client, 'StdDouble');
        $this->cf_time                 = new ColumnFamily($this->client, 'StdTimeUUID');
        $this->cf_lex                  = new ColumnFamily($this->client, 'StdLexicalUUID');
        $this->cf_date                 = new ColumnFamily($this->client, 'StdDate');
        $this->cf_composite            = new ColumnFamily($this->client, 'StdComposite');
        $this->cf_non_scalar_composite = new ColumnFamily($this->client, 'StdNonScalarComposite');

        $this->cfs = array($this->cf_float, $this->cf_double,
                           $this->cf_time, $this->cf_lex,
                           $this->cf_composite);

        // make a millisecond precision timestamp
        if (PHP_INT_MAX === 2147483647) {
            $base_time = (double)time();
        } else {
            // make a millisecond precision timestamp
            $base_time = Clock::get_time();
            $base_time -= ((int)$base_time) % 1000;
            $base_time /= 1e6;
        }

        $this->DATE1 = $base_time + 0;
        $this->DATE2 = $base_time + 1;
        $this->DATE3 = $base_time + 2;

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
        $type_groups[] = $this->make_group($this->cf_float, $float_cols);

        $double_cols = array(1.25, 1.5, 1.75);
        $type_groups[] = $this->make_group($this->cf_double, $double_cols);

        $time_cols = array($this->TIME1, $this->TIME2, $this->TIME3);
        $type_groups[] = $this->make_group($this->cf_time, $time_cols);

        $lex_cols = array($this->LEX1, $this->LEX2, $this->LEX3);
        $type_groups[] = $this->make_group($this->cf_lex, $lex_cols);

        $date_cols = array($this->DATE1, $this->DATE2, $this->DATE3);
        $type_groups[] = $this->make_group($this->cf_date, $date_cols);

        $composite_cols = array(
            array(1, 'a'),
            array(2, 'b'),
            array(3, 'c')
        );
        $type_groups[] = $this->make_group($this->cf_composite, $composite_cols);

        $non_scalar_composite_cols = array(
            array($this->LEX1, $this->DATE1),
            array($this->LEX2, $this->DATE1),
            array($this->LEX3, $this->DATE1)
        );
        $type_groups[] = $this->make_group($this->cf_non_scalar_composite, $non_scalar_composite_cols);

        return $type_groups;
    }

    public function test_uuid1_generation() {
        if (PHP_INT_MAX === 2147483647) {
            $this->markTestSkipped();
        }

        $micros = 1293769171436849;
        $uuid = UUID::import(UUID::uuid1(null, $micros));
        $t = (int)($uuid->time * 1000000);
        $this->assertEquals($micros, $t, '', 100);
    }

    public function test_date_type_formats() {
        $time1 = \microtime();
        \settype($time1, 'string'); //convert to string to keep trailing zeroes
        $time2 = explode(" ", $time1);
        $sub_secs = \preg_replace('/0./', '', $time2[0], 1);
        $time3 = ($time2[1].$sub_secs)/100;
        $time3 -= ((int)$time3) % 1000;
        $time3 /= 1000000;

        $dtInstance = new DateType();
        $unpacked = $dtInstance->unpack($dtInstance->pack($time1), false);
        $this->assertEquals($time3, $unpacked, "", 0.005);
    }
}
