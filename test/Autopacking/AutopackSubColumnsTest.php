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

class AutopackSubColumnsTest extends PHPUnit_Framework_TestCase {

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


        $cfattrs = array("column_type" => "Super", "comparator_type" => DataType::LONG_TYPE);

        $cfattrs["subcomparator_type"] = DataType::LONG_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubLong', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::INTEGER_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubInt', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::TIME_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubTime', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::LEXICAL_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubLex', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::ASCII_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubAscii', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::UTF8_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLongSubUTF8', $cfattrs);

        $cfattrs["subcomparator_type"] = "CompositeType(LongType, AsciiType)";
        $sys->create_column_family(self::$KS, 'SuperLongSubComposite', $cfattrs);
    }

    public static function tearDownAfterClass() {
        $sys = new SystemManager();
        $sys->drop_keyspace(self::$KS);
        $sys->close();
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);

        $this->cf_suplong_sublong      = new ColumnFamily($this->client, 'SuperLongSubLong');
        $this->cf_suplong_subint       = new ColumnFamily($this->client, 'SuperLongSubInt');
        $this->cf_suplong_subtime      = new ColumnFamily($this->client, 'SuperLongSubTime');
        $this->cf_suplong_sublex       = new ColumnFamily($this->client, 'SuperLongSubLex');
        $this->cf_suplong_subascii     = new ColumnFamily($this->client, 'SuperLongSubAscii');
        $this->cf_suplong_subutf8      = new ColumnFamily($this->client, 'SuperLongSubUTF8');
        $this->cf_suplong_subcomposite = new ColumnFamily($this->client, 'SuperLongSubComposite');

        $this->cfs = array($this->cf_suplong_sublong, $this->cf_suplong_subint,
                           $this->cf_suplong_subtime, $this->cf_suplong_sublex,
                           $this->cf_suplong_subascii, $this->cf_suplong_subutf8);

        $this->TIME1 = UUIDGen::uuid1();
        $this->TIME2 = UUIDGen::uuid1();
        $this->TIME3 = UUIDGen::uuid1();

        $this->LEX1 = UUID::import('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')->bytes;
        $this->LEX2 = UUID::import('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')->bytes;
        $this->LEX3 = UUID::import('cccccccccccccccccccccccccccccccc')->bytes;
    }

    public function tearDown() {
        foreach($this->cfs as $cf) {
            foreach(self::$KEYS as $key)
                $cf->remove($key);
        }
    }

    private function make_sub_group($cf, $cols) {
        if (is_array($cols[0])) {
            $subs = array();
            $serialized_cols = array();
            for ($i = 0; $i < count($cols); $i++) {
                $name = $cols[$i];
                $name = serialize($name);
                $subs[$name] = self::$VALS[$i];
            }
            $dict = array(222222222222 => $subs);
        } else {
            $dict = array(222222222222 => array($cols[0] => self::$VALS[0],
                                                $cols[1] => self::$VALS[1],
                                                $cols[2] => self::$VALS[2]));
        }
        return array(
            'cf' => $cf,
            'cols' => $cols,
            'dict' => $dict
        );
    }

    public function test_super_column_family_subs() {
        $LONG = 222222222222;

        $type_groups = array();

        $long_cols = array(111111111111,
                           222222222222,
                           333333333333);
        $type_groups[] = self::make_sub_group($this->cf_suplong_sublong, $long_cols);

        $int_cols = array(1, 2, 3);
        $type_groups[] = self::make_sub_group($this->cf_suplong_subint, $int_cols);

        $time_cols = array($this->TIME1, $this->TIME2, $this->TIME3);
        $type_groups[] = self::make_sub_group($this->cf_suplong_subtime, $time_cols);

        $lex_cols = array($this->LEX1, $this->LEX2, $this->LEX3);
        $type_groups[] = self::make_sub_group($this->cf_suplong_sublex, $lex_cols);

        $ascii_cols = array('aaaa', 'bbbb', 'cccc');
        $type_groups[] = self::make_sub_group($this->cf_suplong_subascii, $ascii_cols);

        $utf8_cols = array("a&#1047;", "b&#1048;", "c&#1049;"); 
        $type_groups[] = self::make_sub_group($this->cf_suplong_subutf8, $utf8_cols);

        $composite_cols = array(array(1, 'a'), array(2, 'b'), array(3, 'c'));
        $type_groups[] = self::make_sub_group($this->cf_suplong_subcomposite, $composite_cols);

        foreach($type_groups as $group) {
            $cf = $group['cf'];
            $dict = $group['dict'];

            $cf->insert(self::$KEYS[0], $dict);
            $this->assertEquals($dict, $cf->get(self::$KEYS[0]));
            $this->assertEquals($dict, $cf->get(self::$KEYS[0], $columns=array($LONG)));

            # A start and end that are the same
            $this->assertEquals($dict,
                $cf->get(self::$KEYS[0], $columns=null,
                                         $column_start=$LONG,
                                         $column_finish=$LONG));

            $this->assertEquals(1, $cf->get_count(self::$KEYS[0]));

            ### remove() tests ###

            $cf->remove(self::$KEYS[0], $columns=null, $super_column=$LONG);
            $this->assertEquals(0, $cf->get_count(self::$KEYS[0]));

            # Insert more than one row
            $cf->insert(self::$KEYS[0], $dict);
            $cf->insert(self::$KEYS[1], $dict);
            $cf->insert(self::$KEYS[2], $dict);


            ### multiget() tests ###

            $result = $cf->multiget(self::$KEYS);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[0]]);

            $result = $cf->multiget(array(self::$KEYS[2]));
            $this->assertEquals($dict, $result[self::$KEYS[2]]);

            $result = $cf->multiget(self::$KEYS, $columns=array($LONG));
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[$i]]);

            $result = $cf->multiget(self::$KEYS,
                                    $columns=null,
                                    $column_start='',
                                    $column_finish='',
                                    $column_reverse=False,
                                    $count=ColumnFamily::DEFAULT_COLUMN_COUNT,
                                    $supercolumn=$LONG);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict[$LONG], $result[self::$KEYS[$i]]);

            $result = $cf->multiget(self::$KEYS,
                                    $columns=null,
                                    $column_start=$LONG,
                                    $column_finish=$LONG);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[$i]]);

            ### get_range() tests ###

            $result = $cf->get_range($key_start=self::$KEYS[0]);
            foreach($result as $subres) {
                $this->assertEquals($dict, $subres);
            }

            $result = $cf->get_range($key_start=self::$KEYS[0], $key_finish='',
                                     $row_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $columns=null,
                                     $column_start=$LONG,
                                     $column_finish=$LONG);
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);

            $result = $cf->get_range($key_start=self::$KEYS[0],
                                     $key_finish='',
                                     $row_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $columns=array($LONG));
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);

            $result = $cf->get_range($key_start=self::$KEYS[0],
                                     $key_finish='',
                                     $row_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $columns=null,
                                     $column_start='',
                                     $column_finish='',
                                     $column_revered=False,
                                     $column_count=ColumnFamily::DEFAULT_COLUMN_COUNT,
                                     $super_column=$LONG);
            foreach($result as $subres)
                $this->assertEquals($dict[$LONG], $subres);
        }
    }
}
