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

class AutopackSuperColumnsTest extends PHPUnit_Framework_TestCase {

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


        $cfattrs = array("column_type" => "Super");

        $cfattrs["comparator_type"] = DataType::LONG_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLong', $cfattrs);

        $cfattrs["comparator_type"] = DataType::INTEGER_TYPE;
        $sys->create_column_family(self::$KS, 'SuperInt', $cfattrs);

        $cfattrs["comparator_type"] = DataType::TIME_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'SuperTime', $cfattrs);

        $cfattrs["comparator_type"] = DataType::LEXICAL_UUID_TYPE;
        $sys->create_column_family(self::$KS, 'SuperLex', $cfattrs);

        $cfattrs["comparator_type"] = DataType::ASCII_TYPE;
        $sys->create_column_family(self::$KS, 'SuperAscii', $cfattrs);

        $cfattrs["comparator_type"] = DataType::UTF8_TYPE;
        $sys->create_column_family(self::$KS, 'SuperUTF8', $cfattrs);

        $cfattrs["comparator_type"] = "CompositeType(LongType, AsciiType)";
        $sys->create_column_family(self::$KS, 'SuperComposite', $cfattrs);
    }

    public static function tearDownAfterClass() {
        $sys = new SystemManager();
        $sys->drop_keyspace(self::$KS);
        $sys->close();
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);

        $this->cf_suplong      = new ColumnFamily($this->client, 'SuperLong');
        $this->cf_supint       = new ColumnFamily($this->client, 'SuperInt');
        $this->cf_suptime      = new ColumnFamily($this->client, 'SuperTime');
        $this->cf_suplex       = new ColumnFamily($this->client, 'SuperLex');
        $this->cf_supascii     = new ColumnFamily($this->client, 'SuperAscii');
        $this->cf_suputf8      = new ColumnFamily($this->client, 'SuperUTF8');
        $this->cf_supcomposite = new ColumnFamily($this->client, 'SuperComposite');

        $this->cfs = array($this->cf_suplong, $this->cf_supint, $this->cf_suptime,
                           $this->cf_suplex, $this->cf_supascii, $this->cf_suputf8,
                           $this->cf_supcomposite);

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

    private function make_super_group($cf, $cols) {
        if (is_array($cols[0])) {
            $dict = array();
            $serialized_cols = array();
            for ($i = 0; $i < count($cols); $i++) {
                $name = $cols[$i];
                $name = serialize($name);
                $dict[$name] = array('bytes' => self::$VALS[$i]);
                $serialized_cols[] = $name;
            }
        } else {
            $dict = array($cols[0] => array('bytes' => self::$VALS[0]),
                          $cols[1] => array('bytes' => self::$VALS[1]),
                          $cols[2] => array('bytes' => self::$VALS[2]));
            $serialized_cols = $cols;
        }
        return array(
            'cf' => $cf,
            'cols' => $cols,
            'serialized_cols' => $serialized_cols,
            'dict' => $dict
        );
    }

    public function test_super_column_families() {
        $type_groups = array();

        $long_cols = array(111111111111,
                           222222222222,
                           333333333333);
        $type_groups[] = self::make_super_group($this->cf_suplong, $long_cols);

        $int_cols = array(1, 2, 3);
        $type_groups[] = self::make_super_group($this->cf_supint, $int_cols);

        $time_cols = array($this->TIME1, $this->TIME2, $this->TIME3);
        $type_groups[] = self::make_super_group($this->cf_suptime, $time_cols);

        $lex_cols = array($this->LEX1, $this->LEX2, $this->LEX3);
        $type_groups[] = self::make_super_group($this->cf_suplex, $lex_cols);

        $ascii_cols = array('aaaa', 'bbbb', 'cccc');
        $type_groups[] = self::make_super_group($this->cf_supascii, $ascii_cols);

        $utf8_cols = array("a&#1047;", "b&#1048;", "c&#1049;"); 
        $type_groups[] = self::make_super_group($this->cf_suputf8, $utf8_cols);

        $composite_cols = array(array(1, 'a'), array(2, 'b'), array(3, 'c'));
        $type_groups[] = self::make_super_group($this->cf_supcomposite, $composite_cols);

        foreach($type_groups as $group) {
            $cf = $group['cf'];
            $dict = $group['dict'];
            $cols = $group['cols'];
            $serialized_cols = $group['serialized_cols'];

            $cf->insert(self::$KEYS[0], $dict);
            $this->assertEquals($dict, $cf->get(self::$KEYS[0]));

            # Check each column individually
            foreach(range(0,2) as $i)
                $this->assertEquals(
                    array($serialized_cols[$i] => array('bytes' => self::$VALS[$i])),
                    $cf->get(self::$KEYS[0], $columns=array($cols[$i])));

            # Check with list of all columns
            $this->assertEquals($dict, $cf->get(self::$KEYS[0], $columns=$cols));

            # Same thing but with start and end
            $this->assertEquals($dict,
                $cf->get(self::$KEYS[0], $columns=null,
                                         $column_start=$cols[0],
                                         $column_finish=$cols[2]));

            # Start and end are the same
            $this->assertEquals(
                array($serialized_cols[0] => array('bytes' => self::$VALS[0])),
                $cf->get(self::$KEYS[0], $columns=null,
                                         $column_start=$cols[0],
                                         $column_finish=$cols[0]));

            ### remove() tests ###

            $cf->remove(self::$KEYS[0], $columns=array($cols[0]));
            $this->assertEquals(2, $cf->get_count(self::$KEYS[0]));

            $cf->remove(self::$KEYS[0], $columns=array($cols[1], $cols[2]));
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
            $this->assertEquals($result[self::$KEYS[2]], $dict);

            # Check each column individually
            foreach(range(0,2) as $i) {
                $result = $cf->multiget(self::$KEYS, $columns=array($cols[$i]));
                foreach(range(0,2) as $j)
                    $this->assertEquals(
                        array($serialized_cols[$i] => array('bytes' => self::$VALS[$i])),
                        $result[self::$KEYS[$j]]);

            }

            # Check that if we list all columns, we get the full dict
            $result = $cf->multiget(self::$KEYS, $columns=$cols);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[$j]]);

            # The same thing with a start and end instead
            $result = $cf->multiget(self::$KEYS, $columns=null,
                                    $column_start=$cols[0],
                                    $column_finish=$cols[2]);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[$j]]);

            # A start and end that are the same
            $result = $cf->multiget(self::$KEYS, $columns=null,
                                    $column_start=$cols[0],
                                    $column_finish=$cols[0]);
            foreach(range(0,2) as $i)
                $this->assertEquals(
                    array($serialized_cols[0] => array('bytes' => self::$VALS[0])),
                    $result[self::$KEYS[$j]]);


            ### get_range() tests ###

            $result = $cf->get_range($key_start=self::$KEYS[0]);
            foreach($result as $subres)
                $this->assertEquals($subres, $dict);

            $result = $cf->get_range($key_start=self::$KEYS[0], $key_finish='',
                                     $key_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $columns=null,
                                     $column_start=$cols[0],
                                     $column_finish=$cols[2]);
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);

            $result = $cf->get_range($key_start=self::$KEYS[0], $key_finish='',
                                     $key_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $columns=$cols);
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);
        }
    }
}
