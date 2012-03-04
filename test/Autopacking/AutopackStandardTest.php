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

class AutopackStandardTest extends PHPUnit_Framework_TestCase {

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


        $cfattrs = array("comparator_type" => DataType::LONG_TYPE);
        $sys->create_column_family(self::$KS, 'StdLong', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::INTEGER_TYPE);
        $sys->create_column_family(self::$KS, 'StdInteger', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::TIME_UUID_TYPE);
        $sys->create_column_family(self::$KS, 'StdTimeUUID', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::LEXICAL_UUID_TYPE);
        $sys->create_column_family(self::$KS, 'StdLexicalUUID', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::ASCII_TYPE);
        $sys->create_column_family(self::$KS, 'StdAscii', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::UTF8_TYPE);
        $sys->create_column_family(self::$KS, 'StdUTF8', $cfattrs);

        $cfattrs = array("comparator_type" => 'CompositeType(LongType, AsciiType)');
        $sys->create_column_family(self::$KS, 'StdComposite', $cfattrs);
    }

    public static function tearDownAfterClass() {
        $sys = new SystemManager();
        $sys->drop_keyspace(self::$KS);
        $sys->close();
    }

    public function setUp() {
        $this->client = new ConnectionPool(self::$KS);

        $this->cf_long      = new ColumnFamily($this->client, 'StdLong');
        $this->cf_int       = new ColumnFamily($this->client, 'StdInteger');
        $this->cf_time      = new ColumnFamily($this->client, 'StdTimeUUID');
        $this->cf_lex       = new ColumnFamily($this->client, 'StdLexicalUUID');
        $this->cf_ascii     = new ColumnFamily($this->client, 'StdAscii');
        $this->cf_utf8      = new ColumnFamily($this->client, 'StdUTF8');
        $this->cf_composite = new ColumnFamily($this->client, 'StdComposite');

        $this->cfs = array($this->cf_long, $this->cf_int, $this->cf_ascii,
                           $this->cf_time, $this->cf_lex, $this->cf_utf8,
                           $this->cf_composite);

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

    public function test_false_colnames() {
        $this->cf_int->insert(self::$KEYS[0], array(0 => "foo"));
        $this->assertEquals($this->cf_int->get(self::$KEYS[0]), array(0 => "foo"));
        $this->cf_int->remove(self::$KEYS[0]);
        try {
            $this->cf_int->insert(self::$KEYS[0], array(null => "foo"));
            $this->assertTrue(false); // shouldn't get here
        } catch (UnexpectedValueException $exc) {
            $this->assertTrue(true);
        }
    }

    static function make_group($cf, $cols) {
        if (is_array($cols[0])) {
            $dict = array();
            $serialized_cols = array();
            for ($i = 0; $i < count($cols); $i++) {
                $name = $cols[$i];
                $name = serialize($name);
                $dict[$name] = self::$VALS[$i];
                $serialized_cols[] = $name;
            }
        } else {
            $dict = array($cols[0] => self::$VALS[0],
                          $cols[1] => self::$VALS[1],
                          $cols[2] => self::$VALS[2]);
            $serialized_cols = $cols;
        }

        return array(
            'cf' => $cf,
            'cols' => $cols,
            'serialized_cols' => $serialized_cols,
            'dict' => $dict
        );
    }

    public function test_standard_column_family() {
        $type_groups = array();

        $long_cols = array(111111111111,
                           222222222222,
                           333333333333);
        $type_groups[] = self::make_group($this->cf_long, $long_cols);

        $int_cols = array(1, 2, 3);
        $type_groups[] = self::make_group($this->cf_int, $int_cols);

        $time_cols = array($this->TIME1, $this->TIME2, $this->TIME3);
        $type_groups[] = self::make_group($this->cf_time, $time_cols);

        $lex_cols = array($this->LEX1, $this->LEX2, $this->LEX3);
        $type_groups[] = self::make_group($this->cf_lex, $lex_cols);

        $ascii_cols = array('aaaa', 'bbbb', 'cccc');
        $type_groups[] = self::make_group($this->cf_ascii, $ascii_cols);

        $utf8_cols = array("a&#1047;", "b&#1048;", "c&#1049;"); 
        $type_groups[] = self::make_group($this->cf_utf8, $utf8_cols);

        $composite_cols = array(array(1, 'a'), array(2, 'b'), array(3, 'c'));
        $type_groups[] = self::make_group($this->cf_composite, $composite_cols);


        foreach($type_groups as $group) {
            $cf = $group['cf'];
            $dict = $group['dict'];
            $cols = $group['cols'];
            $serialized_cols = $group['serialized_cols'];

            $cf->insert(self::$KEYS[0], $dict);
            $actual = $cf->get(self::$KEYS[0]);
            $this->assertEquals($dict, $cf->get(self::$KEYS[0]));

            # Check each column individually
            foreach(range(0,2) as $i)
                $this->assertEquals(array($serialized_cols[$i] => self::$VALS[$i]),
                    $cf->get(self::$KEYS[0], $columns=array($cols[$i])));

            # Check with list of all columns
            $this->assertEquals($dict, $cf->get(self::$KEYS[0], $columns=$cols));

            # Same thing but with start and end
            $this->assertEquals($dict,
                $cf->get(self::$KEYS[0], $columns=null,
                                         $column_start=$cols[0],
                                         $column_finish=$cols[2]));

            # Start and end are the same
            $this->assertEquals(array($serialized_cols[0] => self::$VALS[0]),
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
            $this->assertEquals($dict, $result[self::$KEYS[2]]);

            # Check each column individually
            foreach(range(0,2) as $i) {
                $result = $cf->multiget(self::$KEYS, $columns=array($cols[$i]));
                foreach(range(0,2) as $j)
                    $this->assertEquals(array($serialized_cols[$i] => self::$VALS[$i]),
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
                $this->assertEquals(array($serialized_cols[0] => self::$VALS[0]),
                                    $result[self::$KEYS[$j]]);


            ### get_range() tests ###

            $result = $cf->get_range($key_start=self::$KEYS[0]);
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);

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

    public function test_uuid1_generation() {
        $micros = 1293769171436849;
        $uuid = UUIDGen::import(UUIDGen::uuid1(null, $micros)); 
        $t = (int)($uuid->time * 1000000);
        $this->assertEquals($micros, $t, '', 100);
    }
}

