<?php
require_once('simpletest/autorun.php');
require_once('../connection.php');
require_once('../columnfamily.php');
require_once('../sysmanager.php');
require_once('../uuid.php');

class TestAutopacking extends UnitTestCase {

    private static $VALS = array('val1', 'val2', 'val3');
    private static $KEYS = array('key1', 'key2', 'key3');
    private static $KS = "TestAutopacking";

    private $client;
    private $cf;

    public function __construct() {
        $this->sys = new SystemManager();

        $ksdefs = $this->sys->describe_keyspaces();
        $exists = False;
        foreach ($ksdefs as $ksdef)
            $exists = $exists || $ksdef->name == self::$KS;

        if ($exists)
            $this->sys->drop_keyspace(self::$KS);

        $this->sys->create_keyspace(self::$KS, array());



        $cfattrs = array("comparator_type" => DataType::LONG_TYPE);
        $this->sys->create_column_family(self::$KS, 'StdLong', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::INTEGER_TYPE);
        $this->sys->create_column_family(self::$KS, 'StdInteger', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::TIME_UUID_TYPE);
        $this->sys->create_column_family(self::$KS, 'StdTimeUUID', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::LEXICAL_UUID_TYPE);
        $this->sys->create_column_family(self::$KS, 'StdLexicalUUID', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::ASCII_TYPE);
        $this->sys->create_column_family(self::$KS, 'StdAscii', $cfattrs);

        $cfattrs = array("comparator_type" => DataType::UTF8_TYPE);
        $this->sys->create_column_family(self::$KS, 'StdUTF8', $cfattrs);



        $cfattrs = array("column_type" => "Super");

        $cfattrs["comparator_type"] = DataType::LONG_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperLong', $cfattrs);

        $cfattrs["comparator_type"] = DataType::INTEGER_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperInt', $cfattrs);

        $cfattrs["comparator_type"] = DataType::TIME_UUID_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperTime', $cfattrs);

        $cfattrs["comparator_type"] = DataType::LEXICAL_UUID_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperLex', $cfattrs);

        $cfattrs["comparator_type"] = DataType::ASCII_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperAscii', $cfattrs);

        $cfattrs["comparator_type"] = DataType::UTF8_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperUTF8', $cfattrs);

        

        $cfattrs = array("column_type" => "Super", "comparator_type" => DataType::LONG_TYPE);

        $cfattrs["subcomparator_type"] = DataType::LONG_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperLongSubLong', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::INTEGER_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperLongSubInt', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::TIME_UUID_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperLongSubTime', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::LEXICAL_UUID_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperLongSubLex', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::ASCII_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperLongSubAscii', $cfattrs);

        $cfattrs["subcomparator_type"] = DataType::UTF8_TYPE;
        $this->sys->create_column_family(self::$KS, 'SuperLongSubUTF8', $cfattrs);



        $cfattrs = array("column_type" => "Standard");

        $cfattrs["default_validation_class"] = DataType::LONG_TYPE;
        $this->sys->create_column_family(self::$KS, 'ValidatorLong', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::INTEGER_TYPE;
        $this->sys->create_column_family(self::$KS, 'ValidatorInt', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::TIME_UUID_TYPE;
        $this->sys->create_column_family(self::$KS, 'ValidatorTime', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::LEXICAL_UUID_TYPE;
        $this->sys->create_column_family(self::$KS, 'ValidatorLex', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::ASCII_TYPE;
        $this->sys->create_column_family(self::$KS, 'ValidatorAscii', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::UTF8_TYPE;
        $this->sys->create_column_family(self::$KS, 'ValidatorUTF8', $cfattrs);

        $cfattrs["default_validation_class"] = DataType::BYTES_TYPE;
        $this->sys->create_column_family(self::$KS, 'ValidatorBytes', $cfattrs);



        $cfattrs["default_validation_class"] = DataType::LONG_TYPE;
        $this->sys->create_column_family(self::$KS, 'DefaultValidator', $cfattrs);
        // Quick way to create a TimeUUIDType validator to subcol
        $this->sys->create_index(self::$KS, 'DefaultValidator', 'subcol',
            DataType::TIME_UUID_TYPE, NULL, NULL);
         
        $this->client = new ConnectionPool(self::$KS);
        $this->cf_long  = new ColumnFamily($this->client, 'StdLong');
        $this->cf_int   = new ColumnFamily($this->client, 'StdInteger');
        $this->cf_time  = new ColumnFamily($this->client, 'StdTimeUUID');
        $this->cf_lex   = new ColumnFamily($this->client, 'StdLexicalUUID');
        $this->cf_ascii = new ColumnFamily($this->client, 'StdAscii');
        $this->cf_utf8  = new ColumnFamily($this->client, 'StdUTF8');

        $this->cf_suplong  = new ColumnFamily($this->client, 'SuperLong');
        $this->cf_supint   = new ColumnFamily($this->client, 'SuperInt');
        $this->cf_suptime  = new ColumnFamily($this->client, 'SuperTime');
        $this->cf_suplex   = new ColumnFamily($this->client, 'SuperLex');
        $this->cf_supascii = new ColumnFamily($this->client, 'SuperAscii');
        $this->cf_suputf8  = new ColumnFamily($this->client, 'SuperUTF8');

        $this->cf_suplong_sublong  = new ColumnFamily($this->client, 'SuperLongSubLong');
        $this->cf_suplong_subint   = new ColumnFamily($this->client, 'SuperLongSubInt');
        $this->cf_suplong_subtime  = new ColumnFamily($this->client, 'SuperLongSubTime');
        $this->cf_suplong_sublex   = new ColumnFamily($this->client, 'SuperLongSubLex');
        $this->cf_suplong_subascii = new ColumnFamily($this->client, 'SuperLongSubAscii');
        $this->cf_suplong_subutf8  = new ColumnFamily($this->client, 'SuperLongSubUTF8');

        $this->cf_valid_long  = new ColumnFamily($this->client, 'ValidatorLong');
        $this->cf_valid_int   = new ColumnFamily($this->client, 'ValidatorInt');
        $this->cf_valid_time  = new ColumnFamily($this->client, 'ValidatorTime');
        $this->cf_valid_lex   = new ColumnFamily($this->client, 'ValidatorLex');
        $this->cf_valid_ascii = new ColumnFamily($this->client, 'ValidatorAscii');
        $this->cf_valid_utf8  = new ColumnFamily($this->client, 'ValidatorUTF8');
        $this->cf_valid_bytes = new ColumnFamily($this->client, 'ValidatorBytes');

        $this->cf_def_valid = new ColumnFamily($this->client, 'DefaultValidator');

        $this->cfs = array($this->cf_long, $this->cf_int, $this->cf_ascii,
                           $this->cf_time, $this->cf_lex, $this->cf_utf8,

                           $this->cf_suplong, $this->cf_supint, $this->cf_suptime,
                           $this->cf_suplex, $this->cf_supascii, $this->cf_suputf8,

                           $this->cf_suplong_sublong, $this->cf_suplong_subint,
                           $this->cf_suplong_subtime, $this->cf_suplong_sublex,
                           $this->cf_suplong_subascii, $this->cf_suplong_subutf8,

                           $this->cf_valid_long, $this->cf_valid_int,
                           $this->cf_valid_time, $this->cf_valid_lex,
                           $this->cf_valid_ascii, $this->cf_valid_utf8,
                           $this->cf_valid_bytes,

                           $this->cf_def_valid);

        $this->TIME1 = CassandraUtil::uuid1();
        $this->TIME2 = CassandraUtil::uuid1();
        $this->TIME3 = CassandraUtil::uuid1();

        $this->LEX1 = UUID::import('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')->bytes;
        $this->LEX2 = UUID::import('bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')->bytes;
        $this->LEX3 = UUID::import('cccccccccccccccccccccccccccccccc')->bytes;


        parent::__construct();
    }

    public function setUp() {
    }

    public function tearDown() {
        foreach($this->cfs as $cf) {
            foreach(self::$KEYS as $key)
                $cf->remove($key);
        }
    }

    public function test_basic_ints() {
        $int_col = array(3 => self::$VALS[0]);
        $this->cf_int->insert(self::$KEYS[0], $int_col);
        self::assertEqual($this->cf_int->get(self::$KEYS[0]), $int_col);

        $this->cf_supint->insert(self::$KEYS[0], array(111123 => $int_col));
        self::assertEqual($this->cf_supint->get(self::$KEYS[0]), array(111123 => $int_col));

        $this->cf_suplong_subint->insert(self::$KEYS[0], array(222222222222 => $int_col));
        self::assertEqual($this->cf_suplong_subint->get(self::$KEYS[0]),
                          array(222222222222 => $int_col));
    }

    public function test_basic_longs() {
        $long_col = array(1111111111111111 => self::$VALS[0]);
        $this->cf_long->insert(self::$KEYS[0], $long_col);
        self::assertEqual($this->cf_long->get(self::$KEYS[0]), $long_col);

        $this->cf_suplong->insert(self::$KEYS[0], array(222222222222 => $long_col));
        self::assertEqual($this->cf_suplong->get(self::$KEYS[0]), array(222222222222 => $long_col));

        $this->cf_suplong_sublong->insert(self::$KEYS[0], array(222222222222 => $long_col));
        self::assertEqual($this->cf_suplong_sublong->get(self::$KEYS[0]),
                          array(222222222222 => $long_col));
    }

    public function test_basic_ascii() {
        $ascii_col = array('foo' => self::$VALS[0]);
        $this->cf_ascii->insert(self::$KEYS[0], $ascii_col);
        self::assertEqual($this->cf_ascii->get(self::$KEYS[0]), $ascii_col);

        $this->cf_supascii->insert(self::$KEYS[0], array('aaaa' => $ascii_col));
        self::assertEqual($this->cf_supascii->get(self::$KEYS[0]), array('aaaa' => $ascii_col));

        $this->cf_suplong_subascii->insert(self::$KEYS[0], array(222222222222 => $ascii_col));
        self::assertEqual($this->cf_suplong_subascii->get(self::$KEYS[0]),
                          array(222222222222 => $ascii_col));
    }

    public function test_basic_time() {
        $time_col = array($this->TIME1 => self::$VALS[0]);
        $this->cf_time->insert(self::$KEYS[0], $time_col);
        $result = $this->cf_time->get(self::$KEYS[0]);
        self::assertEqual($result, $time_col);

        $this->cf_suptime->insert(self::$KEYS[0], array($this->TIME2 => $time_col));
        self::assertEqual($this->cf_suptime->get(self::$KEYS[0]), array($this->TIME2 => $time_col));

        $this->cf_suplong_subtime->insert(self::$KEYS[0], array(222222222222 => $time_col));
        self::assertEqual($this->cf_suplong_subtime->get(self::$KEYS[0]),
                          array(222222222222 => $time_col));
    }

    public function test_basic_lexical() {
        $lex_col = array($this->LEX1 => self::$VALS[0]);
        $this->cf_lex->insert(self::$KEYS[0], $lex_col);
        $result = $this->cf_lex->get(self::$KEYS[0]);
        self::assertEqual($result, $lex_col);

        $this->cf_suplex->insert(self::$KEYS[0], array($this->LEX2 => $lex_col));
        self::assertEqual($this->cf_suplex->get(self::$KEYS[0]), array($this->LEX2 => $lex_col));

        $this->cf_suplong_sublex->insert(self::$KEYS[0], array(222222222222 => $lex_col));
        self::assertEqual($this->cf_suplong_sublex->get(self::$KEYS[0]),
                          array(222222222222 => $lex_col));
    }

    public function test_basic_utf8() {
        # Fun fact - "hello" in Russian:
        $uni = "&#1047;&#1076;&#1088;&#1072;&#1074;&#1089;".
               "&#1089;&#1090;&#1074;&#1091;&#1081;".
               "&#1090;&#1077;";

        $utf8_col = array($uni => self::$VALS[0]);
        $this->cf_utf8->insert(self::$KEYS[0], $utf8_col);
        $result = $this->cf_utf8->get(self::$KEYS[0]);
        self::assertEqual($result, $utf8_col);

        $this->cf_suputf8->insert(self::$KEYS[0], array($uni => $utf8_col));
        self::assertEqual($this->cf_suputf8->get(self::$KEYS[0]), array($uni => $utf8_col));

        $this->cf_suplong_subutf8->insert(self::$KEYS[0], array(222222222222 => $utf8_col));
        self::assertEqual($this->cf_suplong_subutf8->get(self::$KEYS[0]),
                          array(222222222222 => $utf8_col));
    }

    static function make_group($cf, $cols) {
        $dict = array($cols[0] => self::$VALS[0],
                      $cols[1] => self::$VALS[1],
                      $cols[2] => self::$VALS[2]);
        return array('cf' => $cf, 'cols' => $cols, 'dict' => $dict);
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


        foreach($type_groups as $group) {

            $group['cf']->insert(self::$KEYS[0], $group['dict']);
            self::assertEqual($group['cf']->get(self::$KEYS[0]), $group['dict']);

            # Check each column individually
            foreach(range(0,2) as $i)
                self::assertEqual($group['cf']->get(self::$KEYS[0], $columns=array($group['cols'][$i])),
                                  array($group['cols'][$i] => self::$VALS[$i]));

            # Check with list of all columns
            self::assertEqual($group['cf']->get(self::$KEYS[0], $columns=$group['cols']),
                              $group['dict']);

            # Same thing but with start and end
            self::assertEqual($group['cf']->get(self::$KEYS[0], $columns=null,
                                                $column_start=$group['cols'][0],
                                                $column_finish=$group['cols'][2]),
                              $group['dict']);

            # Start and end are the same
            self::assertEqual($group['cf']->get(self::$KEYS[0], $columns=null,
                                                $column_start=$group['cols'][0],
                                                $column_finish=$group['cols'][0]),

                              array($group['cols'][0] => self::$VALS[0]));


            ### remove() tests ###

            $group['cf']->remove(self::$KEYS[0], $columns=array($group['cols'][0]));
            self::assertEqual($group['cf']->get_count(self::$KEYS[0]), 2);

            $group['cf']->remove(self::$KEYS[0], $columns=array($group['cols'][1], $group['cols'][2]));
            self::assertEqual($group['cf']->get_count(self::$KEYS[0]), 0);

            # Insert more than one row
            $group['cf']->insert(self::$KEYS[0], $group['dict']);
            $group['cf']->insert(self::$KEYS[1], $group['dict']);
            $group['cf']->insert(self::$KEYS[2], $group['dict']);


            ### multiget() tests ###

            $result = $group['cf']->multiget(self::$KEYS);
            foreach(range(0,2) as $i)            
                self::assertEqual($result[self::$KEYS[0]], $group['dict']);

            $result = $group['cf']->multiget(array(self::$KEYS[2]));
            self::assertEqual($result[self::$KEYS[2]], $group['dict']);

            # Check each column individually
            foreach(range(0,2) as $i) {
                $result = $group['cf']->multiget(self::$KEYS, $columns=array($group['cols'][$i]));
                foreach(range(0,2) as $j)
                    self::assertEqual($result[self::$KEYS[$j]],
                                      array($group['cols'][$i] => self::$VALS[$i]));

            }

            # Check that if we list all columns, we get the full dict
            $result = $group['cf']->multiget(self::$KEYS, $columns=$group['cols']);
            foreach(range(0,2) as $i)
                self::assertEqual($result[self::$KEYS[$j]], $group['dict']);

            # The same thing with a start and end instead
            $result = $group['cf']->multiget(self::$KEYS, $columns=null,
                                             $column_start=$group['cols'][0],
                                             $column_finish=$group['cols'][2]);
            foreach(range(0,2) as $i)
                self::assertEqual($result[self::$KEYS[$j]], $group['dict']);

            # A start and end that are the same
            $result = $group['cf']->multiget(self::$KEYS, $columns=null,
                                             $column_start=$group['cols'][0],
                                             $column_finish=$group['cols'][0]);
            foreach(range(0,2) as $i)
                self::assertEqual($result[self::$KEYS[$j]],
                                  array($group['cols'][0] => self::$VALS[0]));


            ### get_range() tests ###

            $result = $group['cf']->get_range($key_start=self::$KEYS[0]);
            foreach($result as $subres)
                self::assertEqual($subres, $group['dict']);

            $result = $group['cf']->get_range($key_start=self::$KEYS[0], $key_finish='',
                                              $key_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                              $columns=null,
                                              $column_start=$group['cols'][0],
                                              $column_finish=$group['cols'][2]);
            foreach($result as $subres)
                self::assertEqual($subres, $group['dict']);

            $result = $group['cf']->get_range($key_start=self::$KEYS[0], $key_finish='',
                                              $key_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                              $columns=$group['cols']);
            foreach($result as $subres)
                self::assertEqual($subres, $group['dict']);
        }
    }

    private function make_super_group($cf, $cols) {
        $diction = array($cols[0] => array('bytes' => self::$VALS[0]),
                         $cols[1] => array('bytes' => self::$VALS[1]),
                         $cols[2] => array('bytes' => self::$VALS[2]));
        return array('cf' => $cf, 'cols' => $cols, 'dict' => $diction);
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

        foreach($type_groups as $group) {

            $group['cf']->insert(self::$KEYS[0], $group['dict']);
            self::assertEqual($group['cf']->get(self::$KEYS[0]), $group['dict']);

            # Check each column individually
            foreach(range(0,2) as $i)
                self::assertEqual($group['cf']->get(self::$KEYS[0], $columns=array($group['cols'][$i])),
                                  array($group['cols'][$i] => array('bytes' => self::$VALS[$i])));

            # Check with list of all columns
            self::assertEqual($group['cf']->get(self::$KEYS[0], $columns=$group['cols']),
                              $group['dict']);

            # Same thing but with start and end
            self::assertEqual($group['cf']->get(self::$KEYS[0], $columns=null,
                                                $column_start=$group['cols'][0],
                                                $column_finish=$group['cols'][2]),
                              $group['dict']);

            # Start and end are the same
            self::assertEqual($group['cf']->get(self::$KEYS[0], $columns=null,
                                                $column_start=$group['cols'][0],
                                                $column_finish=$group['cols'][0]),

                              array($group['cols'][0] => array('bytes' => self::$VALS[0])));


            ### remove() tests ###

            $group['cf']->remove(self::$KEYS[0], $columns=array($group['cols'][0]));
            self::assertEqual($group['cf']->get_count(self::$KEYS[0]), 2);

            $group['cf']->remove(self::$KEYS[0], $columns=array($group['cols'][1], $group['cols'][2]));
            self::assertEqual($group['cf']->get_count(self::$KEYS[0]), 0);

            # Insert more than one row
            $group['cf']->insert(self::$KEYS[0], $group['dict']);
            $group['cf']->insert(self::$KEYS[1], $group['dict']);
            $group['cf']->insert(self::$KEYS[2], $group['dict']);


            ### multiget() tests ###

            $result = $group['cf']->multiget(self::$KEYS);
            foreach(range(0,2) as $i)            
                self::assertEqual($result[self::$KEYS[0]], $group['dict']);

            $result = $group['cf']->multiget(array(self::$KEYS[2]));
            self::assertEqual($result[self::$KEYS[2]], $group['dict']);

            # Check each column individually
            foreach(range(0,2) as $i) {
                $result = $group['cf']->multiget(self::$KEYS, $columns=array($group['cols'][$i]));
                foreach(range(0,2) as $j)
                    self::assertEqual($result[self::$KEYS[$j]],
                                      array($group['cols'][$i] => array('bytes' => self::$VALS[$i])));

            }

            # Check that if we list all columns, we get the full dict
            $result = $group['cf']->multiget(self::$KEYS, $columns=$group['cols']);
            foreach(range(0,2) as $i)
                self::assertEqual($result[self::$KEYS[$j]], $group['dict']);

            # The same thing with a start and end instead
            $result = $group['cf']->multiget(self::$KEYS, $columns=null,
                                             $column_start=$group['cols'][0],
                                             $column_finish=$group['cols'][2]);
            foreach(range(0,2) as $i)
                self::assertEqual($result[self::$KEYS[$j]], $group['dict']);

            # A start and end that are the same
            $result = $group['cf']->multiget(self::$KEYS, $columns=null,
                                             $column_start=$group['cols'][0],
                                             $column_finish=$group['cols'][0]);
            foreach(range(0,2) as $i)
                self::assertEqual($result[self::$KEYS[$j]],
                                  array($group['cols'][0] => array('bytes' => self::$VALS[0])));


            ### get_range() tests ###

            $result = $group['cf']->get_range($key_start=self::$KEYS[0]);
            foreach($result as $subres)
                self::assertEqual($subres, $group['dict']);

            $result = $group['cf']->get_range($key_start=self::$KEYS[0], $key_finish='',
                                              $key_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                              $columns=null,
                                              $column_start=$group['cols'][0],
                                              $column_finish=$group['cols'][2]);
            foreach($result as $subres)
                self::assertEqual($subres, $group['dict']);

            $result = $group['cf']->get_range($key_start=self::$KEYS[0], $key_finish='',
                                              $key_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                              $columns=$group['cols']);
            foreach($result as $subres)
                self::assertEqual($subres, $group['dict']);
        }
    }

    private function make_sub_group($cf, $cols) {
        $diction = array(222222222222 => array($cols[0] => self::$VALS[0],
                                               $cols[1] => self::$VALS[1],
                                               $cols[2] => self::$VALS[2]));
        return array('cf' => $cf, 'cols' => $cols, 'dict' => $diction);
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


        foreach($type_groups as $group) {

            $group['cf']->insert(self::$KEYS[0], $group['dict']);
            self::assertEqual($group['cf']->get(self::$KEYS[0]),
                              $group['dict']);
            self::assertEqual($group['cf']->get(self::$KEYS[0], $columns=array($LONG)),
                              $group['dict']);

            # A start and end that are the same
            self::assertEqual($group['cf']->get(self::$KEYS[0], $columns=null,
                                                $column_start=$LONG,
                                                $column_finish=$LONG),
                              $group['dict']);

            self::assertEqual($group['cf']->get_count(self::$KEYS[0]), 1);

            ### remove() tests ###

            $group['cf']->remove(self::$KEYS[0], $columns=null, $super_column=$LONG);
            self::assertEqual($group['cf']->get_count(self::$KEYS[0]), 0);

            # Insert more than one row
            $group['cf']->insert(self::$KEYS[0], $group['dict']);
            $group['cf']->insert(self::$KEYS[1], $group['dict']);
            $group['cf']->insert(self::$KEYS[2], $group['dict']);


            ### multiget() tests ###

            $result = $group['cf']->multiget(self::$KEYS);
            foreach(range(0,2) as $i)            
                self::assertEqual($result[self::$KEYS[0]], $group['dict']);

            $result = $group['cf']->multiget(array(self::$KEYS[2]));
            self::assertEqual($result[self::$KEYS[2]], $group['dict']);

            $result = $group['cf']->multiget(self::$KEYS, $columns=array($LONG));
            foreach(range(0,2) as $i)
                self::assertEqual($result[self::$KEYS[$i]], $group['dict']);

            $result = $group['cf']->multiget(self::$KEYS,
                                             $columns=null,
                                             $column_start='',
                                             $column_finish='',
                                             $column_reverse=False,
                                             $count=ColumnFamily::DEFAULT_COLUMN_COUNT,
                                             $supercolumn=$LONG);
            foreach(range(0,2) as $i)
                self::assertEqual($result[self::$KEYS[$i]], $group['dict'][$LONG]);

            $result = $group['cf']->multiget(self::$KEYS,
                                             $columns=null,
                                             $column_start=$LONG,
                                             $column_finish=$LONG);
            foreach(range(0,2) as $i)
                self::assertEqual($result[self::$KEYS[$i]], $group['dict']);

            ### get_range() tests ###

            $result = $group['cf']->get_range($key_start=self::$KEYS[0]);
            foreach($result as $subres) {
                self::assertEqual($subres, $group['dict']);
            }

            $result = $group['cf']->get_range($key_start=self::$KEYS[0], $key_finish='',
                                              $row_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                              $columns=null,
                                              $column_start=$LONG,
                                              $column_finish=$LONG);
            foreach($result as $subres)
                self::assertEqual($subres, $group['dict']);

            $result = $group['cf']->get_range($key_start=self::$KEYS[0],
                                              $key_finish='',
                                              $row_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                              $columns=array($LONG));
            foreach($result as $subres)
                self::assertEqual($subres, $group['dict']);

            $result = $group['cf']->get_range($key_start=self::$KEYS[0],
                                              $key_finish='',
                                              $row_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                              $columns=null,
                                              $column_start='',
                                              $column_finish='',
                                              $column_revered=False,
                                              $column_count=ColumnFamily::DEFAULT_COLUMN_COUNT,
                                              $super_column=$LONG);
            foreach($result as $subres)
                self::assertEqual($subres, $group['dict'][$LONG]);
        }
    }

    public function test_validated_columns(){

        # Longs
        $col = array('subcol' => 222222222222);
        $this->cf_valid_long->insert(self::$KEYS[0], $col);
        self::assertEqual($this->cf_valid_long->get(self::$KEYS[0]), $col);

        # Integers
        $col = array('subcol' => 2);
        $this->cf_valid_int->insert(self::$KEYS[0], $col);
        self::assertEqual($this->cf_valid_int->get(self::$KEYS[0]), $col);

        # TimeUUIDs
        $col = array('subcol' => $this->TIME1);
        $this->cf_valid_time->insert(self::$KEYS[0], $col);
        self::assertEqual($this->cf_valid_time->get(self::$KEYS[0]), $col);

        # LexicalUUIDs
        $col = array('subcol' => $this->LEX1);
        $this->cf_valid_lex->insert(self::$KEYS[0], $col);
        self::assertEqual($this->cf_valid_lex->get(self::$KEYS[0]), $col);

        # ASCII
        $col = array('subcol' => 'aaa');
        $this->cf_valid_ascii->insert(self::$KEYS[0], $col);
        self::assertEqual($this->cf_valid_ascii->get(self::$KEYS[0]), $col);

        # UTF8
        $col = array('subcol' => "a&#1047;");
        $this->cf_valid_utf8->insert(self::$KEYS[0], $col);
        self::assertEqual($this->cf_valid_utf8->get(self::$KEYS[0]), $col);

        # BytesType
        $col = array('subcol' => 'aaa123');
        $this->cf_valid_bytes->insert(self::$KEYS[0], $col);
        self::assertEqual($this->cf_valid_bytes->get(self::$KEYS[0]), $col);
    }

    public function test_default_validated_columns() {
        $col_cf = array('aaaaaa' => 222222222222);
        $col_cm = array('subcol' => $this->TIME1);

        # Both of these inserts work, as cf allows
        # longs and cm for 'subcol' allows TimeUUIDs
        $this->cf_def_valid->insert(self::$KEYS[0], $col_cf);
        $this->cf_def_valid->insert(self::$KEYS[0], $col_cm);
        self::assertEqual($this->cf_def_valid->get(self::$KEYS[0]),
                          array('aaaaaa' => 222222222222, 'subcol' => $this->TIME1));
    }

    public function test_uuid1_generation() {
        $micros = 1293769171436849;
        $uuid = CassandraUtil::import(CassandraUtil::uuid1(null, $micros)); 
        $t = (int)($uuid->time * 1000000);
        self::assertWithinMargin($micros, $t, 100);
    }
}
?>
