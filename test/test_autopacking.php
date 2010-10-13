<?php
require_once('simpletest/autorun.php');
require_once('../connection.php');
require_once('../columnfamily.php');
require_once('../uuid.php');

class TestAutopacking extends UnitTestCase {

    private static $VALS = array('val1', 'val2', 'val3');
    private static $KEYS = array('key1', 'key2', 'key3');

    private $client;
    private $cf;

    public function setUp() {
        $this->client = new Connection('Keyspace1');

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

        $this->cfs = array($this->cf_long, $this->cf_int, $this->cf_ascii,
                           $this->cf_time, $this->cf_lex, $this->cf_utf8,

                           $this->cf_suplong, $this->cf_supint, $this->cf_suptime,
                           $this->cf_suplex, $this->cf_supascii, $this->cf_suputf8,

                           $this->cf_suplong_sublong, $this->cf_suplong_subint,
                           $this->cf_suplong_subtime, $this->cf_suplong_sublex,
                           $this->cf_suplong_subascii, $this->cf_suplong_subutf8);
         
        $this->TIME1 = CassandraUtil::uuid1();
        $this->TIME2 = CassandraUtil::uuid1();
        $this->TIME3 = CassandraUtil::uuid1();

        $this->LEX1 = CassandraUtil::uuid4();
        $this->LEX2 = CassandraUtil::uuid4();
        $this->LEX3 = CassandraUtil::uuid4();
    }

    public function tearDown() {
        foreach($this->cfs as $cf)
            $cf->truncate();
    }

    public function test_basic_ints() {
        $int_col = array(3 => self::$VALS[0]);
        $this->cf_int->insert(self::$KEYS[0], $int_col);
        self::assertEqual($this->cf_int->get(self::$KEYS[0]), $int_col);

        $this->cf_supint->insert(self::$KEYS[0], array(111123 => $int_col));
        self::assertEqual($this->cf_supint->get(self::$KEYS[0]), array(111123 => $int_col));

        $this->cf_suplong_subint->insert(self::$KEYS[0], array(2222222222 => $int_col));
        self::assertEqual($this->cf_suplong_subint->get(self::$KEYS[0]),
                          array(2222222222 => $int_col));
    }

    public function test_basic_longs() {
        $long_col = array(1111111111111111 => self::$VALS[0]);
        $this->cf_long->insert(self::$KEYS[0], $long_col);
        self::assertEqual($this->cf_long->get(self::$KEYS[0]), $long_col);

        $this->cf_suplong->insert(self::$KEYS[0], array(2222222222 => $long_col));
        self::assertEqual($this->cf_suplong->get(self::$KEYS[0]), array(2222222222 => $long_col));

        $this->cf_suplong_sublong->insert(self::$KEYS[0], array(2222222222 => $long_col));
        self::assertEqual($this->cf_suplong_sublong->get(self::$KEYS[0]),
                          array(2222222222 => $long_col));
    }

    public function test_basic_ascii() {
        $ascii_col = array('foo' => self::$VALS[0]);
        $this->cf_ascii->insert(self::$KEYS[0], $ascii_col);
        self::assertEqual($this->cf_ascii->get(self::$KEYS[0]), $ascii_col);

        $this->cf_supascii->insert(self::$KEYS[0], array('aaaa' => $ascii_col));
        self::assertEqual($this->cf_supascii->get(self::$KEYS[0]), array('aaaa' => $ascii_col));

        $this->cf_suplong_subascii->insert(self::$KEYS[0], array(2222222222 => $ascii_col));
        self::assertEqual($this->cf_suplong_subascii->get(self::$KEYS[0]),
                          array(2222222222 => $ascii_col));
    }

    public function test_basic_time() {
        $time_col = array($this->TIME1 => self::$VALS[0]);
        $this->cf_time->insert(self::$KEYS[0], $time_col);
        $result = $this->cf_time->get(self::$KEYS[0]);
        self::assertEqual($result, $time_col);

        $this->cf_suptime->insert(self::$KEYS[0], array($this->TIME2 => $time_col));
        self::assertEqual($this->cf_suptime->get(self::$KEYS[0]), array($this->TIME2 => $time_col));

        $this->cf_suplong_subtime->insert(self::$KEYS[0], array(2222222222 => $time_col));
        self::assertEqual($this->cf_suplong_subtime->get(self::$KEYS[0]),
                          array(2222222222 => $time_col));
    }

    public function test_basic_lexical() {
        $lex_col = array($this->LEX1 => self::$VALS[0]);
        $this->cf_lex->insert(self::$KEYS[0], $lex_col);
        $result = $this->cf_lex->get(self::$KEYS[0]);
        self::assertEqual($result, $lex_col);

        $this->cf_suplex->insert(self::$KEYS[0], array($this->LEX2 => $lex_col));
        self::assertEqual($this->cf_suplex->get(self::$KEYS[0]), array($this->LEX2 => $lex_col));

        $this->cf_suplong_sublex->insert(self::$KEYS[0], array(2222222222 => $lex_col));
        self::assertEqual($this->cf_suplong_sublex->get(self::$KEYS[0]),
                          array(2222222222 => $lex_col));
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

        $this->cf_suplong_subutf8->insert(self::$KEYS[0], array(2222222222 => $utf8_col));
        self::assertEqual($this->cf_suplong_subutf8->get(self::$KEYS[0]),
                          array(2222222222 => $utf8_col));
    }
}
?>
