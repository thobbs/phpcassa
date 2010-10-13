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

        $this->cf_long = new ColumnFamily($this->client, 'StdLong');
        $this->cf_int = new ColumnFamily($this->client, 'StdInteger');
        $this->cf_ascii = new ColumnFamily($this->client, 'StdAscii');
        $this->cf_time = new ColumnFamily($this->client, 'StdTimeUUID');

        $this->cfs = array($this->cf_long, $this->cf_int, $this->cf_ascii,
                           $this->cf_time);
         
        $this->TIME1 = CassandraUtil::uuid1();
        $this->TIME2 = CassandraUtil::uuid1();
        $this->TIME3 = CassandraUtil::uuid1();
    }

    public function tearDown() {
        foreach($this->cfs as $cf)
            $cf->truncate();
    }

    public function test_basic_ints() {
        $int_col = array(3 => self::$VALS[0]);
        $this->cf_int->insert(self::$KEYS[0], $int_col);
        self::assertEqual($this->cf_int->get(self::$KEYS[0]), $int_col);
    }

    public function test_basic_longs() {
        $long_col = array(1111111111111111 => self::$VALS[0]);
        $this->cf_long->insert(self::$KEYS[0], $long_col);
        self::assertEqual($this->cf_long->get(self::$KEYS[0]), $long_col);
    }

    public function test_basic_ascii() {
        $ascii_col = array('foo' => self::$VALS[0]);
        $this->cf_ascii->insert(self::$KEYS[0], $ascii_col);
        self::assertEqual($this->cf_ascii->get(self::$KEYS[0]), $ascii_col);
    }

    public function test_basic_time() {
        $time_col = array($this->TIME1 => self::$VALS[0]);
        $this->cf_time->insert(self::$KEYS[0], $time_col);
        $result = $this->cf_time->get(self::$KEYS[0]);
        self::assertEqual($result, $time_col);
    }
}
?>
