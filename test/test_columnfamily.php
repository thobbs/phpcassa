<?php
require_once('simpletest/autorun.php');
require_once('../connection.php');
require_once('../columnfamily.php');

class TestColumnFamily extends UnitTestCase {

    private $client;
    private $cf;

    private static $KEYS = array('key1', 'key2', 'key3');

    public function setUp() {
        $this->client = new Connection('Keyspace1');
        $this->cf = new ColumnFamily($this->client, 'Standard1');
    }

    public function tearDown() {
        foreach(self::$KEYS as $key)
            $this->cf->remove($key);
    }

    public function test_opening_connection() {
        $this->client->connect();
    }

    public function test_empty() {
        $key = 'TestColumnFamily.test_empty';
        try {
            $this->cf->get(self::$KEYS[0]);
            self::asertTrue(false);
        } catch (cassandra_NotFoundException $e) {
        }
    }

    public function test_insert_get() {
        $this->cf->insert(self::$KEYS[0], array('col' => 'val'));
        self::assertEqual($this->cf->get(self::$KEYS[0]), array('col' => 'val'));
    }

    public function test_insert_multiget() {
        $columns1 = array('1' => 'val1', '2' => 'val2');
        $columns2 = array('3' => 'val1', '4' => 'val2');

        $this->cf->insert(self::$KEYS[0], $columns1);
        $this->cf->insert(self::$KEYS[1], $columns2);
        $rows = $this->cf->multiget(self::$KEYS);
        self::assertEqual(count($rows), 2);
        self::assertEqual($rows[self::$KEYS[0]], $columns1);
        self::assertEqual($rows[self::$KEYS[1]], $columns2);
        self::assertFalse(in_array(self::$KEYS[2], $rows));
    }

    public function test_insert_get_count() {
        $key = 'TestColumnFamily.test_insert_get_count';
        $cols = array('1' => 'val1', '2' => 'val2');
        $this->cf->insert(self::$KEYS[0], $cols);
        self::assertEqual($this->cf->get_count(self::$KEYS[0]), 2);

        self::assertEqual($this->cf->get_count(self::$KEYS[0], $columns=null, $column_start='1'), 2);
        self::assertEqual($this->cf->get_count(self::$KEYS[0], $columns=null, $column_start='',
                                               $column_finish='2'), 2);
        self::assertEqual($this->cf->get_count(self::$KEYS[0], $columns=null, $column_start='1', $column_finish='2'), 2);
        self::assertEqual($this->cf->get_count(self::$KEYS[0], $columns=null, $column_start='1', $column_finish='1'), 1);
        self::assertEqual($this->cf->get_count(self::$KEYS[0], $columns=array('1', '2')), 2);
        self::assertEqual($this->cf->get_count(self::$KEYS[0], $columns=array('1')), 1);
    }

    public function test_insert_multiget_count() {
        $columns = array('1' => 'val1', '2' => 'val2');
        foreach(self::$KEYS as $key)
            $this->cf->insert($key, $columns);

        $result = $this->cf->multiget_count(self::$KEYS);
        foreach(self::$KEYS as $key)
            self::assertEqual($result[$key], 2);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=null, $column_start='1');
        self::assertEqual(count($result), 3);
        self::assertEqual($result[self::$KEYS[0]], 2);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=null, $column_start='', $column_finish='2');
        self::assertEqual(count($result), 3);
        self::assertEqual($result[self::$KEYS[0]], 2);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=null, $column_start='1', $column_finish='2');
        self::assertEqual(count($result), 3);
        self::assertEqual($result[self::$KEYS[0]], 2);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=null, $column_start='1', $column_finish='1');
        self::assertEqual(count($result), 3);
        self::assertEqual($result[self::$KEYS[0]], 1);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=array('1', '2'));
        self::assertEqual(count($result), 3);
        self::assertEqual($result[self::$KEYS[0]], 2);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=array('1'));
        self::assertEqual(count($result), 3);
        self::assertEqual($result[self::$KEYS[0]], 1);
    }

    public function test_insert_get_range() {
        $columns = array('1' => 'val1', '2' => 'val2');
        foreach (self::$KEYS as $key)
            $this->cf->insert($key, $columns);

        $rows = $this->cf->get_range($start_key=self::$KEYS[0], $finish_key=self::$KEYS[2]);
        self::assertEqual(count($rows), count(self::$KEYS));
        foreach($rows as $row)
            self::assertEqual($row, $columns);

        $this->cf->insert('test_get_range5', $columns);
        $rows = $this->cf->get_range($start_key=self::$KEYS[0], $finish_key=self::$KEYS[2]);
        self::assertEqual(count($rows), count(self::$KEYS));
    }

    public function test_remove() {
        $columns = array('1' => 'val1', '2' => 'val2');
        $this->cf->insert(self::$KEYS[0], $columns);

        self::assertEqual($this->cf->get(self::$KEYS[0]), $columns);

        $this->cf->remove(self::$KEYS[0], array('2'));
        unset($columns['2']);
        self::assertEqual($this->cf->get(self::$KEYS[0]), $columns);

        $this->cf->remove(self::$KEYS[0]);
        try {
            $this->cf->get(self::$KEYS[0]);
            self::assertTrue(false);
        } catch (cassandra_NotFoundException $e) {
        }
    }
}

class TestSuperColumnFamily extends UnitTestCase {

    private $client;
    private $cf;

    private static $KEYS = array('key1', 'key2', 'key3');

    public function setUp() {
        $this->client = new Connection('Keyspace1');
        $this->cf = new ColumnFamily($this->client, 'Super1');
    }

    public function tearDown() {
        foreach(self::$KEYS as $key)
            $this->cf->remove($key);
    }

    public function test_super() {
        $columns = array('1' => array('sub1' => 'val1', 'sub2' => 'val2'),
                         '2' => array('sub3' => 'val3', 'sub3' => 'val3'));
        try {
            $this->cf->get(self::$KEYS[0]);
            assert(false);
        } catch (cassandra_NotFoundException $e) {
        }

        $this->cf->insert(self::$KEYS[0], $columns);
        self::assertEqual($this->cf->get(self::$KEYS[0]), $columns);
        self::assertEqual($this->cf->multiget(array(self::$KEYS[0])), array(self::$KEYS[0] => $columns));
        self::assertEqual($this->cf->get_range($start_key=self::$KEYS[0],
                                               $finish_key=self::$KEYS[0]),
                          array(self::$KEYS[0] => $columns));
    }

    public function test_super_column_argument() {
        $key = self::$KEYS[0];
        $sub12 = array('sub1' => 'val1', 'sub2' => 'val2');
        $sub34 = array('sub3' => 'val3', 'sub4' => 'val4');
        $cols = array('1' => $sub12, '2' => $sub34);
        $this->cf->insert($key, $cols);
        self::assertEqual($this->cf->get($key, null, '', '', false, 100, $super_column='1'), $sub12);
        try {
            $this->cf->get($key, null, '', '', false, 100, $super_column='3');
            assert(false);
        } catch (cassandra_NotFoundException $e) {
        }
        self::assertEqual($this->cf->multiget(array($key), null, '', '', false, 100, $super_column='1'),
                          array($key => $sub12));
        self::assertEqual($this->cf->get_range($start_key=$key, $end_key=$key, 100, null, '',
                                               '', false, 100, $super_column='1'),
                          array($key => $sub12));
    }
}
?>
