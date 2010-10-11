<?php
require_once('simpletest/autorun.php');
require_once('../phpcassa.php');

class TestColumnFamily extends UnitTestCase {

    private $client;
    private $cf;

    public function setUp() {
        $this->client = new Connection('Keyspace1');
        $this->cf = new ColumnFamily($this->client, 'Standard1');
    }

    public function tearDown() {
        $this->cf->truncate();
    }

    public function test_opening_connection() {
        $this->client->connect();
    }

    public function test_empty() {
        $key = 'TestColumnFamily.test_empty';
        try {
            $this->cf->get($key);
            self::asertTrue(false);
        } catch (cassandra_NotFoundException $e) {
        }
    }

    public function test_insert_get() {
        $this->cf->insert('key', array('col' => 'val'));
        self::assertEqual($this->cf->get('key'), array('col' => 'val'));
    }

    public function test_insert_multiget() {
        $key1 = 'TestColumnFamily.test_insert_multiget1';
        $columns1 = array('1' => 'val1', '2' => 'val2');
        $key2 = 'TestColumnFamily.test_insert_multiget2';
        $columns2 = array('3' => 'val1', '4' => 'val2');
        $missing_key = 'key3';

        $this->cf->insert($key1, $columns1);
        $this->cf->insert($key2, $columns2);
        $rows = $this->cf->multiget(array($key1, $key2, $missing_key));
        self::assertEqual(count($rows), 2);
        self::assertEqual($rows[$key1], $columns1);
        self::assertEqual($rows[$key2], $columns2);
        self::assertFalse(in_array($missing_key, $rows));
    }

    public function test_insert_get_count() {
        $key = 'TestColumnFamily.test_insert_get_count';
        $cols = array('1' => 'val1', '2' => 'val2');
        $this->cf->insert($key, $cols);
        self::assertEqual($this->cf->get_count($key), 2);

        self::assertEqual($this->cf->get_count($key, $columns=null, $column_start='1'), 2);
        self::assertEqual($this->cf->get_count($key, $columns=null, $column_start='',
                                               $column_finish='2'), 2);
        self::assertEqual($this->cf->get_count($key, $columns=null, $column_start='1', $column_finish='2'), 2);
        self::assertEqual($this->cf->get_count($key, $columns=null, $column_start='1', $column_finish='1'), 1);
        self::assertEqual($this->cf->get_count($key, $columns=array('1', '2')), 2);
        self::assertEqual($this->cf->get_count($key, $columns=array('1')), 1);
    }

    public function test_insert_multiget_count() {
        $keys = array('TestColumnFamily.test_insert_multiget_count1',
                      'TestColumnCamily.test_insert_multiget_count2',
                      'TestColumnCamily.test_insert_multiget_count3');
        $columns = array('1' => 'val1', '2' => 'val2');
        foreach($keys as $key)
            $this->cf->insert($key, $columns);

        $result = $this->cf->multiget_count($keys);
        self::assertEqual($result[$keys[0]], 2);
        self::assertEqual($result[$keys[1]], 2);
        self::assertEqual($result[$keys[2]], 2);

        $result = $this->cf->multiget_count($keys, $columns=null, $column_start='1');
        self::assertEqual(count($result), 3);
        self::assertEqual($result[$keys[0]], 2);

        $result = $this->cf->multiget_count($keys, $columns=null, $column_start='', $column_finish='2');
        self::assertEqual(count($result), 3);
        self::assertEqual($result[$keys[0]], 2);

        $result = $this->cf->multiget_count($keys, $columns=null, $column_start='1', $column_finish='2');
        self::assertEqual(count($result), 3);
        self::assertEqual($result[$keys[0]], 2);

        $result = $this->cf->multiget_count($keys, $columns=null, $column_start='1', $column_finish='1');
        self::assertEqual(count($result), 3);
        self::assertEqual($result[$keys[0]], 1);

        $result = $this->cf->multiget_count($keys, $columns=array('1', '2'));
        self::assertEqual(count($result), 3);
        self::assertEqual($result[$keys[0]], 2);

        $result = $this->cf->multiget_count($keys, $columns=array('1'));
        self::assertEqual(count($result), 3);
        self::assertEqual($result[$keys[0]], 1);
    }

    public function test_insert_get_range() {
        $keys = array_map(function($x) {return 'test_get_range'.$x;}, range(0,4));
        $columns = array('1' => 'val1', '2' => 'val2');
        foreach ($keys as $key)
            $this->cf->insert($key, $columns);

        $rows = $this->cf->get_range($start_key=$keys[0], $finish_key=$keys[4]);
        self::assertEqual(count($rows), count($keys));
        foreach($rows as $row)
            self::assertEqual($row, $columns);

        $this->cf->insert('test_get_range5', $columns);
        $rows = $this->cf->get_range($start_key=$keys[0], $finish_key=$keys[4]);
        self::assertEqual(count($rows), count($keys));
    }
}
?>
