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

    public function test_insert_get_small_range_as_array() {
        $columns = array('1' => 'val1', '2' => 'val2');
        foreach (self::$KEYS as $key)
            $this->cf->insert($key, $columns);

        $rows = $this->cf->get_small_range_as_array($start_key=self::$KEYS[0], $finish_key=self::$KEYS[2]);
        self::assertEqual(count($rows), count(self::$KEYS));
        foreach($rows as $row)
            self::assertEqual($row, $columns);

        $this->cf->insert(self::$KEYS[0], $columns);
        $rows = $this->cf->get_small_range_as_array($start_key=self::$KEYS[0], $finish_key=self::$KEYS[2]);
        self::assertEqual(count($rows), count(self::$KEYS));
    }

    public function test_insert_get_range() {
        $cl = cassandra_ConsistencyLevel::ONE;
        $cf = new ColumnFamily($this->client,
                               'Standard1', true, true,
                               $read_consistency_level=$cl,
                               $write_consistency_level=$cl,
                               $buffer_size=10);
        $keys = array();
        $columns = array('c' => 'v');
        foreach (range(100, 200) as $i) {
            $keys[] = 'key'.$i;
            $cf->insert('key'.$i, $columns);
        }

        # Keys at the end that we don't want
        foreach (range(201, 300) as $i)
            $cf->insert('key'.$i, $columns);


        # Buffer size = 10; rowcount is divisible by buffer size
        $count = 0;
        foreach ($cf->get_range() as $key => $cols) {
            self::assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        self::assertEqual($count, 100);


        # Buffer size larger than row count
        $cf = new ColumnFamily($this->client, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=1000);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=100) as $key => $cols) {
            self::assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        self::assertEqual($count, 100);


        # Buffer size larger than row count, less than total number of rows
        $cf = new ColumnFamily($this->client, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=150);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=100) as $key => $cols) {
            self::assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        self::assertEqual($count, 100);


        # Odd number for batch size
        $cf = new ColumnFamily($this->client, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=7);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=100) as $key => $cols) {
            self::assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        self::assertEqual($count, 100);


        # Smallest buffer size available
        $cf = new ColumnFamily($this->client, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=2);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=100) as $key => $cols) {
            self::assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        self::assertEqual($count, 100);


        # Put the remaining keys in our list
        foreach (range(201, 300) as $i)
            $keys[] = 'key'.$i;


        # Row count above total number of rows
        $cf = new ColumnFamily($this->client, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=2);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=10000) as $key => $cols) {
            self::assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        self::assertEqual($count, 201);


        # Row count above total number of rows
        $cf = new ColumnFamily($this->client, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=7);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=10000) as $key => $cols) {
            self::assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        self::assertEqual($count, 201);


 
        # Row count above total number of rows, buffer_size = total number of rows
        $cf = new ColumnFamily($this->client, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=200);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=10000) as $key => $cols) {
            self::assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        self::assertEqual($count, 201);
 
 
        # Row count above total number of rows, buffer_size = total number of rows
        $cf = new ColumnFamily($this->client, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=10000);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=10000) as $key => $cols) {
            self::assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        self::assertEqual($count, 201);
     

        $cf->truncate();
    }

    public function test_get_indexed_slices() {
        $indexed_cf = new ColumnFamily($this->client, 'Indexed1');

        $columns = array('birthdate' => 1);

        foreach(range(1,3) as $i)
            $indexed_cf->insert('key'.$i, $columns);

        $expr = CassandraUtil::create_index_expression($column_name='birthdate', $value=1);
        $clause = CassandraUtil::create_index_clause(array($expr));
        $result = $indexed_cf->get_indexed_slices($clause);
        self::assertEqual(count($result), 3);
        foreach(range(1,3) as $i)
            self::assertEqual($result['key'.$i], $columns);

        $indexed_cf->truncate();
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
        self::assertEqual($this->cf->get_small_range_as_array($start_key=self::$KEYS[0],
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
        self::assertEqual($this->cf->get_small_range_as_array($start_key=$key, $end_key=$key, 100, null, '',
                                               '', false, 100, $super_column='1'),
                          array($key => $sub12));
    }
}
?>
