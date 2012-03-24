<?php

use phpcassa\ConsistencyLevel;
use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\Schema\DataType;
use phpcassa\SystemManager;

use phpcassa\Index\IndexExpression;
use phpcassa\Index\IndexClause;

class ColumnFamilyTest extends PHPUnit_Framework_TestCase {

    private $pool;
    private $cf;

    private static $KEYS = array('key1', 'key2', 'key3');
    private static $KS = "TestColumnFamily";

    public static function setUpBeforeClass() {
        try {
            $sys = new SystemManager();

            $ksdefs = $sys->describe_keyspaces();
            $exists = False;
            foreach ($ksdefs as $ksdef)
                $exists = $exists || $ksdef->name == self::$KS;

            if ($exists)
                $sys->drop_keyspace(self::$KS);

            $sys->create_keyspace(self::$KS, array());

            $cfattrs = array("column_type" => "Standard");
            $sys->create_column_family(self::$KS, 'Standard1', $cfattrs);

            $sys->create_column_family(self::$KS, 'Indexed1', $cfattrs);
            $sys->create_index(self::$KS, 'Indexed1', 'birthdate',
                                     DataType::LONG_TYPE, 'birthday_index');
            $sys->close();

        } catch (Exception $e) {
            print($e);
            throw $e;
        }
    }

    public static function tearDownAfterClass() {
        $sys = new SystemManager();
        $sys->drop_keyspace(self::$KS);
        $sys->close();
    }

    public function setUp() {
        $this->pool = new ConnectionPool(self::$KS);
        $this->cf = new ColumnFamily($this->pool, 'Standard1');
    }

    public function tearDown() {
        if ($this->cf) {
            foreach(self::$KEYS as $key)
                $this->cf->remove($key);
        }
        $this->pool->dispose();
    }

    public function test_empty() {
        try {
            $this->cf->get(self::$KEYS[0]);
            $this->assertTrue(false);
        } catch (cassandra_NotFoundException $e) {
        }
    }

    public function test_insert_get() {
        $this->cf->insert(self::$KEYS[0], array('col' => 'val'));
        $this->assertEquals($this->cf->get(self::$KEYS[0]), array('col' => 'val'));
    }

    public function test_insert_multiget() {
        $columns1 = array('1' => 'val1', '2' => 'val2');
        $columns2 = array('3' => 'val1', '4' => 'val2');

        $this->cf->insert(self::$KEYS[0], $columns1);
        $this->cf->insert(self::$KEYS[1], $columns2);
        $rows = $this->cf->multiget(self::$KEYS);
        $this->assertEquals(count($rows), 2);
        $this->assertEquals($rows[self::$KEYS[0]], $columns1);
        $this->assertEquals($rows[self::$KEYS[1]], $columns2);
        $this->assertFalse(in_array(self::$KEYS[2], $rows));

        $keys = array();
        for ($i = 0; $i < 100; $i++)
            $keys[] = "key" + (string)$i;
        foreach ($keys as $key) {
            $this->cf->insert($key, $columns1);
        }
        shuffle($keys);
        $rows = $this->cf->multiget($keys);
        $this->assertEquals(count($rows), 100);

        $i = 0;
        foreach ($rows as $key => $cols) {
            $this->assertEquals($key, $keys[$i]);
            $i++;
        }

        foreach ($keys as $key) {
            $this->cf->remove($key);
        }
    }

    public function test_batch_insert() {
        $columns1 = array('1' => 'val1', '2' => 'val2');
        $columns2 = array('3' => 'val1', '4' => 'val2');
        $rows = array(self::$KEYS[0] => $columns1,
                      self::$KEYS[1] => $columns2);
        $this->cf->batch_insert($rows);
        $rows = $this->cf->multiget(self::$KEYS);
        $this->assertEquals(count($rows), 2);
        $this->assertEquals($rows[self::$KEYS[0]], $columns1);
        $this->assertEquals($rows[self::$KEYS[1]], $columns2);
        $this->assertFalse(in_array(self::$KEYS[2], $rows));
    }

    public function test_insert_get_count() {
        $cols = array('1' => 'val1', '2' => 'val2');
        $this->cf->insert(self::$KEYS[0], $cols);
        $this->assertEquals($this->cf->get_count(self::$KEYS[0]), 2);

        $this->assertEquals($this->cf->get_count(self::$KEYS[0], $columns=null, $column_start='1'), 2);
        $this->assertEquals($this->cf->get_count(self::$KEYS[0], $columns=null, $column_start='',
                                               $column_finish='2'), 2);
        $this->assertEquals($this->cf->get_count(self::$KEYS[0], $columns=null, $column_start='1', $column_finish='2'), 2);
        $this->assertEquals($this->cf->get_count(self::$KEYS[0], $columns=null, $column_start='1', $column_finish='1'), 1);
        $this->assertEquals($this->cf->get_count(self::$KEYS[0], $columns=array('1', '2')), 2);
        $this->assertEquals($this->cf->get_count(self::$KEYS[0], $columns=array('1')), 1);
    }

    public function test_insert_multiget_count() {
        $columns = array('1' => 'val1', '2' => 'val2');
        foreach(self::$KEYS as $key)
            $this->cf->insert($key, $columns);

        $result = $this->cf->multiget_count(self::$KEYS);
        foreach(self::$KEYS as $key)
            $this->assertEquals($result[$key], 2);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=null, $column_start='1');
        $this->assertEquals(count($result), 3);
        $this->assertEquals($result[self::$KEYS[0]], 2);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=null, $column_start='', $column_finish='2');
        $this->assertEquals(count($result), 3);
        $this->assertEquals($result[self::$KEYS[0]], 2);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=null, $column_start='1', $column_finish='2');
        $this->assertEquals(count($result), 3);
        $this->assertEquals($result[self::$KEYS[0]], 2);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=null, $column_start='1', $column_finish='1');
        $this->assertEquals(count($result), 3);
        $this->assertEquals($result[self::$KEYS[0]], 1);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=array('1', '2'));
        $this->assertEquals(count($result), 3);
        $this->assertEquals($result[self::$KEYS[0]], 2);

        $result = $this->cf->multiget_count(self::$KEYS, $columns=array('1'));
        $this->assertEquals(count($result), 3);
        $this->assertEquals($result[self::$KEYS[0]], 1);

        // Test that multiget_count preserves the key order
        $columns = array('1' => 'val1', '2' => 'val2');
        $keys = array();
        for ($i = 0; $i < 100; $i++)
            $keys[] = "key" + (string)$i;
        foreach ($keys as $key) {
            $this->cf->insert($key, $columns);
        }
        shuffle($keys);
        $rows = $this->cf->multiget_count($keys);
        $this->assertEquals(count($rows), 100);

        $i = 0;
        foreach ($rows as $key => $count) {
            $this->assertEquals($key, $keys[$i]);
            $i++;
        }

        foreach ($keys as $key) {
            $this->cf->remove($key);
        }
    }

    public function test_insert_get_range() {
        $cl = ConsistencyLevel::ONE;
        $cf = new ColumnFamily($this->pool,
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
            $this->assertTrue(in_array($key, $keys), "$key was not expected");
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 100);


        # Buffer size larger than row count
        $cf = new ColumnFamily($this->pool, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=1000);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=100) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 100);


        # Buffer size larger than row count, less than total number of rows
        $cf = new ColumnFamily($this->pool, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=150);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=100) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 100);


        # Odd number for batch size
        $cf = new ColumnFamily($this->pool, 'Standard1', true, true,
                               $read_consistency_level=$cl,
                               $write_consistency_level=$cl,
                               $buffer_size=7);
        $count = 0;
        $rows = $cf->get_range($key_start='', $key_finish='', $row_count=100);
        foreach ($rows as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 100);


        # Smallest buffer size available
        $cf = new ColumnFamily($this->pool, 'Standard1', true, true,
                               $read_consistency_level=$cl,
                               $write_consistency_level=$cl,
                               $buffer_size=2);
        $count = 0;
        $rows = $cf->get_range($key_start='', $key_finish='', $row_count=100);
        foreach ($rows as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 100);


        # Put the remaining keys in our list
        foreach (range(201, 300) as $i)
            $keys[] = 'key'.$i;


        # Row count above total number of rows
        $cf = new ColumnFamily($this->pool, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=2);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=10000) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 201);


        # Row count above total number of rows
        $cf = new ColumnFamily($this->pool, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=7);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=10000) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 201);


 
        # Row count above total number of rows, buffer_size = total number of rows
        $cf = new ColumnFamily($this->pool, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=200);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=10000) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 201);


        # Row count above total number of rows, buffer_size = total number of rows
        $cf = new ColumnFamily($this->pool, 'Standard1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=10000);
        $count = 0;
        foreach ($cf->get_range($key_start='', $key_finish='', $row_count=10000) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 201);


        $cf->truncate();
    }

    public function test_batched_get_indexed_slices() {

        $cl = ConsistencyLevel::ONE;
        $cf = new ColumnFamily($this->pool, 'Indexed1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=10);
        $cf->truncate();

        $keys = array();
        $columns = array('birthdate' => 1);
        foreach (range(100, 200) as $i) {
            $keys[] = 'key'.$i;
            $cf->insert('key'.$i, $columns);
        }

        # Keys at the end that we don't want
        foreach (range(201, 300) as $i)
            $cf->insert('key'.$i, $columns);


        $expr = new IndexExpression($column_name='birthdate', $value=1);
        $clause = new IndexClause(array($expr), 100);

        # Buffer size = 10; rowcount is divisible by buffer size
        $count = 0;
        foreach ($cf->get_indexed_slices($clause) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 100);

        # Buffer size larger than row count
        $cf = new ColumnFamily($this->pool, 'Indexed1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=1000);
        $count = 0;
        foreach ($cf->get_indexed_slices($clause) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 100);


        # Buffer size larger than row count, less than total number of rows
        $cf = new ColumnFamily($this->pool, 'Indexed1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=150);
        $count = 0;
        foreach ($cf->get_indexed_slices($clause) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 100);


        # Odd number for batch size
        $cf = new ColumnFamily($this->pool, 'Indexed1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=7);
        $count = 0;
        foreach ($cf->get_indexed_slices($clause) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 100);


        # Smallest buffer size available
        $cf = new ColumnFamily($this->pool, 'Indexed1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=2);
        $count = 0;
        foreach ($cf->get_indexed_slices($clause) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 100);


        # Put the remaining keys in our list
        foreach (range(201, 300) as $i)
            $keys[] = 'key'.$i;


        # Row count above total number of rows
        $clause->count = 10000;
        $cf = new ColumnFamily($this->pool, 'Indexed1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=2);
        $count = 0;
        foreach ($cf->get_indexed_slices($clause) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 201);


        # Row count above total number of rows
        $cf = new ColumnFamily($this->pool, 'Indexed1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=7);
        $count = 0;
        foreach ($cf->get_indexed_slices($clause) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 201);


 
        # Row count above total number of rows, buffer_size = total number of rows
        $cf = new ColumnFamily($this->pool, 'Indexed1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=200);
        $count = 0;
        foreach ($cf->get_indexed_slices($clause) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 201);
 
 
        # Row count above total number of rows, buffer_size = total number of rows
        $cf = new ColumnFamily($this->pool, 'Indexed1', true, true,
                               $read_consistency_level=$cl, $write_consistency_level=$cl,
                               $buffer_size=10000);
        $count = 0;
        foreach ($cf->get_indexed_slices($clause) as $key => $cols) {
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
            $count++;
        }
        $this->assertEquals($count, 201);

        $cf->truncate();
    }

    public function test_get_indexed_slices() {
        $indexed_cf = new ColumnFamily($this->pool, 'Indexed1');
        $indexed_cf->truncate();

        $columns = array('birthdate' => 1);

        foreach(range(1,3) as $i)
            $indexed_cf->insert('key'.$i, $columns);

        $expr = new IndexExpression($column_name='birthdate', $value=1);
        $clause = new IndexClause(array($expr), 10000);
        $result = $indexed_cf->get_indexed_slices($clause);

        $count = 0;
        foreach($result as $key => $cols) {
            $count++;
            $this->assertEquals($columns, $cols);
            $this->assertEquals($key, "key$count");
        }
        $this->assertEquals($count, 3);

        # Insert and remove a matching row at the beginning
        $indexed_cf->insert('key0', $columns);
        $indexed_cf->remove('key0');
        # Insert and remove a matching row at the end
        $indexed_cf->insert('key4', $columns);
        $indexed_cf->remove('key4');
        # Remove a matching row from the middle 
        $indexed_cf->remove('key2');

        $result = $indexed_cf->get_indexed_slices($clause);

        $count = 0;
        foreach($result as $key => $cols) {
            $count++;
            $this->assertTrue($key == "key1" || $key == "key3");
        }
        $this->assertEquals($count, 2);

        $indexed_cf->truncate();

        $keys = array();
        foreach(range(1,1000) as $i) {
            $indexed_cf->insert("key$i", $columns);
            if ($i % 50 != 0)
                $indexed_cf->remove("key$i");
            else
                $keys[] = "key$i";
        }

        $count = 0;
        foreach($result as $key => $cols) {
            $count++;
            $this->assertTrue(in_array($key, $keys));
            unset($keys[$key]);
        }
        $this->assertEquals($count, 20);

        $indexed_cf->truncate();
    }

    public function test_remove() {
        $columns = array('1' => 'val1', '2' => 'val2');
        $this->cf->insert(self::$KEYS[0], $columns);

        $this->assertEquals($this->cf->get(self::$KEYS[0]), $columns);

        $this->cf->remove(self::$KEYS[0], array('2'));
        unset($columns['2']);
        $this->assertEquals($this->cf->get(self::$KEYS[0]), $columns);

        $this->cf->remove(self::$KEYS[0]);
        try {
            $this->cf->get(self::$KEYS[0]);
            $this->assertTrue(false);
        } catch (cassandra_NotFoundException $e) {
        }
    }
}

