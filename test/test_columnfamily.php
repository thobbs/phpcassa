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
}
?>
