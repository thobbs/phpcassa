<?php
require_once('simpletest/autorun.php');
require_once('../connection.php');
require_once('../columnfamily.php');
require_once('../uuid.php');

class TestPooling extends UnitTestCase {

    public function test_failover_under_limit() {
        $pool = new ConnectionPool('Keyspace1', array('localhost:9160'));
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 5);
        foreach (range(1, 4) as $i) {
            $conn = $pool->get();
            $conn->client = new MockClient($conn->transport);
            $pool->return_connection($conn);
        }
        $cf = new ColumnFamily($pool, 'Standard1');
        $cf->insert('key', array('col' => 'val'));
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 9);
        self::assertEqual($stats['failed'], 4);
        self::assertEqual($stats['recycled'], 0);
    }

    public function test_failover_over_limit() {
        $pool = new ConnectionPool('Keyspace1', NULL, 5, 4);
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 5);
        foreach (range(1, 5) as $i) {
            $conn = $pool->get();
            $conn->client = new MockClient($conn->transport);
            $pool->return_connection($conn);
        }
        $cf = new ColumnFamily($pool, 'Standard1');
        try {
            $cf->insert('key', array('col' => 'val'));
            self::assertTrue(false);
        } catch (MaxRetriesException $ex) {
        }
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 10);
        self::assertEqual($stats['failed'], 5);
        self::assertEqual($stats['recycled'], 0);
    }

    public function test_recycle() {
        $pool = new ConnectionPool('Keyspace1', NULL, 5, 5, 5000, 5000, 10);
        $cf = new ColumnFamily($pool, 'Standard1');
        foreach (range(1, 50) as $i) {
            $cf->insert('key', array('c' => 'v'));
        }
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 10);
        self::assertEqual($stats['failed'], 0);
        self::assertEqual($stats['recycled'], 5);

        foreach (range(1, 50) as $i) {
            $cf->insert('key', array('c' => 'v'));
        }
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 15);
        self::assertEqual($stats['failed'], 0);
        self::assertEqual($stats['recycled'], 10);
    }

    public function test_multiple_servers() {
        $servers = array('localhost:9160', '127.0.0.1:9160', '127.0.0.1');
        $pool = new ConnectionPool('Keyspace1', $servers);
        $cf = new ColumnFamily($pool, 'Standard1');
        foreach (range(1, 50) as $i) {
            $cf->insert('key', array('c' => 'v'));
        }
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 6);
        self::assertEqual($stats['failed'], 0);
    }

    public function test_initial_connection_failure() {
        $servers = array('localhost', 'foobar');
        $pool = new ConnectionPool('Keyspace1', $servers);
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 5);
        self::assertTrue($stats['failed'] == 5 || $stats['failed'] == 4);
        $cf = new ColumnFamily($pool, 'Standard1');
        foreach (range(1, 50) as $i) {
            $cf->insert('key', array('c' => 'v'));
        }
        $pool->dispose();

        $servers = array('barfoo', 'foobar');
        try {
            $pool = new ConnectionPool('Keyspace1', $servers);
            self::assertTrue(false);
        } catch (NoServerAvailable $ex) {
        }
    }
}

class MockClient extends CassandraClient {

    public function __construct($transport) {
        parent::__construct(new TBinaryProtocolAccelerated($transport));
    }

    public function batch_mutate($mutation_map, $consistency_level) {
        throw new cassandra_TimedOutException();
    }

}
?>
