<?php
require_once('simpletest/autorun.php');
require_once('../connection.php');
require_once('../columnfamily.php');
require_once('../uuid.php');
require_once('../sysmanager.php');

class TestPooling extends UnitTestCase {

    private $sys;

    private static $KS = "TestPooling";
    private static $CF = "Standard1";

    public function __construct() {
        try {
            $this->sys = new SystemManager();

            $ksdefs = $this->sys->describe_keyspaces();
            $exists = False;
            foreach ($ksdefs as $ksdef)
                $exists = $exists || $ksdef->name == self::$KS;

            if ($exists)
                $this->sys->drop_keyspace(self::$KS);

            $this->sys->create_keyspace(self::$KS, array());

            $cfattrs = array("column_type" => "Standard");
            $this->sys->create_column_family(self::$KS, self::$CF, $cfattrs);

        } catch (Exception $e) {
            print($e);
            throw $e;
        }

        parent::__construct();
    }

    public function __destruct() {
        $this->sys->drop_keyspace(self::$KS);
        $this->sys->close();
    }

    public function test_failover_under_limit() {
        $pool = new ConnectionPool(self::$KS, array('localhost:9160'));
        $pool->fill();
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 5);
        foreach (range(1, 4) as $i) {
            $conn = $pool->get();
            $conn->client = new MockClient($conn->transport);
            $pool->return_connection($conn);
        }
        $cf = new ColumnFamily($pool, self::$CF);
        $cf->insert('key', array('col' => 'val'));
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 9);
        self::assertEqual($stats['failed'], 4);
        self::assertEqual($stats['recycled'], 0);
    }

    public function test_failover_over_limit() {
        $pool = new ConnectionPool(self::$KS, NULL, 5, 4);
        $pool->fill();
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 5);
        foreach (range(1, 5) as $i) {
            $conn = $pool->get();
            $conn->client = new MockClient($conn->transport);
            $pool->return_connection($conn);
        }
        $cf = new ColumnFamily($pool, self::$CF);
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
        $pool = new ConnectionPool(self::$KS, NULL, 5, 5, 5000, 5000, 10);
        $pool->fill();
        $cf = new ColumnFamily($pool, self::$CF);
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
        $pool = new ConnectionPool(self::$KS, $servers);
        $pool->fill();
        $cf = new ColumnFamily($pool, self::$CF);
        foreach (range(1, 50) as $i) {
            $cf->insert('key', array('c' => 'v'));
        }
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 6);
        self::assertEqual($stats['failed'], 0);
    }

    public function test_initial_connection_failure() {
        $servers = array('localhost', 'foobar');
        $pool = new ConnectionPool(self::$KS, $servers);
        $pool->fill();
        $stats = $pool->stats();
        self::assertEqual($stats['created'], 5);
        self::assertTrue($stats['failed'] == 5 || $stats['failed'] == 4);
        $cf = new ColumnFamily($pool, self::$CF);
        foreach (range(1, 50) as $i) {
            $cf->insert('key', array('c' => 'v'));
        }
        $pool->dispose();

        $servers = array('barfoo', 'foobar');
        try {
            $pool = new ConnectionPool(self::$KS, $servers);
            $pool->fill();
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
