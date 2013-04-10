<?php

use Thrift\Protocol\TBinaryProtocolAccelerated;

use phpcassa\Connection\ConnectionPool;
use phpcassa\Connection\MaxRetriesException;
use phpcassa\Connection\NoServerAvailable;
use phpcassa\ColumnFamily;
use phpcassa\SystemManager;

use cassandra\TimedOutException;
use cassandra\CassandraClient;

class MockClient extends CassandraClient {

    public function __construct($transport) {
        parent::__construct(new TBinaryProtocolAccelerated($transport));
    }

    public function batch_mutate($mutation_map, $consistency_level) {
        throw new TimedOutException();
    }
}

class SilentConnectionPool extends ConnectionPool {
    protected function error_log($errorMsg, $messageType=0) { }
}

class ConnectionPoolTest extends PHPUnit_Framework_TestCase {

    private static $KS = "TestPooling";
    private static $CF = "Standard1";

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
            $sys->create_column_family(self::$KS, self::$CF, $cfattrs);

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

    public function test_failover_under_limit() {
        $pool = new SilentConnectionPool(self::$KS, array('localhost:9160'));
        $pool->fill();
        $stats = $pool->stats();
        $this->assertEquals($stats['created'], 5);
        foreach (range(1, 4) as $i) {
            $conn = $pool->get();
            $conn->client = new MockClient($conn->transport);
            $pool->return_connection($conn);
        }
        $cf = new ColumnFamily($pool, self::$CF);
        $cf->insert('key', array('col' => 'val'));
        $stats = $pool->stats();
        $this->assertEquals($stats['created'], 9);
        $this->assertEquals($stats['failed'], 4);
        $this->assertEquals($stats['recycled'], 0);
    }

    public function test_failover_over_limit() {
        $pool = new SilentConnectionPool(self::$KS, NULL, 5, 4);
        $pool->fill();
        $stats = $pool->stats();
        $this->assertEquals($stats['created'], 5);
        foreach (range(1, 5) as $i) {
            $conn = $pool->get();
            $conn->client = new MockClient($conn->transport);
            $pool->return_connection($conn);
        }

        $cf = new ColumnFamily($pool, self::$CF);
        $this->setExpectedException('\phpcassa\Connection\MaxRetriesException');
        $cf->insert('key', array('col' => 'val'));

        $stats = $pool->stats();
        $this->assertEquals($stats['created'], 10);
        $this->assertEquals($stats['failed'], 5);
        $this->assertEquals($stats['recycled'], 0);
    }

    public function test_recycle() {
        $pool = new SilentConnectionPool(self::$KS, NULL, 5, 5, 5000, 5000, 10);
        $pool->fill();
        $cf = new ColumnFamily($pool, self::$CF);
        foreach (range(1, 50) as $i) {
            $cf->insert('key', array('c' => 'v'));
        }
        $stats = $pool->stats();
        $this->assertEquals($stats['created'], 10);
        $this->assertEquals($stats['failed'], 0);
        $this->assertEquals($stats['recycled'], 5);

        foreach (range(1, 50) as $i) {
            $cf->insert('key', array('c' => 'v'));
        }
        $stats = $pool->stats();
        $this->assertEquals($stats['created'], 15);
        $this->assertEquals($stats['failed'], 0);
        $this->assertEquals($stats['recycled'], 10);
    }

    public function test_multiple_servers() {
        $servers = array('localhost:9160', '127.0.0.1:9160', '127.0.0.1');
        $pool = new SilentConnectionPool(self::$KS, $servers);
        $pool->fill();
        $cf = new ColumnFamily($pool, self::$CF);
        foreach (range(1, 50) as $i) {
            $cf->insert('key', array('c' => 'v'));
        }
        $stats = $pool->stats();
        $this->assertEquals($stats['created'], 6);
        $this->assertEquals($stats['failed'], 0);
    }

    public function test_initial_connection_failure() {
        $servers = array('localhost', 'foobar');
        $pool = new SilentConnectionPool(self::$KS, $servers);
        $pool->fill();
        $stats = $pool->stats();
        $this->assertEquals($stats['created'], 5);
        $this->assertTrue($stats['failed'] == 5 || $stats['failed'] == 4);
        $cf = new ColumnFamily($pool, self::$CF);
        foreach (range(1, 50) as $i) {
            $cf->insert('key', array('c' => 'v'));
        }
        $pool->dispose();

        $servers = array('barfoo', 'foobar');
        $pool = new SilentConnectionPool(self::$KS, $servers);
        $this->setExpectedException('\phpcassa\Connection\NoServerAvailable');
        $pool->fill();
    }
}

