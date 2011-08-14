<?php
require_once('simpletest/autorun.php');
require_once('../connection.php');
require_once('../columnfamily.php');
require_once('../uuid.php');
require_once('../sysmanager.php');

class TestLargeOps extends UnitTestCase {

    private static $VALS = array('val1', 'val2', 'val3');
    private static $KEYS = array('key1', 'key2', 'key3');
    private static $KS = "TestLargeOps";

    private $client;
    private $cf;

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
            $this->sys->create_column_family(self::$KS, 'Standard1', $cfattrs);

            $this->pool = new ConnectionPool(self::$KS);
            $this->cf = new ColumnFamily($this->pool, 'Standard1');
        } catch (Exception $e) {
            print($e);
            throw $e;
        }

        parent::__construct();
    }

    public function __destruct() {
        $this->sys->drop_keyspace(self::$KS);
        $this->pool->dispose();
    }

    public function test_large_ops() {
        $str = '';
        foreach(range(0,255) as $i) {
            # each addition is 64 bytes
            $str .= 'aaaaaaa aaaaaaa aaaaaaa aaaaaa aaaaaaa aaaaaaa aaaaaaa aaaaaaa ';
        }

        foreach(range(0, 99) as $i)
            $this->cf->insert("key$i", array($str => $str));

        foreach(range(0, 99) as $i) {
            $res = $this->cf->get("key$i");
            self::assertEqual($res[$str], $str);
        }
    }
}
?>
