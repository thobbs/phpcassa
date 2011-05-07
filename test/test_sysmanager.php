<?php
require_once('simpletest/autorun.php');
require_once('../sysmanager.php');
require_once('../thrift/packages/cassandra/cassandra_types.php');

class TestSystemManager extends UnitTestCase {

    public function setUp() {
        $this->sys = new SystemManager();
    }

    public function tearDown() {
        $this->sys->close();
    }

    public function test_basic() {
        $this->sys->describe_cluster_name();
        $this->sys->describe_schema_versions();
        $this->sys->describe_partitioner();
        $this->sys->describe_snitch();
    }

    public function test_keyspace_manipulation() {
        $ksname = "PhpcassaKeyspace";
        try {
            $this->sys->drop_keyspace($ksname);
        } catch (cassandra_InvalidRequestException $e) {
            // don't care
        }

        $ksdef = new cassandra_KsDef();
        $ksdef->name = $ksname;
        $ksdef->strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
        $ksdef->strategy_options = NULL;
        $ksdef->replication_factor = 1;
        $ksdef->cf_defs = array();
        $this->sys->create_keyspace($ksdef);

        $ksdef = $this->sys->describe_keyspace($ksname);
        self::assertEqual($ksdef->name, $ksname);
        self::assertEqual($ksdef->replication_factor, 1);

        $attrs = array("strategy_options" => "org.apache.cassandra.locator.OldNetworkTopologyStrategy");
        $this->sys->alter_keyspace($ksname, $attrs);
        $ksdef = $this->sys->describe_keyspace($ksname);
        self::assertEqual($ksdef->name, $ksname);
        self::assertEqual($ksdef->replication_factor, 1);

        $this->sys->drop_keyspace($ksname);
    }

    private function get_cfdef($ksname, $cfname) {
        $ksdef = $this->sys->describe_keyspace($ksname);
        $cfdefs = $ksdef->cf_defs;
        foreach($cfdefs as $cfdef) {
            if ($cfdef->name == $cfname)
                return $cfdef;
        }
        return;
    }

    public function test_cf_manipulation() {
        $ksname = "PhpcassaKeyspace";
        $ksdef = new cassandra_KsDef();
        $ksdef->name = $ksname;
        $ksdef->strategy_class = "org.apache.cassandra.locator.SimpleStrategy";
        $ksdef->strategy_options = NULL;
        $ksdef->replication_factor = 1;
        $ksdef->cf_defs = array();
        $this->sys->create_keyspace($ksdef);

        $cfname = "CF";
        $cfdef = new cassandra_CfDef();
        $cfdef->keyspace = $ksname;
        $cfdef->name = $cfname;
        $cfdef->column_type = 'Standard';
        $cfdef->comment = 'this is a comment';
        $this->sys->create_column_family($cfdef);

        $cfdef = $this->get_cfdef($ksname, $cfname);
        self::assertEqual($cfdef->comment, 'this is a comment');
        $cfdef->comment = 'this is a new comment';
        $this->sys->alter_column_family($cfdef);
        $cfdef = $this->get_cfdef($ksname, $cfname);
        self::assertEqual($cfdef->comment, 'this is a new comment');

        $this->sys->drop_column_family($ksname, $cfname);

        $this->sys->drop_keyspace($ksname);
    }
}
?>
