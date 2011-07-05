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

        $attrs = array();
        $attrs["strategy_class"] = StrategyClass::SIMPLE_STRATEGY;
        $attrs["strategy_options"] = NULL;
        $attrs["replication_factor"] = 1;
        $this->sys->create_keyspace($ksname, $attrs);

        $ksdef = $this->sys->describe_keyspace($ksname);
        self::assertEqual($ksdef->name, $ksname);
        self::assertEqual($ksdef->replication_factor, 1);

        $attrs["strategy_class"] = StrategyClass::OLD_NETWORK_TOPOLOGY_STRATEGY;
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
        $attrs = array();
        $attrs["strategy_class"] = StrategyClass::SIMPLE_STRATEGY;
        $attrs["strategy_options"] = NULL;
        $attrs["replication_factor"] = 1;
        $this->sys->create_keyspace($ksname, $attrs);

        $cfname = "CF";
        $attrs = array();
        $attrs["column_type"] = 'Standard';
        $attrs["comment"] = 'this is a comment';
        $this->sys->create_column_family($ksname, $cfname, $attrs);

        $cfdef = $this->get_cfdef($ksname, $cfname);
        self::assertEqual($cfdef->comment, 'this is a comment');

        $attrs = array("comment" => "this is a new comment");
        $this->sys->alter_column_family($ksname, $cfname, $attrs);
        $cfdef = $this->get_cfdef($ksname, $cfname);
        self::assertEqual($cfdef->comment, 'this is a new comment');

        $this->sys->create_index($ksname, $cfname, "name", DataType::ASCII_TYPE,
            "name_index", IndexType::KEYS);

        $this->sys->create_index($ksname, $cfname, "name2", DataType::ASCII_TYPE,
            "name_index2");

        $this->sys->create_index($ksname, $cfname, "name3", DataType::ASCII_TYPE);

        $this->sys->drop_index($ksname, $cfname, "name");
        $this->sys->drop_index($ksname, $cfname, "name2");
        $this->sys->drop_index($ksname, $cfname, "name3");
        $cfdef = $this->get_cfdef($ksname, $cfname);
        $col_meta = $cfdef->column_metadata;
        for ($i = 0; $i < count($col_meta); $i++) {
            $col = $col_meta[$i];
            self::assertEqual($col->index_type, NULL);
        }

        $this->sys->drop_column_family($ksname, $cfname);

        $this->sys->drop_keyspace($ksname);
    }
}
?>
