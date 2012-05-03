<?php
/**
 *  @generated
 */
namespace cassandra;
class CfDef extends \TBase {
  static $_TSPEC;

  public $keyspace = null;
  public $name = null;
  public $column_type = "Standard";
  public $comparator_type = "BytesType";
  public $subcomparator_type = null;
  public $comment = null;
  public $read_repair_chance = null;
  public $column_metadata = null;
  public $gc_grace_seconds = null;
  public $default_validation_class = null;
  public $id = null;
  public $min_compaction_threshold = null;
  public $max_compaction_threshold = null;
  public $replicate_on_write = null;
  public $key_validation_class = null;
  public $key_alias = null;
  public $compaction_strategy = null;
  public $compaction_strategy_options = null;
  public $compression_options = null;
  public $bloom_filter_fp_chance = null;
  public $caching = "keys_only";
  public $dclocal_read_repair_chance = 0;
  public $row_cache_size = null;
  public $key_cache_size = null;
  public $row_cache_save_period_in_seconds = null;
  public $key_cache_save_period_in_seconds = null;
  public $memtable_flush_after_mins = null;
  public $memtable_throughput_in_mb = null;
  public $memtable_operations_in_millions = null;
  public $merge_shards_chance = null;
  public $row_cache_provider = null;
  public $row_cache_keys_to_save = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'keyspace',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'name',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'column_type',
          'type' => \TType::STRING,
          ),
        5 => array(
          'var' => 'comparator_type',
          'type' => \TType::STRING,
          ),
        6 => array(
          'var' => 'subcomparator_type',
          'type' => \TType::STRING,
          ),
        8 => array(
          'var' => 'comment',
          'type' => \TType::STRING,
          ),
        12 => array(
          'var' => 'read_repair_chance',
          'type' => \TType::DOUBLE,
          ),
        13 => array(
          'var' => 'column_metadata',
          'type' => \TType::LST,
          'etype' => \TType::STRUCT,
          'elem' => array(
            'type' => \TType::STRUCT,
            'class' => '\cassandra\ColumnDef',
            ),
          ),
        14 => array(
          'var' => 'gc_grace_seconds',
          'type' => \TType::I32,
          ),
        15 => array(
          'var' => 'default_validation_class',
          'type' => \TType::STRING,
          ),
        16 => array(
          'var' => 'id',
          'type' => \TType::I32,
          ),
        17 => array(
          'var' => 'min_compaction_threshold',
          'type' => \TType::I32,
          ),
        18 => array(
          'var' => 'max_compaction_threshold',
          'type' => \TType::I32,
          ),
        24 => array(
          'var' => 'replicate_on_write',
          'type' => \TType::BOOL,
          ),
        26 => array(
          'var' => 'key_validation_class',
          'type' => \TType::STRING,
          ),
        28 => array(
          'var' => 'key_alias',
          'type' => \TType::STRING,
          ),
        29 => array(
          'var' => 'compaction_strategy',
          'type' => \TType::STRING,
          ),
        30 => array(
          'var' => 'compaction_strategy_options',
          'type' => \TType::MAP,
          'ktype' => \TType::STRING,
          'vtype' => \TType::STRING,
          'key' => array(
            'type' => \TType::STRING,
          ),
          'val' => array(
            'type' => \TType::STRING,
            ),
          ),
        32 => array(
          'var' => 'compression_options',
          'type' => \TType::MAP,
          'ktype' => \TType::STRING,
          'vtype' => \TType::STRING,
          'key' => array(
            'type' => \TType::STRING,
          ),
          'val' => array(
            'type' => \TType::STRING,
            ),
          ),
        33 => array(
          'var' => 'bloom_filter_fp_chance',
          'type' => \TType::DOUBLE,
          ),
        34 => array(
          'var' => 'caching',
          'type' => \TType::STRING,
          ),
        37 => array(
          'var' => 'dclocal_read_repair_chance',
          'type' => \TType::DOUBLE,
          ),
        9 => array(
          'var' => 'row_cache_size',
          'type' => \TType::DOUBLE,
          ),
        11 => array(
          'var' => 'key_cache_size',
          'type' => \TType::DOUBLE,
          ),
        19 => array(
          'var' => 'row_cache_save_period_in_seconds',
          'type' => \TType::I32,
          ),
        20 => array(
          'var' => 'key_cache_save_period_in_seconds',
          'type' => \TType::I32,
          ),
        21 => array(
          'var' => 'memtable_flush_after_mins',
          'type' => \TType::I32,
          ),
        22 => array(
          'var' => 'memtable_throughput_in_mb',
          'type' => \TType::I32,
          ),
        23 => array(
          'var' => 'memtable_operations_in_millions',
          'type' => \TType::DOUBLE,
          ),
        25 => array(
          'var' => 'merge_shards_chance',
          'type' => \TType::DOUBLE,
          ),
        27 => array(
          'var' => 'row_cache_provider',
          'type' => \TType::STRING,
          ),
        31 => array(
          'var' => 'row_cache_keys_to_save',
          'type' => \TType::I32,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'CfDef';
  }

  public function read($input)
  {
    return $this->_read('CfDef', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('CfDef', self::$_TSPEC, $output);
  }
}


?>
