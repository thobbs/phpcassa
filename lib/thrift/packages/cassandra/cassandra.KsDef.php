<?php
/**
 *  @generated
 */
namespace cassandra;
class KsDef extends \TBase {
  static $_TSPEC;

  public $name = null;
  public $strategy_class = null;
  public $strategy_options = null;
  public $replication_factor = null;
  public $cf_defs = null;
  public $durable_writes = true;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'name',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'strategy_class',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'strategy_options',
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
        4 => array(
          'var' => 'replication_factor',
          'type' => \TType::I32,
          ),
        5 => array(
          'var' => 'cf_defs',
          'type' => \TType::LST,
          'etype' => \TType::STRUCT,
          'elem' => array(
            'type' => \TType::STRUCT,
            'class' => '\cassandra\CfDef',
            ),
          ),
        6 => array(
          'var' => 'durable_writes',
          'type' => \TType::BOOL,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'KsDef';
  }

  public function read($input)
  {
    return $this->_read('KsDef', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('KsDef', self::$_TSPEC, $output);
  }
}


?>
