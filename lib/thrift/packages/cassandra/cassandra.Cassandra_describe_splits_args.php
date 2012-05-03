<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_describe_splits_args extends \TBase {
  static $_TSPEC;

  public $cfName = null;
  public $start_token = null;
  public $end_token = null;
  public $keys_per_split = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'cfName',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'start_token',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'end_token',
          'type' => \TType::STRING,
          ),
        4 => array(
          'var' => 'keys_per_split',
          'type' => \TType::I32,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_describe_splits_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_describe_splits_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_describe_splits_args', self::$_TSPEC, $output);
  }
}


?>
