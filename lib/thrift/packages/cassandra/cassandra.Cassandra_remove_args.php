<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_remove_args extends \TBase {
  static $_TSPEC;

  public $key = null;
  public $column_path = null;
  public $timestamp = null;
  public $consistency_level =   1;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'key',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'column_path',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\ColumnPath',
          ),
        3 => array(
          'var' => 'timestamp',
          'type' => \TType::I64,
          ),
        4 => array(
          'var' => 'consistency_level',
          'type' => \TType::I32,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_remove_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_remove_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_remove_args', self::$_TSPEC, $output);
  }
}


?>
