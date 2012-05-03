<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_insert_args extends \TBase {
  static $_TSPEC;

  public $key = null;
  public $column_parent = null;
  public $column = null;
  public $consistency_level =   1;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'key',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'column_parent',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\ColumnParent',
          ),
        3 => array(
          'var' => 'column',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\Column',
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
    return 'Cassandra_insert_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_insert_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_insert_args', self::$_TSPEC, $output);
  }
}


?>
