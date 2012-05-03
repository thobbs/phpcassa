<?php
/**
 *  @generated
 */
namespace cassandra;
class ColumnOrSuperColumn extends \TBase {
  static $_TSPEC;

  public $column = null;
  public $super_column = null;
  public $counter_column = null;
  public $counter_super_column = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'column',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\Column',
          ),
        2 => array(
          'var' => 'super_column',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\SuperColumn',
          ),
        3 => array(
          'var' => 'counter_column',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\CounterColumn',
          ),
        4 => array(
          'var' => 'counter_super_column',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\CounterSuperColumn',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'ColumnOrSuperColumn';
  }

  public function read($input)
  {
    return $this->_read('ColumnOrSuperColumn', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('ColumnOrSuperColumn', self::$_TSPEC, $output);
  }
}


?>
