<?php
/**
 *  @generated
 */
namespace cassandra;
class ColumnPath extends \TBase {
  static $_TSPEC;

  public $column_family = null;
  public $super_column = null;
  public $column = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        3 => array(
          'var' => 'column_family',
          'type' => \TType::STRING,
          ),
        4 => array(
          'var' => 'super_column',
          'type' => \TType::STRING,
          ),
        5 => array(
          'var' => 'column',
          'type' => \TType::STRING,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'ColumnPath';
  }

  public function read($input)
  {
    return $this->_read('ColumnPath', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('ColumnPath', self::$_TSPEC, $output);
  }
}


?>
