<?php
/**
 *  @generated
 */
namespace cassandra;
class ColumnParent extends \TBase {
  static $_TSPEC;

  public $column_family = null;
  public $super_column = null;

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
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'ColumnParent';
  }

  public function read($input)
  {
    return $this->_read('ColumnParent', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('ColumnParent', self::$_TSPEC, $output);
  }
}


?>
