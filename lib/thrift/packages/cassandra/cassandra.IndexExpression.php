<?php
/**
 *  @generated
 */
namespace cassandra;
class IndexExpression extends \TBase {
  static $_TSPEC;

  public $column_name = null;
  public $op = null;
  public $value = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'column_name',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'op',
          'type' => \TType::I32,
          ),
        3 => array(
          'var' => 'value',
          'type' => \TType::STRING,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'IndexExpression';
  }

  public function read($input)
  {
    return $this->_read('IndexExpression', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('IndexExpression', self::$_TSPEC, $output);
  }
}


?>
