<?php
/**
 *  @generated
 */
namespace cassandra;
class CqlPreparedResult extends \TBase {
  static $_TSPEC;

  public $itemId = null;
  public $count = null;
  public $variable_types = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'itemId',
          'type' => \TType::I32,
          ),
        2 => array(
          'var' => 'count',
          'type' => \TType::I32,
          ),
        3 => array(
          'var' => 'variable_types',
          'type' => \TType::LST,
          'etype' => \TType::STRING,
          'elem' => array(
            'type' => \TType::STRING,
            ),
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'CqlPreparedResult';
  }

  public function read($input)
  {
    return $this->_read('CqlPreparedResult', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('CqlPreparedResult', self::$_TSPEC, $output);
  }
}


?>
