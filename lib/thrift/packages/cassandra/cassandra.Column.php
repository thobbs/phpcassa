<?php
/**
 *  @generated
 */
namespace cassandra;
class Column extends \TBase {
  static $_TSPEC;

  public $name = null;
  public $value = null;
  public $timestamp = null;
  public $ttl = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'name',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'value',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'timestamp',
          'type' => \TType::I64,
          ),
        4 => array(
          'var' => 'ttl',
          'type' => \TType::I32,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Column';
  }

  public function read($input)
  {
    return $this->_read('Column', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Column', self::$_TSPEC, $output);
  }
}


?>
