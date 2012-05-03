<?php
/**
 *  @generated
 */
namespace cassandra;
class CounterColumn extends \TBase {
  static $_TSPEC;

  public $name = null;
  public $value = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'name',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'value',
          'type' => \TType::I64,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'CounterColumn';
  }

  public function read($input)
  {
    return $this->_read('CounterColumn', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('CounterColumn', self::$_TSPEC, $output);
  }
}


?>
