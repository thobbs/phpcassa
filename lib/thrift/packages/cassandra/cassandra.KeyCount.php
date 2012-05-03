<?php
/**
 *  @generated
 */
namespace cassandra;
class KeyCount extends \TBase {
  static $_TSPEC;

  public $key = null;
  public $count = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'key',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'count',
          'type' => \TType::I32,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'KeyCount';
  }

  public function read($input)
  {
    return $this->_read('KeyCount', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('KeyCount', self::$_TSPEC, $output);
  }
}


?>
