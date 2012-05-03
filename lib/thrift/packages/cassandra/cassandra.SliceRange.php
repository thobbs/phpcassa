<?php
/**
 *  @generated
 */
namespace cassandra;
class SliceRange extends \TBase {
  static $_TSPEC;

  public $start = null;
  public $finish = null;
  public $reversed = false;
  public $count = 100;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'start',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'finish',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'reversed',
          'type' => \TType::BOOL,
          ),
        4 => array(
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
    return 'SliceRange';
  }

  public function read($input)
  {
    return $this->_read('SliceRange', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('SliceRange', self::$_TSPEC, $output);
  }
}


?>
