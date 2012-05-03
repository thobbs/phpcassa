<?php
/**
 *  @generated
 */
namespace cassandra;
class KeyRange extends \TBase {
  static $_TSPEC;

  public $start_key = null;
  public $end_key = null;
  public $start_token = null;
  public $end_token = null;
  public $row_filter = null;
  public $count = 100;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'start_key',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'end_key',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'start_token',
          'type' => \TType::STRING,
          ),
        4 => array(
          'var' => 'end_token',
          'type' => \TType::STRING,
          ),
        6 => array(
          'var' => 'row_filter',
          'type' => \TType::LST,
          'etype' => \TType::STRUCT,
          'elem' => array(
            'type' => \TType::STRUCT,
            'class' => '\cassandra\IndexExpression',
            ),
          ),
        5 => array(
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
    return 'KeyRange';
  }

  public function read($input)
  {
    return $this->_read('KeyRange', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('KeyRange', self::$_TSPEC, $output);
  }
}


?>
