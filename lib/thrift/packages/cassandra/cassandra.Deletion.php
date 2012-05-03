<?php
/**
 *  @generated
 */
namespace cassandra;
class Deletion extends \TBase {
  static $_TSPEC;

  public $timestamp = null;
  public $super_column = null;
  public $predicate = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'timestamp',
          'type' => \TType::I64,
          ),
        2 => array(
          'var' => 'super_column',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'predicate',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\SlicePredicate',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Deletion';
  }

  public function read($input)
  {
    return $this->_read('Deletion', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Deletion', self::$_TSPEC, $output);
  }
}


?>
