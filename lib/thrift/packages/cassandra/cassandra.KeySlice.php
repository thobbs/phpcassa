<?php
/**
 *  @generated
 */
namespace cassandra;
class KeySlice extends \TBase {
  static $_TSPEC;

  public $key = null;
  public $columns = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'key',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'columns',
          'type' => \TType::LST,
          'etype' => \TType::STRUCT,
          'elem' => array(
            'type' => \TType::STRUCT,
            'class' => '\cassandra\ColumnOrSuperColumn',
            ),
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'KeySlice';
  }

  public function read($input)
  {
    return $this->_read('KeySlice', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('KeySlice', self::$_TSPEC, $output);
  }
}


?>
