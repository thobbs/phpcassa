<?php
/**
 *  @generated
 */
namespace cassandra;
class CqlRow extends \TBase {
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
            'class' => '\cassandra\Column',
            ),
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'CqlRow';
  }

  public function read($input)
  {
    return $this->_read('CqlRow', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('CqlRow', self::$_TSPEC, $output);
  }
}


?>
