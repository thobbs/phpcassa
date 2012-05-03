<?php
/**
 *  @generated
 */
namespace cassandra;
class CqlResult extends \TBase {
  static $_TSPEC;

  public $type = null;
  public $rows = null;
  public $num = null;
  public $schema = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'type',
          'type' => \TType::I32,
          ),
        2 => array(
          'var' => 'rows',
          'type' => \TType::LST,
          'etype' => \TType::STRUCT,
          'elem' => array(
            'type' => \TType::STRUCT,
            'class' => '\cassandra\CqlRow',
            ),
          ),
        3 => array(
          'var' => 'num',
          'type' => \TType::I32,
          ),
        4 => array(
          'var' => 'schema',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\CqlMetadata',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'CqlResult';
  }

  public function read($input)
  {
    return $this->_read('CqlResult', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('CqlResult', self::$_TSPEC, $output);
  }
}


?>
