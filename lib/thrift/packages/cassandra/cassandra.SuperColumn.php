<?php
/**
 *  @generated
 */
namespace cassandra;
class SuperColumn extends \TBase {
  static $_TSPEC;

  public $name = null;
  public $columns = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'name',
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
    return 'SuperColumn';
  }

  public function read($input)
  {
    return $this->_read('SuperColumn', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('SuperColumn', self::$_TSPEC, $output);
  }
}


?>
