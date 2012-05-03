<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_system_add_column_family_args extends \TBase {
  static $_TSPEC;

  public $cf_def = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'cf_def',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\CfDef',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_system_add_column_family_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_system_add_column_family_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_system_add_column_family_args', self::$_TSPEC, $output);
  }
}


?>
