<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_system_add_keyspace_args extends \TBase {
  static $_TSPEC;

  public $ks_def = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'ks_def',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\KsDef',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_system_add_keyspace_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_system_add_keyspace_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_system_add_keyspace_args', self::$_TSPEC, $output);
  }
}


?>
