<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_set_keyspace_result extends \TBase {
  static $_TSPEC;

  public $ire = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'ire',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\InvalidRequestException',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_set_keyspace_result';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_set_keyspace_result', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_set_keyspace_result', self::$_TSPEC, $output);
  }
}


?>
