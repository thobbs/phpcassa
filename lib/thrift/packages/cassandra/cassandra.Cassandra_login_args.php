<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_login_args extends \TBase {
  static $_TSPEC;

  public $auth_request = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'auth_request',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\AuthenticationRequest',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_login_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_login_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_login_args', self::$_TSPEC, $output);
  }
}


?>
