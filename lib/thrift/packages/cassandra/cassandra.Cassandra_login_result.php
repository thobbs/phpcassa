<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_login_result extends \TBase {
  static $_TSPEC;

  public $authnx = null;
  public $authzx = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'authnx',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\AuthenticationException',
          ),
        2 => array(
          'var' => 'authzx',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\AuthorizationException',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_login_result';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_login_result', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_login_result', self::$_TSPEC, $output);
  }
}


?>
