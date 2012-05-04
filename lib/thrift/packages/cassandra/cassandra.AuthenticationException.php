<?php
/**
 *  @generated
 */
namespace cassandra;
class AuthenticationException extends \TException {
  static $_TSPEC;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'message',
          'type' => \TType::STRING,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'AuthenticationException';
  }

  public function read($input)
  {
    return $this->_read('AuthenticationException', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('AuthenticationException', self::$_TSPEC, $output);
  }
}


?>
