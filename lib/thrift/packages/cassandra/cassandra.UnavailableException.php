<?php
/**
 *  @generated
 */
namespace cassandra;
class UnavailableException extends \TException {
  static $_TSPEC;


  public function __construct() {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        );
    }
  }

  public function getName() {
    return 'UnavailableException';
  }

  public function read($input)
  {
    return $this->_read('UnavailableException', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('UnavailableException', self::$_TSPEC, $output);
  }
}


?>
