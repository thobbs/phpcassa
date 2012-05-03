<?php
/**
 *  @generated
 */
namespace cassandra;
class TimedOutException extends \TException {
  static $_TSPEC;


  public function __construct() {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        );
    }
  }

  public function getName() {
    return 'TimedOutException';
  }

  public function read($input)
  {
    return $this->_read('TimedOutException', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('TimedOutException', self::$_TSPEC, $output);
  }
}


?>
