<?php
/**
 *  @generated
 */
namespace cassandra;
class NotFoundException extends \TException {
  static $_TSPEC;


  public function __construct() {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        );
    }
  }

  public function getName() {
    return 'NotFoundException';
  }

  public function read($input)
  {
    return $this->_read('NotFoundException', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('NotFoundException', self::$_TSPEC, $output);
  }
}


?>
