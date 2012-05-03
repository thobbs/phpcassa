<?php
/**
 *  @generated
 */
namespace cassandra;
class SchemaDisagreementException extends \TException {
  static $_TSPEC;


  public function __construct() {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        );
    }
  }

  public function getName() {
    return 'SchemaDisagreementException';
  }

  public function read($input)
  {
    return $this->_read('SchemaDisagreementException', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('SchemaDisagreementException', self::$_TSPEC, $output);
  }
}


?>
