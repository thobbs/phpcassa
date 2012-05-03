<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_describe_version_args extends \TBase {
  static $_TSPEC;


  public function __construct() {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        );
    }
  }

  public function getName() {
    return 'Cassandra_describe_version_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_describe_version_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_describe_version_args', self::$_TSPEC, $output);
  }
}


?>
