<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_set_cql_version_args extends \TBase {
  static $_TSPEC;

  public $version = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'version',
          'type' => \TType::STRING,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_set_cql_version_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_set_cql_version_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_set_cql_version_args', self::$_TSPEC, $output);
  }
}


?>
