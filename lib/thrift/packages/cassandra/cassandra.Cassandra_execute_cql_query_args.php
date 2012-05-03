<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_execute_cql_query_args extends \TBase {
  static $_TSPEC;

  public $query = null;
  public $compression = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'query',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'compression',
          'type' => \TType::I32,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_execute_cql_query_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_execute_cql_query_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_execute_cql_query_args', self::$_TSPEC, $output);
  }
}


?>
