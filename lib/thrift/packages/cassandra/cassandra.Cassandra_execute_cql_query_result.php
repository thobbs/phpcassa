<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_execute_cql_query_result extends \TBase {
  static $_TSPEC;

  public $success = null;
  public $ire = null;
  public $ue = null;
  public $te = null;
  public $sde = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        0 => array(
          'var' => 'success',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\CqlResult',
          ),
        1 => array(
          'var' => 'ire',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\InvalidRequestException',
          ),
        2 => array(
          'var' => 'ue',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\UnavailableException',
          ),
        3 => array(
          'var' => 'te',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\TimedOutException',
          ),
        4 => array(
          'var' => 'sde',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\SchemaDisagreementException',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_execute_cql_query_result';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_execute_cql_query_result', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_execute_cql_query_result', self::$_TSPEC, $output);
  }
}


?>
