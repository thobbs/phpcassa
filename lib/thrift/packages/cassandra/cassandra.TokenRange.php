<?php
/**
 *  @generated
 */
namespace cassandra;
class TokenRange extends \TBase {
  static $_TSPEC;

  public $start_token = null;
  public $end_token = null;
  public $endpoints = null;
  public $rpc_endpoints = null;
  public $endpoint_details = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'start_token',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'end_token',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'endpoints',
          'type' => \TType::LST,
          'etype' => \TType::STRING,
          'elem' => array(
            'type' => \TType::STRING,
            ),
          ),
        4 => array(
          'var' => 'rpc_endpoints',
          'type' => \TType::LST,
          'etype' => \TType::STRING,
          'elem' => array(
            'type' => \TType::STRING,
            ),
          ),
        5 => array(
          'var' => 'endpoint_details',
          'type' => \TType::LST,
          'etype' => \TType::STRUCT,
          'elem' => array(
            'type' => \TType::STRUCT,
            'class' => '\cassandra\EndpointDetails',
            ),
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'TokenRange';
  }

  public function read($input)
  {
    return $this->_read('TokenRange', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('TokenRange', self::$_TSPEC, $output);
  }
}


?>
