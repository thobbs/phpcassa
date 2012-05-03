<?php
/**
 *  @generated
 */
namespace cassandra;
class EndpointDetails extends \TBase {
  static $_TSPEC;

  public $host = null;
  public $datacenter = null;
  public $rack = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'host',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'datacenter',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'rack',
          'type' => \TType::STRING,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'EndpointDetails';
  }

  public function read($input)
  {
    return $this->_read('EndpointDetails', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('EndpointDetails', self::$_TSPEC, $output);
  }
}


?>
