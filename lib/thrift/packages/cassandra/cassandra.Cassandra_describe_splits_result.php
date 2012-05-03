<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_describe_splits_result extends \TBase {
  static $_TSPEC;

  public $success = null;
  public $ire = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        0 => array(
          'var' => 'success',
          'type' => \TType::LST,
          'etype' => \TType::STRING,
          'elem' => array(
            'type' => \TType::STRING,
            ),
          ),
        1 => array(
          'var' => 'ire',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\InvalidRequestException',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_describe_splits_result';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_describe_splits_result', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_describe_splits_result', self::$_TSPEC, $output);
  }
}


?>
