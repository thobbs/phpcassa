<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_get_paged_slice_result extends \TBase {
  static $_TSPEC;

  public $success = null;
  public $ire = null;
  public $ue = null;
  public $te = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        0 => array(
          'var' => 'success',
          'type' => \TType::LST,
          'etype' => \TType::STRUCT,
          'elem' => array(
            'type' => \TType::STRUCT,
            'class' => '\cassandra\KeySlice',
            ),
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
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_get_paged_slice_result';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_get_paged_slice_result', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_get_paged_slice_result', self::$_TSPEC, $output);
  }
}


?>
