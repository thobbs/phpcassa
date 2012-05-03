<?php
/**
 *  @generated
 */
namespace cassandra;
class AuthenticationRequest extends \TBase {
  static $_TSPEC;

  public $credentials = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'credentials',
          'type' => \TType::MAP,
          'ktype' => \TType::STRING,
          'vtype' => \TType::STRING,
          'key' => array(
            'type' => \TType::STRING,
          ),
          'val' => array(
            'type' => \TType::STRING,
            ),
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'AuthenticationRequest';
  }

  public function read($input)
  {
    return $this->_read('AuthenticationRequest', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('AuthenticationRequest', self::$_TSPEC, $output);
  }
}


?>
