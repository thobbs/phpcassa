<?php
/**
 *  @generated
 */
namespace cassandra;
class CqlMetadata extends \TBase {
  static $_TSPEC;

  public $name_types = null;
  public $value_types = null;
  public $default_name_type = null;
  public $default_value_type = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'name_types',
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
        2 => array(
          'var' => 'value_types',
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
        3 => array(
          'var' => 'default_name_type',
          'type' => \TType::STRING,
          ),
        4 => array(
          'var' => 'default_value_type',
          'type' => \TType::STRING,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'CqlMetadata';
  }

  public function read($input)
  {
    return $this->_read('CqlMetadata', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('CqlMetadata', self::$_TSPEC, $output);
  }
}


?>
