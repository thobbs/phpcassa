<?php
/**
 *  @generated
 */
namespace cassandra;
class ColumnDef extends \TBase {
  static $_TSPEC;

  public $name = null;
  public $validation_class = null;
  public $index_type = null;
  public $index_name = null;
  public $index_options = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'name',
          'type' => \TType::STRING,
          ),
        2 => array(
          'var' => 'validation_class',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'index_type',
          'type' => \TType::I32,
          ),
        4 => array(
          'var' => 'index_name',
          'type' => \TType::STRING,
          ),
        5 => array(
          'var' => 'index_options',
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
    return 'ColumnDef';
  }

  public function read($input)
  {
    return $this->_read('ColumnDef', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('ColumnDef', self::$_TSPEC, $output);
  }
}


?>
