<?php
/**
 *  @generated
 */
namespace cassandra;
class SlicePredicate extends \TBase {
  static $_TSPEC;

  public $column_names = null;
  public $slice_range = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'column_names',
          'type' => \TType::LST,
          'etype' => \TType::STRING,
          'elem' => array(
            'type' => \TType::STRING,
            ),
          ),
        2 => array(
          'var' => 'slice_range',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\SliceRange',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'SlicePredicate';
  }

  public function read($input)
  {
    return $this->_read('SlicePredicate', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('SlicePredicate', self::$_TSPEC, $output);
  }
}


?>
