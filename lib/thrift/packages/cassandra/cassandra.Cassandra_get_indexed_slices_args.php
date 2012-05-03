<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_get_indexed_slices_args extends \TBase {
  static $_TSPEC;

  public $column_parent = null;
  public $index_clause = null;
  public $column_predicate = null;
  public $consistency_level =   1;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'column_parent',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\ColumnParent',
          ),
        2 => array(
          'var' => 'index_clause',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\IndexClause',
          ),
        3 => array(
          'var' => 'column_predicate',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\SlicePredicate',
          ),
        4 => array(
          'var' => 'consistency_level',
          'type' => \TType::I32,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_get_indexed_slices_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_get_indexed_slices_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_get_indexed_slices_args', self::$_TSPEC, $output);
  }
}


?>
