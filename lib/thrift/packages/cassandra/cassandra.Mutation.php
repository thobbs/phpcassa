<?php
/**
 *  @generated
 */
namespace cassandra;
class Mutation extends \TBase {
  static $_TSPEC;

  public $column_or_supercolumn = null;
  public $deletion = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'column_or_supercolumn',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\ColumnOrSuperColumn',
          ),
        2 => array(
          'var' => 'deletion',
          'type' => \TType::STRUCT,
          'class' => '\cassandra\Deletion',
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Mutation';
  }

  public function read($input)
  {
    return $this->_read('Mutation', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Mutation', self::$_TSPEC, $output);
  }
}


?>
