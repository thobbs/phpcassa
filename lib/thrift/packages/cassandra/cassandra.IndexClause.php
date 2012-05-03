<?php
/**
 *  @generated
 */
namespace cassandra;
class IndexClause extends \TBase {
  static $_TSPEC;

  public $expressions = null;
  public $start_key = null;
  public $count = 100;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'expressions',
          'type' => \TType::LST,
          'etype' => \TType::STRUCT,
          'elem' => array(
            'type' => \TType::STRUCT,
            'class' => '\cassandra\IndexExpression',
            ),
          ),
        2 => array(
          'var' => 'start_key',
          'type' => \TType::STRING,
          ),
        3 => array(
          'var' => 'count',
          'type' => \TType::I32,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'IndexClause';
  }

  public function read($input)
  {
    return $this->_read('IndexClause', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('IndexClause', self::$_TSPEC, $output);
  }
}


?>
