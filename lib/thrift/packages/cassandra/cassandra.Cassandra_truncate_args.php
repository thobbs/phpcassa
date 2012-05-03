<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_truncate_args extends \TBase {
  static $_TSPEC;

  public $cfname = null;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'cfname',
          'type' => \TType::STRING,
          ),
        );
    }
    if (is_array($vals)) {
      parent::__construct(self::$_TSPEC, $vals);
    }
  }

  public function getName() {
    return 'Cassandra_truncate_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_truncate_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_truncate_args', self::$_TSPEC, $output);
  }
}


?>
