<?php
/**
 *  @generated
 */
namespace cassandra;
class Cassandra_batch_mutate_args extends \TBase {
  static $_TSPEC;

  public $mutation_map = null;
  public $consistency_level =   1;

  public function __construct($vals=null) {
    if (!isset(self::$_TSPEC)) {
      self::$_TSPEC = array(
        1 => array(
          'var' => 'mutation_map',
          'type' => \TType::MAP,
          'ktype' => \TType::STRING,
          'vtype' => \TType::MAP,
          'key' => array(
            'type' => \TType::STRING,
          ),
          'val' => array(
            'type' => \TType::MAP,
            'ktype' => \TType::STRING,
            'vtype' => \TType::LST,
            'key' => array(
              'type' => \TType::STRING,
            ),
            'val' => array(
              'type' => \TType::LST,
              'etype' => \TType::STRUCT,
              'elem' => array(
                'type' => \TType::STRUCT,
                'class' => '\cassandra\Mutation',
                ),
              ),
            ),
          ),
        2 => array(
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
    return 'Cassandra_batch_mutate_args';
  }

  public function read($input)
  {
    return $this->_read('Cassandra_batch_mutate_args', self::$_TSPEC, $input);
  }
  public function write($output) {
    return $this->_write('Cassandra_batch_mutate_args', self::$_TSPEC, $output);
  }
}


?>
