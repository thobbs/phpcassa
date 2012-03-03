<?php
namespace phpcassa\Index;

use phpcassa\ColumnFamily;

/**
 * @package phpcassa\Index
 */
class IndexExpression extends \cassandra_IndexExpression {

    /**
     * Constructs an IndexExpression to be used in an IndexClause, which can
     * be used with get_indexed_slices().
     * @param mixed $column_name the name of the column this expression will apply to;
     *        this column may or may not be indexed
     * @param mixed $value the value that will be compared to column values using op
     * @param \classandra_IndexOperator $op the binary operator to apply to column values
     *        and the 'value' parameter.  Defaults to testing for equality.
     * @return \cassandra_IndexExpression
     */
    public function __construct($column_name, $value, $op="EQ") {
        parent::__construct();
        $this->column_name = $column_name;
        $this->value = $value;
        if (is_int($op)) {
            $this->op = $op;
        } else {
            $operators = $GLOBALS['cassandra_E_IndexOperator'];
            $this->op = $operators[$op];
        }
    }
}
