<?php
namespace phpcassa\Index;

use cassandra\IndexOperator;

use phpcassa\ColumnFamily;

/**
 * @package phpcassa\Index
 */
class IndexExpression extends \cassandra\IndexExpression {

    static protected $names_to_values = array(
        "EQ" => IndexOperator::EQ,
        "GTE" => IndexOperator::GTE,
        "GT" => IndexOperator::GT,
        "LTE" => IndexOperator::LTE,
        "LT" => IndexOperator::LT
    );

    /**
     * Constructs an IndexExpression to be used in an IndexClause, which can
     * be used with get_indexed_slices().
     * @param mixed $column_name the name of the column this expression will apply to;
     *        this column may or may not be indexed
     * @param mixed $value the value that will be compared to column values using op
     * @param string $op the operator to apply to column values
     *        and the 'value' parameter.  Valid options include "EQ", "LT", "LTE",
     *        "GT", and "GTE". Defaults to testing for equality.
     */
    public function __construct($column_name, $value, $op="EQ") {
        parent::__construct();
        $this->column_name = $column_name;
        $this->value = $value;
        if (is_int($op)) {
            $this->op = $op;
        } else {
            $this->op = IndexExpression::$names_to_values[$op];
        }
    }
}
