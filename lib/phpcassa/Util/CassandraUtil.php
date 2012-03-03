<?php
namespace phpcassa\Util;

use phpcassa\ColumnFamily;

/**
 * @package phpcassa\Util
 */
class CassandraUtil {

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
    static public function create_index_expression($column_name, $value,
                                                   $op=\cassandra_IndexOperator::EQ) {
        $ie = new \cassandra_IndexExpression();
        $ie->column_name = $column_name;
        $ie->value = $value;
        $ie->op = $op;
        return $ie;
    }

    /**
     * Constructs a \cassandra_IndexClause for use with get_indexed_slices().
     * @param \cassandra_IndexExpression[] $expr_list the list of expressions to match; at
     *        least one of these must be on an indexed column
     * @param string $start_key the key to begin searching from
     * @param int $count the number of results to return
     * @return \cassandra_IndexClause
     */
    static public function create_index_clause($expr_list, $start_key='',
                                               $count=ColumnFamily::DEFAULT_COLUMN_COUNT) {
        $ic = new \cassandra_IndexClause();
        $ic->expressions = $expr_list;
        $ic->start_key = $start_key;
        $ic->count = $count;
        return $ic;
    }
}
