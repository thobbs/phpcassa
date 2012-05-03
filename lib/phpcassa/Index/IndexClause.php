<?php
namespace phpcassa\Index;

use phpcassa\ColumnFamily;

/**
 * @package phpcassa\Index
 */
class IndexClause extends \cassandra\IndexClause {

    /**
     * Constructs an index clause for use with get_indexed_slices().
     *
     * @param phpcassa\Index\IndexExpression[] $expr_list the list of expressions
     *        to match; at least one of these must be on an indexed column
     * @param mixed $start_key the key to begin searching from
     * @param int $count the number of results to return
     */
    public function __construct($expr_list, $start_key='',
                                $count=ColumnFamily::DEFAULT_ROW_COUNT) {
        parent::__construct();
        $this->expressions = $expr_list;
        $this->start_key = $start_key;
        $this->count = $count;
    }
}
