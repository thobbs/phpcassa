<?php
namespace phpcassa\Util;

use phpcassa\ColumnFamily;

/**
 * @package phpcassa
 * @subpackage columnfamily
 */
class CassandraUtil {

    /**
     * Creates a UUID object from a byte representation.
     * @param string $bytes the byte representation of a UUID, which is
     *        what is returned from functions like uuid1()
     * @return a UUID object
     */
    static public function import($bytes) {
        return UUID::import($bytes);
    }

    /**
     * Generate a v1 UUID (timestamp based)
     * @return string a byte[] representation of a UUID 
     * @param string $node what to use for the MAC portion of the UUID.  This will be generated
     *        randomly if left as NULL
     * @param int $time timestamp to use for the UUID.  This should be a number of microseconds
     *        since the UNIX epoch.
     */
    static public function uuid1($node=null, $time=null) {
        $uuid = UUID::mint(1, $node, null, $time);
        return $uuid->bytes;
    }

    /**
     * Generate a v3 UUID
     * @return string a byte[] representation of a UUID 
     */
    static public function uuid3($node=null, $namespace=null) {
        $uuid = UUID::mint(3, $node, $namespace);
        return $uuid->bytes;
    }

    /**
     * Generate a v4 UUID
     * @return string a byte[] representation of a UUID 
     */
    static public function uuid4() {
        $uuid = UUID::mint(4);
        return $uuid->bytes;
    }

    /**
     * Generate a v5 UUID
     * @return string a byte[] representation of a UUID 
     */
    static public function uuid5($node, $namespace=null) {
        $uuid = UUID::mint(5, $node, $namespace);
        return $uuid->bytes;
    }

    /**
     * Get a timestamp with microsecond precision
     */
    static public function get_time() {
        // By Zach Buller (zachbuller@gmail.com)
        $time1 = \microtime();
        \settype($time1, 'string'); //convert to string to keep trailing zeroes
        $time2 = explode(" ", $time1);
        $sub_secs = \preg_replace('/0./', '', $time2[0], 1);
        $time3 = ($time2[1].$sub_secs)/100;
        return $time3;
    }

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