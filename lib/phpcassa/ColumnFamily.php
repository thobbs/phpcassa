<?php
namespace phpcassa;

use cassandra\ConsistencyLevel;
use cassandra\CounterColumn;

/**
 * Representation of a column family in Cassandra.
 *
 * All data insertions, deletions, or retrievals will go through a ColumnFamily.
 * This may only be used for standard column families; you must use
 * \phpcassa\SuperColumnFamily for super column families.
 *
 * @package phpcassa
 */
class ColumnFamily extends AbstractColumnFamily {

    /**
     * Increment or decrement a counter.
     *
     * `value` should be an integer, either positive or negative, to be added
     * to a counter column. By default, `value` is 1.
     *
     * This method is not idempotent. Retrying a failed add may result
     * in a double count. You should consider using a separate
     * ConnectionPool with retries disabled for column families
     * with counters.
     *
     * Only available in Cassandra 0.8.0 and later.
     *
     * @param string $key the row to insert or update the columns in
     * @param mixed $column the column name of the counter
     * @param int $value the amount to adjust the counter by
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     */
    public function add($key, $column, $value=1, $consistency_level=null) {
        $packed_key = $this->pack_key($key);
        $cp = $this->create_column_parent();
        $counter = new CounterColumn();
        $counter->name = $this->pack_name($column);
        $counter->value = $value;
        return $this->pool->call("add", $packed_key, $cp, $counter,
            $this->wcl($consistency_level));
    }
}
