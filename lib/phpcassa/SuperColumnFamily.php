<?php
namespace phpcassa;

use phpcassa\ColumnFamily;

use cassandra\Deletion;
use cassandra\ColumnParent;
use cassandra\ColumnPath;
use cassandra\CounterColumn;
use cassandra\CounterSuperColumn;
use cassandra\ColumnOrSuperColumn;
use cassandra\SuperColumn;

class SuperColumnFamily extends ColumnFamily {

    /**
     * Fetch a row from this column family.
     *
     * @param string $key row key to fetch
     * @param mixed $super_column return only columns in this super column
     * @param mixed[] $columns limit the columns or super columns fetched to this list
     * @param mixed $column_start only fetch columns with name >= this
     * @param mixed $column_finish only fetch columns with name <= this
     * @param bool $column_reversed fetch the columns in reverse order
     * @param int $column_count limit the number of columns returned to this amount
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return mixed array(column_name => column_value)
     */
    public function get_super_column($key,
                                     $super_column,
                                     $column_slice=null,
                                     $column_names=null,
                                     $consistency_level=null) {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate($column_names, $column_slice);

        return $this->_get($key, $cp, $slice, $consistency_level);
    }

    /**
     * Fetch a set of rows from this column family.
     *
     * @param string[] $keys row keys to fetch
     * @param mixed $super_column return only columns in this super column
     * @param mixed[] $columns limit the columns or super columns fetched to this list
     * @param mixed $column_start only fetch columns with name >= this
     * @param mixed $column_finish only fetch columns with name <= this
     * @param bool $column_reversed fetch the columns in reverse order
     * @param int $column_count limit the number of columns returned to this amount
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     * @param int $buffer_size the number of keys to multiget at a single time. If your
     *        rows are large, having a high buffer size gives poor performance; if your
     *        rows are small, consider increasing this value.
     *
     * @return mixed array(key => array(column_name => column_value))
     */
    public function multiget_super_column($keys,
                                         $super_column,
                                         $column_slice=null,
                                         $column_names=null,
                                         $consistency_level=null,
                                         $buffer_size=16)  {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate($column_names, $column_slice);

        return $this->_multiget($keys, $cp, $slice, $consistency_level, $buffer_size);
    }

    /**
     * Count the number of subcolumns in a supercolumn.
     *
     * @param string $key row to be counted
     * @param mixed $super_column count only subcolumns in this super column
     * @param mixed[] $columns limit the possible columns or super columns counted to this list
     * @param mixed $column_start only count columns with name >= this
     * @param mixed $column_finish only count columns with name <= this
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int
     */
    public function get_subcolumn_count($key,
                                        $super_column,
                                        $column_slice=null,
                                        $column_names=null,
                                        $consistency_level=null) {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate($column_names, $column_slice);

        return $this->_get_count($key, $cp, $slice, $consistency_level);
    }

    /**
     * Count the number of subcolumns in a particular super column
     * across a set of rows.
     *
     * @param string[] $keys rows to be counted
     * @param mixed $super_column count only columns in this super column
     * @param mixed[] $columns limit the possible columns or super columns counted to this list
     * @param mixed $column_start only count columns with name >= this
     * @param mixed $column_finish only count columns with name <= this
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return mixed array(row_key => row_count)
     */
    public function multiget_count($keys,
                                   $super_column=null,
                                   $column_slice=null,
                                   $column_names=null,
                                   $consistency_level=null) {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate($column_names, $column_slice);

        return $this->_multiget_count($keys, $cp, $slice, $consistency_level);
    }

    /**
     * Get an iterator over a range of rows.
     *
     * @param mixed $super_column return only columns in this super column
     * @param string $key_start fetch rows with a key >= this
     * @param string $key_finish fetch rows with a key <= this
     * @param int $row_count limit the number of rows returned to this amount
     * @param mixed[] $columns limit the columns or super columns fetched to this list
     * @param mixed $column_start only fetch columns with name >= this
     * @param mixed $column_finish only fetch columns with name <= this
     * @param bool $column_reversed fetch the columns in reverse order
     * @param int $column_count limit the number of columns returned to this amount
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     * @param int $buffer_size When calling `get_range`, the intermediate results need
     *        to be buffered if we are fetching many rows, otherwise the Cassandra
     *        server will overallocate memory and fail.  This is the size of
     *        that buffer in number of rows.
     *
     * @return phpcassa\Iterator\RangeColumnFamilyIterator
     */
    public function get_super_column_range($super_column,
                                           $key_start="",
                                           $key_finish="",
                                           $row_count=self::DEFAULT_ROW_COUNT,
                                           $column_slice=null,
                                           $column_names=null,
                                           $consistency_level=null,
                                           $buffer_size=null) {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate($column_names, $column_slice);

        return $this->_get_range($key_start, $key_finish, $row_count,
            $cp, $slice, $consistency_level, $buffer_size);
    }

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
     * @param mixed $super_column the super column to use
     * @param mixed $column the column name of the counter
     * @param int $value the amount to adjust the counter by
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     */
    public function add($key, $super_column, $column, $value=1,
                        $consistency_level=null) {
        $packed_key = $this->pack_key($key);
        $cp = $this->create_column_parent($super_column);
        $counter = new CounterColumn();
        $counter->name = $this->pack_name($column);
        $counter->value = $value;
        return $this->pool->call("add", $packed_key, $cp, $counter,
            $this->wcl($consistency_level));
    }

    /**
     * Remove columns from a row.
     *
     * @param string $key the row to remove columns from
     * @param mixed $super_column only remove this super column or its subcolumns
     * @param mixed[] $subcolumns the subcolumns to remove. If null, the entire
     *                            supercolumn will be removed.
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int the timestamp for the operation
     */
    public function remove_super_column($key, $super_column, $subcolumns=null,
                                        $consistency_level=null) {

        if ($subcolumns === null || count($subcolumns) == 1) {
            $cp = new ColumnPath();
            $cp->column_family = $this->column_family;
            $cp->super_column = $this->pack_name($super_column, true);
            if ($subcolumns !== null) {
                $cp->column = $this->pack_name($subcolumns[0], false);
            }
            return $this->_remove_single($key, $cp, $consistency_level);
        } else {
            $deletion = new Deletion();
            $deletion->super_column = $this->pack_name($super_column, true);
            if ($subcolumns !== null) {
                $predicate = $this->create_slice_predicate($subcolumns, '', '', false,
                                                           self::DEFAULT_COLUMN_COUNT);
                $deletion->predicate = $predicate;
            }
            return $this->_remove_multi($key, $deletion, $consistency_level);
        }
    }

    /**
     * Remove a counter at the specified location.
     *
     * Note that counters have limited support for deletes: if you remove a
     * counter, you must wait to issue any following update until the delete
     * has reached all the nodes and all of them have been fully compacted.
     *
     * Available in Cassandra 0.8.0 and later.
     *
     * @param string $key the key for the row
     * @param mixed $super_column the super column the counter is in
     * @param mixed $column the column name of the counter; if left as null,
     *                      the entire super column will be removed
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     */
    public function remove_counter($key, $super_column, $column=null,
                                   $consistency_level=null) {
        $cp = new ColumnPath();
        $packed_key = $this->pack_key($key);
        $cp->column_family = $this->column_family;
        $cp->super_column = $this->pack_name($super_column, true);
        if ($column !== null)
            $cp->column = $this->pack_name($column);
        $this->pool->call("remove_counter", $packed_key, $cp,
            $this->wcl($consistency_level));
    }

    protected function array_to_coscs($data, $timestamp=null, $ttl=null) {
        if($timestamp === null)
            $timestamp = Clock::get_time();

        $have_counters = $this->has_counters;
        $ret = array();
        foreach ($data as $name => $value) {
            $c_or_sc = new ColumnOrSuperColumn();
            if($have_counters) {
                $sub = new CounterSuperColumn();
                $c_or_sc->counter_super_column = $sub;
            } else {
                $sub = new SuperColumn();
                $c_or_sc->super_column = $sub;
            }
            $sub->name = $this->pack_name($name, true, self::NON_SLICE, true);
            $sub->columns = $this->array_to_columns($value, $timestamp, $ttl);
            $sub->timestamp = $timestamp;
            $ret[] = $c_or_sc;
        }

        return $ret;
    }
}
