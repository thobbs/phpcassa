<?php

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
     * @param ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return mixed array(column_name => column_value)
     */
    public function get_supercolumn($key,
                                    $super_column,
                                    $columns=null,
                                    $column_start="",
                                    $column_finish="",
                                    $column_reversed=false,
                                    $column_count=self::DEFAULT_COLUMN_COUNT,
                                    $read_consistency_level=null) {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                                   $column_reversed, $column_count);

        return $this->_get($key, $column_parent, $predicate, $read_consistency_level);
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
     * @param ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     * @param int $buffer_size the number of keys to multiget at a single time. If your
     *        rows are large, having a high buffer size gives poor performance; if your
     *        rows are small, consider increasing this value.
     *
     * @return mixed array(key => array(column_name => column_value))
     */
    public function multiget_supercolumn($keys,
                                         $super_column,
                                         $columns=null,
                                         $column_start="",
                                         $column_finish="",
                                         $column_reversed=false,
                                         $column_count=self::DEFAULT_COLUMN_COUNT,
                                         $read_consistency_level=null,
                                         $buffer_size=16)  {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                                   $column_reversed, $column_count);

        return $this->_multiget($keys, $cp, $slice, $read_consistency_level, $buffer_size);
    }

    /**
     * Count the number of subcolumns in a supercolumn.
     *
     * @param string $key row to be counted
     * @param mixed $super_column count only subcolumns in this super column
     * @param mixed[] $columns limit the possible columns or super columns counted to this list
     * @param mixed $column_start only count columns with name >= this
     * @param mixed $column_finish only count columns with name <= this
     * @param ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int
     */
    public function get_subcolumn_count($key,
                                        $super_column
                                        $columns=null,
                                        $column_start='',
                                        $column_finish='',
                                        $read_consistency_level=null) {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                               false, self::MAX_COUNT);
        return $this->_get_count($key, $cp, $slice, $read_consistency_level);
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
     * @param ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return mixed array(row_key => row_count)
     */
    public function multiget_count($keys,
                                   $super_column=null,
                                   $columns=null,
                                   $column_start='',
                                   $column_finish='',
                                   $read_consistency_level=null) {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                               false, self::MAX_COUNT);
        return $this->_multiget_count($keys, $cp, $slice, $read_consistency_level);
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
     * @param ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     * @param int $buffer_size When calling `get_range`, the intermediate results need
     *        to be buffered if we are fetching many rows, otherwise the Cassandra
     *        server will overallocate memory and fail.  This is the size of
     *        that buffer in number of rows.
     *
     * @return phpcassa\Iterator\RangeColumnFamilyIterator
     */
    public function get_supercolumn_range($super_column,
                                          $key_start="",
                                          $key_finish="",
                                          $row_count=self::DEFAULT_ROW_COUNT,
                                          $columns=null,
                                          $column_start="",
                                          $column_finish="",
                                          $column_reversed=false,
                                          $column_count=self::DEFAULT_COLUMN_COUNT,
                                          $read_consistency_level=null,
                                          $buffer_size=null) {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                               $column_reversed, $column_count);

        return $this->_get_range($key_start, $key_finish, $row_count,
            $cp, $slice, $read_consistency_level, $buffer_size);
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
     * @param ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     */
    public function add($key, $super_column, $column, $value=1,
                        $write_consistency_level=null) {
        $packed_key = $this->pack_key($key);
        $cp = $this->create_column_parent($super_column);
        $counter = new CounterColumn();
        $counter->name = $this->pack_name($column);
        $counter->value = $value;
        return $this->pool->call("add", $packed_key, $cp, $counter,
            $this->wcl($write_consistency_level));
    }

    /**
     * Remove columns from a row.
     *
     * @param string $key the row to remove columns from
     * @param mixed[] $columns the columns to remove. If null, the entire row will be removed.
     * @param mixed $super_column only remove this super column
     * @param ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int the timestamp for the operation
     */
    public function remove_supercolumn($key, $super_column, $columns=null,
                                       $write_consistency_level=null) {

        if ($columns === null || count($columns) == 1) {
            $cp = new ColumnPath();
            $cp->column_family = $this->column_family;
            $cp->super_column = $this->pack_name($super_column, true);
            if ($columns !== null) {
                $cp->column = $this->pack_name($columns[0], false);
            }
            return $this->_remove_single($key, $cp, $write_consistency_level);
        } else {
            $deletion = new Deletion();
            $deletion->super_column = $this->pack_name($super_column, true);
            if ($columns !== null) {
                $predicate = $this->create_slice_predicate($columns, '', '', false,
                                                           self::DEFAULT_COLUMN_COUNT);
                $deletion->predicate = $predicate;
            }
            return $this->_remove_multi($key, $deletion, $write_consistency_level);
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
     * @param mixed $column the column name of the counter
     * @param ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     */
    public function remove_counter($key, $super_column, $column,
                                   $write_consistency_level=null) {
        $cp = new ColumnPath();
        $packed_key = $this->pack_key($key);
        $cp->column_family = $this->column_family;
        $cp->super_column = $this->pack_name($super_column, true);
        $cp->column = $this->pack_name($column);
        $this->pool->call("remove_counter", $packed_key, $cp, $this->wcl($write_consistency_level));
    }

}
