<?php
namespace phpcassa;

use phpcassa\ColumnFamily;
use phpcassa\ColumnSlice;

use cassandra\Deletion;
use cassandra\ColumnParent;
use cassandra\ColumnPath;
use cassandra\CounterColumn;
use cassandra\CounterSuperColumn;
use cassandra\ColumnOrSuperColumn;
use cassandra\Column;
use cassandra\SuperColumn;

/**
 * Representation of a super column family in Cassandra.
 *
 * Subclasses \phpcassa\ColumnFamily, so those methods are also
 * available.
 *
 * @package phpcassa
 */
class SuperColumnFamily extends AbstractColumnFamily {

    /**
     * Fetch a single super column.
     *
     * Returns an array of the subcolumns in that super column.
     *
     * @param string $key row key to fetch
     * @param mixed $super_column return only subcolumns of this super column
     * @param \phpcassa\ColumnSlice a slice of subcolumns to fetch, or null
     * @param mixed[] $column_names limit the subcolumns fetched to this list
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return mixed array(subcolumn_name => subcolumn_value)
     */
    public function get_super_column($key,
                                     $super_column,
                                     $column_slice=null,
                                     $column_names=null,
                                     $consistency_level=null) {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate($column_names, $column_slice, false);
        return $this->_get($key, $cp, $slice, $consistency_level);
    }

    /**
     * Fetch a super column from multiple rows from this column family.
     *
     * The returned array will map directly from keys to the subcolumn
     * array; the super column layer is omitted.
     *
     * @param string[] $keys row keys to fetch
     * @param mixed $super_column return only subcolumns of this super column
     * @param \phpcassa\ColumnSlice a slice of subcolumns to fetch, or null
     * @param mixed[] $column_names limit the subcolumns fetched to this list
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     * @param int $buffer_size the number of keys to multiget at a single time. If your
     *        rows are large, having a high buffer size gives poor performance; if your
     *        rows are small, consider increasing this value.
     *
     * @return mixed array(key => array(subcolumn_name => subcolumn_value))
     */
    public function multiget_super_column($keys,
                                          $super_column,
                                          $column_slice=null,
                                          $column_names=null,
                                          $consistency_level=null,
                                          $buffer_size=16)  {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate($column_names, $column_slice, false);

        return $this->_multiget($keys, $cp, $slice, $consistency_level, $buffer_size);
    }

    /**
     * Count the number of subcolumns in a supercolumn.
     *
     * @param string $key row to be counted
     * @param mixed $super_column count only subcolumns in this super column
     * @param \phpcassa\ColumnSlice a slice of subcolumns to count, or null
     * @param mixed[] $column_names limit the possible subcolumns or counted to this list
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
        $slice = $this->create_slice_predicate(
            $column_names, $column_slice, false, ColumnSlice::MAX_COUNT);

        return $this->_get_count($key, $cp, $slice, $consistency_level);
    }

    /**
     * Count the number of subcolumns in a particular super column
     * across a set of rows.
     *
     * @param string[] $keys rows to be counted
     * @param mixed $super_column count only subcolumns in this super column
     * @param \phpcassa\ColumnSlice a slice of subcolumns to count, or null
     * @param mixed[] $column_names limit the possible subcolumns counted to this list
     * @param ConsistencyLevel $consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return mixed array(row_key => subcolumn_count)
     */
    public function multiget_subcolumn_count($keys,
                                             $super_column,
                                             $column_slice=null,
                                             $column_names=null,
                                             $consistency_level=null) {

        $cp = $this->create_column_parent($super_column);
        $slice = $this->create_slice_predicate(
            $column_names, $column_slice, false, ColumnSlice::MAX_COUNT);

        return $this->_multiget_count($keys, $cp, $slice, $consistency_level);
    }

    /**
     * Get an iterator over a particular super column across a range of rows.
     *
     * The returned iterator will return one array per row. This array will
     * look like array($rowkey, $subcolumns). Note that the super column layer
     * is omitted from the results.
     *
     * @param mixed $super_column return only columns in this super column
     * @param mixed $key_start fetch rows with a key >= this
     * @param mixed $key_finish fetch rows with a key <= this
     * @param int $row_count limit the number of rows returned to this amount
     * @param \phpcassa\ColumnSlice a slice of subcolumns to fetch, or null
     * @param mixed[] $column_names limit the subcolumns fetched to this list
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
        $slice = $this->create_slice_predicate($column_names, $column_slice, false);

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
     * Remove a super column from a row or a set of subcolumns from
     * a single super column.
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
                $predicate = $this->create_slice_predicate($subcolumns, null, false);
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

    protected function dict_to_coscs($data, $timestamp, $ttl) {
        $have_counters = $this->has_counters;
        $ret = array();
        foreach ($data as $name => $value) {
            $cosc = new ColumnOrSuperColumn();
            if($have_counters) {
                $sub = new CounterSuperColumn();
                $cosc->counter_super_column = $sub;
            } else {
                $sub = new SuperColumn();
                $cosc->super_column = $sub;
            }
            $sub->name = $this->pack_name($name, true, self::NON_SLICE, true);
            $sub->columns = $this->dict_to_columns($value, $timestamp, $ttl);
            $ret[] = $cosc;
        }

        return $ret;
    }

    protected function array_to_coscs($data, $timestamp, $ttl) {
        $have_counters = $this->has_counters;
        $ret = array();
        foreach ($data as $supercol) {
            list($name, $columns) = $supercol;
            $cosc = new ColumnOrSuperColumn();
            if($have_counters) {
                $sub = new CounterSuperColumn();
                $cosc->counter_super_column = $sub;
            } else {
                $sub = new SuperColumn();
                $cosc->super_column = $sub;
            }
            $sub->name = $this->pack_name($name, true, self::NON_SLICE, false);
            $sub->columns = $this->array_to_columns($columns, $timestamp, $ttl);
            $ret[] = $cosc;
        }

        return $ret;
    }

    protected function dict_to_columns($array, $timestamp, $ttl) {
        $ret = array();
        foreach($array as $name => $value) {
            if($this->has_counters) {
                $column = new CounterColumn();
            } else {
                $column = new Column();
                $column->timestamp = $timestamp;
                $column->ttl = $ttl;
            }
            $column->name = $this->pack_name(
                $name, false, self::NON_SLICE, true);
            $column->value = $this->pack_value($value, $column->name);
            $ret[] = $column;
        }
        return $ret;
    }

    protected function array_to_columns($array, $timestamp, $ttl) {
        $ret = array();
        foreach($array as $col) {
            list($name, $value) = $col;
            if($this->has_counters) {
                $column = new CounterColumn();
            } else {
                $column = new Column();
                $column->timestamp = $timestamp;
                $column->ttl = $ttl;
            }
            $column->name = $this->pack_name(
                $name, false, self::NON_SLICE, false);
            $column->value = $this->pack_value($value, $column->name);
            $ret[] = $column;
        }
        return $ret;
    }

    protected function coscs_to_dict($array_of_coscs) {
        $ret = array();
        $first = $array_of_coscs[0];
        if($first->column) { // normal columns
            foreach($array_of_coscs as $cosc) {
                $name = $this->unpack_name($cosc->column->name, false);
                $value = $this->unpack_value($cosc->column->value, $cosc->column->name);
                $ret[$name] = $value;
            }
        } else if($first->super_column) { // super columns
            foreach($array_of_coscs as $cosc) {
                $name = $this->unpack_name($cosc->super_column->name, true);
                $columns = $cosc->super_column->columns;
                $ret[$name] = $this->columns_to_dict($columns, false);
            }
        } else if ($first->counter_column) {
            foreach($array_of_coscs as $cosc) {
                $name = $this->unpack_name($cosc->counter_column->name, false);
                $ret[$name] = $cosc->counter_column->value;
            }
        } else { // counter_super_column
            foreach($array_of_coscs as $cosc) {
                $name = $this->unpack_name($cosc->counter_super_column->name, true);
                $columns = $cosc->counter_super_column->columns;
                $ret[$name] = $this->columns_to_dict($columns, true);
            }
        }
        return $ret;
    }

    protected function coscs_to_array($array_of_coscs) {
        $ret = array();
        $first = $array_of_coscs[0];
        if($first->column) { // normal columns
            foreach($array_of_coscs as $cosc) {
                $name = $this->unpack_name($cosc->column->name, false, false);
                $value = $this->unpack_value($cosc->column->value, $cosc->column->name);
                $ret[] = array($name, $value);
            }
        } else if($first->super_column) { // super columns
            foreach($array_of_coscs as $cosc) {
                $name = $this->unpack_name($cosc->super_column->name, true, false);
                $columns = $cosc->super_column->columns;
                $ret[] = array($name, $this->columns_to_array($columns, false));
            }
        } else if ($first->counter_column) {
            foreach($array_of_coscs as $cosc) {
                $name = $this->unpack_name($cosc->counter_column->name, false, false);
                $ret[] = array($name, $cosc->counter_column->value);
            }
        } else { // counter_super_column
            foreach($array_of_coscs as $cosc) {
                $name = $this->unpack_name($cosc->counter_super_column->name, true, false);
                $columns = $cosc->counter_super_column->columns;
                $ret[] = array($name, $this->columns_to_array($columns, true));
            }
        }
        return $ret;
    }

    protected function unpack_coscs_attrs($array_of_coscs) {
        $ret = array();
        $first = $array_of_coscs[0];
        if($first->column) { // normal columns
            foreach($array_of_coscs as $cosc) {
                $col = $cosc->column;
                $col->value = $this->unpack_value($col->value, $col->name);
                $col->name = $this->unpack_name($col->name, false, false);
                $ret[] = $col;
            }
        } else if($first->super_column) { // super columns
            foreach($array_of_coscs as $cosc) {
                $supercol = $cosc->super_column;
                $supercol->name = $this->unpack_name(
                        $supercol->name, true, false);
                $supercol->columns = $this->unpack_subcolumn_attrs(
                        $supercol->columns, false);
                $ret[] = $supercol;
            }
        } else if ($first->counter_column) {
            foreach($array_of_coscs as $cosc) {
                $col = $cosc->counter_column;
                $col->name = $this->unpack_name($col->name, false, false);
                $ret[] = $col;
            }
        } else { // counter_super_column
            foreach($array_of_coscs as $cosc) {
                $supercol = $cosc->super_column;
                $supercol->name = $this->unpack_name(
                        $supercol->name, true, false);
                $supercol->columns = $this->unpack_subcolumn_attrs(
                        $supercol->columns, true);
                $ret[] = $supercol;
            }
        }
        return $ret;
    }

    protected function unpack_subcolumn_attrs($columns, $have_counters) {
        $ret = array();
        if (!$have_counters) {
            foreach($columns as $c) {
                $c->value = $this->unpack_value($c->value, $c->name);
                $c->name = $this->unpack_name($c->name, false, false);
                $ret[] = $c;
            }
        } else {
            foreach($columns as $c) {
                $c->name = $this->unpack_name($c->name, false, false);
                $ret[] = $c;
            }
        }
        return $ret;
    }

    protected function columns_to_dict($columns, $have_counters) {
        $ret = array();
        if (!$have_counters) {
            foreach($columns as $c) {
                $value = $this->unpack_value($c->value, $c->name);
                $name  = $this->unpack_name($c->name, false);
                $ret[$name] = $value;
            }
        } else {
            foreach($columns as $c) {
                $name = $this->unpack_name($c->name, false);
                $ret[$name] = $c->value;
            }
        }
        return $ret;
    }

    protected function columns_to_array($columns, $have_counters) {
        $ret = array();
        if (!$have_counters) {
            foreach($columns as $c) {
                $value = $this->unpack_value($c->value, $c->name);
                $name  = $this->unpack_name($c->name, false, false);
                $ret[] = array($name, $value);
            }
        } else {
            foreach($columns as $c) {
                $name = $this->unpack_name($c->name, false, false);
                $ret[] = array($name, $c->value);
            }
        }
        return $ret;
    }

}
