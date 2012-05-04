<?php
namespace phpcassa;

use phpcassa\Schema\DataType;
use phpcassa\Schema\DataType\BytesType;
use phpcassa\Schema\DataType\CompositeType;

use phpcassa\Iterator\IndexedColumnFamilyIterator;
use phpcassa\Iterator\RangeColumnFamilyIterator;

use phpcassa\Batch\CfMutator;

use phpcassa\Util\Clock;

use cassandra\InvalidRequestException;
use cassandra\NotFoundException;

use cassandra\Mutation;
use cassandra\Deletion;
use cassandra\ConsistencyLevel;
use cassandra\Column;
use cassandra\ColumnParent;
use cassandra\ColumnPath;
use cassandra\ColumnOrSuperColumn;
use cassandra\SuperColumn;
use cassandra\CounterSuperColumn;
use cassandra\CounterColumn;
use cassandra\IndexClause;
use cassandra\IndexExpression;
use cassandra\SlicePredicate;
use cassandra\SliceRange;

/**
 * Representation of a ColumnFamily in Cassandra.  This may be used for
 * standard column families or super column families. All data insertions,
 * deletions, or retrievals will go through a ColumnFamily.
 *
 * @package phpcassa
 */
class ColumnFamily {

    /** The default limit to the number of rows retrieved in queries. */
    const DEFAULT_ROW_COUNT = 100; // default max # of rows for get_range()
    /** The default limit to the number of columns retrieved in queries. */
    const DEFAULT_COLUMN_COUNT = 100; // default max # of columns for get()
    /** The maximum number that can be returned by get_count(). */
    const MAX_COUNT = 2147483647; # 2^31 - 1

    const DEFAULT_BUFFER_SIZE = 1024;

    public $column_family;
    public $is_super;
    protected $cf_data_type;
    protected $col_name_type;
    protected $supercol_name_type;
    protected $col_type_dict;


    public $autopack_names;
    public $autopack_values;
    public $autopack_keys;

    /** @var ConsistencyLevel the default read consistency level */
    public $read_consistency_level;
    /** @var ConsistencyLevel the default write consistency level */
    public $write_consistency_level;

    /** @var bool If true, column values in data fetching operations will be
     * replaced by an array of the form array(column_value, column_timestamp). */
    public $include_timestamp = false;

    /**
     * @var int When calling `get_range`, the intermediate results need
     *       to be buffered if we are fetching many rows, otherwise the Cassandra
     *       server will overallocate memory and fail.  This is the size of
     *       that buffer in number of rows. The default is 1024.
     */
    public $buffer_size = 1024;

    /**
     * Constructs a ColumnFamily.
     *
     * @param phpcassa\Connection\ConnectionPool $pool the pool to use when
     *        querying Cassandra
     * @param string $column_family the name of the column family in Cassandra
     * @param bool $autopack_names whether or not to automatically convert column names 
     *        to and from their binary representation in Cassandra
     *        based on their comparator type
     * @param bool $autopack_values whether or not to automatically convert column values
     *        to and from their binary representation in Cassandra
     *        based on their validator type
     * @param ConsistencyLevel $read_consistency_level the default consistency
     *        level for read operations on this column family
     * @param ConsistencyLevel $write_consistency_level the default consistency
     *        level for write operations on this column family
     * @param int $buffer_size When calling `get_range`, the intermediate results need
     *        to be buffered if we are fetching many rows, otherwise the Cassandra
     *        server will overallocate memory and fail.  This is the size of
     *        that buffer in number of rows.
     */
    public function __construct($pool,
                                $column_family,
                                $autopack_names=true,
                                $autopack_values=true,
                                $read_consistency_level=ConsistencyLevel::ONE,
                                $write_consistency_level=ConsistencyLevel::ONE,
                                $buffer_size=self::DEFAULT_BUFFER_SIZE) {

        $this->pool = $pool;
        $this->column_family = $column_family;
        $this->read_consistency_level = $read_consistency_level;
        $this->write_consistency_level = $write_consistency_level;
        $this->buffer_size = $buffer_size;

        $ks = $this->pool->describe_keyspace();

        $cf_def = null;
        foreach($ks->cf_defs as $cfdef) {
            if ($cfdef->name == $this->column_family) {
                $cf_def = $cfdef;
                break;
            }
        }
        if ($cf_def == null)
            throw new NotFoundException();
        else
            $this->cfdef = $cf_def;

        $this->cf_data_type = new BytesType();
        $this->col_name_type = new BytesType();
        $this->supercol_name_type = new BytesType();
        $this->key_type = new BytesType();
        $this->col_type_dict = array();

        $this->is_super = $this->cfdef->column_type === 'Super';
        $this->has_counters = self::endswith(
            $this->cfdef->default_validation_class,
            "CounterColumnType");

        $this->set_autopack_names($autopack_names);
        $this->set_autopack_values($autopack_values);
        $this->set_autopack_keys(true);
    }

    protected static function endswith($str, $suffix) {
        $suffix_len = strlen($suffix);
        return substr_compare($str, $suffix, strlen($str)-$suffix_len, $suffix_len) === 0;
    }

    /**
     * @param bool $pack_names whether or not column names are automatically packed/unpacked
     */
    public function set_autopack_names($pack_names) {
        if ($pack_names) {
            if ($this->autopack_names)
                return;
            $this->autopack_names = true;
            if (!$this->is_super) {
                $this->col_name_type = DataType::get_type_for($this->cfdef->comparator_type);
            } else {
                $this->col_name_type = DataType::get_type_for($this->cfdef->subcomparator_type);
                $this->supercol_name_type = DataType::get_type_for($this->cfdef->comparator_type);
            }
        } else {
            $this->autopack_names = false;
        }
    }

    /**
     * @param bool $pack_values whether or not column values are automatically packed/unpacked
     */
    public function set_autopack_values($pack_values) {
        if ($pack_values) {
            $this->autopack_values = true;
            $this->cf_data_type = DataType::get_type_for($this->cfdef->default_validation_class);
            foreach($this->cfdef->column_metadata as $coldef) {
                $this->col_type_dict[$coldef->name] =
                        DataType::get_type_for($coldef->validation_class);
            }
        } else {
            $this->autopack_values = false;
        }
    }

    /**
     * @param bool $pack_keys whether or not keys are automatically packed/unpacked
     *
     * Available since Cassandra 0.8.0.
     */
    public function set_autopack_keys($pack_keys) {
        if ($pack_keys) {
            $this->autopack_keys = true;
            if (property_exists('\cassandra\CfDef', "key_validation_class")) {
                $this->key_type = DataType::get_type_for($this->cfdef->key_validation_class);
            } else {
                $this->key_type = new BytesType();
            }
        } else {
            $this->autopack_keys = false;
        }
    }

    /**
     * Fetch a row from this column family.
     *
     * @param string $key row key to fetch
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
    public function get($key,
                        $columns=null,
                        $column_start="",
                        $column_finish="",
                        $column_reversed=false,
                        $column_count=self::DEFAULT_COLUMN_COUNT,
                        $read_consistency_level=null) {

        $column_parent = $this->create_column_parent();
        $predicate = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                                   $column_reversed, $column_count);

        return $this->_get($key, $column_parent, $predicate, $read_consistency_level);
    }

    protected function _get($key, $cp, $slice, $cl) {
        $resp = $this->pool->call("get_slice",
            $this->pack_key($key),
            $cp, $slice, $this->rcl($cl));

        if (count($resp) == 0)
            throw new NotFoundException();

        return $this->coscs_to_array($resp);
    }

    /**
     * Fetch a set of rows from this column family.
     *
     * @param string[] $keys row keys to fetch
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
    public function multiget($keys,
                             $columns=null,
                             $column_start="",
                             $column_finish="",
                             $column_reversed=false,
                             $column_count=self::DEFAULT_COLUMN_COUNT,
                             $read_consistency_level=null,
                             $buffer_size=16)  {

        $cp = $this->create_column_parent();
        $slice = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                               $column_reversed, $column_count);

        return $this->_multiget($keys, $cp, $slice, $read_consistency_level, $buffer_size);
    }

    protected function _multiget($keys, $cp, $slice, $cl, $buffsz) {
        $ret = array();
        foreach($keys as $key) {
            $ret[$key] = null;
        }

        $cl = $this->rcl($cl);

        $resp = array();
        if(count($keys) <= $buffsz) {
            $resp = $this->pool->call("multiget_slice",
                array_map(array($this, "pack_key"), $keys),
                $cp, $slice, $cl);
        } else {
            $subset_keys = array();
            $i = 0;
            foreach($keys as $key) {
                $i += 1;
                $subset_keys[] = $key;
                if ($i == $buffsz) {
                    $sub_resp = $this->pool->call("multiget_slice",
                        array_map(array($this, "pack_key"), $subset_keys),
                        $cp, $slice, $cl);
                    $subset_keys = array();
                    $i = 0;
                    $resp = $resp + $sub_resp;
                }
            }
            if (count($subset_keys) != 0) {
                $sub_resp = $this->pool->call("multiget_slice",
                    array_map(array($this, "pack_key"), $subset_keys),
                    $cp, $slice, $cl);
                $resp = $resp + $sub_resp;
            }
        }

        $non_empty_keys = array();
        foreach($resp as $key => $val) {
            if (count($val) > 0) {
                $unpacked_key = $this->unpack_key($key);
                $non_empty_keys[$unpacked_key] = 1;
                $ret[$unpacked_key] = $this->coscs_to_array($val);
            }
        }

        foreach($keys as $key) {
            if (!isset($non_empty_keys[$key]))
                unset($ret[$key]);
        }
        return $ret;
    }

    /**
     * Count the number of columns in a row.
     *
     * @param string $key row to be counted
     * @param mixed[] $columns limit the possible columns or super columns counted to this list
     * @param mixed $column_start only count columns with name >= this
     * @param mixed $column_finish only count columns with name <= this
     * @param ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int
     */
    public function get_count($key,
                              $columns=null,
                              $column_start='',
                              $column_finish='',
                              $read_consistency_level=null) {

        $cp = $this->create_column_parent();
        $slice = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                               false, self::MAX_COUNT);
        return $this->_get_count($key, $cp, $slice, $read_consistency_level);
    }

    protected function _get_count($key, $cp, $slice, $cl) {
        $packed_key = $this->pack_key($key);
        return $this->pool->call("get_count", $packed_key, $cp, $slice, $this->rcl($cl));
    }

    /**
     * Count the number of columns in a set of rows.
     *
     * @param string[] $keys rows to be counted
     * @param mixed[] $columns limit the possible columns or super columns counted to this list
     * @param mixed $column_start only count columns with name >= this
     * @param mixed $column_finish only count columns with name <= this
     * @param ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return mixed array(row_key => row_count)
     */
    public function multiget_count($keys,
                                   $columns=null,
                                   $column_start='',
                                   $column_finish='',
                                   $read_consistency_level=null) {

        $cp = $this->create_column_parent();
        $slice = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                               false, self::MAX_COUNT);
        return $this->_multiget_count($keys, $cp, $slice, $read_consistency_level);
    }

    protected function _multiget_count($keys, $cp, $slice, $cl) {

        $ret = array();
        foreach($keys as $key) {
            $ret[$key] = null;
        }

        $packed_keys = array_map(array($this, "pack_key"), $keys);
        $results = $this->pool->call("multiget_count", $packed_keys, $cp, $slice,
            $this->rcl($cl));

        $non_empty_keys = array();
        foreach ($results as $key => $count) {
            $unpacked_key = $this->unpack_key($key);
            $non_empty_keys[$unpacked_key] = 1;
            $ret[$unpacked_key] = $count;
        }

        foreach($keys as $key) {
            if (!isset($non_empty_keys[$key]))
                unset($ret[$key]);
        }

        return $ret;
    }

    /**
     * Get an iterator over a range of rows.
     *
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
    public function get_range($key_start="",
                              $key_finish="",
                              $row_count=self::DEFAULT_ROW_COUNT,
                              $columns=null,
                              $column_start="",
                              $column_finish="",
                              $column_reversed=false,
                              $column_count=self::DEFAULT_COLUMN_COUNT,
                              $read_consistency_level=null,
                              $buffer_size=null) {

        $cp = $this->create_column_parent();
        $slice = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                               $column_reversed, $column_count);

        return $this->_get_range($key_start, $key_finish, $row_count,
            $cp, $slice, $read_consistency_level, $buffer_size);
    }

    protected function _get_range($start, $finish, $count, $cp, $slice, $cl, $buffsz) {

        if ($buffsz == null)
            $buffsz = $this->buffer_size;
        if ($buffsz < 2) {
            $ire = new InvalidRequestException();
            $ire->message = 'buffer_size cannot be less than 2';
            throw $ire;
        }

        $packed_key_start = $this->pack_key($start);
        $packed_key_finish = $this->pack_key($finish);

        return new RangeColumnFamilyIterator($this, $buffsz,
                                             $packed_key_start, $packed_key_finish,
                                             $count, $cp, $slice, $this->rcl($cl));
    }

   /**
    * Fetch a set of rows from this column family based on an index clause.
    *
    * @param phpcassa\Index\IndexClause $index_clause limits the keys that are returned based
    *        on expressions that compare the value of a column to a given value.  At least
    *        one of the expressions in the IndexClause must be on an indexed column.
    * @param mixed[] $columns limit the columns or super columns fetched to this list
    * @param mixed $column_start only fetch columns with name >= this
    * @param mixed $column_finish only fetch columns with name <= this
    * @param bool $column_reversed fetch the columns in reverse order
    * @param int $column_count limit the number of columns returned to this amount
    * @param ConsistencyLevel $read_consistency_level affects the guaranteed
    * number of nodes that must respond before the operation returns
    *
    * @return phpcassa\Iterator\IndexedColumnFamilyIterator
    */
    public function get_indexed_slices($index_clause,
                                       $columns=null,
                                       $column_start='',
                                       $column_finish='',
                                       $column_reversed=false,
                                       $column_count=self::DEFAULT_COLUMN_COUNT,
                                       $read_consistency_level=null,
                                       $buffer_size=null) {

        if ($buffer_size == null)
            $buffer_size = $this->buffer_size;
        if ($buffer_size < 2) {
            $ire = new InvalidRequestException();
            $ire->message = 'buffer_size cannot be less than 2';
            throw $ire;
        }

        $new_clause = new IndexClause();
        foreach($index_clause->expressions as $expr) {
            $new_expr = new IndexExpression();
            $new_expr->value = $this->pack_value($expr->value, $expr->column_name);
            $new_expr->column_name = $this->pack_name($expr->column_name);
            $new_expr->op = $expr->op;
            $new_clause->expressions[] = $new_expr;
        }
        $new_clause->start_key = $this->pack_key($index_clause->start_key);
        $new_clause->count = $index_clause->count;

        $column_parent = $this->create_column_parent();
        $predicate = $this->create_slice_predicate($columns, $column_start,
                                                   $column_finish, $column_reversed,
                                                   $column_count);

        return new IndexedColumnFamilyIterator($this, $new_clause, $buffer_size,
                                               $column_parent, $predicate,
                                               $this->rcl($read_consistency_level));
    }

    /**
     * Insert or update columns in a row.
     *
     * @param string $key the row to insert or update the columns in
     * @param mixed $columns array(column_name => column_value) the columns to insert or update
     * @param int $timestamp the timestamp to use for this insertion. Leaving this as null will
     *        result in a timestamp being generated for you
     * @param int $ttl time to live for the columns; after ttl seconds they will be deleted
     * @param ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int the timestamp for the operation
     */
    public function insert($key,
                           $columns,
                           $timestamp=null,
                           $ttl=null,
                           $write_consistency_level=null) {

        if ($timestamp === null)
            $timestamp = Clock::get_time();

        $cfmap = array();
        $packed_key = $this->pack_key($key);
        $cfmap[$packed_key][$this->column_family] =
                $this->array_to_mutation($columns, $timestamp, $ttl);

        return $this->pool->call("batch_mutate", $cfmap, $this->wcl($write_consistency_level));
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
     * @param mixed $column the column name of the counter
     * @param int $value the amount to adjust the counter by
     * @param ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     */
    public function add($key, $column, $value=1, $write_consistency_level=null) {
        $cp = $this->create_column_parent();
        return $this->_add($key, $cp, $value, $write_consistency_level);
    }

    protected function _add($key, $cp, $value, $cl) {
        $packed_key = $this->pack_key($key);
        $counter = new CounterColumn();
        $counter->name = $this->pack_name($column);
        $counter->value = $value;
        return $this->pool->call("add", $packed_key, $cp, $counter, $this->wcl($cl));
    }

    /**
     * Insert or update columns in multiple rows. Note that this operation is only atomic
     * per row.
     *
     * @param array $rows an array of keys, each of which maps to an array of columns. This
     *        looks like array(key => array(column_name => column_value))
     * @param int $timestamp the timestamp to use for these insertions. Leaving this as null will
     *        result in a timestamp being generated for you
     * @param int $ttl time to live for the columns; after ttl seconds they will be deleted
     * @param ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int the timestamp for the operation
     */
    public function batch_insert($rows, $timestamp=null, $ttl=null, $write_consistency_level=null) {
        if ($timestamp === null)
            $timestamp = Clock::get_time();

        $cfmap = array();
        foreach($rows as $key => $columns) {
            $packed_key = $this->pack_key($key);
            $cfmap[$packed_key][$this->column_family] =
                    $this->array_to_mutation($columns, $timestamp, $ttl);
        }

        return $this->pool->call("batch_mutate", $cfmap, $this->wcl($write_consistency_level));
    }

    public function batch($write_consistency_level=null) {
        return new CfMutator($this, $write_consistency_level);
    }

    /**
     * Delete a row or a set of columns or supercolumns from a row.
     *
     * @param string $key the row to remove columns from
     * @param mixed[] $columns the columns or supercolumns to remove.
     *                If null, the entire row will be removed.
     * @param ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int the timestamp for the operation
     */
    public function remove($key, $columns=null, $write_consistency_level=null) {

        if ($columns === null || count($columns) == 1)
        {
            $cp = new ColumnPath();
            $cp->column_family = $this->column_family;

            if ($columns !== null) {
                if ($this->is_super)
                    $cp->super_column = $this->pack_name($columns[0], true);
                else
                    $cp->column = $this->pack_name($columns[0], false);
            }
            return $this->_remove_single($key, $cp, $write_consistency_level);
        } else {
            $deletion = new Deletion();
            if ($columns !== null) {
                $predicate = $this->create_slice_predicate($columns, '', '', false,
                                                           self::DEFAULT_COLUMN_COUNT);
                $deletion->predicate = $predicate;
            }

            return $this->_remove_multi($key, $deletion, $write_consistency_level);
        }
    }

    protected function _remove_single($key, $cp, $cl) {
        $timestamp = Clock::get_time();
        $packed_key = $this->pack_key($key);
        return $this->pool->call("remove", $packed_key, $cp, $timestamp,
            $this->wcl($cl));
    }

    protected function _remove_multi($key, $deletion, $cl) {
        $timestamp = Clock::get_time();
        $deletion->timestamp = $timestamp;
        $mutation = new Mutation();
        $mutation->deletion = $deletion;

        $packed_key = $this->pack_key($key);
        $mut_map = array($packed_key => array($this->column_family => array($mutation))); 

        return $this->pool->call("batch_mutate", $mut_map, $this->wcl($write_consistency_level));
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
     * @param mixed $column the column name of the counter
     * @param ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     */
    public function remove_counter($key, $column, $write_consistency_level=null) {
        $cp = new ColumnPath();
        $packed_key = $this->pack_key($key);
        $cp->column_family = $this->column_family;
        $cp->column = $this->pack_name($column);
        $this->pool->call("remove_counter", $packed_key, $cp,
            $this->wcl($write_consistency_level));
    }

    /**
     * Mark the entire column family as deleted.
     *
     * From the user's perspective a successful call to truncate will result
     * complete data deletion from cfname. Internally, however, disk space
     * will not be immediatily released, as with all deletes in cassandra,
     * this one only marks the data as deleted.
     *
     * The operation succeeds only if all hosts in the cluster at available
     * and will throw an UnavailableException if some hosts are down.
     */
    public function truncate() {
        return $this->pool->call("truncate", $this->column_family);
    }


    /********************* Helper functions *************************/

    protected function rcl($read_consistency_level) {
        if ($read_consistency_level === null)
            return $this->read_consistency_level;
        else
            return $read_consistency_level;
    }

    protected function wcl($write_consistency_level) {
        if ($write_consistency_level === null)
            return $this->write_consistency_level;
        else
            return $write_consistency_level;
    }

    protected function create_slice_predicate($columns, $column_start, $column_finish,
                                              $column_reversed, $column_count) {

        $predicate = new SlicePredicate();
        if ($columns !== null) {
            $packed_cols = array();
            foreach($columns as $col)
                $packed_cols[] = $this->pack_name($col, $this->is_super);
            $predicate->column_names = $packed_cols;
        } else {
            if ($column_start !== null and $column_start != '')
                $column_start = $this->pack_name($column_start,
                                                 $this->is_super,
                                                 self::SLICE_START);
            if ($column_finish !== null and $column_finish != '')
                $column_finish = $this->pack_name($column_finish,
                                                  $this->is_super,
                                                  self::SLICE_FINISH);

            $slice_range = new SliceRange();
            $slice_range->count = $column_count;
            $slice_range->reversed = $column_reversed;
            $slice_range->start  = $column_start;
            $slice_range->finish = $column_finish;
            $predicate->slice_range = $slice_range;
        }
        return $predicate;
    }

    protected function create_column_parent($super_column=null) {
        $column_parent = new ColumnParent();
        $column_parent->column_family = $this->column_family;
        if ($super_column !== null) {
            $column_parent->super_column = $this->pack_name($super_column, true);
        } else {
            $column_parent->super_column = null;
        }
        return $column_parent;
    }

    const NON_SLICE = 0;
    const SLICE_START = 1;
    const SLICE_FINISH = 2;

    public function pack_name($value,
                              $is_supercol_name=false,
                              $slice_end=self::NON_SLICE,
                              $is_data=false) {
        if (!$this->autopack_names)
            return $value;
        if ($slice_end === self::NON_SLICE && ($value === null || $value === "")) {
            throw new \UnexpectedValueException("Column names may not be null");
        }
        if ($is_supercol_name)
            return $this->supercol_name_type->pack($value, true, $slice_end, $is_data);
        else
            return $this->col_name_type->pack($value, true, $slice_end, $is_data);
    }

    protected function unpack_name($b, $is_supercol_name=false) {
        if (!$this->autopack_names)
            return $b;
        if ($b === null)
            return;

        if ($is_supercol_name)
            return $this->supercol_name_type->unpack($b, true);
        else
            return $this->col_name_type->unpack($b, true);
    }

    public function pack_key($key) {
        if (!$this->autopack_keys || $key === "")
            return $key;
        return $this->key_type->pack($key, false);
    }

    public function unpack_key($b) {
        if (!$this->autopack_keys)
            return $b;
        return $this->key_type->unpack($b, true);
    }

    protected function get_data_type_for_col($col_name) {
		if (isset($this->col_type_dict[$col_name]))
			return $this->col_type_dict[$col_name];
		else 
			return $this->cf_data_type;
    }

    protected function pack_value($value, $col_name) {
        if (!$this->autopack_values)
            return $value;

        if (isset($this->col_type_dict[$col_name])) {
            $dtype = $this->col_type_dict[$col_name];
            return $dtype->pack($value, false);
        } else {
            return $this->cf_data_type->pack($value, false);
        }
    }

    protected function unpack_value($value, $col_name) {
        if (!$this->autopack_values)
            return $value;

        if (isset($this->col_type_dict[$col_name])) {
            $dtype = $this->col_type_dict[$col_name];
            return $dtype->unpack($value, false);
        } else {
            return $this->cf_data_type->unpack($value, false);
        }
    }

    public function keyslices_to_array($keyslices) {
        $ret = array();
        foreach($keyslices as $keyslice) {
            $key = $this->unpack_key($keyslice->key);
            $columns = $keyslice->columns;
            $ret[$key] = $this->coscs_to_array($columns);
        }
        return $ret;
    }

    protected function coscs_to_array($array_of_c_or_sc) {
        $ret = null;
        if(count($array_of_c_or_sc) == 0) {
            return $ret;
        }
        $first = $array_of_c_or_sc[0];
        if($first->column) { // normal columns
            if ($this->include_timestamp) {
                foreach($array_of_c_or_sc as $c_or_sc) {
                    $name = $this->unpack_name($c_or_sc->column->name, false);
                    $value = $this->unpack_value($c_or_sc->column->value, $c_or_sc->column->name);
                    $ret[$name] = array($value, $c_or_sc->column->timestamp);
                }
            } else {
                foreach($array_of_c_or_sc as $c_or_sc) {
                    $name = $this->unpack_name($c_or_sc->column->name, false);
                    $value = $this->unpack_value($c_or_sc->column->value, $c_or_sc->column->name);
                    $ret[$name] = $value;
                }
            }
        } else if($first->super_column) { // super columns
            foreach($array_of_c_or_sc as $c_or_sc) {
                $name = $this->unpack_name($c_or_sc->super_column->name, true);
                $columns = $c_or_sc->super_column->columns;
                $ret[$name] = $this->columns_to_array($columns);
            }
        } else if ($first->counter_column) {
            foreach($array_of_c_or_sc as $c_or_sc) {
                $name = $this->unpack_name($c_or_sc->counter_column->name, false);
                $ret[$name] = $c_or_sc->counter_column->value;
            }
        } else { // counter_super_column
            foreach($array_of_c_or_sc as $c_or_sc) {
                $name = $this->unpack_name($c_or_sc->counter_super_column->name, true);
                $columns = $c_or_sc->counter_super_column->columns;
                $ret[$name] = $this->counter_columns_to_array($columns);
            }
        }
        return $ret;
    }

    protected function columns_to_array($array_of_c) {
        $ret = array();
        if ($this->include_timestamp) {
            foreach($array_of_c as $c) {
                $name  = $this->unpack_name($c->name, false);
                $value = $this->unpack_value($c->value, $c->name);
                $ret[$name] = array($value, $c->timestamp);
            }
        } else {
            foreach($array_of_c as $c) {
                $name  = $this->unpack_name($c->name, false);
                $value = $this->unpack_value($c->value, $c->name);
                $ret[$name] = $value;
            }
        }
        return $ret;
    }

    protected function counter_columns_to_array($array_of_c) {
        $ret = array();
        foreach($array_of_c as $c) {
            $name = $this->unpack_name($c->name, false);
            $ret[$name] = $c->value;
        }
        return $ret;
    }

    public function array_to_mutation($array, $timestamp=null, $ttl=null) {
        if($timestamp === null)
            $timestamp = Clock::get_time();

        $c_or_sc = $this->array_to_coscs($array, $timestamp, $ttl);
        $ret = array();
        foreach($c_or_sc as $row) {
            $mutation = new Mutation();
            $mutation->column_or_supercolumn = $row;
            $ret[] = $mutation;
        }
        return $ret;
    }

    protected function array_to_coscs($data, $timestamp=null, $ttl=null) {
        if($timestamp === null)
            $timestamp = Clock::get_time();

        $ret = array();
        foreach ($data as $name => $value) {
            $c_or_sc = new ColumnOrSuperColumn();
            if($this->is_super) {
                if($this->has_counters) {
                    $sub = new CounterSuperColumn();
                    $c_or_sc->counter_super_column = $sub;
                } else {
                    $sub = new SuperColumn();
                    $c_or_sc->super_column = $sub;
                }
                $sub->name = $this->pack_name($name, true, self::NON_SLICE, true);
                $sub->columns = $this->array_to_columns($value, $timestamp, $ttl);
                $sub->timestamp = $timestamp;
            } else {
                if($this->has_counters) {
                    $sub = new CounterColumn();
                    $c_or_sc->counter_column = $sub;
                } else {
                    $sub = new Column();
                    $c_or_sc->column = $sub;
                    $sub->timestamp = $timestamp;
                    $sub->ttl = $ttl;
                }
                $sub->name = $this->pack_name(
                    $name, false, self::NON_SLICE, true);
                $sub->value = $this->pack_value($value, $name);
            }
            $ret[] = $c_or_sc;
        }

        return $ret;
    }

    protected function array_to_columns($array, $timestamp=null, $ttl=null) {
        if($timestamp === null)
            $timestamp = Clock::get_time();

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
            $column->value = $this->pack_value($value, $name);
            $ret[] = $column;
        }
        return $ret;
    }
}

