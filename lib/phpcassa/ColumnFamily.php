<?php
namespace phpcassa;

use phpcassa\Iterator\IndexedColumnFamilyIterator;
use phpcassa\Iterator\RangeColumnFamilyIterator;
use phpcassa\Util\CassandraUtil;

/**
 * Representation of a ColumnFamily in Cassandra.  This may be used for
 * standard column families or super column families. All data insertions,
 * deletions, or retrievals will go through a ColumnFamily.
 *
 * @package phpcassa
 * @subpackage columnfamily
 */
class ColumnFamily {

    /** The default limit to the number of rows retrieved in queries. */
    const DEFAULT_ROW_COUNT = 100; // default max # of rows for get_range()
    /** The default limit to the number of columns retrieved in queries. */
    const DEFAULT_COLUMN_COUNT = 100; // default max # of columns for get()
    /** The maximum number that can be returned by get_count(). */
    const MAX_COUNT = 2147483647; # 2^31 - 1

    const DEFAULT_BUFFER_SIZE = 8096;

    public $client;
    private $column_family;
    private $is_super;
    private $cf_data_type;
    private $col_name_type;
    private $supercol_name_type;
    private $col_type_dict;

    /** @var bool whether or not column names are automatically packed/unpacked */
    public $autopack_names;
    /** @var bool whether or not column values are automatically packed/unpacked */
    public $autopack_values;
    /** @var \cassandra_ConsistencyLevel the default read consistency level */
    public $read_consistency_level;
    /** @var \cassandra_ConsistencyLevel the default write consistency level */
    public $write_consistency_level;

    /**
     * Constructs a ColumnFamily.
     *
     * @param Connection $connection the connection to use for this ColumnFamily
     * @param string $column_family the name of the column family in Cassandra
     * @param bool $autopack_names whether or not to automatically convert column names 
     *        to and from their binary representation in Cassandra
     *        based on their comparator type
     * @param bool $autopack_values whether or not to automatically convert column values
     *        to and from their binary representation in Cassandra
     *        based on their validator type
     * @param \cassandra_ConsistencyLevel $read_consistency_level the default consistency
     *        level for read operations on this column family
     * @param \cassandra_ConsistencyLevel $write_consistency_level the default consistency
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
                                $read_consistency_level=\cassandra_ConsistencyLevel::ONE,
                                $write_consistency_level=\cassandra_ConsistencyLevel::ONE,
                                $buffer_size=self::DEFAULT_BUFFER_SIZE) {

        $this->pool = $pool;
        $this->column_family = $column_family;
        $this->autopack_names = $autopack_names;
        $this->autopack_values = $autopack_values;
        $this->read_consistency_level = $read_consistency_level;
        $this->write_consistency_level = $write_consistency_level;
        $this->buffer_size = $buffer_size;

        $this->cf_data_type = 'BytesType';
        $this->col_name_type = 'BytesType';
        $this->supercol_name_type = 'BytesType';
        $this->col_type_dict = array();

        $ks = $this->pool->describe_keyspace();

        $cf_def = null;
        foreach($ks->cf_defs as $cfdef) {
            if ($cfdef->name == $this->column_family) {
                $cf_def = $cfdef;
                break;
            }
        }
        if ($cf_def == null)
            throw new \cassandra_NotFoundException();

        $this->is_super = $cf_def->column_type == 'Super';       
        if ($this->autopack_names) {
            if (!$this->is_super) {
                $this->col_name_type = self::extract_type_name($cfdef->comparator_type);
            } else {
                $this->col_name_type = self::extract_type_name($cfdef->subcomparator_type);
                $this->supercol_name_type = self::extract_type_name($cfdef->comparator_type);
            }
        }
        if ($this->autopack_values) {
            $this->cf_data_type = self::extract_type_name($cfdef->default_validation_class);
            foreach($cfdef->column_metadata as $coldef) {
                $this->col_type_dict[$coldef->name] =
                        self::extract_type_name($coldef->validation_class);
            }
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
     * @param mixed $super_column return only columns in this super column
     * @param \cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return mixed array(column_name => column_value)
     */
    public function get($key,
                        $columns=null,
                        $column_start="",
                        $column_finish="",
                        $column_reversed=False,
                        $column_count=self::DEFAULT_COLUMN_COUNT,
                        $super_column=null,
                        $read_consistency_level=null) {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                                   $column_reversed, $column_count);

        $resp = $this->pool->call("get_slice",
            $key, $column_parent, $predicate,
            $this->rcl($read_consistency_level));

        if (count($resp) == 0)
            throw new \cassandra_NotFoundException();

        return $this->supercolumns_or_columns_to_array($resp);
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
     * @param mixed $super_column return only columns in this super column
     * @param \cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
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
                             $column_reversed=False,
                             $column_count=self::DEFAULT_COLUMN_COUNT,
                             $super_column=null,
                             $read_consistency_level=null,
                             $buffer_size=16)  {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                                   $column_reversed, $column_count);

        $ret = array();
        foreach($keys as $key) {
            $ret[$key] = null;
        }

        $resp = array();
        if(count($keys) <= $buffer_size) {
            $resp = $this->pool->call("multiget_slice",
                $keys, $column_parent, $predicate,
                $this->rcl($read_consistency_level));
        } else {
            $subset_keys = array();
            $i = 0;
            foreach($keys as $key) {
                $i += 1;
                $subset_keys[] = $key;
                if ($i == $buffer_size) {
                    $sub_resp = $this->pool->call("multiget_slice",
                        $subset_keys, $column_parent, $predicate,
                        $this->rcl($read_consistency_level));
                    $subset_keys = array();
                    $i = 0;
                    $resp = array_merge($resp, $sub_resp);
                }
            }
            if (count($subset_keys) != 0) {
                $sub_resp = $this->pool->call("multiget_slice",
                    $subset_keys, $column_parent, $predicate,
                    $this->rcl($read_consistency_level));
                $resp = array_merge($resp, $sub_resp);
            }
        }

        $non_empty_keys = array();
        foreach($resp as $key => $val) {
            if (count($val) > 0) {
                $non_empty_keys[] = $key;
                $ret[$key] = $this->supercolumns_or_columns_to_array($val);
            }
        }

        foreach($keys as $key) {
            if (!in_array($key, $non_empty_keys))
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
     * @param mixed $super_column count only columns in this super column
     * @param \cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int
     */
    public function get_count($key,
                              $columns=null,
                              $column_start='',
                              $column_finish='',
                              $super_column=null,
                              $read_consistency_level=null) {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                                   false, self::MAX_COUNT);

        return $this->pool->call("get_count", $key, $column_parent, $predicate,
            $this->rcl($read_consistency_level));
    }

    /**
     * Count the number of columns in a set of rows.
     *
     * @param string[] $keys rows to be counted
     * @param mixed[] $columns limit the possible columns or super columns counted to this list
     * @param mixed $column_start only count columns with name >= this
     * @param mixed $column_finish only count columns with name <= this
     * @param mixed $super_column count only columns in this super column
     * @param \cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return mixed array(row_key => row_count)
     */
    public function multiget_count($keys,
                                   $columns=null,
                                   $column_start='',
                                   $column_finish='',
                                   $super_column=null,
                                   $read_consistency_level=null) {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                                   false, self::MAX_COUNT);

        return $this->pool->call("multiget_count", $keys, $column_parent, $predicate,
            $this->rcl($read_consistency_level));
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
     * @param mixed $super_column return only columns in this super column
     * @param \cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     * @param int $buffer_size When calling `get_range`, the intermediate results need
     *        to be buffered if we are fetching many rows, otherwise the Cassandra
     *        server will overallocate memory and fail.  This is the size of
     *        that buffer in number of rows.
     *
     * @return phpcassa_Iterator_RangeColumnFamilyIterator
     */
    public function get_range($key_start="",
                              $key_finish="",
                              $row_count=self::DEFAULT_ROW_COUNT,
                              $columns=null,
                              $column_start="",
                              $column_finish="",
                              $column_reversed=false,
                              $column_count=self::DEFAULT_COLUMN_COUNT,
                              $super_column=null,
                              $read_consistency_level=null,
                              $buffer_size=null) {

        if ($buffer_size == null)
            $buffer_size = $this->buffer_size;
        if ($buffer_size < 2) {
            $ire = new \cassandra_InvalidRequestException();
            $ire->message = 'buffer_size cannot be less than 2';
            throw $ire;
        }

        $column_parent = $this->create_column_parent($super_column);
        $predicate = $this->create_slice_predicate($columns, $column_start,
                                                   $column_finish, $column_reversed,
                                                   $column_count);

        return new RangeColumnFamilyIterator($this, $buffer_size,
                                             $key_start, $key_finish, $row_count,
                                             $column_parent, $predicate,
                                             $this->rcl($read_consistency_level));
    }

   /**
    * Fetch a set of rows from this column family based on an index clause.
    *
    * @param \cassandra_IndexClause $index_clause limits the keys that are returned based
    *        on expressions that compare the value of a column to a given value.  At least
    *        one of the expressions in the IndexClause must be on an indexed column. You
    *        can use the CassandraUtil::create_index_expression() and
    *        CassandraUtil::create_index_clause() methods to help build this.
    * @param mixed[] $columns limit the columns or super columns fetched to this list
    * @param mixed $column_start only fetch columns with name >= this
    * @param mixed $column_finish only fetch columns with name <= this
    * @param bool $column_reversed fetch the columns in reverse order
    * @param int $column_count limit the number of columns returned to this amount
    * @param mixed $super_column return only columns in this super column
    * @param \cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
    * number of nodes that must respond before the operation returns
    *
    * @return mixed array(row_key => array(column_name => column_value))
    */
    public function get_indexed_slices($index_clause,
                                       $columns=null,
                                       $column_start='',
                                       $column_finish='',
                                       $column_reversed=false,
                                       $column_count=self::DEFAULT_COLUMN_COUNT,
                                       $super_column=null,
                                       $read_consistency_level=null,
                                       $buffer_size=null) {

        if ($buffer_size == null)
            $buffer_size = $this->buffer_size;
        if ($buffer_size < 2) {
            $ire = new \cassandra_InvalidRequestException();
            $ire->message = 'buffer_size cannot be less than 2';
            throw $ire;
        }

        $new_clause = new \cassandra_IndexClause();
        foreach($index_clause->expressions as $expr) {
            $new_expr = new \cassandra_IndexExpression();
            $new_expr->value = $this->pack_value($expr->value, $expr->column_name);
            $new_expr->column_name = $this->pack_name($expr->column_name);
            $new_expr->op = $expr->op;
            $new_clause->expressions[] = $new_expr;
        }
        $new_clause->start_key = $index_clause->start_key;
        $new_clause->count = $index_clause->count;

        $column_parent = $this->create_column_parent($super_column);
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
     * @param \cassandra_ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int the timestamp for the operation
     */
    public function insert($key,
                           $columns,
                           $timestamp=null,
                           $ttl=null,
                           $write_consistency_level=null) {

        if ($timestamp == null)
            $timestamp = CassandraUtil::get_time();

        $cfmap = array();
        $cfmap[$key][$this->column_family] = $this->array_to_mutation($columns, $timestamp, $ttl);

        return $this->pool->call("batch_mutate", $cfmap, $this->wcl($write_consistency_level));
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
     * @param \cassandra_ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int the timestamp for the operation
     */
    public function batch_insert($rows, $timestamp=null, $ttl=null, $write_consistency_level=null) {
        if ($timestamp == null)
            $timestamp = CassandraUtil::get_time();


        $cfmap = array();
        foreach($rows as $key => $columns)
            $cfmap[$key][$this->column_family] = $this->array_to_mutation($columns, $timestamp, $ttl);

        return $this->pool->call("batch_mutate", $cfmap, $this->wcl($write_consistency_level));
    }

    /**
     * Remove columns from a row.
     *
     * @param string $key the row to remove columns from
     * @param mixed[] $columns the columns to remove. If null, the entire row will be removed.
     * @param mixed $super_column only remove this super column
     * @param \cassandra_ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int the timestamp for the operation
     */
    public function remove($key, $columns=null, $super_column=null, $write_consistency_level=null) {
        $timestamp = CassandraUtil::get_time();

        if ($columns == null || count($columns) == 1)
        {
            $cp = new \cassandra_ColumnPath();
            $cp->column_family = $this->column_family;
            $cp->super_column = $this->pack_name($super_column, true);
            if ($columns != null) {
                if ($this->is_super && $super_column == null)
                    $cp->super_column = $this->pack_name($columns[0], true);
                else
                    $cp->column = $this->pack_name($columns[0], false);
            }
            return $this->pool->call("remove", $key, $cp, $timestamp,
                $this->wcl($write_consistency_level));
        }

        $deletion = new \cassandra_Deletion();
        $deletion->timestamp = $timestamp;
        $deletion->super_column = $this->pack_name($super_column, true);

        if ($columns != null) {
            $predicate = $this->create_slice_predicate($columns, '', '', false,
                                                       self::DEFAULT_COLUMN_COUNT);
            $deletion->predicate = $predicate;
        }

        $mutation = new \cassandra_Mutation();
        $mutation->deletion = $deletion;

        $mut_map = array($key => array($this->column_family => array($mutation))); 

        return $this->pool->call("batch_mutate", $mut_map, $this->wcl($write_consistency_level));
    }

    /*
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

    private static $TYPES = array('BytesType', 'LongType', 'IntegerType',
                                  'UTF8Type', 'AsciiType', 'LexicalUUIDType',
                                  'TimeUUIDType');

    private static function extract_type_name($type_string) {
        if ($type_string == null or $type_string == '')
            return 'BytesType';

        $index = strrpos($type_string, '.');
        if ($index == false)
            return 'BytesType';
        
        $type = substr($type_string, $index + 1);
        if (!in_array($type, self::$TYPES))
            return 'BytesType';

        return $type;
    }

    private function rcl($read_consistency_level) {
        if ($read_consistency_level == null)
            return $this->read_consistency_level;
        else
            return $read_consistency_level;
    }

    private function wcl($write_consistency_level) {
        if ($write_consistency_level == null)
            return $this->write_consistency_level;
        else
            return $write_consistency_level;
    }

    private function create_slice_predicate($columns, $column_start, $column_finish,
                                                   $column_reversed, $column_count) {

        $predicate = new \cassandra_SlicePredicate();
        if ($columns !== null) {
            $packed_cols = array();
            foreach($columns as $col)
                $packed_cols[] = $this->pack_name($col, $this->is_super);
            $predicate->column_names = $packed_cols;
        } else {
            if ($column_start != null and $column_start != '')
                $column_start = $this->pack_name($column_start,
                                                 $this->is_super,
                                                 self::SLICE_START);
            if ($column_finish != null and $column_finish != '')
                $column_finish = $this->pack_name($column_finish,
                                                  $this->is_super,
                                                  self::SLICE_FINISH);

            $slice_range = new \cassandra_SliceRange();
            $slice_range->count = $column_count;
            $slice_range->reversed = $column_reversed;
            $slice_range->start  = $column_start;
            $slice_range->finish = $column_finish;
            $predicate->slice_range = $slice_range;
        }
        return $predicate;
    }

    private function create_column_parent($super_column=null) {
        $column_parent = new \cassandra_ColumnParent();
        $column_parent->column_family = $this->column_family;
        $column_parent->super_column = $this->pack_name($super_column, true);
        return $column_parent;
    }

    const NON_SLICE = 0;
    const SLICE_START = 1;
    const SLICE_FINISH = 2;

    private function pack_name($value, $is_supercol_name=false, $slice_end=self::NON_SLICE) {
        if (!$this->autopack_names)
            return $value;
        if ($value == null)
            return;
        if ($is_supercol_name)
            $d_type = $this->supercol_name_type;
        else
            $d_type = $this->col_name_type;

        if ($d_type == 'TimeUUIDType') {
            if ($slice_end) {

            } else {

            }
        }

        return $this->pack($value, $d_type);
    }

    private function unpack_name($b, $is_supercol_name=false) {
        if (!$this->autopack_names)
            return $b;
        if ($b == null)
            return;

        if ($is_supercol_name)
            $d_type = $this->supercol_name_type;
        else
            $d_type = $this->col_name_type;

        return $this->unpack($b, $d_type);
    }

    private function get_data_type_for_col($col_name) {
        if (!in_array($col_name, array_keys($this->col_type_dict)))
            return $this->cf_data_type;
        else
            return $this->col_type_dict[$col_name];
    }

    private function pack_value($value, $col_name) {
        if (!$this->autopack_values)
            return $value;
        return $this->pack($value, $this->get_data_type_for_col($col_name));
    }

    private function unpack_value($value, $col_name) {
        if (!$this->autopack_values)
            return $value;
        return $this->unpack($value, $this->get_data_type_for_col($col_name));
    }

    private static function unpack_str($str, $len) {
        $tmp_arr = unpack("c".$len."chars", $str);
        $out_str = "";
        foreach($tmp_arr as $v)
            if($v > 0) { $out_str .= chr($v); }
        return $out_str;
    }
   
    private static function pack_str($str, $len) {       
        $out_str = "";
        for($i=0; $i<$len; $i++)
            $out_str .= pack("c", ord(substr($str, $i, 1)));
        return $out_str;
    }

    private static function pack_long($value) {
        // If we are on a 32bit architecture we have to explicitly deal with
        // 64-bit twos-complement arithmetic since PHP wants to treat all ints
        // as signed and any int over 2^31 - 1 as a float
        if (\PHP_INT_SIZE == 4) {
            $neg = $value < 0;

            if ($neg) {
              $value *= -1;
            }

            $hi = (int)($value / 4294967296);
            $lo = (int)$value;

            if ($neg) {
                $hi = ~$hi;
                $lo = ~$lo;
                if (($lo & (int)0xffffffff) == (int)0xffffffff) {
                    $lo = 0;
                    $hi++;
                } else {
                    $lo++;
                }
            }
            $data = pack('N2', $hi, $lo);
        } else {
            $hi = $value >> 32;
            $lo = $value & 0xFFFFFFFF;
            $data = pack('N2', $hi, $lo);
        }
        return $data;
    }

    private static function unpack_long($data) {
        $arr = unpack('N2', $data);

        // If we are on a 32bit architecture we have to explicitly deal with
        // 64-bit twos-complement arithmetic since PHP wants to treat all ints
        // as signed and any int over 2^31 - 1 as a float
        if (PHP_INT_SIZE == 4) {

            $hi = $arr[1];
            $lo = $arr[2];
            $isNeg = $hi  < 0;

            // Check for a negative
            if ($isNeg) {
                $hi = ~$hi & (int)0xffffffff;
                $lo = ~$lo & (int)0xffffffff;

                if ($lo == (int)0xffffffff) {
                    $hi++;
                    $lo = 0;
                } else {
                    $lo++;
                }
            }

            // Force 32bit words in excess of 2G to pe positive - we deal wigh sign
            // explicitly below

            if ($hi & (int)0x80000000) {
                $hi &= (int)0x7fffffff;
                $hi += 0x80000000;
            }

            if ($lo & (int)0x80000000) {
                $lo &= (int)0x7fffffff;
                $lo += 0x80000000;
            }

            $value = $hi * 4294967296 + $lo;

            if ($isNeg)
                $value = 0 - $value;

        } else {
            // Upcast negatives in LSB bit
            if ($arr[2] & 0x80000000)
                $arr[2] = $arr[2] & 0xffffffff;

            // Check for a negative
            if ($arr[1] & 0x80000000) {
                $arr[1] = $arr[1] & 0xffffffff;
                $arr[1] = $arr[1] ^ 0xffffffff;
                $arr[2] = $arr[2] ^ 0xffffffff;
                $value = 0 - $arr[1]*4294967296 - $arr[2] - 1;
            } else {
                $value = $arr[1]*4294967296 + $arr[2];
            }
        }
        return $value;
    }

    private function pack($value, $data_type) {
        if ($data_type == 'LongType')
            return self::pack_long($value);
        else if ($data_type == 'IntegerType')
            return pack('N', $value); // Unsigned 32bit big-endian
        else if ($data_type == 'AsciiType')
            return self::pack_str($value, strlen($value));
        else if ($data_type == 'UTF8Type') {
            if (mb_detect_encoding($value, "UTF-8") != "UTF-8")
                $value = utf8_encode($value);
            return self::pack_str($value, strlen($value));
        }
        else if ($data_type == 'TimeUUIDType' or $data_type == 'LexicalUUIDType')
            return self::pack_str($value, 16);
        else
            return $value;
    }
            
    private function unpack($value, $data_type) {
        if ($data_type == 'LongType')
            return self::unpack_long($value);
        else if ($data_type == 'IntegerType') {
            $res = unpack('N', $value);
            return $res[1];
        }
        else if ($data_type == 'AsciiType')
            return self::unpack_str($value, strlen($value));
        else if ($data_type == 'UTF8Type')
            return utf8_decode(self::unpack_str($value, strlen($value)));
        else if ($data_type == 'TimeUUIDType' or $data_type == 'LexicalUUIDType')
            return $value;
        else
            return $value;
    }

    public function keyslices_to_array($keyslices) {
        $ret = null;
        foreach($keyslices as $keyslice) {
            $key = $keyslice->key;
            $columns = $keyslice->columns;
            $ret[$key] = $this->supercolumns_or_columns_to_array($columns);
        }
        return $ret;
    }

    private function supercolumns_or_columns_to_array($array_of_c_or_sc) {
        $ret = null;
        foreach($array_of_c_or_sc as $c_or_sc) {
            if($c_or_sc->column) { // normal columns
                $name = $this->unpack_name($c_or_sc->column->name, false);
                $value = $this->unpack_value($c_or_sc->column->value, $c_or_sc->column->name);
                $ret[$name] = $value;
            } else if($c_or_sc->super_column) { // super columns
                $name = $this->unpack_name($c_or_sc->super_column->name, true);
                $columns = $c_or_sc->super_column->columns;
                $ret[$name] = $this->columns_to_array($columns);
            }
        }
        return $ret;
    }

    private function columns_to_array($array_of_c) {
        $ret = null;
        foreach($array_of_c as $c) {
            $name  = $this->unpack_name($c->name, false);
            $value = $this->unpack_value($c->value, $c->name);
            $ret[$name] = $value;
        }
        return $ret;
    }

    private function array_to_mutation($array, $timestamp=null, $ttl=null) {
        if(empty($timestamp)) $timestamp = CassandraUtil::get_time();


        $c_or_sc = $this->array_to_supercolumns_or_columns($array, $timestamp, $ttl);
        $ret = null;
        foreach($c_or_sc as $row) {
            $mutation = new \cassandra_Mutation();
            $mutation->column_or_supercolumn = $row;
            $ret[] = $mutation;
        }
        return $ret;
    }
    
    private function array_to_supercolumns_or_columns($array, $timestamp=null, $ttl=null) {
        if(empty($timestamp)) $timestamp = CassandraUtil::get_time();


        $ret = null;
        foreach($array as $name => $value) {
            $c_or_sc = new \cassandra_ColumnOrSuperColumn();
            if(is_array($value)) {
                $c_or_sc->super_column = new \cassandra_SuperColumn();
                $c_or_sc->super_column->name = $this->pack_name($name, true);
                $c_or_sc->super_column->columns = $this->array_to_columns($value, $timestamp, $ttl);
                $c_or_sc->super_column->timestamp = $timestamp;
            } else {
                $c_or_sc = new \cassandra_ColumnOrSuperColumn();
                $c_or_sc->column = new \cassandra_Column();
                $c_or_sc->column->name = $this->pack_name($name, false);
                $c_or_sc->column->value = $this->pack_value($value, $name);
                $c_or_sc->column->timestamp = $timestamp;
                $c_or_sc->column->ttl = $ttl;
            }
            $ret[] = $c_or_sc;
        }

        return $ret;
    }

    private function array_to_columns($array, $timestamp=null, $ttl=null) {
        if(empty($timestamp)) $timestamp = CassandraUtil::get_time();

        $ret = null;
        foreach($array as $name => $value) {
            $column = new \cassandra_Column();
            $column->name = $this->pack_name($name, false);
            $column->value = $this->pack_value($value, $name);
            $column->timestamp = $timestamp;
            $column->ttl = $ttl;
            $ret[] = $column;
        }
        return $ret;
    }
}
