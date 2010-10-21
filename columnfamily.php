<?php
require_once('connection.php');
require_once('uuid.php');

/**
 * @package phpcassa
 * @subpackage columnfamily
 */
class CassandraUtil {

    /**
     * Generate a v1 UUID (timestamp based)
     * @return string a byte[] representation of a UUID 
     */
    static public function uuid1($node=null) {
        $uuid = UUID::mint(1, $node);
        return $uuid->bytes;
    }

    /**
     * Generate a v3 UUID
     * @return string a byte[] representation of a UUID 
     */
    static public function uuid3($null=null, $namespace=null) {
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
        $time1 = microtime();
        settype($time1, 'string'); //convert to string to keep trailing zeroes
        $time2 = explode(" ", $time1);
        $sub_secs = preg_replace('/0./', '', $time2[0], 1);
        $time3 = ($time2[1].$sub_secs)/100;
        return $time3;
    }
}

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

    private $client;
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
    /** @var cassandra_ConsistencyLevel the default read consistency level */
    public $read_consistency_level;
    /** @var cassandra_ConsistencyLevel the default write consistency level */
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
     * @param cassandra_ConsistencyLevel $read_consistency_level the default consistency
     *        level for read operations on this column family
     * @param cassandra_ConsistencyLevel $write_consistency_level the default consistency
     *        level for write operations on this column family
     * @param int $buffer_size When calling `get_range`, the intermediate results need
     *        to be buffered if we are fetching many rows, otherwise the Cassandra
     *        server will overallocate memory and fail.  This is the size of
     *        that buffer in number of rows.
     */
    public function __construct($connection,
                                $column_family,
                                $autopack_names=true,
                                $autopack_values=true,
                                $read_consistency_level=cassandra_ConsistencyLevel::ONE,
                                $write_consistency_level=cassandra_ConsistencyLevel::ONE,
                                $buffer_size=self::DEFAULT_BUFFER_SIZE) {

        $this->client = $connection->connect();
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

        $ks = $this->client->describe_keyspace($connection->keyspace);
        $cf_def = null;
        foreach($ks->cf_defs as $cfdef) {
            if ($cfdef->name == $this->column_family) {
                $cf_def = $cfdef;
                break;
            }
        }
        if ($cf_def == null)
            throw new cassandra_NotFoundException();

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
     * @param cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
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
        $predicate = self::create_slice_predicate($columns, $column_start, $column_finish,
                                                  $column_reversed, $column_count);

        $resp = $this->client->get_slice($key, $column_parent, $predicate,
                                         $this->rcl($read_consistency_level));
        if (count($resp) == 0)
            throw new cassandra_NotFoundException();

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
     * @param cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
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
                             $read_consistency_level=null)  {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = self::create_slice_predicate($columns, $column_start, $column_finish,
                                                  $column_reversed, $column_count);

        $resp = $this->client->multiget_slice($keys, $column_parent, $predicate,
                                              $this->rcl($read_consistency_level));

        $ret = array();
        foreach($keys as $key) {
            $ret[$key] = null;
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
     * @param cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
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

        return $this->client->get_count($key, $column_parent, $predicate,
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
     * @param cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
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

        return $this->client->multiget_count($keys, $column_parent, $predicate,
                                             $this->rcl($read_consistency_level));
    }


   /**
    * Fetch a range of rows from this column family.
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
    * @param cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
    * number of nodes that must respond before the operation returns
    *
    * @return mixed array(row_key => array(column_name => column_value))
    */
    public function get_small_range_as_array($key_start="",
                                             $key_finish="",
                                             $row_count=self::DEFAULT_ROW_COUNT,
                                             $columns=null,
                                             $column_start="",
                                             $column_finish="",
                                             $column_reversed=false,
                                             $column_count=self::DEFAULT_COLUMN_COUNT,
                                             $super_column=null,
                                             $read_consistency_level=null) {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = self::create_slice_predicate($columns, $column_start,
                                                  $column_finish, $column_reversed,
                                                  $column_count);

        $key_range = new cassandra_KeyRange();
        $key_range->start_key = $key_start;
        $key_range->end_key = $key_finish;
        $key_range->count = $row_count;

        $resp = $this->client->get_range_slices($column_parent, $predicate, $key_range,
                                                $this->rcl($read_consistency_level));

        return $this->keyslices_to_array($resp);
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
     * @param cassandra_ConsistencyLevel $read_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     * @param int $buffer_size When calling `get_range`, the intermediate results need
     *        to be buffered if we are fetching many rows, otherwise the Cassandra
     *        server will overallocate memory and fail.  This is the size of
     *        that buffer in number of rows.
     *
     * @return ColumnFamilyIterator
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
            $ire = new cassandra_InvalidRequestException();
            $ire->setMessage('buffer_size cannot be less than 2');
            throw $ire;
        }

        return new ColumnFamilyIterator($this, $buffer_size,
                                        $key_start, $key_finish, $row_count,
                                        $columns, $column_start, $column_finish,
                                        $column_reversed, $column_count,
                                        $super_column,
                                        $read_consistency_level);
    }

    /**
     * Insert or update columns in a row.
     *
     * @param string $key the row to insert or update the columns in
     * @param mixed $columns array(column_name => column_value) the columns to insert or update
     * @param int $timestamp the timestamp to use for this insertion. Leaving this as null will
     *        result in a timestamp being generated for you
     * @param cassandra_ConsistencyLevel $write_consistency_level affects the guaranteed
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
        $cfmap[$key][$this->column_family] = $this->array_to_mutation($columns, $timestamp);

        return $this->client->batch_mutate($cfmap, $this->wcl($write_consistency_level));
    }

    /**
     * Remove columns from a row.
     *
     * @param string $key the row to remove columns from
     * @param mixed[] $columns the columns to remove. If null, the entire row will be removed.
     * @param mixed $super_column only remove this super column
     * @param cassandra_ConsistencyLevel $write_consistency_level affects the guaranteed
     *        number of nodes that must respond before the operation returns
     *
     * @return int the timestamp for the operation
     */
    public function remove($key, $columns=null, $super_column=null, $write_consistency_level=null) {

        $deletion = new cassandra_Deletion();
        $deletion->timestamp = CassandraUtil::get_time();
        $deletion->super_column = $this->pack_name($super_column, true);

        if ($columns != null) {
            $predicate = $this->create_slice_predicate($columns, '', '', false,
                                                       self::DEFAULT_COLUMN_COUNT);
            $deletion->predicate = $predicate;
        }

        $mutation = new cassandra_Mutation();
        $mutation->deletion = $deletion;

        $mut_map = array($key => array($this->column_family => array($mutation))); 

        return $this->client->batch_mutate($mut_map, $this->wcl($write_consistency_level));
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
        return $this->client->truncate($this->column_family);
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

        $predicate = new cassandra_SlicePredicate();
        if ($columns !== null) {
            $packed_cols = array();
            foreach($columns as $col)
                $packed_cols[] = $this->pack_name($col, $is_supercol_name=$this->is_super);
            $predicate->column_names = $packed_cols;
        } else {
            if ($column_start != null and $column_start != '')
                $column_start = $this->pack_name($column_start,
                                                 $is_supercol_name=$this->is_super,
                                                 $slice_end=self::SLICE_START);
            if ($column_finish != null and $column_finish != '')
                $column_finish = $this->pack_name($column_finish,
                                                 $is_supercol_name=$this->is_super,
                                                  $slice_end=self::SLICE_FINISH);

            $slice_range = new cassandra_SliceRange();
            $slice_range->count = $column_count;
            $slice_range->reversed = $column_reversed;
            $slice_range->start  = $column_start;
            $slice_range->finish = $column_finish;
            $predicate->slice_range = $slice_range;
        }
        return $predicate;
    }

    private function create_column_parent($super_column=null) {
        $column_parent = new cassandra_ColumnParent();
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
        if (PHP_INT_SIZE == 4) {
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

    private function keyslices_to_array($keyslices) {
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

    private function array_to_mutation($array, $timestamp=null) {
        if(empty($timestamp)) $timestamp = CassandraUtil::get_time();

        $c_or_sc = $this->array_to_supercolumns_or_columns($array, $timestamp);
        $ret = null;
        foreach($c_or_sc as $row) {
            $mutation = new cassandra_Mutation();
            $mutation->column_or_supercolumn = $row;
            $ret[] = $mutation;
        }
        return $ret;
    }
    
    private function array_to_supercolumns_or_columns($array, $timestamp=null) {
        if(empty($timestamp)) $timestamp = CassandraUtil::get_time();

        $ret = null;
        foreach($array as $name => $value) {
            $c_or_sc = new cassandra_ColumnOrSuperColumn();
            if(is_array($value)) {
                $c_or_sc->super_column = new cassandra_SuperColumn();
                $c_or_sc->super_column->name = $this->pack_name($name, true);
                $c_or_sc->super_column->columns = $this->array_to_columns($value, $timestamp);
                $c_or_sc->super_column->timestamp = $timestamp;
            } else {
                $c_or_sc = new cassandra_ColumnOrSuperColumn();
                $c_or_sc->column = new cassandra_Column();
                $c_or_sc->column->name = $this->pack_name($name, false);
                $c_or_sc->column->value = $this->pack_value($value, $name);
                $c_or_sc->column->timestamp = $timestamp;
            }
            $ret[] = $c_or_sc;
        }

        return $ret;
    }

    private function array_to_columns($array, $timestamp=null) {
        if(empty($timestamp)) $timestamp = CassandraUtil::get_time();

        $ret = null;
        foreach($array as $name => $value) {
            $column = new cassandra_Column();
            $column->name = $this->pack_name($name, false);
            $column->value = $this->pack_value($value, $name);
            $column->timestamp = $timestamp;

            $ret[] = $column;
        }
        return $ret;
    }
}

/**
 * Iterates over a column family row-by-row, typically with only a subset
 * of each row's columns.
 *
 * @package phpcassa
 * @subpackage columnfamily
 */
class ColumnFamilyIterator implements Iterator {

    private $column_family;
    private $buffer_size;
    private $key_start, $key_finish, $row_count;
    private $columns, $column_start, $column_finish, $columns_reversed, $column_count;
    private $super_column;
    private $read_consistency_level;

    private $current_buffer;
    private $next_start_key;
    private $is_valid;
    private $rows_seen, $rows_seen_this_page;

    public function __construct($column_family,
                                $buffer_size,
                                $key_start="",
                                $key_finish="",
                                $row_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                $columns=null,
                                $column_start="",
                                $column_finish="",
                                $column_reversed=false,
                                $column_count=ColumnFamily::DEFAULT_COLUMN_COUNT,
                                $super_column=null,
                                $read_consistency_level=cassandra_ConsistencyLevel::ONE) {

        $this->column_family = $column_family;
        $this->key_start = $key_start;
        $this->key_finish = $key_finish;
        $this->row_count = $row_count;
        $this->columns = $columns;
        $this->column_start = $column_start;
        $this->column_finish = $column_finish;
        $this->column_reversed = $column_reversed;
        $this->column_count = $column_count;
        $this->super_column = $super_column;
        $this->read_consistency_level = $read_consistency_level;

        $this->buffer_size = $buffer_size;
        if ($row_count != null)
            $this->buffer_size = min($row_count, $buffer_size);
    }

    private function get_buffer() {
        $this->current_buffer = $this->column_family->get_small_range_as_array(
            $this->next_start_key,
            $this->key_finish,
            $this->buffer_size,
            $this->columns,
            $this->column_start,
            $this->column_finish,
            $this->column_reversed,
            $this->column_count,
            $this->super_column,
            $this->read_consistency_level);
        $this->rows_seen_this_page = 0;
    }

    public function rewind() {
        // Setup first buffer
        $this->rows_seen = 0;
        $this->is_valid = true;
        $this->next_start_key = $this->key_start;
        $this->get_buffer();

        # If nothing was inserted, this may happen
        if (count($this->current_buffer) == 0) {
            $this->is_valid = false;
            return;
        }

        if (count(current($this->current_buffer)) == 0)
            $this->next();
    }

    public function current() {
        return current($this->current_buffer);
    }

    public function key() {
        return key($this->current_buffer);
    }

    public function next() {
        $next = next($this->current_buffer);
        if (count(current($this->current_buffer)) == 0)
            $this->next();

        $key = key($this->current_buffer);

        $this->rows_seen++;
        $this->rows_seen_this_page++;
        if ($this->rows_seen > $this->row_count) {
            $this->is_valid = false;
            return;
        }

        $beyond_last_field = !isset($key);

        if($beyond_last_field && count($this->current_buffer) < $this->buffer_size) {
            // This was the last page in the column family
            $this->is_valid = false;
        } else if($beyond_last_field) {
            // Set the next start key
            end($this->current_buffer);
            $this->next_start_key = key($this->current_buffer);

            // Get the next buffer
            $this->get_buffer();

            // If the result set is 1, we can stop
            // because the first item should always
            // be skipped
            if(count($this->current_buffer) == 1) {
                $this->is_valid = false;
            } else {
                next($this->current_buffer);
                $this->is_valid = true;
            }
        }
    }

    public function valid() {
        return $this->is_valid;
    }
}

?>
