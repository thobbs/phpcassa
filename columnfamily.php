<?php
require_once('connection.php');
require_once('uuid.php');

class CassandraUtil {

    static public function uuid1($node=null) {
        $uuid = UUID::mint(1, $node);
        return $uuid->bytes;
    }

    static public function uuid3($null=null, $namespace=null) {
        $uuid = UUID::mint(3, $node, $namespace);
        return $uuid->bytes;
    }

    static public function uuid4() {
        $uuid = UUID::mint(4);
        return $uuid->bytes;
    }

    static public function uuid5($node, $namespace=null) {
        $uuid = UUID::mint(5, $node, $namespace);
        return $uuid->bytes;
    }

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

class ColumnFamily {

    const DEFAULT_ROW_COUNT = 100; // default max # of rows for get_range()
    const DEFAULT_COLUMN_COUNT = 100; // default max # of columns for get()
    const DEFAULT_COLUMN_TYPE = "BytesType";
    const DEFAULT_SUBCOLUMN_TYPE = null;
    const MAX_COUNT = 2147483647; # 2^31 - 1

    private $client;
    public $column_family;
    private $is_super;
    private $cf_data_type;
    private $col_name_type;
    private $supercol_name_type;
    private $col_type_dict;

    public $autopack_names;
    public $autopack_values;
    public $read_consistency_level;
    public $write_consistency_level;

    /*
    BytesType: Simple sort by byte value. No validation is performed.
    AsciiType: Like BytesType, but validates that the input can be parsed as US-ASCII.
    UTF8Type: A string encoded as UTF8
    LongType: A 64bit long
    LexicalUUIDType: A 128bit UUID, compared lexically (by byte value)
    TimeUUIDType: a 128bit version 1 UUID, compared by timestamp
    */

    public function __construct($connection,
                                $column_family,
                                $autopack_names=true,
                                $autopack_values=true,
                                $read_consistency_level=cassandra_ConsistencyLevel::ONE,
                                $write_consistency_level=cassandra_ConsistencyLevel::ONE) {

        $this->client = $connection->connect();
        $this->column_family = $column_family;
        $this->autopack_names = $autopack_names;
        $this->autopack_values = $autopack_values;
        $this->read_consistency_level = $read_consistency_level;
        $this->write_consistency_level = $write_consistency_level;

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
            foreach($cfdef->column_metadata as $coldef)
                $this->col_type_dict[$coldef->name] = self::extract_type_name($coldef->validation_class);
        }
    }

    private static $TYPES = array('BytesType', 'LongType', 'IntegerType', 'UTF8Type', 'AsciiType',
         'LexicalUUIDType', 'TimeUUIDType');

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

            if ($isNeg) {
                $value = 0 - $value;
            }
        } else {
            // Upcast negatives in LSB bit
            if ($arr[2] & 0x80000000) {
                $arr[2] = $arr[2] & 0xffffffff;
            }

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
        else if ($data_type == 'IntegerType') {
            return pack('N', $value); // Unsigned 32bit big-endian
        }
        else if ($data_type == 'AsciiType')
            return self::pack_str($value, strlen($value));
        else if ($data_type == 'UTF8Type') {
            if (mb_detect_encoding($value, "UTF-8") != "UTF-8")
                $value = utf8_encode($value);
            return self::pack_str($value, strlen($value));
        }
        else if ($data_type == 'TimeUUIDType' or $data_type == 'LexicalUUIDType') {
            return self::pack_str($value, 16);
        }
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
        else if ($data_type == 'TimeUUIDType' or $data_type == 'LexicalUUIDType') {
            return $value;
        }
        else
            return $value;
    }

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

        $resp = $this->client->get_slice($key, $column_parent, $predicate, $this->rcl($read_consistency_level));
        if (count($resp) == 0)
            throw new cassandra_NotFoundException();

        return $this->supercolumns_or_columns_to_array($resp);
    }

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

    public function get_count($key,
                              $columns=null,
                              $column_start='',
                              $column_finish='',
                              $super_column=null,
                              $read_consistency_level=null) {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                                   false, self::MAX_COUNT);

        return $this->client->get_count($key, $column_parent, $predicate, $this->rcl($read_consistency_level));
    }

    public function multiget_count($keys,
                                   $columns=null,
                                   $column_start='',
                                   $column_finish='',
                                   $super_column=null,
                                   $read_consistency_level=null) {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = $this->create_slice_predicate($columns, $column_start, $column_finish,
                                                   false, self::MAX_COUNT);

        return $this->client->multiget_count($keys, $column_parent, $predicate, $this->rcl($read_consistency_level));
    }

    public function get_range($start_key="",
                              $end_key="",
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
        $key_range->start_key = $start_key;
        $key_range->end_key   = $end_key;
        $key_range->count     = $row_count;

        $resp = $this->client->get_range_slices($column_parent, $predicate, $key_range,
                                                $this->rcl($read_consistency_level));

        return $this->keyslices_to_array($resp);
    }

    public function get_range_iterator($start_key="", $end_key="", $row_count=self::DEFAULT_ROW_LIMIT, $slice_start="", $slice_finish="") {
        return new CassandraIterator($this, $start_key, $end_key, $row_count, $slice_start, $slice_end);
    }

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

    public function remove($key, $columns=null, $super_column=null, $write_consistency_level=null) {

        $deletion = new cassandra_Deletion();
        $deletion->timestamp = CassandraUtil::get_time();
        $deletion->super_column = $this->pack_name($super_column, true);

        if ($columns != null) {
            $predicate = $this->create_slice_predicate($columns, '', '', false, self::DEFAULT_COLUMN_COUNT);
            $deletion->predicate = $predicate;
        }

        $mutation = new cassandra_Mutation();
        $mutation->deletion = $deletion;

        $mut_map = array($key => array($this->column_family => array($mutation))); 

        return $this->client->batch_mutate($mut_map, $this->wcl($write_consistency_level));
    }

    public function truncate() {
        return $this->client->truncate($this->column_family);
    }

    // Wrappers
    public function get_list($key, $key_name='key', $slice_start="", $slice_finish="") {
        // Must be on supercols!
        $resp = $this->get($key, NULL, $slice_start, $slice_finish);
        $ret = array();
        foreach($resp as $_key => $_value) {
            $_value[$key_name] = $_key;
            $ret[] = $_value;
        }
        return $ret;
    }

    public function get_range_list($key_name='key', $start_key="", $end_key="",
                                   $row_count=self::DEFAULT_ROW_LIMIT, $slice_start="", $slice_finish="") {
        $resp = $this->get_range($start_key, $end_key, $row_count, $slice_start, $slice_finish);
        $ret = array();
        foreach($resp as $_key => $_value) {
            if(!empty($_value)) { // filter nulls
                $_value[$key_name] = $_key;
                $ret[] = $_value;
            }
        }
        return $ret;
    }

    public function multiget_list($keys, $key_name='key', $slice_start="", $slice_finish="") {
        $resp = $this->multiget($keys, $slice_start, $slice_finish);
        $ret = array();
        foreach($resp as $_key => $_value) {
            $_value[$key_name] = $_key;
            $ret[] = $_value;
        }
        return $ret;
    }

    // Helpers for parsing Cassandra's thrift objects into PHP arrays
    public function keyslices_to_array($keyslices) {
        $ret = null;
        foreach($keyslices as $keyslice) {
            $key     = $keyslice->key;
            $columns = $keyslice->columns;

            $ret[$key] = $this->supercolumns_or_columns_to_array($columns);
        }
        return $ret;
    }

    public function supercolumns_or_columns_to_array($array_of_c_or_sc) {
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

    public function columns_to_array($array_of_c) {
        $ret = null;
        foreach($array_of_c as $c) {
            $name  = $this->unpack_name($c->name, false);
            $value = $this->unpack_value($c->value, $c->name);
            $ret[$name] = $value;
        }
        return $ret;
    }

    // Helpers for turning PHP arrays into Cassandra's thrift objects
    public function array_to_mutation($array, $timestamp=null) {
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
    
    public function array_to_supercolumns_or_columns($array, $timestamp=null) {
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

    public function array_to_columns($array, $timestamp=null) {
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

class CassandraIterator implements Iterator {
    const DEFAULT_BUFFER_SIZE = 1024; // default max # of rows for get_range()

    // Options
    public $column_family;
    public $buffer_size;
    public $start_key, $end_key;
    public $start_slice, $end_slice;

    // State
    public $current_buffer;
    public $next_start_key;
    public $beyond_last_field;

    public function __construct($column_family,
                                $start_key="",
                                $end_key="",
                                $buffer_size=self::DEFAULT_BUFFER_SIZE,
                                $start_slice="",
                                $end_slice="") {
        // Lets go
        $this->column_family = $column_family;
        $this->start_key     = $start_key;
        $this->end_key       = $end_key;
        $this->buffer_size   = $buffer_size;
        $this->start_slice   = $start_slice;
        $this->end_slice     = $end_slice;
    }

    // Interface
    public function rewind() {
        // Setup first buffer
        $this->beyond_last_field = false;
        $this->next_start_key = $this->start_key;
        $this->current_buffer = $this->column_family->get_range(
            $this->next_start_key,
            $this->end_key,
            $this->buffer_size,
            $this->start_slice,
            $this->end_slice
        );
    }

    public function current() {
        return current($this->current_buffer);
    }

    public function key() {
        return key($this->current_buffer);
    }

    public function next() {
        // See http://www.php.net/manual/en/function.current.php#81431
        // for figuring if we are at the end
        $next = next($this->current_buffer);
        $key  = key($this->current_buffer);
        if(!isset($key)) {
            $this->beyond_last_field = true;
            return false;
        } else {
            return $next;
        }
    }

    public function valid() {
        if($this->beyond_last_field && count($this->current_buffer) < $this->buffer_size) {
            // Stop if we were at the last buffer (we got less that $buffer_size elements returned)
            return false;
        } else if($this->beyond_last_field) {
            // Set the next start key
            end($this->current_buffer);
            $this->next_start_key = key($this->current_buffer);

            // Get the next buffer
            $this->current_buffer = $this->column_family->get_range(
                $this->next_start_key,
                $this->end_key,
                $this->buffer_size,
                $this->start_slice,
                $this->end_slice
            );

            // If the result set is 1, we can stop
            // because the first item should always
            // be skipped
            if(count($this->current_buffer) == 1) {
                return false;
            } else {
                // Skip 1st item (because it is the last buffer's last key)
                next($this->current_buffer);

                // Let us iterate again
                $this->beyond_last_field = false;
                return true;
            }
        } else {
            // Normal iteration
            return true;
        }
    }
}

?>
