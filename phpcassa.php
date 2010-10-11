<?php
$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__) . '/thrift/';
require_once $GLOBALS['THRIFT_ROOT'].'/packages/cassandra/Cassandra.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php';

class NoServerAvailable extends Exception { }

class Connection {

    private static $default_servers = array(array('host' => 'localhost', 'port' => 9160));

    public function __construct($keyspace,
                                $servers=null,
                                $credentials=null,
                                $framed_transport=True,
                                $send_timeout=null,
                                $recv_timeout=null,
                                $retry_time=10) {

        $this->keyspace = $keyspace;
        if ($servers == null)
            $servers = self::$default_servers;
        $this->servers = new ServerSet($servers, $retry_time);
        $this->credentials = $credentials;
        $this->framed_transport = $framed_transport;
        $this->send_timeout = $send_timeout;
        $this->recv_timeout = $recv_timeout;

        $this->connection = null;
    }

    public function connect() {
        try {
            $server = $this->servers->get();
            if (!$this->connection) {
                $this->connection = new ClientTransport($this->keyspace,
                                                         $server,
                                                         $this->credentials,
                                                         $this->framed_transport,
                                                         $this->send_timeout,
                                                         $this->recv_timeout);
            }
        } catch (TException $e) {
            $this->servers->mark_dead($server);
            return $this->connect();
        }
        return $this->connection->client;
    }

    public function close() {
        if ($this->connection)
            $this->connection->transport->close();
    }
}

class ClientTransport {

    public function __construct($keyspace,
                                $server,
                                $credentials,
                                $framed_transport,
                                $send_timeout,
                                $recv_timeout) {

        $host = $server['host'];
        $port = $server['port'];
        $socket = new TSocket($host, $port);

        if($send_timeout) $socket->setSendTimeout($send_timeout);
        if($recv_timeout) $socket->setRecvTimeout($recv_timeout);

        if($framed_transport) {
            $transport = new TFramedTransport($socket, true, true);
        } else {
            $transport = new TBufferedTransport($socket, 1024, 1024);
        }

        $client = new CassandraClient(new TBinaryProtocolAccelerated($transport));
        $transport->open();

        # TODO check API major version match

        $client->set_keyspace($keyspace);

        if ($credentials) {
            $request = cassandra_AuthenticationRequest($credentials);
            $client->login($request);
        }

        $this->keyspace = $keyspace;
        $this->client = $client;
        $this->transport = $transport;
    }
}

class ServerSet {

    private $dead = array();

    public function __construct($servers, $retry_time=10) {
        $this->servers = $servers;
        $this->retry_time = $retry_time;
    }

    public function get() {
        if (count($this->dead) != 0) {
            $revived = array_pop($this->dead);
            if ($revived['time'] > time())  # Not yet, put it back
                $this->dead[] = $revived;
            else 
                $this->servers[] = $revived;
        }
        if (!count($this->servers))
            throw new NoServerAvailable();
        
        return $this->servers[array_rand($this->servers)];
    }

    public function mark_dead($server) {
        unset($this->servers[$server]);
        array_unshift($this->dead,
                array('time' => time() + $this->retry_time, 'server' => $server));
    }
}

class CassandraUtil {
    // UUID
    static public function uuid1($node="", $ns="") {
        return UUID::generate(UUID::UUID_TIME,UUID::FMT_STRING, $node, $ns);
    }

    // Time
    static public function get_time() {
        // By Zach Buller (zachbuller@gmail.com)
        $time1 = microtime();
        settype($time1, 'string'); //needs converted to string, otherwise will omit trailing zeroes
        $time2 = explode(" ", $time1);
        $sub_secs = preg_replace('/0./', '', $time2[0], 1);
        $time3 = ($time2[1].$sub_secs)/100;
        return $time3;
    }
}

class ColumnFamily {

    const DEFAULT_ROW_COUNT = 100; // default max # of rows for get_range()
    const DEFAULT_COLUMN_COUNT = 100; // default max # of columns for get()
    const DEFAULT_COLUMN_TYPE = "UTF8Type";
    const DEFAULT_SUBCOLUMN_TYPE = null;
    const MAX_COUNT = 2147483647; # 2^31 - 1

    private $client;
    private $column_family;
    public $is_super;
    public $read_consistency_level;
    public $write_consistency_level;
    public $column_type; // CompareWith (TODO: actually use this)
    public $subcolumn_type; // CompareSubcolumnsWith (TODO: actually use this)
    public $parse_columns;

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
                                $is_super=false,
                                $column_type=self::DEFAULT_COLUMN_TYPE,
                                $subcolumn_type=self::DEFAULT_SUBCOLUMN_TYPE,
                                $read_consistency_level=cassandra_ConsistencyLevel::ONE,
                                $write_consistency_level=cassandra_ConsistencyLevel::ZERO) {

        $this->client = $connection->connect();
        $this->column_family = $column_family;
        $this->is_super = $is_super;
        $this->column_type = $column_type;
        $this->subcolumn_type = $subcolumn_type;
        $this->read_consistency_level = $read_consistency_level;
        $this->write_consistency_level = $write_consistency_level;
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

    static private function create_slice_predicate($columns, $column_start, $column_finish,
                                                   $column_reversed, $column_count) {

        $predicate = new cassandra_SlicePredicate();
        if ($columns !== null) {
            $predicate->column_names = $columns;
        } else {
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
        $column_parent->super_column = $this->unparse_column_name($super_column, true);
        return $column_parent;
    }

    public function get($key,
                        $columns=null,
                        $column_start="",
                        $column_finish="",
                        $column_reversed=False,
                        $column_count=100,
                        $super_column=null,
                        $read_consistency_level=null) {

        $column_parent = $this->create_column_parent($super_column);
        $predicate = self::create_slice_predicate($columns, $column_start, $column_finish,
                                                  $column_reversed, $column_count);

        $resp = $this->client->get_slice($key, $column_parent, $predicate, $this->rcl($read_consistency_level));
        if (count($resp) == 0)
            throw new cassandra_NotFoundException();

        if ($super_column)
            return $this->supercolumns_or_columns_to_array($resp, false);
        else
            return $this->supercolumns_or_columns_to_array($resp);
    }

    public function multiget($keys,
                             $columns=null,
                             $column_start="",
                             $column_finish="",
                             $column_reversed=False,
                             $column_count=100,
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
        #$deletion->super_column = $super_column;

        if ($columns != null) {
            $predicate = new cassandra_SlicePredicate();
            $predicate->column_names = $columns;
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

    public function supercolumns_or_columns_to_array($array_of_c_or_sc, $parse_as_columns=true) {
        $ret = null;
        foreach($array_of_c_or_sc as $c_or_sc) {
            if($c_or_sc->column) { // normal columns
                $name  = $this->parse_column_name($c_or_sc->column->name, $parse_as_columns);
                $value = $c_or_sc->column->value;
                $ret[$name] = $value;
            } else if($c_or_sc->super_column) { // super columns
                $name    = $this->parse_column_name($c_or_sc->super_column->name, $parse_as_columns);
                $columns = $c_or_sc->super_column->columns;
                $ret[$name] = $this->columns_to_array($columns);
            }
        }
        return $ret;
    }

    public function columns_to_array($array_of_c) {
        $ret = null;
        foreach($array_of_c as $c) {
            $name  = $this->parse_column_name($c->name, false);
            $value = $c->value;

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
                $c_or_sc->super_column->name = $this->unparse_column_name($name, true);
                $c_or_sc->super_column->columns = $this->array_to_columns($value, $timestamp);
                $c_or_sc->super_column->timestamp = $timestamp;
            } else {
                $c_or_sc = new cassandra_ColumnOrSuperColumn();
                $c_or_sc->column = new cassandra_Column();
                $c_or_sc->column->name = $this->unparse_column_name($name, true);
                $c_or_sc->column->value = $this->to_column_value($value);;
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
            $column->name = $this->unparse_column_name($name, false);
            $column->value = $this->to_column_value($value);
            $column->timestamp = $timestamp;

            $ret[] = $column;
        }
        return $ret;
    }

    public function to_column_value($thing) {
        if($thing === null) return "";

        return $thing;
    }

    // ARGH
    public function parse_column_name($column_name, $is_column=true) {
        if(!$this->parse_columns) return $column_name;
        if(!$column_name) return NULL;

        $type = $is_column ? $this->column_type : $this->subcolumn_type;
        if($type == "LexicalUUIDType" || $type == "TimeUUIDType") {
            return UUID::convert($column_name, UUID::FMT_BINARY, UUID::FMT_STRING);
        } else if($type == "LongType") {
            return $this->unpack_longtype($column_name);
        } else {
            return $column_name;
        }
    }

    public function unparse_column_name($column_name, $is_column=true) {
        if(!$this->parse_columns) return $column_name;
        if(!$column_name) return NULL;

        $type = $is_column ? $this->column_type : $this->subcolumn_type;
        if($type == "LexicalUUIDType" || $type == "TimeUUIDType") {
            return UUID::convert($column_name, UUID::FMT_STRING, UUID::FMT_BINARY);
        } else if($type == "LongType") {
            return $this->pack_longtype($column_name);
        } else {
            return $column_name;
        }
    }
    
    // See http://webcache.googleusercontent.com/search?q=cache:9jjbeSy434UJ:wiki.apache.org/cassandra/FAQ+cassandra+php+%22A+long+is+exactly+8+bytes%22&cd=1&hl=en&ct=clnk&gl=us
    public function pack_longtype($x) {
        return pack('C8',
            ($x >> 56) & 0xff, ($x >> 48) & 0xff, ($x >> 40) & 0xff, ($x >> 32) & 0xff,
            ($x >> 24) & 0xff, ($x >> 16) & 0xff, ($x >> 8) & 0xff, $x & 0xff
        );
    }

    public function unpack_longtype($x) {
        $a = unpack('C8', $x);
        return ($a[1] << 56) + ($a[2] << 48) + ($a[3] << 40) + ($a[4] << 32) + ($a[5] << 24) + ($a[6] << 16) + ($a[7] << 8) + $a[8];
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
