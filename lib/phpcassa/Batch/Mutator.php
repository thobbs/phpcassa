<?
namespace phpcassa\Batch;

use phpcassa\Util\Clock;

use cassandra\ConsistencyLevel;
use cassandra\Deletion;
use cassandra\Mutation;
use cassandra\SlicePredicate;

/**
 * Allows you to group multiple mutations across one or more
 * keys and column families into a single batch operation.
 *
 * @package phpcassa\Batch
 */
class Mutator
{
    protected $pool;
    protected $buffer;
    protected $cl;

    /**
     * Intialize a mutator with a connection pool and consistency level.
     *
     * @param phpcassa\Connection\ConnectionPool $pool the connection pool to
     *        use for all operations
     * @param cassandra\ConsistencyLevel $consistency_level the default consistency
     *        level this mutator will write at, with a default of
     *        ConsistencyLevel::ONE
     */
    public function __construct($pool,
            $consistency_level=ConsistencyLevel::ONE) {
        $this->pool = $pool;
        $this->buffer = array();
        $this->cl =  $consistency_level;
    }

    protected function enqueue($key, $cf, $mutations) {
        $mut = array($key, $cf->column_family, $mutations);
        $this->buffer[] = $mut;
    }

    /**
     * Send all buffered mutations.
     *
     * If an error occurs, the buffer will be preserverd, allowing you to
     * attempt to call send() again later or take other recovery actions.
     *
     * @param cassandra\ConsistencyLevel $consistency_level optional
     *        override for the mutator's default consistency level
     */
    public function send($consistency_level=null) {
        if ($consistency_level === null)
            $wcl = $this->cl;
        else
            $wcl = $consistency_level;

        $mutations = array();
        foreach ($this->buffer as $mut_set) {
            list($key, $cf, $cols) = $mut_set;

            if (isset($mutations[$key])) {
                $key_muts = $mutations[$key];
            } else {
                $key_muts = array();
            }

            if (isset($key_muts[$cf])) {
                $cf_muts = $key_muts[$cf];
            } else {
                $cf_muts = array();
            }

            $cf_muts = array_merge($cf_muts, $cols);
            $key_muts[$cf] = $cf_muts;
            $mutations[$key] = $key_muts;
        }

        if (!empty($mutations)) {
            $this->pool->call('batch_mutate', $mutations, $wcl);
        }
        $this->buffer = array();
    }

    /**
     * Add an insertion to the buffer.
     *
     * @param phpcassa\ColumnFamily $column_family an initialized
     *        ColumnFamily instance
     * @param mixed $key the row key
     * @param mixed[] $columns an array of columns to insert, whose format
     *        should match $column_family->insert_format
     * @param int $timestamp an optional timestamp (default is "now", when
     *        this function is called, not when send() is called)
     * @param int $ttl a TTL to apply to all columns inserted here
     */
    public function insert($column_family, $key, $columns, $timestamp=null, $ttl=null) {
        if (!empty($columns)) {
            if ($timestamp === null)
                $timestamp = Clock::get_time();
            $key = $column_family->pack_key($key);
            $mut_list = $column_family->make_mutation($columns, $timestamp, $ttl);
            $this->enqueue($key, $column_family, $mut_list);
        }
        return $this;
    }

    /**
     * Add a deletion to the buffer.
     *
     * @param phpcassa\ColumnFamily $column_family an initialized
     *        ColumnFamily instance
     * @param mixed $key the row key
     * @param mixed[] $columns a list of columns or super columns to delete
     * @param mixed $supercolumn if you want to delete only some subcolumns from
     *        a single super column, set this to the super column name
     * @param int $timestamp an optional timestamp (default is "now", when
     *        this function is called, not when send() is called)
     */
    public function remove($column_family, $key, $columns=null, $super_column=null, $timestamp=null) {
        if ($timestamp === null)
            $timestamp = Clock::get_time();
        $deletion = new Deletion();
        $deletion->timestamp = $timestamp;

        if ($super_column !== null) {
            $deletion->super_column = $column_family->pack_name($super_column, true);
        }
        if ($columns !== null) {
            $is_super = $column_family->is_super && $super_column === null;
            $packed_cols = array();
            foreach ($columns as $col) {
                $packed_cols[] = $column_family->pack_name($col, $is_super);
            }
            $predicate = new SlicePredicate();
            $predicate->column_names = $packed_cols;
            $deletion->predicate = $predicate;
        }

        $mutation = new Mutation();
        $mutation->deletion = $deletion;
        $packed_key = $column_family->pack_key($key);
        $this->enqueue($packed_key, $column_family, array($mutation));

        return $this;
    }
}
