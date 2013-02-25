<?php
namespace phpcassa\Batch;

use phpcassa\Batch\AbstractMutator;

/**
 * A convenience subclass of phpcassa\Batch\Mutator for dealing
 * with batch operations on a single column family.
 *
 * @package phpcassa\Batch
 */
class CfMutator extends AbstractMutator {

    protected $cf;

    /**
     * Initialize a mutator for a given column family.
     *
     * @param phpcassa\ColumnFamily $column_family an initialized instanced
     *        of ColumnFamily; this object's pool will be used for all
     *        operations.
     * @param phpcassa\ConsistencyLevel $write_consistency_level the consistency
     *        level this mutator will write at; if left as NULL, this defaults to
     *        $column_family->write_consistency_level.
     */
    public function __construct($column_family, $write_consistency_level=null) {
        $this->cf = $column_family;
        $this->pool = $column_family->pool;
        $this->buffer = array();
        if ($write_consistency_level === null)
            $this->cl = $column_family->write_consistency_level;
        else
            $this->cl = $write_consistency_level;
    }

    /**
     * Add an insertion to the buffer.
     *
     * @param mixed $key the row key
     * @param mixed[] $columns an array of columns to insert, whose format
     *        should match $column_family->insert_format
     * @param int $timestamp an optional timestamp (default is "now", when
     *        this function is called, not when send() is called)
     * @param int $ttl a TTL to apply to all columns inserted here
     */
    public function insert($key, $columns, $timestamp=null, $ttl=null) {
        return $this->insert_cf($this->cf, $key, $columns, $timestamp, $ttl);
    }

    /**
     * Add a deletion to the buffer.
     *
     * @param mixed $key the row key
     * @param mixed[] $columns a list of columns or super columns to delete
     * @param mixed $supercolumn if you want to delete only some subcolumns from
     *        a single super column, set this to the super column name
     * @param int $timestamp an optional timestamp (default is "now", when
     *        this function is called, not when send() is called)
     */
    public function remove($key, $columns=null, $super_column=null, $timestamp=null) {
        return $this->remove_cf($this->cf, $key, $columns, $super_column, $timestamp);
    }
}
