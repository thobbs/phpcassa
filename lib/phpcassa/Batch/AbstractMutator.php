<?php

namespace phpcassa\Batch;

use phpcassa\Util\Clock;
use cassandra\Deletion;
use cassandra\Mutation;
use cassandra\SlicePredicate;

/**
 * Common methods shared by CfMutator and Mutator classes
 */
abstract class AbstractMutator
{
    protected $pool;
    protected $buffer = array();
    protected $cl;

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

    protected function enqueue($key, $cf, $mutations) {
        $mut = array($key, $cf->column_family, $mutations);
        $this->buffer[] = $mut;
    }

    protected function insert_cf($column_family, $key, $columns, $timestamp=null, $ttl=null) {
        if (!empty($columns)) {
            if ($timestamp === null)
                $timestamp = Clock::get_time();
            $key = $column_family->pack_key($key);
            $mut_list = $column_family->make_mutation($columns, $timestamp, $ttl);
            $this->enqueue($key, $column_family, $mut_list);
        }
        return $this;
    }

    protected function remove_cf($column_family, $key, $columns=null, $super_column=null, $timestamp=null) {
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
