<?php

namespace phpcassa\Batch;

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
}