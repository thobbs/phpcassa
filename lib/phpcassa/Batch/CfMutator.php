<?
namespace phpcassa\Batch;

use phpcassa\Batch\Mutator;

class CfMutator extends Mutator {

    protected $cf;

    public function __construct($column_family, $write_consistency_level=null) {
        $this->cf = $column_family;
        if ($write_consistency_level === null)
            $wcl = $column_family->write_consistency_level;
        else
            $wcl = $write_consistency_level;
        parent::__construct($column_family->pool, $wcl);
    }

    public function insert($key, $columns, $timestamp=null, $ttl=null) {
        return parent::insert($this->cf, $key, $columns, $timestamp, $ttl);
    }

    public function remove($key, $columns=null, $super_column=null, $timestamp=null) {
        return parent::remove($this->cf, $key, $columns, $super_column, $timestamp);
    }
}
