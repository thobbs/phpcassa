<?php
namespace phpcassa\Iterator;

use cassandra\KeyRange;

/**
 * Iterates over a column family row-by-row, typically with only a subset
 * of each row's columns.
 *
 * @package phpcassa\Iterator
 */
class RangeColumnFamilyIterator extends ColumnFamilyIterator {

    private $key_start, $key_finish;

    public function __construct($column_family, $buffer_size,
                                $key_start, $key_finish, $row_count,
                                $column_parent, $predicate,
                                $read_consistency_level) {

        $this->key_start = $key_start;
        $this->key_finish = $key_finish;

        parent::__construct($column_family, $buffer_size, $row_count,
                            $key_start, $column_parent, $predicate,
                            $read_consistency_level);
    }

    protected function get_buffer() {
        if($this->row_count != null)
            $buff_sz = min($this->row_count - $this->rows_seen + 1, $this->buffer_size);
        else
            $buff_sz = $this->buffer_size;
        $this->expected_page_size = $buff_sz;

        $key_range = new KeyRange();
        $key_range->start_key = $this->column_family->pack_key($this->next_start_key);
        $key_range->end_key = $this->key_finish;
        $key_range->count = $buff_sz;

        $resp = $this->column_family->pool->call("get_range_slices", $this->column_parent, $this->predicate,
            $key_range, $this->read_consistency_level);

        $this->current_buffer = $this->column_family->keyslices_to_array($resp);
        $this->current_page_size = count($this->current_buffer);
    }
}
