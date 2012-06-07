<?php
namespace phpcassa\Iterator;

use phpcassa\Schema\DataType\Serialized;
use cassandra\KeyRange;

/**
 * Iterates over a column family row-by-row, typically with only a subset
 * of each row's columns.
 *
 * @package phpcassa\Iterator
 */
class RangeColumnFamilyIterator extends ColumnFamilyIterator {

    private $key_start;
    private $key_finish;

    public function __construct($column_family, $buffer_size,
                                $key_start, $key_finish, $row_count,
                                $column_parent, $predicate,
                                $read_consistency_level) {

        // The key_start will be packed during the first get_buffer call
        $this->key_start = $key_start;
        $this->key_finish = $column_family->pack_key($key_finish);

        parent::__construct($column_family, $buffer_size, $row_count,
                            $key_start, $column_parent, $predicate,
                            $read_consistency_level);
    }

    protected function get_buffer() {
        $buff_sz = $this->buffer_size;
        if($this->row_count !== null) {
            if ($this->buffer_number == 0 && $this->row_count <= $buff_sz) {
                // we don't need to chunk, grab exactly the right number of rows
                $buff_sz = $this->row_count;
            } else {
                $buff_sz = min($this->row_count - $this->rows_seen + 1, $this->buffer_size);
            }
        }

        // when fetching a second buffer or later, we have to fetch a minimum
        // of two rows since the first will be a repeat
        if ($this->buffer_number >= 1) {
            $buff_sz = max($buff_sz, 2);
        }

        $this->expected_page_size = $buff_sz;
        $this->buffer_number++;

        $key_range = new KeyRange();
        if (is_string($this->next_start_key) && $this->column_family->key_type instanceof Serialized) {
            $handle_serialize = true;
        } else {
            $handle_serialize = false;
        }
        $key_range->start_key = $this->column_family->pack_key($this->next_start_key, $handle_serialize);
        $key_range->end_key = $this->key_finish;
        $key_range->count = $buff_sz;

        $resp = $this->column_family->pool->call("get_range_slices", $this->column_parent, $this->predicate,
            $key_range, $this->read_consistency_level);

        $this->current_buffer = $this->column_family->keyslices_to_array($resp);
        $this->current_page_size = count($this->current_buffer);
    }
}
