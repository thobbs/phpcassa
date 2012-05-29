<?php
namespace phpcassa\Iterator;

use phpcassa\Schema\DataType\Serialized;

/**
 * Iterates over a column family row-by-row, typically with only a subset
 * of each row's columns.
 *
 * @package phpcassa\Iterator
 */
class IndexedColumnFamilyIterator extends ColumnFamilyIterator {

    private $index_clause;

    public function __construct($column_family, $index_clause, $buffer_size,
                                $column_parent, $predicate,
                                $read_consistency_level) {

        $this->index_clause = $index_clause;

        $row_count = $index_clause->count;
        $orig_start_key = $index_clause->start_key;

        parent::__construct($column_family, $buffer_size, $row_count,
                            $orig_start_key, $column_parent, $predicate,
                            $read_consistency_level);
    }

    protected function get_buffer() {
        # Figure out how many rows we need to get and record that
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
        $this->index_clause->count = $buff_sz;
        $this->buffer_number++;

        if (is_string($this->next_start_key) && $this->column_family->key_type instanceof Serialized) {
            $handle_serialize = true;
        } else {
            $handle_serialize = false;
        }
        $this->index_clause->start_key = $this->column_family->pack_key($this->next_start_key, $handle_serialize);
        $resp = $this->column_family->pool->call("get_indexed_slices",
                $this->column_parent, $this->index_clause, $this->predicate,
                $this->read_consistency_level);

        $this->current_buffer = $this->column_family->keyslices_to_array($resp);
        $this->current_page_size = count($this->current_buffer);
    }
}

