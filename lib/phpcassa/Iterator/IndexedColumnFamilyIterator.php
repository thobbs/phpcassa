<?php
namespace phpcassa\Iterator;

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
        if($this->row_count != null)
            $this->index_clause->count = min($this->row_count - $this->rows_seen + 1, $this->buffer_size);
        else
            $this->index_clause->count = $this->buffer_size;
        $this->expected_page_size = $this->index_clause->count;

        $this->index_clause->start_key = $this->column_family->pack_key($this->next_start_key);
        $resp = $this->column_family->pool->call("get_indexed_slices",
                $this->column_parent, $this->index_clause, $this->predicate,
                $this->read_consistency_level);

        $this->current_buffer = $this->column_family->keyslices_to_array($resp);
        $this->current_page_size = count($this->current_buffer);
    }
}

