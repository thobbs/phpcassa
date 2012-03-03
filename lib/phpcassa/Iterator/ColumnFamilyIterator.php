<?php
namespace phpcassa\Iterator;

/**
 * @package phpcassa/Iterator
 */
class ColumnFamilyIterator implements \Iterator {

    protected $column_family;
    protected $buffer_size;
    protected $row_count;
    protected $read_consistency_level;
    protected $column_parent, $predicate;

    protected $current_buffer;
    protected $next_start_key, $orig_start_key;
    protected $is_valid;
    protected $rows_seen;

    protected function __construct($column_family,
                                   $buffer_size,
                                   $row_count,
                                   $orig_start_key,
                                   $column_parent,
                                   $predicate,
                                   $read_consistency_level) {

        $this->column_family = $column_family;
        $this->buffer_size = $buffer_size;
        $this->row_count = $row_count;
        $this->orig_start_key = $orig_start_key;
        $this->next_start_key = $orig_start_key;
        $this->column_parent = $column_parent;
        $this->predicate = $predicate;
        $this->read_consistency_level = $read_consistency_level;

        if ($this->row_count !== null)
            $this->buffer_size = min($this->row_count, $buffer_size);
    }

    public function rewind() {
        // Setup first buffer
        $this->rows_seen = 0;
        $this->is_valid = true;
        $this->next_start_key = $this->orig_start_key;
        $this->get_buffer();

        # If nothing was inserted, this may happen
        if (count($this->current_buffer) == 0) {
            $this->is_valid = false;
            return;
        }

        # If the very first row is a deleted row
        if (count(current($this->current_buffer)) == 0)
            $this->next();
        else
            $this->rows_seen++;
    }

    public function current() {
        return current($this->current_buffer);
    }

    public function key() {
        return key($this->current_buffer);
    }

    public function valid() {
        return $this->is_valid;
    }

    public function next() {
        $beyond_last_row = false;

        # If we haven't run off the end
        if ($this->current_buffer != null)
        {
            # Save this key incase we run off the end
            $this->next_start_key = key($this->current_buffer);
            next($this->current_buffer);

            if (count(current($this->current_buffer)) == 0)
            {
                # this is an empty row, skip it
                do {
	            	$this->next_start_key = key($this->current_buffer);
	            	next($this->current_buffer);
            		$key = key($this->current_buffer);
            		if ( !isset($key) ) {
            			$beyond_last_row = true;
            			break;
            		}
            	} while (count(current($this->current_buffer)) == 0);
            }
            else
            {
	            $key = key($this->current_buffer);
	            $beyond_last_row = !isset($key);
            }

            if (!$beyond_last_row)
            {
                $this->rows_seen++;
                if ($this->rows_seen > $this->row_count) {
                    $this->is_valid = false;
                    return;
                }
            }
        }
        else
        {
            $beyond_last_row = true;
        }

        if($beyond_last_row && $this->current_page_size < $this->expected_page_size)
        {
            # The page was shorter than we expected, so we know that this
            # was the last page in the column family
            $this->is_valid = false;
        }
        else if($beyond_last_row)
        {
            # We've reached the end of this page, but there should be more
            # in the CF

            # Get the next buffer (next_start_key has already been set)
            $this->get_buffer();

            # If the result set is 1, we can stop because the first item
            # should always be skipped
            if(count($this->current_buffer) == 1)
                $this->is_valid = false;
            else
                $this->next();
        }
    }
}
