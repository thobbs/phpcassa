<?php
namespace phpcassa\Iterator;

use phpcassa\ColumnFamily;

/**
 * @package phpcassa/Iterator
 */
class ColumnFamilyIterator implements \Iterator {

    protected $column_family;
    protected $buffer_size;
    protected $row_count;
    protected $read_consistency_level;
    protected $column_parent, $predicate;

    protected $current_buffer, $buffer_number;
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
        $this->array_format = $column_family->return_format == ColumnFamily::ARRAY_FORMAT;

        if ($this->row_count !== null)
            $this->buffer_size = min($this->row_count, $buffer_size);

        $this->buffer_number = 0;
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
        if ($this->rows_seen === $this->row_count) {
            $this->is_valid = false;
            return;
        }

        $beyond_last_row = false;
        # If we haven't run off the end
        if ($this->current_buffer != null)
        {
            # Save this key in case we run off the end
            if ($this->array_format) {
                $cur = current($this->current_buffer);
                $this->next_start_key = $cur[0];
            } else {
                $this->next_start_key = key($this->current_buffer);
            }
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
            # we loop through multiple calls of get_buffer() in case the entire
            # buffer load is tombstones, the old method of just calling next() could
            # result in excessive recursion
            while ( $this->is_valid ) {
                $this->get_buffer();

                # If the result set is 1, we can stop because the first item
                # should always be skipped if we're not on the first buffer
                if(count($this->current_buffer) == 1 && $this->buffer_number != 1)
                {
                    $this->is_valid = false;
                    break;
                }
                else
                {
                    // skip leading tombstone rows
                    $need_new_buffer = false;
                    $skipped_a_row = false;
                    while (count(current($this->current_buffer)) == 0) {
                        $skipped_a_row = true;

                        $this->next_start_key = key($this->current_buffer);
                        next($this->current_buffer);
                        $key = key($this->current_buffer);

                        if ( !isset($key) ) {
                            if ( $this->current_page_size < $this->expected_page_size ) {
                                // looks like we're at the last page of data so just give up
                                $this->is_valid = false;
                            } else {
                                // found nothing but tombstones, fetch a new buffer
                                $need_new_buffer = true;
                            }
                            break;
                        }
                    }

                    if ( !$need_new_buffer ) {
                        if (!$skipped_a_row) {
                            // The first row in the new buffer needs to be skipped
                            $this->next();
                        } else {
                            // We already took care of skipping the first row
                            // because it happened to be a tombstone. We're now
                            // pointing at a valid row, so increment our seen
                            // row count.
                            $this->rows_seen++;
                            if ($this->rows_seen > $this->row_count) {
                                $this->is_valid = false;
                                return;
                            }
                        }
                        break;
                    }
                }
            }
        }
    }
}
