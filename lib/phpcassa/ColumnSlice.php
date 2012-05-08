<?php
namespace phpcassa;

use cassandra\SliceRange;

/**
 * Represents a range of columns to slice from a row, multiple rows,
 * or a super column.
 *
 * @package phpcassa
 */
class ColumnSlice extends SliceRange {

    /** The default limit to the number of columns retrieved in queries. */
    const DEFAULT_COLUMN_COUNT = 100; // default max # of columns for get()

    /** The maximum number number of columns that can be fetch at once. */
    const MAX_COUNT = 2147483647; # 2^31 - 1

    /**
     * Defines a range of columns.
     *
     * @param mixed $start the column to start with, or '' for the
     *        beginning of the row
     * @param mixed $finish the column to finish with, or '' for the
     *        end of the row
     * @param int $count an upper bound on the number of columns to
     *        fetch. The default limit is 100 columns.
     * @param bool $reversed whether or not to reverse the column
     *        slice, going backwards from $start to $finish.
     */
    function __construct($start="", $finish="",
            $count=self::DEFAULT_COLUMN_COUNT, $reversed=False) {
        parent::__construct();
        $this->start = $start;
        $this->finish = $finish;
        $this->count = $count;
        $this->reversed = $reversed;
    }
}
