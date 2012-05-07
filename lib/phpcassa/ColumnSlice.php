<?php
namespace phpcassa;

use cassandra\SliceRange;

class ColumnSlice extends SliceRange {

    /** The default limit to the number of columns retrieved in queries. */
    const DEFAULT_COLUMN_COUNT = 100; // default max # of columns for get()

    /** The maximum number number of columns that can be fetch at once. */
    const MAX_COUNT = 2147483647; # 2^31 - 1

    function __construct($start="", $finish="",
            $count=self::DEFAULT_COLUMN_COUNT, $reversed=False) {
        parent::__construct();
        $this->start = $start;
        $this->finish = $finish;
        $this->count = $count;
        $this->reversed = $reversed;
    }
}
