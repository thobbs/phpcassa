<?php
namespace phpcassa\Util;

/**
 * A clock that can provide microsecond precision.
 *
 * @package phpcassa\Util
 */
class Clock {

    /**
     * Get a timestamp with microsecond precision
     */
    static public function get_time() {
        // By David Maciel (jdmaciel@gmail.com)
        list($microsecond, $second) = explode(" ", \microtime());
        // remove '0.' from the beginning and trailing zeros(2) from the end
        $microsecond = substr($microsecond, 2, -2);
        return (int) $second . $microsecond;
    }
}
