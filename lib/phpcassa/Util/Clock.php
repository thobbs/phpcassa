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
        // By Zach Buller (zachbuller@gmail.com)
        $time1 = \microtime();
        \settype($time1, 'string'); //convert to string to keep trailing zeroes
        $time2 = explode(" ", $time1);
        $sub_secs = \preg_replace('/0./', '', $time2[0], 1);
        $time3 = ($time2[1].$sub_secs)/100;
        return $time3;
    }
}
