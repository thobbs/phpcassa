<?php
namespace phpcassa\Schema\DataType;

use phpcassa\Schema\DataType\LongType;
use phpcassa\Schema\DataType\Serialized;

/**
 * Stores a date as a number of milliseconds since the unix epoch.
 *
 * @package phpcassa\Schema\DataType
 */
class DateType extends LongType implements Serialized {

    public function pack($value, $is_name=true, $slice_end=null, $is_data=false)
    {
        if (false !== strpos($value, ' ')) {
            list($usec, $sec) = explode(' ', $value);
            $value = $sec + $usec;
        } else if ($is_name && $is_data) {
            $value = unserialize($value);
        }

        $value = floor($value * 1e3);
        return parent::pack($value);
    }

    public function unpack($data, $handle_serialize=true)
    {
        $value = parent::unpack($data, false) / 1e3;
        if ($handle_serialize) {
            return serialize($value);
        } else {
            return $value;
        }
    }
}
