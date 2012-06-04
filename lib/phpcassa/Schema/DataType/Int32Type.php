<?php
namespace phpcassa\Schema\DataType;

/**
 * Stores data as an 4-byte signed integer.
 *
 * @package phpcassa\Schema\DataType
 */
class Int32Type extends CassandraType {

    public function pack($value, $is_name=null, $slice_end=null, $is_data=null)
    {
        // signed/unsigned doesn't matter when packing
        return pack('N', $value);
    }

    public function unpack($data, $is_name=null)
    {
        return current(unpack('l', strrev($data)));
    }
}

