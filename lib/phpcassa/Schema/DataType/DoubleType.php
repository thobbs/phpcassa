<?php
namespace phpcassa\Schema\DataType;

use phpcassa\Schema\DataType\Serialized;

/**
 * Stores data as an 8-byte double-precision float.
 *
 * @package phpcassa\Schema\DataType
 */
class DoubleType extends CassandraType implements Serialized
{
    public function pack($value, $is_name=true, $slice_end=null, $is_data=false) {
        if ($is_name && $is_data)
            $value = unserialize($value);
        return strrev(pack("d", $value));
    }

    public function unpack($data, $is_name=true) {
        $value = current(unpack("d", strrev($data)));
        if ($is_name) {
            return serialize($value);
        } else {
            return $value;
        }
    }
}
