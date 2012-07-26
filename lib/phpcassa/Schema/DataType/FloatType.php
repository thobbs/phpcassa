<?php
namespace phpcassa\Schema\DataType;

use phpcassa\Schema\DataType\Serialized;

/**
 * Stores data as a 4-byte single-precision float.
 *
 * @package phpcassa\Schema\DataType
 */
class FloatType extends CassandraType implements Serialized
{
    public function pack($value, $is_name=true, $slice_end=null, $is_data=false) {
        if ($is_name && $is_data)
            $value = unserialize($value);
        return strrev(pack("f", $value));
    }

    public function unpack($data, $handle_serialize=true) {
        $value = current(unpack("f", strrev($data)));
        if ($handle_serialize) {
            return serialize($value);
        } else {
            return $value;
        }
    }
}
