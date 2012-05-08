<?php
namespace phpcassa\Schema\DataType;

/**
 * Stores data as a 4-byte single-precision float.
 *
 * @package phpcassa\Schema\DataType
 */
class FloatType extends CassandraType
{
    public function pack($value, $is_name=true, $slice_end=null, $is_data=false) {
        if ($is_name && $is_data)
            $value = unserialize($value);
        return pack("f", $value);
    }

    public function unpack($data, $handle_serialize=true) {
        $value = array_shift(unpack("f", $data));
        if ($handle_serialize) {
            return serialize($value);
        } else {
            return $value;
        }
    }
}
