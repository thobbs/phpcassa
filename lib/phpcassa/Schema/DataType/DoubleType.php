<?php
namespace phpcassa\Schema\DataType;

/**
 * Stores data as an 8-byte double-precision float.
 *
 * @package phpcassa\Schema\DataType
 */
class DoubleType extends CassandraType
{
    public function pack($value, $is_name=true, $slice_end=null, $is_data=false) {
        if ($is_name && $is_data)
            $value = unserialize($value);
        return pack("d", $value);
    }

    public function unpack($data, $is_name=true) {
        $value = array_shift(unpack("d", $data));
        if ($is_name) {
            return serialize($value);
        } else {
            return $value;
        }
    }
}
