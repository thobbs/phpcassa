<?php
namespace phpcassa\Schema\DataType;

/**
 * Stores data as single-byte boolean values.
 *
 * @package phpcassa\Schema\DataType
 */
class BooleanType extends CassandraType {

    public function pack($value, $is_name=null, $slice_end=null, $is_data=null)
    {
        return pack('C', $value);
    }

    public function unpack($data, $is_name=null)
    {
        return current(unpack('C', $data)) === 1;
    }
}

