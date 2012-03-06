<?php
namespace phpcassa\Schema\DataType;

/**
 * @package phpcassa\Schema\DataType
 */
class IntegerType extends CassandraType {

    public function pack($value, $is_name=null, $slice_end=null, $is_data=null)
    {
        return pack('C', $value);
    }

    public function unpack($data, $is_name=null)
    {
        return unpack('C', $value);
    }
}

