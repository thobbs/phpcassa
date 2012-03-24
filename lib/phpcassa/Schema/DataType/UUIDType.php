<?php
namespace phpcassa\Schema\DataType;

use phpcassa\UUID;

/**
 * @package phpcassa\Schema\DataType
 */
class UUIDType extends CassandraType
{
    public function pack($value, $is_name=true, $slice_end=null, $is_data=false) {
        if ($is_name && $is_data)
            $value = unserialize($value);
        return $value->bytes;
    }

    public function unpack($data, $is_name=true) {
        $value = UUID::import($data);
        if ($is_name) {
            return serialize($value);
        } else {
            return $value;
        }
    }
}
