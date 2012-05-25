<?php
namespace phpcassa\Schema\DataType;

use phpcassa\Schema\DataType\Serialized;
use phpcassa\UUID;

/**
 * Handles any type of UUID.
 *
 * @package phpcassa\Schema\DataType
 */
class UUIDType extends CassandraType implements Serialized
{
    public function pack($value, $is_name=true, $slice_end=null, $is_data=false) {
        if ($is_name && $is_data)
            $value = unserialize($value);
        return $value->bytes;
    }

    public function unpack($data, $handle_serialize=true) {
        $value = UUID::import($data);
        if ($handle_serialize) {
            return serialize($value);
        } else {
            return $value;
        }
    }
}
