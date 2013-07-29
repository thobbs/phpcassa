<?php
namespace phpcassa\Schema\DataType;

use phpcassa\Schema\DataType\Serialized;
use phpcassa\UUID;
use phpcassa\UUID\UUIDException;

/**
 * Handles any type of UUID.
 *
 * @package phpcassa\Schema\DataType
 */
class UUIDType extends CassandraType implements Serialized
{
    public function pack($value, $is_name=true, $slice_end=null, $is_data=false) {
        if ($is_name && $is_data) {
            $value = unserialize($value);
        }
        $is_uuid = ($value instanceof UUID);
        if (!$is_uuid && is_string($value)) {
            try {
                $value = UUID::import($value);
            } catch (Exception $e) {
                throw new UUIDException("Error casting '$value' to UUID: $e");
            }
        } else if (!$is_uuid) {
            throw new UUIDException("A UUID object or a string representation of a UUID was expected, but '$value' was found");
        }
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
