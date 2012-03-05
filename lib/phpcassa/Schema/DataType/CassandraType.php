<?php
namespace phpcassa\Schema\DataType;

/**
 * @package phpcassa\Schema\DataType
 */
class CassandraType {
    public function pack($value, $is_name=null, $slice_end=null, $is_data=null) {
        return $value;
    }

    public function unpack($data, $is_name=null) {
        return $data;
    }

    public function __toString() {
        return get_class($this);
    }
}
