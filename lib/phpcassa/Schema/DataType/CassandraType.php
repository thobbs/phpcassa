<?php
namespace phpcassa\Schema\DataType;

/**
 * @package phpcassa\Schema\DataType
 */
class CassandraType {
    public function pack($value) { return $value; }
    public function unpack($data) { return $data; }

    public function __toString() {
        return get_class($this);
    }
}
