<?php
namespace phpcassa\Schema\DataType;

/**
 * Stores data in a variable-length BigInteger-compatible format.
 *
 * This is the most space-efficient integer format.
 *
 * @package phpcassa\Schema\DataType
 */
class IntegerType extends CassandraType {

    public function pack($value, $is_name=null, $slice_end=null, $is_data=null)
    {
        $value = (int)$value;
        $out = array();
        if ($value >= 0) {
            while ($value >= 256) {
                $out[] = pack('C', 0xff & $value);
                $value >>= 8;
            }
            $out[] = pack('C', 0xff & $value);
            if ($value > 127) {
                $out[] = chr('00');
            }
        } else {
            $value = -1 - $value;
            while ($value >= 256) {
                $out[] = pack('C', 0xff & ~$value);
                $value >>= 8;
            }
            if ($value <= 127) {
                $out[] = pack('C', 0xff & ~$value);
            } else {
                $top = pack('n', 0xffff & ~$value);
                // we have two bytes, need to reverse them
                $out[] = strrev($top);
            }
        }

        return strrev(implode($out));
    }

    public function unpack($data, $is_name=null)
    {
        $val = hexdec(bin2hex($data));
        if ((ord($data[0]) & 128) != 0)
            $val = $val - (1 << (strlen($data) * 8));
        return $val;
    }
}

