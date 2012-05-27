<?php
namespace phpcassa\Schema\DataType;

/**
 * Stores data as an 8-byte integer.
 *
 * @package phpcassa\Schema\DataType
 */
class DateType extends LongType {

    public function pack($value, $is_name=null, $slice_end=null, $is_data=null)
    {
        if (false !== ($p = strpos($value, ' ')))
            $value = substr($value, $p+1) + substr($value, 0, 1);

        $value *= 1e3;

        return parent::pack(floor($value), $is_name, $slice_end, $is_data);
    }

    public function unpack($data, $is_name=null)
    {
        return parent::unpack($data, $is_name) / 1e3;
    }
}