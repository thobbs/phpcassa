<?php
namespace phpcassa\Schema;

use phpcassa\Schema\DataType\CompositeType;

/**
 * Maps type strings to packer and unpacker functions.
 *
 * @package phpcassa\Schema
 */
class DataType
{
    const BYTES_TYPE = "BytesType";
    const LONG_TYPE = "LongType";
    const INTEGER_TYPE = "IntegerType";
    const INT32_TYPE = "Int32Type";
    const FLOAT_TYPE = "FloatType";
    const DOUBLE_TYPE = "DoubleType";
    const ASCII_TYPE = "AsciiType";
    const UTF8_TYPE = "UTF8Type";
    const TIME_UUID_TYPE = "TimeUUIDType";
    const LEXICAL_UUID_TYPE = "LexicalUUIDType";
    const UUID_TYPE = "UUIDType";
    const DATE_TYPE = "DateType";

    public static $class_map;

    public static function init() {
        self::$class_map = array(
            'BytesType'       => 'phpcassa\Schema\DataType\BytesType',
            'AsciiType'       => 'phpcassa\Schema\DataType\AsciiType',
            'UTF8Type'        => 'phpcassa\Schema\DataType\UTF8Type',
            'LongType'        => 'phpcassa\Schema\DataType\LongType',
            'IntegerType'     => 'phpcassa\Schema\DataType\IntegerType',
            'FloatType'       => 'phpcassa\Schema\DataType\FloatType',
            'DoubleType'      => 'phpcassa\Schema\DataType\DoubleType',
            'TimeUUIDType'    => 'phpcassa\Schema\DataType\TimeUUIDType',
            'LexicalUUIDType' => 'phpcassa\Schema\DataType\LexicalUUIDType',
            'UUIDType'        => 'phpcassa\Schema\DataType\UUIDType',
            'BooleanType'     => 'phpcassa\Schema\DataType\BooleanType',
            'DateType'        => 'phpcassa\Schema\DataType\DateType',
            'Int32Type'        => 'phpcassa\Schema\DataType\Int32Type',
        );
    }

    protected static function extract_type_name($typestr) {
        if ($typestr == null or $typestr == '')
            return 'BytesType';

        $index = strrpos($typestr, '.');
        if ($index == false)
            return 'BytesType';

        $type = substr($typestr, $index + 1);
        if (!isset(self::$class_map[$type]))
            return 'BytesType';

        return $type;
    }

    /** Given a typestr like "Reversed(AsciiType)", returns "AsciiType". */
    protected static function get_inner_type($typestr) {
        $paren_index = strpos($typestr, '(');
        $end = strlen($typestr) - $paren_index;
        return substr($typestr, $paren_index + 1, $end - 2);
    }

    protected static function get_inner_types($typestr) {
        $inner = self::get_inner_type($typestr);
        $inner_typestrs = explode(',', $inner);
        $inner_types = array();

        foreach ($inner_typestrs as $inner_type) {
            $inner_types[] = self::get_type_for(trim($inner_type));
        }
        return $inner_types;
    }

    public static function get_type_for($typestr) {
        if (strpos($typestr, 'CompositeType') !== false) {
            return new CompositeType(self::get_inner_types($typestr));
        } else if (strpos($typestr, 'ReversedType') !== false) {
            return self::get_type_for(self::get_inner_type($typestr));
        } else {
            $type_name = self::extract_type_name($typestr);
            $type_class = self::$class_map[$type_name];
            return new $type_class;
        }
    }
}

DataType::init();
