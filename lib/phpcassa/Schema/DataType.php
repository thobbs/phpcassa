<?php
namespace phpcassa\Schema;

/**
 * @package phpcassa\Schema
 */
class DataType
{
    const BYTES_TYPE = "BytesType";
    const LONG_TYPE = "LongType";
    const INTEGER_TYPE = "IntegerType";
    const ASCII_TYPE = "AsciiType";
    const UTF8_TYPE = "UTF8Type";
    const TIME_UUID_TYPE = "TimeUUIDType";
    const LEXICAL_UUID_TYPE = "LexicalUUIDType";
    const UUID_TYPE = "UUIDType";

    private static $class_map;

    public static function init() {
        self::$class_map = array(
            'BytesType'       => 'phpcassa\Schema\DataType\BytesType',
            'AsciiType'       => 'phpcassa\Schema\DataType\AsciiType',
            'UTF8Type'        => 'phpcassa\Schema\DataType\UTF8Type',
            'LongType'        => 'phpcassa\Schema\DataType\LongType',
            'IntegerType'     => 'phpcassa\Schema\DataType\IntegerType',
            'TimeUUIDType'    => 'phpcassa\Schema\DataType\TimeUUIDType',
            'LexicalUUIDType' => 'phpcassa\Schema\DataType\LexicalUUIDType',
            'UUIDType'        => 'phpcassa\Schema\DataType\UUIDType'
        );
    }

    private static function extract_type_name($typestr) {
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

    public static function get_type_for($typestr) {
        $type_name = self::extract_type_name($typestr);
        $type_class = self::$class_map[$type_name];
        return new $type_class;
    }
}

DataType::init();
