<?php
namespace phpcassa\Schema\DataType;

/**
 * Handles any type of Map.
 *
 * @package phpcassa\Schema\DataType
 */
class MapType extends CassandraType
{
    protected $keyType;
    protected $valueType;

    public function __construct(CassandraType $keyType, CassandraType $valueType){
        $this->keyType = $keyType;
        $this->valueType = $valueType;
    }

    private function readUInt16BE($value,$offset)
    {
        list(, $int) = unpack('s*', strrev(substr($value,$offset,2)));
        return $int;
    }

    public function unpack($data) {
        $offset = 0;
        $total = self::readUInt16BE($data,$offset);
        $offset += 2;

        $items = [];
        for ($i = 0;$i < $total;$i++){
            $keyLength = self::readUInt16BE($data,$offset);
            $offset += 2;
            $key = $this->keyType->unpack(substr($data,$offset,$keyLength));
            $offset += $keyLength;
            $valueLength = self::readUInt16BE($data,$offset);
            $offset += 2;
            $value = $this->keyType->unpack(substr($data,$offset,$valueLength));
            $offset += $valueLength;

            $items[$key] = $value;
        }

        return $items;
    }
}
