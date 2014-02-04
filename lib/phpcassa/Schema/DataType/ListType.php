<?php
namespace phpcassa\Schema\DataType;

/**
 * Handles any type of List.
 *
 * @package phpcassa\Schema\DataType
 */
class ListType extends CassandraType
{
    protected $innerType;

    public function __construct(CassandraType $innerType){
        $this->innerType = $innerType;
    }

    private function readUInt16BE($value,$offset)
    {
        list(, $int) = unpack('s*', strrev(substr($value,$offset,2)));
        return $int;
    }

    public function unpack($data, $handle_serialize=true) {
        $offset = 0;
        $total = self::readUInt16BE($data,$offset);
        $offset += 2;

        $items = [];
        for ($i = 0;$i < $total;$i++){
            $length = self::readUInt16BE($data,$offset);
            $offset += 2;
            $items[] = $this->innerType->unpack(substr($data,$offset,$length));
            $offset += $length;
        }

        return $items;
    }
}
