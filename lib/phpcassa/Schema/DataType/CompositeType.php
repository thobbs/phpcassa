<?php
namespace phpcassa\Schema\DataType;

use phpcassa\Schema\DataType\Serialized;
use phpcassa\ColumnFamily;

/**
 * Holds multiple types as subcomponents.
 *
 * @package phpcassa\Schema\DataType
 */
class CompositeType extends CassandraType implements Serialized
{
    /**
     * @param phpcassa\Schema\DataType\CassandraType[] $inner_types an array
     *        of other CassandraType instances.
     */
    public function __construct($inner_types) {
        $this->inner_types = $inner_types;
    }

    public function pack($value, $is_name=true, $slice_end=null, $is_data=false) {
        if ($is_name && $is_data)
            $value = unserialize($value);

        $res = "";
        $num_items = count($value);
        for ($i = 0; $i < $num_items; $i++) {
            $item = $value[$i];
            $eoc = 0x00;
            if (is_array($item)) {
                list($actual_item, $inclusive) = $item;
                $item = $actual_item;
                if ($inclusive) {
                    if ($slice_end == ColumnFamily::SLICE_START)
                        $eoc = 0xFF;
                    else if ($slice_end == ColumnFamily::SLICE_FINISH)
                        $eoc = 0x01;
                } else {
                    if ($slice_end == ColumnFamily::SLICE_START)
                        $eoc = 0x01;
                    else if ($slice_end == ColumnFamily::SLICE_FINISH)
                        $eoc = 0xFF;
                }
            } else if ($i === ($num_items - 1)) {
                if ($slice_end == ColumnFamily::SLICE_START)
                    $eoc = 0xFF;
                else if ($slice_end == ColumnFamily::SLICE_FINISH)
                    $eoc = 0x01;
            }

            $type = $this->inner_types[$i];
            $packed = $type->pack($item, false);
            $len = strlen($packed);
            $res .= pack("C2", $len&0xFF00, $len&0xFF).$packed.pack("C1", $eoc);
        }

        return $res;
    }

    public function unpack($data, $is_name=true) {
        $component_idx = 0;
        $components = array();
        while (empty($data) !== true) {
            $bytes = unpack("Chi/Clow", substr($data, 0, 2));
            $len = $bytes["hi"]*256 + $bytes["low"];
            $component_data = substr($data, 2, $len);

            $type = $this->inner_types[$component_idx];
            $unpacked = $type->unpack($component_data, false);
            $components[] = $unpacked;

            $data = substr($data, $len + 3);
            $component_idx++;
        }

        if ($is_name) {
            return serialize($components);
        } else {
            return $components;
        }
    }

    public function __toString() {
        $inner_strs = array();
        foreach ($inner_types as $inner_type) {
            $inner_strs[] = (string)$inner_type;
        }

        return 'CompositeType(' . join(', ', $inner_strs) . ')';
    }
}
