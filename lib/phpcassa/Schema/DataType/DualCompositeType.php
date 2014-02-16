<?php
namespace phpcassa\Schema\DataType;

use phpcassa\Schema\DataType\Serialized;
use phpcassa\Schema\DataType\CompositeType;
use phpcassa\ColumnFamily;

/**
 * Holds 2 types as subcomponents.
 *
 * @package phpcassa\Schema\DataType
 */
class DualCompositeType extends CompositeType
{

    public function unpack($data, $is_name=true) {
        $mark = unpack("Cm", $data[1]);
        $components = array(
            $this->inner_types[0]->unpack(substr($data, 2, $mark['m'])),
            $this->inner_types[1]->unpack(substr($data, $mark['m'] + 5, -1)),
        );

        if ($is_name) {
            return serialize($components);
        } else {
            return $components;
        }
    }

}
