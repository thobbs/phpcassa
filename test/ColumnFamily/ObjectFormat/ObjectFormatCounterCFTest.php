<?php

use phpcassa\UUID;

require_once(__DIR__.'/ObjectFormatCFTest.php');

class ObjectFormatCounterCFTest extends ObjectFormatCFTest {

    protected static $CF = "Counter1";

    protected static $cfattrs = array(
        "column_type" => "Standard",
        "comparator_type" => "TimeUUIDType",
        "default_validation_class" => "CounterColumnType"
    );

    public function setUp() {
        parent::setUp();
        $this->cols = array(array(UUID::uuid1(), 'val1'),
                            array(UUID::uuid1(), 'val2'));
    }

    public function test_indexed_slices() { }
}
