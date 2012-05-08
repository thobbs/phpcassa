<?php

use phpcassa\UUID;

require_once(__DIR__.'/ArrayFormatCFTest.php');

class ArrayFormatCounterCFTest extends ArrayFormatCFTest {

    protected static $CF = "Counter1";

    protected static $cfattrs = array(
        "column_type" => "Standard",
        "default_validation_class" => "CounterColumnType"
    );

    public function setUp() {
        parent::setUp();
        $this->cols = array(array(UUID::uuid1(), 1),
                            array(UUID::uuid1(), 2));
    }

    public function test_indexed_slices() { }
}
