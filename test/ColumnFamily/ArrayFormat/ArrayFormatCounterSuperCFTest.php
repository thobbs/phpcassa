<?php

use phpcassa\UUID;

require_once(__DIR__.'/ArrayFormatSuperCFTest.php');

class ArrayFormatCounterSuperCFTest extends ArrayFormatSuperCFTest {

    protected static $CF = "SuperCounter1";

    protected static $cfattrs = array(
        "column_type" => "Super",
        "subcomparator_type" => "TimeUUIDType",
        "default_validation_class" => "CounterColumnType"
    );

    public function setUp() {
        parent::setUp();
        $this->subcols = array(array(UUID::uuid1(), 1),
                               array(UUID::uuid1(), 2));
    }
}
