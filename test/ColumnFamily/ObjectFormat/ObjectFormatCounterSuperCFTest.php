<?php

use phpcassa\UUID;

require_once(__DIR__.'/ObjectFormatSuperCFTest.php');

class ObjectFormatCounterSuperCFTest extends ObjectFormatSuperCFTest {

    protected static $CF = "SuperCounter1";

    protected static $cfattrs = array(
        "column_type" => "Super",
        "subcomparator_type" => "TimeUUIDType",
        "default_validation_class" => "CounterColumnType"
    );

    public function setUp() {
        parent::setUp();
        $this->subcols = array(array(UUID::uuid1(), 'val1'),
                               array(UUID::uuid1(), 'val2'));
    }
}
