<?php

require_once(__DIR__.'/ArrayFormatSuperCFTest.php');

class ArrayFormatCounterSuperCFTest extends ArrayFormatSuperCFTest {

    protected static $CF = "SuperCounter1";

    protected static $cfattrs = array(
        "column_type" => "Super",
        "default_validation_class" => "CounterColumnType"
    );

    protected $subcols = array(array('col1', 1), array('col2', 2));
}
