<?php
require_once('simpletest/autorun.php');

class AllTests extends TestSuite {
    function AllTests() {
        $this->TestSuite('All tests');
        $this->addFile('test_columnfamily.php');
    }
}
?>
