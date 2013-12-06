<?php

use phpcassa\UUID;

class UUIDTest extends PHPUnit_Framework_TestCase {

    const MICROSECONDS = 1e6;

    public function testMaxTimeUuid() {
        $time = time();
        $obUuid = UUID::maxTimeuuid($time * self::MICROSECONDS );
        $this->assertInstanceOf('\phpcassa\UUID',$obUuid);
        $this->assertEquals($time,$obUuid->time);
    }

    public function testMinTimeUuid() {
        $time = time();
        $obUuid = UUID::minTimeuuid($time * self::MICROSECONDS );
        $this->assertInstanceOf('\phpcassa\UUID',$obUuid);
        $this->assertEquals($time,$obUuid->time);
    }

}
