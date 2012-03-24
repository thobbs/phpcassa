<?php
namespace phpcassa\UUID;

use phpcassa\UUID;

/**
 * @package phpcassa\Util
 */
class UUIDGen {

    /**
     * Creates a UUID object from a byte representation.
     * @param string $bytes the byte representation of a UUID, which is
     *        what is returned from functions like uuid1()
     * @return a UUID object
     */
    static public function import($bytes) {
        return UUID::import($bytes);
    }

    /**
     * Generate a v1 UUID (timestamp based)
     * @return string a byte[] representation of a UUID 
     * @param string $node what to use for the MAC portion of the UUID.  This will be generated
     *        randomly if left as NULL
     * @param int $time timestamp to use for the UUID.  This should be a number of microseconds
     *        since the UNIX epoch.
     */
    static public function uuid1($node=null, $time=null) {
        return UUID::mint(1, $node, null, $time);
    }

    /**
     * Generate a v3 UUID
     * @return string a byte[] representation of a UUID 
     */
    static public function uuid3($node=null, $namespace=null) {
        return UUID::mint(3, $node, $namespace);
    }

    /**
     * Generate a v4 UUID
     * @return string a byte[] representation of a UUID 
     */
    static public function uuid4() {
        return UUID::mint(4);
    }

    /**
     * Generate a v5 UUID
     * @return string a byte[] representation of a UUID 
     */
    static public function uuid5($node, $namespace=null) {
        return UUID::mint(5, $node, $namespace);
    }
}
