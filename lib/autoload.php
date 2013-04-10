<?php

spl_autoload_register(function($className){
    if (strpos($className, 'phpcassa') === 0 || strpos($className, 'Thrift') === 0) {
        require_once __DIR__.'/'.str_replace('\\', '/', $className).'.php';
    }
});

// This sets up the THRIFT_AUTOLOAD map
require_once __DIR__.'/cassandra/Cassandra.php';
require_once __DIR__.'/cassandra/Types.php';
