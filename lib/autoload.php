<?php

spl_autoload_register(function($className){
    if (strpos($className, 'phpcassa') != 0) {
        return false;
    }

    require __DIR__.'/'.str_replace('\\', '/', $className).'.php';
});

require __DIR__.'/phpcassa/Connection/ConnectionPool.php';