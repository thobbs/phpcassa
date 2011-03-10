<?php

spl_autoload_register(function($className){
    if (strpos($className, 'phpcassa_') != 0) {
        return false;
    }
    if ($className === 'ColumnFamily') throw new Exception();
    require __DIR__.'/'.str_replace('_', '/', $className).'.php';
});

require __DIR__.'/phpcassa/Connection/ConnectionPool.php';