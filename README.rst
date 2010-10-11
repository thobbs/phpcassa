phpcassa
========

* In progress toward compatibility with Cassandra 0.7
* Direct port of pycassa

Including files
---------------

    <?php

    // Copy all the files in this repository to your include directory.

    $GLOBALS['THRIFT_ROOT'] = dirname(__FILE__) . '/include/thrift/';
    require_once $GLOBALS['THRIFT_ROOT'].'/packages/cassandra/Cassandra.php';
    require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
    require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
    require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';
    require_once $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php';

    include_once(dirname(__FILE__) . '/include/phpcassa.php');
    include_once(dirname(__FILE__) . '/include/uuid.php');

    ?>

Setting up nodes
----------------

    <?php
    CassandraConn::add_node('192.168.1.1', 9160);
    CassandraConn::add_node('192.168.1.2', 5000);
    ?>

Create a column family object
-----------------------------

    <?php
    $users = new CassandraCF('Keyspace1', 'Users'); // ColumnFamily
    $super = new CassandraCF('Keyspace1', 'SuperColumn', true); // SuperColumnFamily
    ?>

Inserting
---------

    <?php
    $users->insert('1', array('email' => 'hoan.tonthat@gmail.com', 'password' => 'test'));
    ?>

Querying
--------

    <?php
    $users->get('1'); // array('email' => 'hoan.tonthat@gmail.com', 'password' => 'test')
    $users->multiget(array('1', '2')); // array('1' => array('email' => 'hoan.tonthat@gmail.com', 'password' => 'test'))
    ?>

Removing
--------

    <?php
    $users->remove('1'); // removes whole object
    $users->remove('1', 'password'); // removes 'password' field
    ?>

Other
-----

    <?php
    $users->get_count('1'); // counts the number of columns in user 1 (in this case 2)
    $users->get_range('1', '10'); // gets all users between '1' and '10'
    ?>

Getting Help
------------

* Mailing list: http://groups.google.com/group/phpcassa
* IRC: Channel #cassandra on irc.freenode.net

AUTHORS
-------

* Hoan Ton-That (hoan.tonthat@gmail.com)
* Benjamin Sussman (ben@fwix.com)
* Anthony ROUX (anthony.rx43@gmail.com)
* Vadim Derkach
* Zach Buller (zachbuller@gmail.com)
* Timandes
* Todd Zusman
* Yancho Georgiev (yancho@inspirestudio.net)
* Pieter Maes (maescool@gmail.com)
