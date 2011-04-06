.. _installing:

Installing
==========
Copying the `phpcassa` directory into your path should be enough to
begin using **phpcassa**.  This will not automatically allow the
C extension to be used, though.

C Extension
-----------
The C extension is crucial for phpcassa's performance.

You need to configure and make to be able to use the C extension:

.. code-block:: bash

    cd thrift/ext/thrift_protocol
    phpize
    ./configure
    make
    sudo make install

Add the following line to your php.ini file:

::

    extension=thrift_protocol.so

