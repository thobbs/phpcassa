.. _installing:

Installing
==========
Copying the `lib` directory into your include path and requiring the
`lib/autoload.php` file should be enough to begin using phpcassa.
This will not automatically allow the C extension to be used, though.

C Extension
-----------
The C extension is crucial for phpcassa's performance.

You need to configure and make to be able to use the C extension:

*Note*: if `checkinstall` is available, run `sudo checkinstall` in place of
`sudo make install`.

.. code-block:: bash

    cd ext/thrift_protocol
    phpize
    ./configure
    make
    sudo make install

Add the following line to your php.ini file:

::

    extension=thrift_protocol.so

