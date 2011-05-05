phpcassa |release| Documentation
================================

Contents
--------
**phpcassa** is a PHP client for
`Apache Cassandra <http://cassandra.apache.org>`_.


:doc:`installation`
  How to install **phpcassa**.

:doc:`tutorial`
  A short overview of **phpcassa** usage.

`API Documentation <api/index.html>`_
  The **phpcassa** API documentation.

:doc:`troubleshooting`
  Troubleshooting connections and other problems.


Help
------------
**Mailing List**

* Mail to `phpcassa@googlegroups.com <mailto:phpcassa@googlegroups.com>`_ or
  `view online <http://groups.google.com/group/phpcassa>`_.

**IRC**

* Use the #cassandra channel on `irc.freenode.net <http://freenode.net>`_.
  If you don't have an IRC client, you can use
  `freenode's web based client <http://webchat.freenode.net/?channels=#cassandra>`_.

Issues
------
Bugs and feature requests for **phpcassa** are currently tracked through the `github issue tracker <http://github.com/thobbs/phpcassa/issues>`_.

Contributing
------------
**phpcassa** encourages you to offer any contributions or ideas you have.
Contributing to the documentation or examples, reporting bugs, requesting
features, and (of course) improving the code are all equally welcome.
To contribute, fork the project on
`github <http://github.com/thobbs/phpcassa/>`_ and make a pull request.

Changes
-------
The :doc:`changelog` lists the changes between versions of **phpcassa**.

About This Documentation
------------------------
This documentation is generated using the `Sphinx
<http://sphinx.pocoo.org/>`_ documentation generator. The source files
for the documentation are located in the `doc/` directory of 
**phpcassa**. To generate the documentation, run the
following command from the `doc` directory:

.. code-block:: bash

  $ make html

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. toctree::
   :hidden:

   installation
   tutorial
   troubleshooting
   changelog
