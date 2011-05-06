Tutorial
========

This tutorial is intended as an introduction to working with
Cassandra and **phpcassa**.

Prerequisites
-------------
Before we start, make sure that you have **phpcassa**
:doc:`installed <installation>`. The following
should execute without raising an exception:

.. code-block:: php

    require_once('phpcassa/connection.php');
    require_once('phpcassa/columnfamily.php');

This tutorial also assumes that a Cassandra instance is running on the
default host and port. Read the `instructions for getting started
with Cassandra <http://wiki.apache.org/cassandra/GettingStarted>`_. 
You can start Cassandra like so:

.. code-block:: bash

  $ pwd
  ~/cassandra
  $ bin/cassandra -f

Creating a Keyspace and Column Families
---------------------------------------
We need to create a keyspace and some column families to work with.

The cassandra-cli utility is included with Cassandra; it allows you to create
and modify the schema, explore or modify data, and examine a few things about
your cluster.  Here's how to create the keyspace and column family we need
for this tutorial:

.. code-block:: none

    user@~ $ cassandra-cli 
    Welcome to cassandra CLI.

    Type 'help;' or '?' for help. Type 'quit;' or 'exit;' to quit.
    [default@unknown] connect localhost/9160;
    Connected to: "Test Cluster" on localhost/9160
    [default@unknown] create keyspace Keyspace1;
    4f9e42c4-645e-11e0-ad9e-e700f669bcfc
    Waiting for schema agreement...
    ... schemas agree across the cluster
    [default@unknown] use Keyspace1;
    Authenticated to keyspace: Keyspace1
    [default@Keyspace1] create column family ColumnFamily1;
    632cf985-645e-11e0-ad9e-e700f669bcfc
    Waiting for schema agreement...
    ... schemas agree across the cluster
    [default@Keyspace1] quit;
    user@~ $

This connects to a local instance of Cassandra and creates a keyspace
named 'Keyspace1' with a column family named 'ColumnFamily1'.

Making a Connection
-------------------
The first step when working with **phpcassa** is to create a
`ConnectionPool <api/phpcassa/connection/ConnectionPool>`_ which
will connect to the Cassandra instance(s):

.. code-block:: php

  $pool = new ConnectionPool("Keyspace");

The above code will connect on the default host and port. We can also
specify a list of 'host:port' combinations like this:

.. code-block:: php

  $servers = array("192.168.2.1:9160", "192.168.2.2:9160");
  $pool = new ConnectionPool("Keyspace1", $servers);

If omitted, the port defaults to 9160.

Getting a ColumnFamily
----------------------
A column family is a collection of rows and columns in Cassandra,
and can be thought of as roughly the equivalent of a table in a
relational database. We'll use one of the column families that
were already included in the schema file:

.. code-block:: php

  $column_family = new ColumnFamily($pool, 'ColumnFamily1');

Inserting Data
--------------
To insert a row into a column family we can use the
`ColumnFamily::insert() <api/phpcassa/columnfamily/ColumnFamily#insert>`_ method:

.. code-block:: php

  $column_family->insert('row_key', array('col_name' => 'col_val'));

We can also insert more than one column at a time:

.. code-block:: php

  $column_family->insert('row_key', array('name1' => 'val1', 'name2' => 'val2'));

And we can insert more than one row at a time:

.. code-block:: php

  $row1 = array('name1' => 'val1', 'name2' => 'val2');
  $row2 = array('foo' => 'bar');
  $column_family->batch_insert(array('row1' => $row1, 'row2' => $row2);

Getting Data
------------
There are many more ways to get data out of Cassandra than there are
to insert data.

The simplest way to get data is to use
`ColumnFamily::get() <api/phpcassa/columnfamily/ColumnFamily#get>`_

.. code-block:: php

  $column_family->get('row_key');
  // returns: array('colname' => 'col_val')

Without any other arguments, :meth:`ColumnFamily::get()`
returns every column in the row (up to `$column_count`, which defaults to 100).
If you only want a few of the columns and you know them by name, you can
specify them using a `$columns` argument:

.. code-block:: php

  $column_family->get('row_key', $columns=array('name1', 'name2'));
  // returns: array('name1' => 'foo', 'name2' => 'bar')

We may also get a slice (or subrange) or the columns in a row. To do this,
use the `$column_start` and `$column_finish` parameters.  One or both of these may
be left empty to allow the slice to extend to one or both ends the.
Note that `$column_finish` is inclusive. Assuming we've inserted several
columns with names '1' through '9', we can do the following:

.. code-block:: php

  $column_family->get('row_key', $columns=null, $column_start='5', $column_finish='7');
  // returns: array('5' => 'foo', '6' => 'bar', '7' => 'baz')

There are also two ways to get multiple rows at the same time.
The first is to specify them by name using
`ColumnFamily::multiget() <api/phpcassa/columnfamily/ColumnFamily#multiget>`_

.. code-block:: php

  $column_family->multiget(['row_key1', 'row_key2']);
  // returns: array('row_key1' => array('name' => 'val'), 'row_key2' => array('name' => 'val'))

The other way is to get a range of keys at once by using
`ColumnFamily::get_range() <api/phpcassa/columnfamily/ColumnFamily#get_range>`_.
The parameter `$key_finish` is also inclusive here, too.  Assuming we've inserted
some rows with keys 'row_key1' through 'row_key9', we can do this:

.. code-block:: php

  $rows = $column_family->get_range($key_start='row_key5', $key_finish='row_key7');
  // returns an Iterator over:
  // array('row_key5' => array('name' => 'val'),
  //       'row_key6' => array('name' => 'val'),
  //       'row_key7' => array('name' => 'val'))

  foreach($rows as $key => $columns) {
      // Do stuff with $key or $columns
      Print_r($columns);
  }

It's also possible to specify a set of columns or a slice for 
`ColumnFamily::multiget() <api/phpcassa/columnfamily/ColumnFamily#multiget>`_
and
`ColumnFamily::get_range() <api/phpcassa/columnfamily/ColumnFamily#get_range>`_,
just like we did for
`ColumnFamily::get() <api/phpcassa/columnfamily/ColumnFamily#get>`_

Removing Data
-------------
You may remove data from a column family with
`ColumnFamily::remove() <api/phpcassa/columnfamily/ColumnFamily#remove>`_.

You can remove an entire row at once:

.. code-block:: php

  $column_family->remove('key');

Or a specific set of columns from a row:

.. code-block:: php

  $column_family->remove('key', array("col1", "col2");

You cannot remove a slice of columns from a row.

Counting
--------
If you just want to know how many columns are in a row, you can use
`ColumnFamily::get_count() <api/phpcassa/columnfamily/ColumnFamily#get_count>`_:

.. code-block:: php

  $column_family->get_count('row_key');
  // returns: 3

If you only want to get a count of the number of columns that are inside
of a slice or have particular names, you can do that as well:

.. code-block:: php

  $column_family->get_count('row_key', $columns=array('foo', 'bar'));
  // returns: 2
  $column_family->get_count('row_key', $column_start='foo');
  // returns: 3

You can also do this in parallel for multiple rows using
`ColumnFamily::multiget_count() <api/phpcassa/columnfamily/ColumnFamily#multiget_count>`_:

.. code-block:: php

  $column_family->multiget_count(array('fib0', 'fib1', 'fib2', 'fib3', 'fib4'));
  // returns: array('fib0' => 1, 'fib1' => 1, 'fib2' => 2, 'fib3' => 3, 'fib4' => 5)

.. code-block:: php

  $column_family->multiget_count(array('fib0', 'fib1', 'fib2', 'fib3', 'fib4'),
                                 $columns=array('col1', 'col2', 'col3'));
  // returns: array('fib0' => 1, 'fib1' => 1, 'fib2' => 2, 'fib3' => 3, 'fib4' => 3)

.. code-block:: php

  $column_family->multiget_count(array('fib0', 'fib1', 'fib2', 'fib3', 'fib4'),
                                 $columns=null, $column_start='col1', $column_finish='col3')
  // returns: array('fib0' => 1, 'fib1' => 1, 'fib2' => 2, 'fib3' => 3, 'fib4' => 3)

Super Columns
-------------
Cassandra allows you to group columns in "super columns" when using
super column families.  You can create a super column family using
cassandra-cli like this:

.. code-block:: none

    [default@Keyspace1] create column family Super1 with column_type=Super;
    632cf985-645e-11e0-ad9e-e700f669bcfc

To use a super column in **phpcassa**, you only need to
add an extra level to the array:

.. code-block:: php

  $column_family = new ColumnFamily($conn, 'Super1');
  $column_family->insert('row_key', array('supercol_name' => array('col_name' => 'col_val')));
  $column_family->get('row_key');
  // returns: array('supercol_name' => ('col_name' => 'col_val'))
  $column_family->remove('row_key', NULL, 'supercolumn_name');

Typed Column Names and Values
-----------------------------
In Cassandra 0.7, you can specify a comparator type for column names
and a validator type for column values.

The types available are:

* BytesType - no type
* IntegerType - 32 bit integer
* LongType - 64 bit integer
* AsciiType - ASCII string
* UTF8Type - UTF8 encoded string
* TimeUUIDType - version 1 UUID (timestamp based)
* LexicalUUID - non-version 1 UUID

The column name comparator types affect how columns are sorted within
a row. You can use these with standard column families as well as with
super column families; with super column families, the subcolumns may
even have a different comparator type.  Here's an example ``cassandra.yaml``:

::

  - name: StandardInt
    column_type: Standard
    compare_with: IntegerType

  - name: SuperLongSubAscii
    column_type: Super
    compare_with: LongType
    compare_subcolumns_with: AsciiType

Cassandra still requires you to pack these types into a binary format it
can understand.  Fortunately, when **phpcassa** sees that a column family
uses these types, it knows to pack and unpack these data types automatically
for you. So, if we want to write to the StandardInt column family, we can do
the following:

.. code-block:: php

  $column_family = new ColumnFamily($conn, 'StandardInt');
  $column_family->insert('row_key', array(42 => 'some_val'));
  $column_family->get('row_key')
  // returns: array(42 => 'some_val')

Notice that 42 is an integer here, not a string.

As mentioned above, Cassandra also offers validators on column values with
the same set of types.  Validators can be set for an entire column family,
for individual columns, or both.  Here's another example ``cassandra.yaml``:

::

  - name: AllLongs
    column_type: Standard
    default_validation_class: LongType

  - name: OneUUID
    column_type: Standard
    column_metadata:
      - name: uuid
        validator_class: TimeUUIDType

  - name: LongsExceptUUID
    column_type: Standard
    default_validation_class: LongType
    column_metadata:
      - name: uuid
        validator_class: TimeUUIDType

**phpcassa** knows to pack these column values automatically too:

.. code-block:: php

  $column_family = new ColumnFamily($connection, 'LongsExceptUUID')
  $column_family->insert('row_key', array('foo'  123456789, 'uuid' => CassandraUtil::uuid1()));
  $column_family->get('row_key');
  // returns: array('foo' => 123456789, 'uuid' => UUID('5880c4b8-bd1a-11df-bbe1-00234d21610a'))

Of course, if **phpcassa**'s automatic behavior isn't working for you, you
can turn it off when you create the
`ColumnFamily <api/phpcassa/columnfamily/ColumnFamily>`_:

.. code-block:: php

  $column_family = new ColumnFamily($conn, 'ColumnFamily1',
                                    $autopack_names=False,
                                    $autopack_values=False);


Indexes
-------
Cassandra 0.7.0 adds support for secondary indexes, which allow you to
efficiently get only rows which match a certain expression.

To use secondary indexes with Cassandra, you need to specify what columns
will be indexed.  In a ``cassandra.yaml`` file, this might look like:

::

  - name: Indexed1
    column_type: Standard
    column_metadata:
      - name: birthdate
        validator_class: LongType
        index_type: KEYS

In order to use 
`ColumnFamily::get_indexed_slices() <api/phpcassa/columnfamily/ColumnFamily#get_indexed_slices>`_
to get data from Indexed1 using the indexed column, we need to create an 
`IndexClause <http://thobbs.github.com/phpcassa/api/phpcassa/cassandra_IndexClause.html>`_
which contains a list of
`IndexExpression <http://thobbs.github.com/phpcassa/api/phpcassa/cassandra_IndexExpression.html>`_
objects.  The functions 
`CassandraUtil::create_index_expression() <api/phpcassa/columnfamily/CassandraUtil#create_index_expression>`_
and
`CassandraUtil::create_index_clause() <api/phpcassa/columnfamily/CassandraUtil#create_index_clause>`_
are designed to make this easier.

Suppose we are only interested in rows where 'birthdate' is 1984. We might do
the following:

.. code-block:: php

  $column_family = new ColumnFamily($conn, 'Indexed1');
  $index_exp = CassandraUtil::create_index_expression('birthdate', 1984);
  $index_clause = CassandraUtil::create_index_clause(array($index_exp));
  $rows = $column_family->get_indexed_slices($index_clause);
  // returns an Iterator over:
  //    array('winston smith' => array('birthdate' => 1984))

  foreach($rows as $key => $columns) {
      // Do stuff with $key and $columns
      Print_r($columns)
  }

Although at least one 
`IndexExpression <http://thobbs.github.com/phpcassa/api/phpcassa/cassandra_IndexExpression.html>`_
in every clause must be on an indexed column, you may also have other expressions
which are on non-indexed columns.
