Tutorial
========

This tutorial is intended as an introduction to working with
Cassandra and phpcassa.

Prerequisites
-------------
Before we start, make sure that you have phpcassa
:doc:`installed <installation>`.

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

    [default@unknown] create keyspace Keyspace1
        with placement_strategy = 'org.apache.cassandra.locator.SimpleStrategy'
        and strategy_options = {replication_factor:1};
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

Autoload and Imports
--------------------
You can bootstrap the PHP autoloader to easily import phpcassa
classes by requiring or including :file:`lib/phpcassa/autoload.php`.

To start the tutorial, require the bootstrap file in one way or
another and import a couple of classes:

.. code-block:: php

    <?php
        require('lib/phpcassa/autoload.php');

        use phpcassa\ColumnFamily;
        use phpcassa\ColumnSlice;
        use phpcassa\Connection\ConnectionPool;

        // tutorial code starts here
    ?>

Making a Connection
-------------------
The first step when working with phpcassa is to create a
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
`ColumnFamily::get() <api/class-phpcassa.ColumnFamily.html#_get>`_

.. code-block:: php

  $column_family->get('row_key');
  // returns: array('colname' => 'col_val')

Without any other arguments, :meth:`ColumnFamily::get()`
returns every column in the row (up to `$column_count`, which defaults to 100).
If you only want a few of the columns and you know them by name, you can
specify them using a `$column_names` argument:

.. code-block:: php

  $column_names = array('name1', 'name2');
  $column_family->get('row_key', $column_slice=null, $column_names=$column_names);
  // returns: array('name1' => 'foo', 'name2' => 'bar')

We may also get a slice (or subrange) or the columns in a row. To do this,
we need to make a `ColumnSlice <api/class-phpcassa.ColumnSlice.html>`_ object.

The first two parameters are `$start` and `$finish`.  One or both of these may
be left as empty strings to allow the slice to extend to one or both ends of
the row.

Assuming we've inserted several
columns with names '1' through '9', we can do the following:

.. code-block:: php

  $slice = new ColumnSlice('5', '7');
  $column_family->get('row_key', $slice);
  // returns: array('5' => 'foo', '6' => 'bar', '7' => 'baz')

There are also two ways to get multiple rows at the same time.
The first is to specify them by name using
`ColumnFamily::multiget() <api/class-phpcassa.ColumnFamily.html#_multiget>`_

.. code-block:: php

  $column_family->multiget(['row_key1', 'row_key2']);
  // returns: array('row_key1' => array('name' => 'val'), 'row_key2' => array('name' => 'val'))

The other way is to get a range of keys at once by using
`ColumnFamily::get_range() <api/class-phpcassa.ColumnFamily.html#_get_range>`_
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

.. note:: Cassandra must be using an OrderPreservingPartitioner for you to be
          able to get a meaningful range of rows; the default, RandomPartitioner,
          stores rows in the order of the MD5 hash of their keys. See
          http://www.datastax.com/docs/1.0/cluster_architecture/partitioning.

It's also possible to specify a set of columns or a slice for
`multiget()` and `get_range()` just like we did for `get()`.

Removing Data
-------------
You may remove data from a column family with
`ColumnFamily::remove() <api/class-phpcassa.ColumnFamily.html#_remove>`_.

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
`ColumnFamily::get_count() <api/class-phpcassa.ColumnFamily.html#_get_count>`_:

.. code-block:: php

  $column_family->get_count('row_key');
  // returns: 3

If you only want to get a count of the number of columns that are inside
of a slice or have particular names, you can do that as well:

.. code-block:: php

  $column_names=array('foo', 'bar');
  $column_family->get_count('row_key', $column_slice=null, $column_names=$column_names);
  // returns: 2

  $slice = new ColumnSlice('foo');
  $column_family->get_count('row_key', $slice);
  // returns: 3

You can also do this in parallel for multiple rows using
`ColumnFamily::multiget_count() <api/class-phpcassa.ColumnFamily.html#_multiget_count>`_:

.. code-block:: php

  $column_family->multiget_count(array('fib0', 'fib1', 'fib2', 'fib3', 'fib4'));
  // returns: array('fib0' => 1, 'fib1' => 1, 'fib2' => 2, 'fib3' => 3, 'fib4' => 5)

.. code-block:: php

  $names = array('col1', 'col2', 'col3');
  $column_family->multiget_count(array('fib0', 'fib1', 'fib2', 'fib3', 'fib4'),
                                 $column_slice=null, $column_names=$names);
  // returns: array('fib0' => 1, 'fib1' => 1, 'fib2' => 2, 'fib3' => 3, 'fib4' => 3)

.. code-block:: php

  $slice = new ColumnSlice('col1', 'col3');
  $column_family->multiget_count(array('fib0', 'fib1', 'fib2', 'fib3', 'fib4'), $slice);
  // returns: array('fib0' => 1, 'fib1' => 1, 'fib2' => 2, 'fib3' => 3, 'fib4' => 3)

Super Columns
-------------
Cassandra allows you to group columns in "super columns" when using
super column families.  You can create a super column family using
cassandra-cli like this:

.. code-block:: none

    [default@Keyspace1] create column family Super1 with column_type=Super;
    632cf985-645e-11e0-ad9e-e700f669bcfc

To use a super columns in phpcassa, you need to create an instance
of ``phpcassa\SuperColumnFamily``:

.. code-block:: php

  use phpcassa\SuperColumnFamily;


  $column_family = new SuperColumnFamily($conn, 'Super1');
  $column_family->insert('row_key', array('supercol_name' => array('col_name' => 'col_val')));
  $column_family->get('row_key');
  // returns: array('supercol_name' => ('col_name' => 'col_val'))
  $column_family->remove_super_column('row_key', 'supercolumn_name');

Typed Column Names and Values
-----------------------------
In Cassandra, you can specify a comparator type for column names
and a validator type for row keys and column values.

There are
`several types supported by default <api/namespace-phpcassa.Schema.DataType.html>`_.

The column name comparator types affect how columns are sorted within
a row. You can use these with standard column families as well as with
super column families; with super column families, the subcolumns may
even have a different comparator type.

Cassandra requires clients to pack these types into a binary format it
can understand.  When phpcassa sees that a column family uses these types,
it knows to pack and unpack these data types automatically.
So, if we want to write to the StandardInt column family, we can do
the following:

.. code-block:: php

  $column_family = new ColumnFamily($conn, 'StandardInt');
  $column_family->insert('row_key', array(42 => 'some_val'));
  $column_family->get('row_key')
  // returns: array(42 => 'some_val')

Notice that 42 is an integer here, not a string.

As mentioned above, Cassandra also offers validators on column values with
the same set of types.  Validators can be set for an entire column family,
for individual columns, or both:

.. code-block:: php

  $column_family = new ColumnFamily($pool, 'Users')
  $column_family->insert($user_id, array('name' => 'joe', 'age' => 23));
  $column_family->get('row_key');
  // returns: array('name' => 'joe', 'age' => 23)

Of course, if phpcassa's automatic behavior isn't working for you, you
can turn it off with
`ColumnFamily::$autopack_names <api/class-phpcassa.ColumnFamily.html#$autopack_names>`_
and the other `$autopack` attributes.

.. code-block:: php

  $column_family = new ColumnFamily($conn, 'ColumnFamily1');
  $column_family->$autopack_names=False;
  $column_family->$autopack_values=False;


Indexes
-------
Cassandra supports built-in secondary indexes, which allow you to
efficiently get only rows which match a certain expression.

In order to use
`ColumnFamily::get_indexed_slices() <api/class-phpcassa.ColumnFamily#_get_indexed_slices>`_
, we need to create an
`IndexClause <api/class-phpcassa.Index.IndexClause>`_
which contains a list of
`IndexExpression <api/class-phpcassa.Index.IndexExpression>`_
objects.

Suppose we are only interested in rows where 'birthdate' is 1984. We might do
the following:

.. code-block:: php

  use phpcassa\SystemManager;
  use phpcassa\Index\IndexExpression;
  use phpcassa\Index\IndexClause;
  use phpcassa\Schema\DataType\LongType;

  $sys = new SystemManager();
  $sys->create_column_family('Keyspace', 'Users');
  $sys->create_index('Keyspace', 'Users', 'birthdate', LongType);

  $column_family = new ColumnFamily($conn, 'Users');
  $column_family->insert('winston', array('birthdate' => 1984));

  $index_exp = new IndexExpression('birthdate', 1984);
  $index_clause = new IndexClause(array($index_exp));
  $rows = $column_family->get_indexed_slices($index_clause);
  // returns an Iterator over:
  //    array('winston smith' => array('birthdate' => 1984))

  foreach($rows as $key => $columns) {
      // Do stuff with $key and $columns
      Print_r($columns);
  }

Although at least one `IndexExpression` in every clause must use an equality
match against an indexed column, you may also have other expressions which are
apply to non-indexed columns or use comparators other than `"EQ"`.
