Changelog
=========

Changes in 0.7.a.4
------------------

The bundled Thrift library has been updated to a post-0.6 trunk
version.  An important bugfix was made in Thrift which greatly
improves performance.

Bugfixes
^^^^^^^^
- Credentials were not properly passed through Thrift, causing
  any authorization attempts to fail.

Features
^^^^^^^^
- Added the ConnectionPool class to connection.php. This allows
  for better failover and loadbalancing of operations. See the
  documentation for the ConnectionPool class when upgrading from
  0.7.a.3 or earlier.

Deprecated
^^^^^^^^^^
- The Connection class in connection.php has been replaced by
  ConnectionPool and is now deprecated.

Changes in 0.7.a.3
------------------

Bugfixes
^^^^^^^^
- Typo in throwing IncompatibleAPIException
- remove() on super column families did not pack names correctly
- CassandraUtil::uuid3() param name should be $node not $null

Features
^^^^^^^^
- Use remove() Thrift API call instead of batch_mutate() when possible
- Allow a microsecond timestamp to be passed in for v1 UUID creation
- Log connection errors with error_log()

Deprecated
^^^^^^^^^^
None

Changes in 0.7.a.2
------------------

Bugfixes
^^^^^^^^
- Fix server revival bug
- Remove print statement from Connection on connection failure

Features
^^^^^^^^
- Add an import() method for UUIDs to CassandraUtil to convert binary UUID
  representations back into UUID objects

Deprecated
^^^^^^^^^^^^
None

Changes in 0.7.a1
-----------------
Initial release
