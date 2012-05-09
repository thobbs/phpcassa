<?php
namespace phpcassa\Connection;

/**
 * The connection pool was unable to successfully open a
 * connection to any of the servers after trying each
 * server twice.
 *
 * @package phpcassa\Connection
 */
class NoServerAvailable extends \Exception { }
