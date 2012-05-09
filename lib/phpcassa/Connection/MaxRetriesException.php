<?php
namespace phpcassa\Connection;

/**
 * The connection pool has retried an operation as many
 * times as is allowed by your $max_retries setting.
 *
 * @package phpcassa\Connection
 */
class MaxRetriesException extends \Exception { }
