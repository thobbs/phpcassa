<?php
namespace phpcassa\Connection;

use Thrift\Exception\TException;
use Thrift\Exception\TTransportException;

use phpcassa\Connection\ConnectionWrapper;
use phpcassa\Connection\MaxRetriesException;
use phpcassa\Connection\NoServerAvailable;

use cassandra\TimedOutException;
use cassandra\NotFoundException;
use cassandra\UnavailableException;

/**
 * A pool of connections to a set of servers in a cluster.
 * Each ConnectionPool is keyspace specific.
 *
 * @package phpcassa\Connection
 */
class ConnectionPool {

    const BASE_BACKOFF = 0.1;
    const MICROS = 1000000;
    const MAX_RETRIES = 2147483647; // 2^31 - 1

    const DEFAULT_MAX_RETRIES = 5;
    const DEFAULT_RECYCLE = 10000;

    protected static $default_servers = array('localhost:9160');

    public $keyspace;
    protected $servers;
    protected $pool_size;
    protected $send_timeout;
    protected $recv_timeout;
    protected $credentials;
    protected $framed_transport;
    protected $queue;
    protected $keyspace_description = NULL;

    /**
     * int $max_retries how many times an operation should be retried before
     *     throwing a MaxRetriesException. Using 0 disables retries; using -1 causes
     *     unlimited retries. The default is 5.
     */
    public $max_retries = self::DEFAULT_MAX_RETRIES;

    /**
     * int $recycle after this many operations, a connection will be automatically
     *     closed and replaced. Defaults to 10,000.
     */
    public $recycle = self::DEFAULT_RECYCLE;

    /**
     * Constructs a ConnectionPool.
     *
     * @param string $keyspace the keyspace all connections will use
     * @param mixed $servers an array of strings representing the servers to
     *        open connections to.  Each item in the array should be a string
     *        of the form 'host' or 'host:port'.  If a port is not given, 9160
     *        is assumed.  If $servers is NULL, 'localhost:9160' will be used.
     * @param int $pool_size the number of open connections to keep in the pool.
     *        If $pool_size is left as NULL, max(5, count($servers) * 2) will be
     *        used.
     * @param int $max_retries how many times an operation should be retried before
     *        throwing a MaxRetriesException. Using 0 disables retries; using -1 causes
     *        unlimited retries. The default is 5.
     * @param int $send_timeout the socket send timeout in milliseconds. Defaults to 5000.
     * @param int $recv_timeout the socket receive timeout in milliseconds. Defaults to 5000.
     * @param int $recycle after this many operations, a connection will be automatically
     *        closed and replaced. Defaults to 10,000.
     * @param mixed $credentials if using authentication or authorization with Cassandra,
     *        a username and password need to be supplied. This should be in the form
     *        array("username" => username, "password" => password)
     * @param bool $framed_transport whether to use framed transport or buffered transport.
     *        This must match Cassandra's configuration.  In Cassandra 0.7, framed transport
     *        is the default. The default value is true.
     */
    public function __construct($keyspace,
                                $servers=NULL,
                                $pool_size=NULL,
                                $max_retries=self::DEFAULT_MAX_RETRIES,
                                $send_timeout=5000,
                                $recv_timeout=5000,
                                $recycle=self::DEFAULT_RECYCLE,
                                $credentials=NULL,
                                $framed_transport=true)
    {
        $this->keyspace = $keyspace;
        $this->send_timeout = $send_timeout;
        $this->recv_timeout = $recv_timeout;
        $this->recycle = $recycle;
        $this->max_retries = $max_retries;
        $this->credentials = $credentials;
        $this->framed_transport = $framed_transport;

        $this->stats = array(
            'created' => 0,
            'failed' => 0,
            'recycled' => 0);

        if (is_null($servers))
            $servers = self::$default_servers;
        $this->servers = $servers;

        if (is_null($pool_size))
            $this->pool_size = max(count($this->servers) * 2, 5);
        else
            $this->pool_size = $pool_size;

        $this->queue = array();

        // Randomly permute the server list
        shuffle($this->servers);
        $this->list_position = 0;
    }

    protected function make_conn() {
        // Keep trying to make a new connection, stopping after we've
        // tried every server twice
        $err = "";
        foreach (range(1, count($this->servers) * 2) as $i)
        {
            try {
                $this->list_position = ($this->list_position + 1) % count($this->servers);
                $new_conn = new ConnectionWrapper($this->keyspace, $this->servers[$this->list_position],
                    $this->credentials, $this->framed_transport, $this->send_timeout, $this->recv_timeout);
                array_push($this->queue, $new_conn);
                $this->stats['created'] += 1;
                return;
            } catch (TException $e) {
                $h = $this->servers[$this->list_position];
                $err = $e;
                $msg = $e->getMessage();
                $class = get_class($e);
                $this->error_log("Error connecting to $h: $class: $msg", 0);
                $this->stats['failed'] += 1;
            }
        }
        throw new NoServerAvailable("An attempt was made to connect to every server twice, but " .
            "all attempts failed. The last error was: " . get_class($err) .":". $err->getMessage());
    }

    /**
     * Adds connections to the pool until $pool_size connections
     * are in the pool.
     */
    public function fill() {
        while (count($this->queue) < $this->pool_size)
            $this->make_conn();
    }

    /**
     * Retrieves a connection from the pool.
     *
     * If the pool has fewer than $pool_size connections in
     * it, a new connection will be created.
     *
     * @return ConnectionWrapper a connection
     */
    public function get() {
        $num_conns = count($this->queue);
        if ($num_conns < $this->pool_size) {
            try {
                $this->make_conn();
            } catch (NoServerAvailable $e) {
                if ($num_conns == 0)
                    throw $e;
            }
        }
        return array_shift($this->queue);
    }

    /**
     * Returns a connection to the pool.
     * @param ConnectionWrapper $connection
     */
    public function return_connection($connection) {
        if ($connection->op_count >= $this->recycle) {
            $this->stats['recycled'] += 1;
            $connection->close();
            $this->make_conn();
            $connection = $this->get();
        }
        array_push($this->queue, $connection);
    }

    /**
     * Gets the keyspace description, caching the results for later lookups.
     * @return mixed
     */
    public function describe_keyspace() {
        if (NULL === $this->keyspace_description) {
            $this->keyspace_description = $this->call("describe_keyspace", $this->keyspace);
        }

        return $this->keyspace_description;
    }

    /**
     * Closes all connections in the pool.
     */
    public function dispose() {
        foreach($this->queue as $conn)
            $conn->close();
    }

    /**
     * Closes all connections in the pool.
     */
    public function close() {
        $this->dispose();
    }

    /**
     * Returns information about the number of opened connections, failed
     * operations, and recycled connections.
     * @return array Stats in the form array("failed" => failure_count,
     *         "created" => creation_count, "recycled" => recycle_count)
     */
    public function stats() {
        return $this->stats;
    }

    /**
     * Performs a Thrift operation using a connection from the pool.
     * The first argument should be the name of the function. The following
     * arguments should be the arguments for that Thrift function.
     *
     * If the connect fails with any exception other than a NotFoundException,
     * the connection will be closed and replaced in the pool. If the
     * Exception is suitable for retrying the operation (TimedOutException,
     * UnavailableException, TTransportException), the operation will be
     * retried with a new connection after an exponentially increasing
     * backoff is performed.
     *
     * To avoid automatic retries, create a ConnectionPool with the
     * $max_retries argument set to 0.
     *
     * In general, this method should *not* be used by users of the
     * library. It is primarily intended for internal use, but is left
     * exposed as an open workaround if needed.
     *
     * @return mixed
     */
    public function call() {
        $args = func_get_args(); // Get all of the args passed to this function
        $f = array_shift($args); // pull the function from the beginning

        $retry_count = 0;
        if ($this->max_retries == -1)
            $tries =  self::MAX_RETRIES;
        elseif ($this->max_retries == 0)
            $tries = 1;
        else
            $tries = $this->max_retries + 1;

        foreach (range(1, $tries) as $retry_count) {
            $conn = $this->get();

            $conn->op_count += 1;
            try {
                $resp = call_user_func_array(array($conn->client, $f), $args);
                $this->return_connection($conn);
                return $resp;
            } catch (NotFoundException $nfe) {
                $this->return_connection($conn);
                throw $nfe;
            } catch (TimedOutException $toe) {
                $last_err = $toe;
                $this->handle_conn_failure($conn, $f, $toe, $retry_count);
            } catch (UnavailableException $ue) {
                $last_err = $ue;
                $this->handle_conn_failure($conn, $f, $ue, $retry_count);
            } catch (TTransportException $tte) {
                $last_err = $tte;
                $this->handle_conn_failure($conn, $f, $tte, $retry_count);
            } catch (\Exception $e) {
                $this->handle_conn_failure($conn, $f, $e, $retry_count);
                throw $e;
            }
        }
        throw new MaxRetriesException("An attempt to execute $f failed $tries times.".
            " The last error was " . get_class($last_err) . ":" . $last_err->getMessage());
    }

    protected function handle_conn_failure($conn, $f, $exc, $retry_count) {
        $err = (string)$exc;
        $this->error_log("Error performing $f on $conn->server: $err", 0);
        $conn->close();
        $this->stats['failed'] += 1;
        usleep(self::BASE_BACKOFF * pow(2, $retry_count) * self::MICROS);
        $this->make_conn();
    }

    /**
     * This method called every time an error is logged. By default, it will
     * call the PHP builtin function error_log() with a messageType of 0. To
     * change this behavior, you can create a subclass and override this
     * method.
     *
     * Note that PHP has strange logging behavior. In particular, if you are
     * running the PHP cli and you haven't set a directive for error_log in
     * your php.ini, this will log to stdout even if you've called
     * error_reporting(0), which is supposed to suppress all logging.
     *
     * @param string $errorMsg
     * @param int $messageType
     */
    protected function error_log($errorMsg, $messageType=0) {
        error_log($errorMsg, $messageType);
    }
}
