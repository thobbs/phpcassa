<?php
$GLOBALS['THRIFT_ROOT'] = dirname(__FILE__) . '/thrift/';
require_once $GLOBALS['THRIFT_ROOT'].'/packages/cassandra/Cassandra.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php';

/**
 * @package phpcassa
 * @subpackage connection
 */
class NoServerAvailable extends Exception { }

/**
 * @package phpcassa
 * @subpackage connection
 */
class Connection {

    private static $default_servers = array(array('host' => 'localhost', 'port' => 9160));

    public $keyspace;

    public function __construct($keyspace,
                                $servers=null,
                                $credentials=null,
                                $framed_transport=True,
                                $send_timeout=null,
                                $recv_timeout=null,
                                $retry_time=10) {

        $this->keyspace = $keyspace;
        if ($servers == null)
            $servers = self::$default_servers;
        $this->servers = new ServerSet($servers, $retry_time);
        $this->credentials = $credentials;
        $this->framed_transport = $framed_transport;
        $this->send_timeout = $send_timeout;
        $this->recv_timeout = $recv_timeout;

        $this->connection = null;
    }

    public function connect() {
        try {
            $server = $this->servers->get();
            if (!$this->connection) {
                $this->connection = new ClientTransport($this->keyspace,
                                                        $server,
                                                        $this->credentials,
                                                        $this->framed_transport,
                                                        $this->send_timeout,
                                                        $this->recv_timeout);
            }
        } catch (TException $e) {
            $this->servers->mark_dead($server);
            return $this->connect();
        }
        return $this->connection->client;
    }

    public function close() {
        if ($this->connection)
            $this->connection->transport->close();
    }
}

/**
 * @access private
 */
class ClientTransport {

    public function __construct($keyspace,
                                $server,
                                $credentials,
                                $framed_transport,
                                $send_timeout,
                                $recv_timeout) {

        $host = $server['host'];
        $port = $server['port'];
        $socket = new TSocket($host, $port);

        if($send_timeout) $socket->setSendTimeout($send_timeout);
        if($recv_timeout) $socket->setRecvTimeout($recv_timeout);

        if($framed_transport) {
            $transport = new TFramedTransport($socket, true, true);
        } else {
            $transport = new TBufferedTransport($socket, 1024, 1024);
        }

        $client = new CassandraClient(new TBinaryProtocolAccelerated($transport));
        $transport->open();

        # TODO check API major version match

        $client->set_keyspace($keyspace);

        if ($credentials) {
            $request = cassandra_AuthenticationRequest($credentials);
            $client->login($request);
        }

        $this->keyspace = $keyspace;
        $this->client = $client;
        $this->transport = $transport;
    }
}

/**
 * @access private
 */
class ServerSet {

    private $servers = array();
    private $dead = array();

    public function __construct($servers, $retry_time=10) {
        foreach($servers as $server)
            $this->servers[$server['host'].$server['port']] = $server;
        $this->retry_time = $retry_time;
    }

    public function get() {
        if (count($this->dead) != 0) {
            $revived = array_pop($this->dead);
            if ($revived['time'] > time())  # Not yet, put it back
                $this->dead[] = $revived;
            else 
                $this->servers[$revived['host'.$revived['port']]] = $revived['server'];
        }
        if (!count($this->servers))
            throw new NoServerAvailable();
        
        return $this->servers[array_rand($this->servers)];
    }

    public function mark_dead($server) {
        unset($this->servers[$server['host'].$server['port']]);
        array_unshift($this->dead,
                array('time' => time() + $this->retry_time, 'server' => $server));
    }
}
?>
