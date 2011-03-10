<?php

class phpcassa_Connection extends phpcassa_Connection_ConnectionPool {
    // Here for backwards compatibility reasons only
    public function __construct($keyspace,
                                $servers=NULL,
                                $max_retries=5,
                                $send_timeout=5000,
                                $recv_timeout=5000,
                                $recycle=10000,
                                $credentials=NULL,
                                $framed_transport=true)
    {
        if ($servers != NULL) {
            $new_servers = array();
            foreach ($servers as $server) {
                $new_servers[] = $server['host'] . ':' . (string)$server['port'];
            }
        } else {
            $new_servers = NULL;
        }

        parent::__construct($keyspace, $new_servers, $max_retries, $send_timeout,
            $recv_timeout, $recycle, $credentials, $framed_transport);
    }
}
