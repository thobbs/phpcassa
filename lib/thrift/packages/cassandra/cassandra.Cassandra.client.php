<?php
/**
 *  @generated
 */

namespace cassandra;

class CassandraClient implements \cassandra\CassandraIf {
  protected $input_ = null;
  protected $output_ = null;

  protected $seqid_ = 0;

  public function __construct($input, $output=null) {
    $this->input_ = $input;
    $this->output_ = $output ? $output : $input;
  }

  public function login($auth_request)
  {
    $this->send_login($auth_request);
    $this->recv_login();
  }

  public function send_login($auth_request)
  {
    $args = new \cassandra\Cassandra_login_args();
    $args->auth_request = $auth_request;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'login', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('login', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_login()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_login_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_login_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->authnx !== null) {
      throw $result->authnx;
    }
    if ($result->authzx !== null) {
      throw $result->authzx;
    }
    return;
  }

  public function set_keyspace($keyspace)
  {
    $this->send_set_keyspace($keyspace);
    $this->recv_set_keyspace();
  }

  public function send_set_keyspace($keyspace)
  {
    $args = new \cassandra\Cassandra_set_keyspace_args();
    $args->keyspace = $keyspace;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'set_keyspace', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('set_keyspace', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_set_keyspace()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_set_keyspace_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_set_keyspace_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    return;
  }

  public function get($key, $column_path, $consistency_level)
  {
    $this->send_get($key, $column_path, $consistency_level);
    return $this->recv_get();
  }

  public function send_get($key, $column_path, $consistency_level)
  {
    $args = new \cassandra\Cassandra_get_args();
    $args->key = $key;
    $args->column_path = $column_path;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'get', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('get', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_get()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_get_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_get_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->nfe !== null) {
      throw $result->nfe;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    throw new \Exception("get failed: unknown result");
  }

  public function get_slice($key, $column_parent, $predicate, $consistency_level)
  {
    $this->send_get_slice($key, $column_parent, $predicate, $consistency_level);
    return $this->recv_get_slice();
  }

  public function send_get_slice($key, $column_parent, $predicate, $consistency_level)
  {
    $args = new \cassandra\Cassandra_get_slice_args();
    $args->key = $key;
    $args->column_parent = $column_parent;
    $args->predicate = $predicate;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'get_slice', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('get_slice', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_get_slice()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_get_slice_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_get_slice_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    throw new \Exception("get_slice failed: unknown result");
  }

  public function get_count($key, $column_parent, $predicate, $consistency_level)
  {
    $this->send_get_count($key, $column_parent, $predicate, $consistency_level);
    return $this->recv_get_count();
  }

  public function send_get_count($key, $column_parent, $predicate, $consistency_level)
  {
    $args = new \cassandra\Cassandra_get_count_args();
    $args->key = $key;
    $args->column_parent = $column_parent;
    $args->predicate = $predicate;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'get_count', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('get_count', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_get_count()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_get_count_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_get_count_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    throw new \Exception("get_count failed: unknown result");
  }

  public function multiget_slice($keys, $column_parent, $predicate, $consistency_level)
  {
    $this->send_multiget_slice($keys, $column_parent, $predicate, $consistency_level);
    return $this->recv_multiget_slice();
  }

  public function send_multiget_slice($keys, $column_parent, $predicate, $consistency_level)
  {
    $args = new \cassandra\Cassandra_multiget_slice_args();
    $args->keys = $keys;
    $args->column_parent = $column_parent;
    $args->predicate = $predicate;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'multiget_slice', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('multiget_slice', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_multiget_slice()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_multiget_slice_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_multiget_slice_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    throw new \Exception("multiget_slice failed: unknown result");
  }

  public function multiget_count($keys, $column_parent, $predicate, $consistency_level)
  {
    $this->send_multiget_count($keys, $column_parent, $predicate, $consistency_level);
    return $this->recv_multiget_count();
  }

  public function send_multiget_count($keys, $column_parent, $predicate, $consistency_level)
  {
    $args = new \cassandra\Cassandra_multiget_count_args();
    $args->keys = $keys;
    $args->column_parent = $column_parent;
    $args->predicate = $predicate;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'multiget_count', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('multiget_count', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_multiget_count()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_multiget_count_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_multiget_count_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    throw new \Exception("multiget_count failed: unknown result");
  }

  public function get_range_slices($column_parent, $predicate, $range, $consistency_level)
  {
    $this->send_get_range_slices($column_parent, $predicate, $range, $consistency_level);
    return $this->recv_get_range_slices();
  }

  public function send_get_range_slices($column_parent, $predicate, $range, $consistency_level)
  {
    $args = new \cassandra\Cassandra_get_range_slices_args();
    $args->column_parent = $column_parent;
    $args->predicate = $predicate;
    $args->range = $range;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'get_range_slices', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('get_range_slices', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_get_range_slices()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_get_range_slices_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_get_range_slices_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    throw new \Exception("get_range_slices failed: unknown result");
  }

  public function get_paged_slice($column_family, $range, $start_column, $consistency_level)
  {
    $this->send_get_paged_slice($column_family, $range, $start_column, $consistency_level);
    return $this->recv_get_paged_slice();
  }

  public function send_get_paged_slice($column_family, $range, $start_column, $consistency_level)
  {
    $args = new \cassandra\Cassandra_get_paged_slice_args();
    $args->column_family = $column_family;
    $args->range = $range;
    $args->start_column = $start_column;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'get_paged_slice', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('get_paged_slice', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_get_paged_slice()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_get_paged_slice_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_get_paged_slice_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    throw new \Exception("get_paged_slice failed: unknown result");
  }

  public function get_indexed_slices($column_parent, $index_clause, $column_predicate, $consistency_level)
  {
    $this->send_get_indexed_slices($column_parent, $index_clause, $column_predicate, $consistency_level);
    return $this->recv_get_indexed_slices();
  }

  public function send_get_indexed_slices($column_parent, $index_clause, $column_predicate, $consistency_level)
  {
    $args = new \cassandra\Cassandra_get_indexed_slices_args();
    $args->column_parent = $column_parent;
    $args->index_clause = $index_clause;
    $args->column_predicate = $column_predicate;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'get_indexed_slices', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('get_indexed_slices', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_get_indexed_slices()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_get_indexed_slices_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_get_indexed_slices_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    throw new \Exception("get_indexed_slices failed: unknown result");
  }

  public function insert($key, $column_parent, $column, $consistency_level)
  {
    $this->send_insert($key, $column_parent, $column, $consistency_level);
    $this->recv_insert();
  }

  public function send_insert($key, $column_parent, $column, $consistency_level)
  {
    $args = new \cassandra\Cassandra_insert_args();
    $args->key = $key;
    $args->column_parent = $column_parent;
    $args->column = $column;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'insert', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('insert', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_insert()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_insert_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_insert_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    return;
  }

  public function add($key, $column_parent, $column, $consistency_level)
  {
    $this->send_add($key, $column_parent, $column, $consistency_level);
    $this->recv_add();
  }

  public function send_add($key, $column_parent, $column, $consistency_level)
  {
    $args = new \cassandra\Cassandra_add_args();
    $args->key = $key;
    $args->column_parent = $column_parent;
    $args->column = $column;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'add', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('add', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_add()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_add_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_add_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    return;
  }

  public function remove($key, $column_path, $timestamp, $consistency_level)
  {
    $this->send_remove($key, $column_path, $timestamp, $consistency_level);
    $this->recv_remove();
  }

  public function send_remove($key, $column_path, $timestamp, $consistency_level)
  {
    $args = new \cassandra\Cassandra_remove_args();
    $args->key = $key;
    $args->column_path = $column_path;
    $args->timestamp = $timestamp;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'remove', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('remove', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_remove()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_remove_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_remove_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    return;
  }

  public function remove_counter($key, $path, $consistency_level)
  {
    $this->send_remove_counter($key, $path, $consistency_level);
    $this->recv_remove_counter();
  }

  public function send_remove_counter($key, $path, $consistency_level)
  {
    $args = new \cassandra\Cassandra_remove_counter_args();
    $args->key = $key;
    $args->path = $path;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'remove_counter', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('remove_counter', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_remove_counter()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_remove_counter_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_remove_counter_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    return;
  }

  public function batch_mutate($mutation_map, $consistency_level)
  {
    $this->send_batch_mutate($mutation_map, $consistency_level);
    $this->recv_batch_mutate();
  }

  public function send_batch_mutate($mutation_map, $consistency_level)
  {
    $args = new \cassandra\Cassandra_batch_mutate_args();
    $args->mutation_map = $mutation_map;
    $args->consistency_level = $consistency_level;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'batch_mutate', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('batch_mutate', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_batch_mutate()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_batch_mutate_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_batch_mutate_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    return;
  }

  public function truncate($cfname)
  {
    $this->send_truncate($cfname);
    $this->recv_truncate();
  }

  public function send_truncate($cfname)
  {
    $args = new \cassandra\Cassandra_truncate_args();
    $args->cfname = $cfname;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'truncate', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('truncate', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_truncate()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_truncate_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_truncate_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    return;
  }

  public function describe_schema_versions()
  {
    $this->send_describe_schema_versions();
    return $this->recv_describe_schema_versions();
  }

  public function send_describe_schema_versions()
  {
    $args = new \cassandra\Cassandra_describe_schema_versions_args();
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'describe_schema_versions', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('describe_schema_versions', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_describe_schema_versions()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_describe_schema_versions_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_describe_schema_versions_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    throw new \Exception("describe_schema_versions failed: unknown result");
  }

  public function describe_keyspaces()
  {
    $this->send_describe_keyspaces();
    return $this->recv_describe_keyspaces();
  }

  public function send_describe_keyspaces()
  {
    $args = new \cassandra\Cassandra_describe_keyspaces_args();
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'describe_keyspaces', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('describe_keyspaces', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_describe_keyspaces()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_describe_keyspaces_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_describe_keyspaces_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    throw new \Exception("describe_keyspaces failed: unknown result");
  }

  public function describe_cluster_name()
  {
    $this->send_describe_cluster_name();
    return $this->recv_describe_cluster_name();
  }

  public function send_describe_cluster_name()
  {
    $args = new \cassandra\Cassandra_describe_cluster_name_args();
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'describe_cluster_name', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('describe_cluster_name', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_describe_cluster_name()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_describe_cluster_name_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_describe_cluster_name_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    throw new \Exception("describe_cluster_name failed: unknown result");
  }

  public function describe_version()
  {
    $this->send_describe_version();
    return $this->recv_describe_version();
  }

  public function send_describe_version()
  {
    $args = new \cassandra\Cassandra_describe_version_args();
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'describe_version', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('describe_version', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_describe_version()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_describe_version_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_describe_version_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    throw new \Exception("describe_version failed: unknown result");
  }

  public function describe_ring($keyspace)
  {
    $this->send_describe_ring($keyspace);
    return $this->recv_describe_ring();
  }

  public function send_describe_ring($keyspace)
  {
    $args = new \cassandra\Cassandra_describe_ring_args();
    $args->keyspace = $keyspace;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'describe_ring', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('describe_ring', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_describe_ring()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_describe_ring_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_describe_ring_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    throw new \Exception("describe_ring failed: unknown result");
  }

  public function describe_partitioner()
  {
    $this->send_describe_partitioner();
    return $this->recv_describe_partitioner();
  }

  public function send_describe_partitioner()
  {
    $args = new \cassandra\Cassandra_describe_partitioner_args();
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'describe_partitioner', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('describe_partitioner', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_describe_partitioner()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_describe_partitioner_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_describe_partitioner_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    throw new \Exception("describe_partitioner failed: unknown result");
  }

  public function describe_snitch()
  {
    $this->send_describe_snitch();
    return $this->recv_describe_snitch();
  }

  public function send_describe_snitch()
  {
    $args = new \cassandra\Cassandra_describe_snitch_args();
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'describe_snitch', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('describe_snitch', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_describe_snitch()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_describe_snitch_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_describe_snitch_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    throw new \Exception("describe_snitch failed: unknown result");
  }

  public function describe_keyspace($keyspace)
  {
    $this->send_describe_keyspace($keyspace);
    return $this->recv_describe_keyspace();
  }

  public function send_describe_keyspace($keyspace)
  {
    $args = new \cassandra\Cassandra_describe_keyspace_args();
    $args->keyspace = $keyspace;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'describe_keyspace', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('describe_keyspace', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_describe_keyspace()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_describe_keyspace_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_describe_keyspace_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->nfe !== null) {
      throw $result->nfe;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    throw new \Exception("describe_keyspace failed: unknown result");
  }

  public function describe_splits($cfName, $start_token, $end_token, $keys_per_split)
  {
    $this->send_describe_splits($cfName, $start_token, $end_token, $keys_per_split);
    return $this->recv_describe_splits();
  }

  public function send_describe_splits($cfName, $start_token, $end_token, $keys_per_split)
  {
    $args = new \cassandra\Cassandra_describe_splits_args();
    $args->cfName = $cfName;
    $args->start_token = $start_token;
    $args->end_token = $end_token;
    $args->keys_per_split = $keys_per_split;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'describe_splits', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('describe_splits', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_describe_splits()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_describe_splits_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_describe_splits_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    throw new \Exception("describe_splits failed: unknown result");
  }

  public function system_add_column_family($cf_def)
  {
    $this->send_system_add_column_family($cf_def);
    return $this->recv_system_add_column_family();
  }

  public function send_system_add_column_family($cf_def)
  {
    $args = new \cassandra\Cassandra_system_add_column_family_args();
    $args->cf_def = $cf_def;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'system_add_column_family', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('system_add_column_family', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_system_add_column_family()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_system_add_column_family_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_system_add_column_family_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->sde !== null) {
      throw $result->sde;
    }
    throw new \Exception("system_add_column_family failed: unknown result");
  }

  public function system_drop_column_family($column_family)
  {
    $this->send_system_drop_column_family($column_family);
    return $this->recv_system_drop_column_family();
  }

  public function send_system_drop_column_family($column_family)
  {
    $args = new \cassandra\Cassandra_system_drop_column_family_args();
    $args->column_family = $column_family;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'system_drop_column_family', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('system_drop_column_family', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_system_drop_column_family()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_system_drop_column_family_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_system_drop_column_family_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->sde !== null) {
      throw $result->sde;
    }
    throw new \Exception("system_drop_column_family failed: unknown result");
  }

  public function system_add_keyspace($ks_def)
  {
    $this->send_system_add_keyspace($ks_def);
    return $this->recv_system_add_keyspace();
  }

  public function send_system_add_keyspace($ks_def)
  {
    $args = new \cassandra\Cassandra_system_add_keyspace_args();
    $args->ks_def = $ks_def;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'system_add_keyspace', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('system_add_keyspace', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_system_add_keyspace()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_system_add_keyspace_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_system_add_keyspace_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->sde !== null) {
      throw $result->sde;
    }
    throw new \Exception("system_add_keyspace failed: unknown result");
  }

  public function system_drop_keyspace($keyspace)
  {
    $this->send_system_drop_keyspace($keyspace);
    return $this->recv_system_drop_keyspace();
  }

  public function send_system_drop_keyspace($keyspace)
  {
    $args = new \cassandra\Cassandra_system_drop_keyspace_args();
    $args->keyspace = $keyspace;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'system_drop_keyspace', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('system_drop_keyspace', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_system_drop_keyspace()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_system_drop_keyspace_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_system_drop_keyspace_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->sde !== null) {
      throw $result->sde;
    }
    throw new \Exception("system_drop_keyspace failed: unknown result");
  }

  public function system_update_keyspace($ks_def)
  {
    $this->send_system_update_keyspace($ks_def);
    return $this->recv_system_update_keyspace();
  }

  public function send_system_update_keyspace($ks_def)
  {
    $args = new \cassandra\Cassandra_system_update_keyspace_args();
    $args->ks_def = $ks_def;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'system_update_keyspace', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('system_update_keyspace', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_system_update_keyspace()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_system_update_keyspace_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_system_update_keyspace_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->sde !== null) {
      throw $result->sde;
    }
    throw new \Exception("system_update_keyspace failed: unknown result");
  }

  public function system_update_column_family($cf_def)
  {
    $this->send_system_update_column_family($cf_def);
    return $this->recv_system_update_column_family();
  }

  public function send_system_update_column_family($cf_def)
  {
    $args = new \cassandra\Cassandra_system_update_column_family_args();
    $args->cf_def = $cf_def;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'system_update_column_family', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('system_update_column_family', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_system_update_column_family()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_system_update_column_family_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_system_update_column_family_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->sde !== null) {
      throw $result->sde;
    }
    throw new \Exception("system_update_column_family failed: unknown result");
  }

  public function execute_cql_query($query, $compression)
  {
    $this->send_execute_cql_query($query, $compression);
    return $this->recv_execute_cql_query();
  }

  public function send_execute_cql_query($query, $compression)
  {
    $args = new \cassandra\Cassandra_execute_cql_query_args();
    $args->query = $query;
    $args->compression = $compression;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'execute_cql_query', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('execute_cql_query', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_execute_cql_query()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_execute_cql_query_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_execute_cql_query_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    if ($result->sde !== null) {
      throw $result->sde;
    }
    throw new \Exception("execute_cql_query failed: unknown result");
  }

  public function prepare_cql_query($query, $compression)
  {
    $this->send_prepare_cql_query($query, $compression);
    return $this->recv_prepare_cql_query();
  }

  public function send_prepare_cql_query($query, $compression)
  {
    $args = new \cassandra\Cassandra_prepare_cql_query_args();
    $args->query = $query;
    $args->compression = $compression;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'prepare_cql_query', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('prepare_cql_query', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_prepare_cql_query()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_prepare_cql_query_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_prepare_cql_query_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    throw new \Exception("prepare_cql_query failed: unknown result");
  }

  public function execute_prepared_cql_query($itemId, $values)
  {
    $this->send_execute_prepared_cql_query($itemId, $values);
    return $this->recv_execute_prepared_cql_query();
  }

  public function send_execute_prepared_cql_query($itemId, $values)
  {
    $args = new \cassandra\Cassandra_execute_prepared_cql_query_args();
    $args->itemId = $itemId;
    $args->values = $values;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'execute_prepared_cql_query', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('execute_prepared_cql_query', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_execute_prepared_cql_query()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_execute_prepared_cql_query_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_execute_prepared_cql_query_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->success !== null) {
      return $result->success;
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    if ($result->ue !== null) {
      throw $result->ue;
    }
    if ($result->te !== null) {
      throw $result->te;
    }
    if ($result->sde !== null) {
      throw $result->sde;
    }
    throw new \Exception("execute_prepared_cql_query failed: unknown result");
  }

  public function set_cql_version($version)
  {
    $this->send_set_cql_version($version);
    $this->recv_set_cql_version();
  }

  public function send_set_cql_version($version)
  {
    $args = new \cassandra\Cassandra_set_cql_version_args();
    $args->version = $version;
    $bin_accel = ($this->output_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_write_binary');
    if ($bin_accel)
    {
      thrift_protocol_write_binary($this->output_, 'set_cql_version', \TMessageType::CALL, $args, $this->seqid_, $this->output_->isStrictWrite());
    }
    else
    {
      $this->output_->writeMessageBegin('set_cql_version', \TMessageType::CALL, $this->seqid_);
      $args->write($this->output_);
      $this->output_->writeMessageEnd();
      $this->output_->getTransport()->flush();
    }
  }

  public function recv_set_cql_version()
  {
    $bin_accel = ($this->input_ instanceof \TProtocol::$TBINARYPROTOCOLACCELERATED) && function_exists('thrift_protocol_read_binary');
    if ($bin_accel) $result = thrift_protocol_read_binary($this->input_, '\cassandra\Cassandra_set_cql_version_result', $this->input_->isStrictRead());
    else
    {
      $rseqid = 0;
      $fname = null;
      $mtype = 0;

      $this->input_->readMessageBegin($fname, $mtype, $rseqid);
      if ($mtype == \TMessageType::EXCEPTION) {
        $x = new \TApplicationException();
        $x->read($this->input_);
        $this->input_->readMessageEnd();
        throw $x;
      }
      $result = new \cassandra\Cassandra_set_cql_version_result();
      $result->read($this->input_);
      $this->input_->readMessageEnd();
    }
    if ($result->ire !== null) {
      throw $result->ire;
    }
    return;
  }

}


?>
