<?php
namespace phpcassa;

/**
 * @package phpcassa
 */
class ConsistencyLevel {
    const ANY          = \cassandra_ConsistencyLevel::ANY;
    const ONE          = \cassandra_ConsistencyLevel::ONE;
    const TWO          = \cassandra_ConsistencyLevel::TWO;
    const THREE        = \cassandra_ConsistencyLevel::THREE;
    const QUORUM       = \cassandra_ConsistencyLevel::QUORUM;
    const LOCAL_QUORUM = \cassandra_ConsistencyLevel::LOCAL_QUORUM;
    const EACH_QUORUM  = \cassandra_ConsistencyLevel::EACH_QUORUM;
    const ALL          = \cassandra_ConsistencyLevel::ALL;
}
