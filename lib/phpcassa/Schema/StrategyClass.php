<?php
namespace phpcassa\Schema;

/**
 * Replication strategies for keyspaces.
 *
 * @package phpcassa\Schema
 */
class StrategyClass {

    /** Ignores node DCs and racks. */
    const SIMPLE_STRATEGY = "SimpleStrategy";

    /** Allows a replication factor per-DC. */
    const NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";

    /** Only available for backwards-compatibility. */
    const OLD_NETWORK_TOPOLOGY_STRATEGY = "OldNetworkTopologyStrategy";
}

