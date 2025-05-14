"""
Session factory for connecting to a Cassandra cluster using a white-list policy.
"""
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy
from cassandra import ConsistencyLevel
from cassandra.cluster import Session


def make_session(host: str, port: int) -> Session:
    """
    Create and return a Cassandra session with a white-list round-robin policy.

    :param host: Hostname or IP of the contact point.
    :param port: Thrift/native transport port of the cluster.
    :return: Connected Cassandra Session.
    """
    # Define consistency and load balancing
    profile = ExecutionProfile(
        consistency_level=ConsistencyLevel.ONE,
        load_balancing_policy=WhiteListRoundRobinPolicy([host]),
    )
    # Build cluster and connect
    cluster = Cluster(
        contact_points=[host], port=port,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    )
    return cluster.connect()
