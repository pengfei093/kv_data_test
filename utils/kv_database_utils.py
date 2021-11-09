from cassandra.cluster import Cluster


def create_cassandra_cluster(cassandra_host, cassandra_port):
    cluster = Cluster([cassandra_host], port=cassandra_port)
    return cluster


def cassandra_set_val():
    pass
