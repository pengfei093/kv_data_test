from cassandra.cluster import Cluster
import redis


def create_cassandra_cluster(cassandra_host, cassandra_port):
    cluster = Cluster([cassandra_host], port=cassandra_port)
    return cluster


def cassandra_set_val():
    pass


def create_pika_cluster(pika_host, pika_port):
    return redis.Redis(host=pika_host, port=pika_port, db=0)
