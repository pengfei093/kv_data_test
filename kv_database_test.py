import profile
import string
import random
import time
from utils.kv_database_utils import create_cassandra_cluster
import signal

# from memory_profiler import profile
from utils.profiling_utils import timefn
from utils.util import load_configs_from_files

STRING_LENGTH = 4100


class KVDatabaseTest:
    def __init__(self, conf):
        self.conf = conf
        self.current_cassandra_cluster = create_cassandra_cluster(self.conf['cassandra_host'],
                                                                  self.conf['cassandra_port'])
        self.current_cassandra_session = self.current_cassandra_cluster.connect()

    # @profile
    def test_insert(self, iter_count=100000):
        t = 0
        for _ in range(iter_count):
            rad_str = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                              for _ in range(STRING_LENGTH))
            t1 = time.time()
            self.current_cassandra_session.execute("INSERT INTO users (name) VALUES (%s)", (rad_str,))
            t2 = time.time()
            t = t + t2 - t1
        print(f"insert tooks {t} seconds")

    def start_test(self, set_val, get_val):
        self.current_cassandra_session.set_keyspace('test')
        self.current_cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                name text,
                PRIMARY KEY (name)
            );
            """
        )
        self.test_insert()

    def start(self):
        self.start_test(set_val=True, get_val=True)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    conf = load_configs_from_files("conf/kv_data_config.yaml")
    test = KVDatabaseTest(conf)
    test.start()
