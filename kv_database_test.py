import profile
import string
import random
import time
from utils.kv_database_utils import create_cassandra_cluster
import signal

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
        file = open('insert_test.txt', 'w')
        for c in range(iter_count):
            rad_str = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                              for _ in range(STRING_LENGTH))
            t1 = time.time()
            self.current_cassandra_session.execute("INSERT INTO users (name) VALUES (%s)", (rad_str,))
            t2 = time.time()
            ct = t2 - t1
            file.write(str(ct) + '\n')
            t = t + ct
            if c % 100 == 1:
                print(f'iterate {c} times: {t / c}s')
        print(f"insert tooks {t} seconds")

    def test_query(self, iter_count=100000):
        t = 0
        file = open('query_test.txt', 'w')
        for c in range(iter_count):
            try:
                t1 = time.time()
                rows = self.current_cassandra_session.execute('SELECT name FROM users')
                t2 = time.time()
                ct = t2 - t1
                file.write(str(ct) + '\n')
                t = t + ct
                if c % 100 == 1:
                    print(f'iterate {c} times: {t / c}s')
            except Exception as e:
                time.sleep(1)
        print(f"query tooks {t} seconds")
        file.close()

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
        self.test_query()

    def start(self):
        self.start_test(set_val=True, get_val=True)


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    conf = load_configs_from_files("conf/kv_data_config.yaml")
    test = KVDatabaseTest(conf)
    test.start()
