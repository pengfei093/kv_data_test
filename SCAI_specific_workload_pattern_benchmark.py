
import signal
import sys
import string
import argparse
import uuid
import time
from datetime import datetime, timedelta

from kv_database_test import KVDatabaseTest
from utils.kv_database_utils import create_cassandra_cluster
from utils.util import load_configs_from_files
import random

STRING_LENGTH = 4100
job_id = '_anomalous_country_communication'


class KVDatabaseSCAITest:
    def __init__(self, conf):
        self.conf = conf
        self.current_cassandra_cluster = create_cassandra_cluster(self.conf['cassandra_host'],
                                                                  self.conf['cassandra_port'])
        self.current_cassandra_session = self.current_cassandra_cluster.connect()

    def test_insert(self, iter_count=500000):
        t = 0
        rad_str = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                          for _ in range(STRING_LENGTH))
        file = open('insert_test.txt', 'w')
        job_ids = []
        for i in range(30):
            job_ids.append(str(i) + job_id)

        for c in range(iter_count):
            for i in range(30):
                try:
                    t1 = time.time()
                    self.current_cassandra_session.execute("INSERT INTO scai (id, time, job_id, data) "
                                                           "VALUES (%s, %s, %s, %s)",
                                                           (uuid.uuid4(), str(datetime.now() + c*timedelta(minutes=5)),
                                                            job_ids[i], rad_str))
                    t2 = time.time()
                    ct = t2 - t1
                    file.write(str(ct) + '\n')
                    t = t + ct
                except Exception as e:
                    time.sleep(0.01)
            if c % 1000 == 1:
                print(f'insert {c * 30} tooks {t} seconds')
        print(f"insert times tooks {t} seconds")

    def start_test(self):
        print('program start')
        self.current_cassandra_session.set_keyspace('scai_data_test')
        self.current_cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS scai (
                id uuid,
                time timestamp,
                job_id text,
                data text,
                PRIMARY KEY (id)
            );
            """
        )
        if self.conf['command'] == 'insert':
            self.test_insert()


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')
    parser.add_argument('--command', metavar='path', required=True,
                        help='the path to workspace')
    args = parser.parse_args()
    conf = load_configs_from_files("conf/kv_data_config.yaml")
    conf['command'] = args.command
    test = KVDatabaseSCAITest(conf)
    test.start_test()
