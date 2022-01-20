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
ITER_COUNT = 500000
JOB_NUM = 30
PRINT_GAP = 1000
KEYSPACE = 'scai_data_test'
INSERT_FILE_NAME = 'cass_test_insert_qps_multi.txt'
QUERY_FILE_NAME = 'cass_test_query_qps_multi.txt'


class KVDatabaseSCAITest:
    def __init__(self, conf):
        self.conf = conf
        self.current_cassandra_cluster = create_cassandra_cluster(self.conf['cassandra_host'],
                                                                  self.conf['cassandra_port'])
        self.current_cassandra_session = self.current_cassandra_cluster.connect()

    def test_insert(self, iter_count=ITER_COUNT):
        t = 0
        rad_str = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                          for _ in range(STRING_LENGTH))
        file = open(INSERT_FILE_NAME, 'w')
        job_ids = []
        for i in range(30):
            job_ids.append(str(i) + job_id)

        datetime_now = datetime.now()
        for c in range(iter_count):
            for i in range(JOB_NUM):
                try:
                    t1 = time.time()
                    self.current_cassandra_session.execute("INSERT INTO scai (id, time, job_id, data) "
                                                           "VALUES (%s, %s, %s, %s)",
                                                           (
                                                               uuid.uuid4(),
                                                               str(datetime_now + c * timedelta(minutes=5)),
                                                               job_ids[i], rad_str))
                    t2 = time.time()
                    time.sleep(300)
                    ct = t2 - t1
                    file.write(str(ct) + '\n')
                    t = t + ct
                except Exception as e:
                    print(e)
                    time.sleep(0.01)
            if c % 1000 == 1:
                print(f'insert {c * JOB_NUM} tooks {t} seconds')
        print(f"insert times tooks {t} seconds")

    def test_query(self, iter_count=ITER_COUNT):
        t = 0
        file = open(QUERY_FILE_NAME, 'w')
        for c in range(iter_count):
            try:
                rand_id = str(random.randint(1, JOB_NUM))
                job = str(rand_id) + job_id
                start_time = datetime.now() + c * timedelta(minutes=5)
                end_time = start_time + timedelta(days=1)
                start_time = str(start_time)
                end_time = str(end_time)

                t1 = time.time()
                rows = self.current_cassandra_session.execute('SELECT * FROM scai where job_id=%s AND '
                                                              'time >= %s AND time <= %s',
                                                              [job, start_time, end_time])
                t2 = time.time()
                ct = t2 - t1
                file.write(str(ct) + '\n')
                t = t + ct
                if c % 1000 == 1:
                    print(f'query {c} times tooks {t} seconds')
            except Exception as e:
                print(e)
                time.sleep(0.01)
        print(f"query tooks {t} seconds")

    def start_test(self):
        print('program start')
        self.current_cassandra_session.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            """ % KEYSPACE)
        self.current_cassandra_session.set_keyspace(KEYSPACE)
        self.current_cassandra_session.execute(
            """
            CREATE TABLE IF NOT EXISTS scai (
                id uuid,
                time timestamp,
                job_id text,
                data text,
                PRIMARY KEY (job_id, time)
            );
            """
        )
        if self.conf['command'] == 'insert':
            self.test_insert()
        elif self.conf['command'] == 'query':
            self.test_query()


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
