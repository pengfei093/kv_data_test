import argparse
import string
import signal
import random
import time

from datetime import datetime, timedelta
from utils.kv_database_utils import create_kvrocksdb_cluster
from utils.util import load_configs_from_files

ITER_COUNT = 1500000
STRING_LENGTH = 4100
job_id_postfix = '_'
INSERT_FILE_NAME = 'kvR_large_test_insert_qps.txt'
QUERY_FILE_NAME = 'kvR_large_test_query_qps.txt'
JOB_NUM = 30


class KvRocksdbSCAITest:
    def __init__(self, conf):
        self.conf = conf
        self.current_kvrocksdb_cluster = create_kvrocksdb_cluster(self.conf['kvrocksdb_host'],
                                                                  self.conf['kvrocksdb_port'])

    def test_insert(self, iter_count=ITER_COUNT):
        t = 0
        rad_str = ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits)
                          for _ in range(STRING_LENGTH))
        file = open(INSERT_FILE_NAME, 'w')
        job_ids = []
        for i in range(JOB_NUM):
            job_ids.append(str(i) + job_id_postfix)

        cur_time = datetime.strptime('26 Sep 2012', '%d %b %Y')
        for c in range(iter_count):
            for i in range(JOB_NUM):
                try:
                    save_time = int((cur_time + c * timedelta(minutes=5)).timestamp())
                    saved_data = str(save_time) + rad_str
                    t1 = time.time()
                    self.current_kvrocksdb_cluster.zadd(job_ids[i], {saved_data: save_time})
                    t2 = time.time()
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
        cur_time = datetime.strptime('26 Sep 2012', '%d %b %Y')
        for c in range(iter_count):
            try:
                rand_id = str(random.randint(1, JOB_NUM) - 1)
                job = str(rand_id) + job_id_postfix
                start_time = cur_time + c * timedelta(minutes=5)
                end_time = start_time + timedelta(days=1)
                start_time = int(start_time.timestamp())
                end_time = int(end_time.timestamp())

                t1 = time.time()
                rows = self.current_kvrocksdb_cluster.zrangebyscore(job, start_time - 1, end_time, withscores=True)
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

    def del_job(self, jobs):
        for job in jobs:
            self.current_kvrocksdb_cluster.add(job)

    def start_test(self):
        print('program start')
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
    test = KvRocksdbSCAITest(conf)
    test.start_test()
