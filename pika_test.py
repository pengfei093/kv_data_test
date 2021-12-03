import argparse
import string
import redis
import signal
import random
import time

from datetime import datetime, timedelta
from utils.kv_database_utils import create_pika_cluster
from utils.util import load_configs_from_files

# r = redis.Redis(host='10.33.4.233', port=9221, db=0)
# r.zadd("job1", {'a': 1.2})
# r.zadd('job1', {'b': 3})
# r.zadd('job1', {'c': 4})
# print(r.zrangebyscore('job1', 0.1, 5))
# compare two method: 1. encode key by ourselves. 2. zrange.

# r.set('foo', 'bar')
# print(r.get('foo'))
ITER_COUNT = 500000
STRING_LENGTH = 4100
job_id = '_anomalous_country_communication'
INSERT_FILE_NAME = 'pika_scai_insert.txt'
QUERY_FILE_NAME = 'pika_scai_query.txt'
JOB_NUM = 30


class PikaSCAITest:
    def __init__(self, conf):
        self.conf = conf
        self.current_pika_cluster = create_pika_cluster(self.conf['pika_host'],
                                                        self.conf['pika_port'])

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
                    # r.zadd("job1", {'a': 1})
                    self.current_pika_cluster.zadd(job_ids[i], {rad_str: datetime_now + c * timedelta(minutes=5)})
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
        for c in range(iter_count):
            try:
                rand_id = str(random.randint(1, JOB_NUM))
                job = str(rand_id) + job_id
                start_time = datetime.now() + c * timedelta(minutes=5)
                end_time = start_time + timedelta(days=1)
                start_time = str(start_time)
                end_time = str(end_time)

                t1 = time.time()
                rows = self.current_pika_cluster.zrangebyscore(job, start_time, end_time)
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
    test = PikaSCAITest(conf)
    test.start_test()
