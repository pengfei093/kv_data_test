import argparse
import string
import signal
import random
import time

from datetime import datetime, timedelta
from utils.kv_database_utils import create_pika_cluster
from utils.util import load_configs_from_files

ITER_COUNT = 500
STRING_LENGTH = 4100
job_id_postfix = '_anomalous_country_communication'
INSERT_FILE_NAME = 'pika_encode_scai_insert.txt'
QUERY_FILE_NAME = 'pika_encode_scai_query.txt'
JOB_NUM = 1


class PikaSCAITestEncoding:
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
        for job_index in range(30):
            job_ids.append(str(job_index) + job_id_postfix)

        cur_time = datetime.strptime('26 Sep 2012', '%d %b %Y')
        for c in range(iter_count):
            for job_index in range(JOB_NUM):
                try:
                    save_time = int((cur_time + c * timedelta(minutes=5)).timestamp())
                    saved_data = rad_str

                    t1 = time.time()
                    self.current_pika_cluster.hset(job_ids[job_index], save_time, saved_data)
                    t2 = time.time()

                    running_time = t2 - t1
                    file.write(str(running_time) + '\n')
                    t = t + running_time

                except Exception as e:
                    print(e)
                    time.sleep(0.01)
            if c % 1000 == 1:
                print(f'insert {c * JOB_NUM} tooks {t} seconds')
        print(f"insert times tooks {t} seconds")

    def start_test(self):
        print('program start')
        if self.conf['command'] == 'insert':
            self.test_insert()
        elif self.conf['command'] == 'query':
            self.test_query()

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
                # data_pipeline = self.current_pika_cluster.pipeline(transaction=False)
                rows = []
                for cur_time_str in range(start_time, end_time):
                    h = self.current_pika_cluster.hget(job, cur_time_str)
                    if h:
                        rows.append(h)
                t2 = time.time()
                # rows = []
                # for h in data_pipeline.execute():
                #     print(h)
                #     if h:
                #         rows.append(h)
                running_time = t2 - t1
                file.write(str(running_time) + '\n')
                t = t + running_time
                if c % 1000 == 1:
                    print(f'query {c} times tooks {t} seconds')
            except Exception as e:
                print(e)
                time.sleep(0.01)
        print(f"query tooks {t} seconds")


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    parser = argparse.ArgumentParser(description='Create a ArcHydro schema')
    parser.add_argument('--command', metavar='path', required=True,
                        help='the path to workspace')
    args = parser.parse_args()
    conf = load_configs_from_files("conf/kv_data_config.yaml")
    conf['command'] = args.command
    test = PikaSCAITestEncoding(conf)
    test.start_test()
