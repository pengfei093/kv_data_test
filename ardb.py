import redis_test

r = redis_test.Redis(host='10.33.4.229', port=6379, db=0)
r.zadd("job1", {'a': 12})
# r.zadd('job1', {'b': 3})
r.zadd('job1', {'c': 4})
print(r.zrangebyscore('job1', 3, 12, withscores=True))