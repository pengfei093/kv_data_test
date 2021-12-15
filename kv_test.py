import redis

r = redis.Redis(host='10.33.4.229', port=6666, db=0)
r.zadd("job1", {'a': 13})
r.zadd('job1', {'b': 3})
# r.zadd('job1', {'c': 4})
print(r.zrangebyscore('job1', '4', '14', withscores=True))