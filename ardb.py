import redis

r = redis.Redis(host='10.33.4.229', port=16379, db=0)
r.zadd("job1", {'a': 1.2})
# r.zadd('job1', {'b': 3})
# r.zadd('job1', {'c': 4})
print(r.zrange('0_anomalous_country_communication', 0, -1))