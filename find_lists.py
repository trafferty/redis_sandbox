import redis
redis_server = redis.StrictRedis(host='localhost', port=6379, db=0)

key = "*"

mid_results = []
cur, results = redis_server.scan(0, key, 1000)
mid_results += results

while cur != 0:
    cur, results = redis_server.scan(cur, key, 1000)
    mid_results += results

keys_that_are_type_list = []
for key in mid_results:
    if redis_server.type(key).decode() == 'list':
        keys_that_are_type_list.append(key)
