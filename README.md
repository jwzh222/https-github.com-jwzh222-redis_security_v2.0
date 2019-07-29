# py-redis-api
store and get sec data by redis
serialise and deserialise by msgpack is the fastest


class Security:
    """Store and get data from redis

    Api will be exposed to users: store() gets() getall() deletes()

    How to use:
        Security.store({'ssm_id':"test1", 'a':9.32})
        Security.store({'ssm_id':"test1", 'a':9.32}, protection=True) can make it faster, but will overwrite old attributes data
        Security.store([{'ssm_id':"test1", 'a':9.32},{'ssm_id':"test2", 'b':4.32, 'jwzh':5.11}])
        Security.gets(['test1','test2'])
        Security.getall()
        Security.deletes(['id1','id2'])

    """




# How to use
 1.pip install -r requirements.txt
 2.configuration:  modify my_redis in security.py ,  modify _AMOUNT in test.py to generate different set of test data
 3.python test.py



