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


# example
"""
data = [{'ssm_id':'V2778286R7', 'duration':7.42, 'oas':8.23, 'pimco':1.33},{'ssm_id':'SD332X6R7', 'c1':7.42, 'b2':8.23}]
(support flexible attributes)
Security.store(data,protection=True) 
in order to avoid overwriting old attributes, a pre-processing should be implemented before redis set.
protection=True means the old attributes should be protected, and default value is True.
however when you don't need old data, set protection=False will save a lot time.


Check in test.py for more details to use

"""

