
"""
securities


"""

import logging

from redis import StrictRedis
from rediscluster import StrictRedisCluster
from multiprocessing import Process, Queue
import msgpack

# Modify this to get best perfomance when it comes to big data store and update, according to your machine
# multiply the number of socket, cores and threads. i.e.for devpmapp3 the number should be 4*1*1=4
CPU_CORES = 4

REDIS_ZJ = {"host":"127.0.0.1", "port":"6379"}
REDIS_CYAN = {"host":"10.155.44.113","port":"6382"}
STARTUP_NODES = [{"host":"10.155.44.115","port":"6380"},
                 {"host":"10.155.44.117","port":"6379"},
                 {"host":"10.155.44.112","port":"6383"},
                 {"host":"10.155.44.113","port":"6384"},
                 {"host":"10.155.44.113","port":"6382"},
                 {"host":"10.155.44.114","port":"6381"},
                 ]

my_redis = STARTUP_NODES
#my_redis = REDIS_ZJ

# Logging configuration
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
DATE_FORMAT = "%m/%d/%Y %H:%M:%S %p"
logging.basicConfig(filename='security.log', level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT)


class Security:
    """Store and get data from redis

    Api will be exposed to users: store() gets() getall() deletes()

    How to use:
        Security.store({'ssm_id':"test1", 'a':9.32})
        Security.store([{'ssm_id':"test1", 'a':9.32},{'ssm_id':"test2", 'b':4.32, 'jwzh':5.11}])
        Security.gets(['test1','test2'])
        Security.getall()
        Security.deletes(['id1','id2'])

    """

    #r = StrictRedis(**my_redis)
    r = StrictRedisCluster(startup_nodes=my_redis)

    # Record all ssm_id of data stored in redis
    stored_ssm_id = 'Security_V2_stored_ssm_id'


    @staticmethod
    def _serialize(obj_dict):
        return msgpack.packb(obj_dict)
        #return jsonpickle.dumps(obj_dict)

    @staticmethod
    def _deserialize(dumps):
        return msgpack.unpackb(dumps)
        #return jsonpickle.loads(dumps)

    @classmethod
    def _pre_processing(cls, sec_datas):
        ids = [sec_data['ssm_id'] for sec_data in sec_datas]
        olds = cls.gets(ids)
        for i in range(len(sec_datas)):
            if olds[i]:
                olds[i].update(sec_datas[i])
                sec_datas[i] = olds[i]

    @classmethod
    def store(cls, sec_datas, protection=True):
        """store data into redis

        Args:
            A list of sec datas:
            [{'ssm_id':'9128286R6', 'a1':7.42, 'b2':8.23},{'ssm_id':'1128286R6', 'c1':7.42, 'b2':8.23}]

            protection:
             protection=False means don't need to protect old data, so will save time in pre-processing
             set protection=False to make batch store fast
             default is True

        Returns:
            A list of failed ssm_id
            if all success, then return []
            TODOexception processing according to real production environment

        """
        failed_ids = []
        # For single data store, allow store({})
        if type(sec_datas)==dict :
            try:
                #sec_datas = cls._pre_processing([sec_datas])[0]
                sec_datas = [sec_datas]
                cls._pre_processing(sec_datas)
                dumps = cls._serialize(sec_datas[0])
                cls.r.set(sec_datas[0]['ssm_id'], dumps)
                cls.r.sadd(cls.stored_ssm_id, sec_datas[0]['ssm_id'])
            except Exception as e:
                logging.error('Security.store() error:')
                logging.error(e)
                failed_ids.append(sec_datas['ssm_id'])
            return failed_ids

        if len(sec_datas)==0:
            return None # Can't return a empty list, which means all data stored successfully

        if len(sec_datas)<10000: # IF the data not too big, use one pipeline to store in redis
            if protection:
                cls._pre_processing(sec_datas)
            return cls._small_store(sec_datas)

        else:# If the data is big, use multi process to store in redis
            #import pdb
            #pdb.set_trace()
            if protection:# Protect old attributes
                cls._pre_processing(sec_datas)

            return cls._multiprocess_store(sec_datas)


    @classmethod
    def gets(cls, ssm_ids):
        """get from redis by a list of ssm_id

        Args:
            a list of ssm id []
        Returns:
            a list of sec data [{},{}]

        """
        #r = redis.StrictRedis(decode_responses=True, **my_redis)
        # Allow gets({}) for single data
        if type(ssm_ids)!=list:
            try:
                dumps = cls.r.get(ssm_ids)
                data = cls._deserialize(dumps)
            except Exception as e:
                data = None
                logging.error(e)
            return [data]

        pipe = cls.r.pipeline(transaction=False)# transaction=False turn off atomic
        for ssm_id in ssm_ids:
            pipe.get(ssm_id)
        try:
            result = pipe.execute()
            return [cls._deserialize(dump) if dump else None for dump in result]
        except Exception as e:
            logging.error('pipeline gets error')
            logging.error(e)

    @classmethod
    def getall(cls):
        """TODOReturn all sec data stored in redis through our API

        Return:
            a list of data
            if error, return False

        """
        try:
            stored_ssm_id = cls.r.smembers(cls.stored_ssm_id)
            return list(stored_ssm_id)
        except Exception as e:
            logging.error('getall redis smembers error: ')
            logging.error(e)
            return False

    @classmethod
    def _small_store(cls, sec_datas):
        """store datas with one single pipeline

        Input:
            sec_datas : must be a list
            [{'ssm_id':'9128286R6', 'a1':7.42, 'b2':8.23},{'ssm_id':'1128286R6', 'c1':7.42, 'b2':8.23}]
        Returns:
            a list of failed ids

            TODO exception processing according to real product environment
        """
        failed_ids = []
        succeed_ids = []
        pipe = cls.r.pipeline(transaction=False)# transaction=False turn off atomic
        for sec_data in sec_datas:
            dumps = cls._serialize(sec_data)
            pipe.set(sec_data['ssm_id'], dumps)
        try:
            result = pipe.execute()
            for v in zip(result,sec_datas):
                if v[0]==True:# if success, records all succeed ssm ids
                    succeed_ids.append(v[1]['ssm_id'])

                else: # if set failed
                    failed_ids.append(v[1]['ssm_id'])

            # records all succeed ssm id
            try:
                cls.r.sadd(cls.stored_ssm_id,*succeed_ids)
            except Exception as e:
                logging.error('small store sadd error: ')
                logging.error(e)

            return failed_ids

        except Exception as e:
            logging.error('small store error: ')
            logging.error(e)
            failed_ids = [sec_data['ssm_id'] for sec_data in sec_datas]
            return failed_ids

    @classmethod
    def _multiprocess_store(cls, sec_datas):
        """
        Use multi process to store a big amount of data

        Input:
            sec_datas : must be a list
            [{'ssm_id':'9128286R6', 'a1':7.42, 'b2':8.23},{'ssm_id':'1128286R6', 'c1':7.42, 'b2':8.23}]
        Returns:
            a list of failed ids

        """

        def pipeline_store(i,q):

            pipe = cls.r.pipeline(transaction=False)# transaction=False turn off atomic
            slices = len(sec_datas)/groups
            data_slice = sec_datas[i*slices:(i+1)*slices]# split the datas into groups
            succeed_ids = []
            for sec_data in data_slice:
                dumps = cls._serialize(sec_data)
                pipe.set(sec_data['ssm_id'], dumps)
            try:
                result = pipe.execute()
                #print 'process: ',os.getpid(),' pipeline returns ',result[:10]
                for v in zip(result,data_slice):
                    if v[0]==True:# if hmset success, records ssm_id
                        succeed_ids.append(v[1]['ssm_id'])
                    else:# if hmset failed
                        q.put(v[1]['ssm_id'])

                # records all succeed ssm id
                try:
                    cls.r.sadd(cls.stored_ssm_id,*succeed_ids)
                except Exception as e:
                    logging.error('small store sadd error: ')
                    logging.error(e)

            except Exception as e:
                # If pipe error, all the data in this thread stored failed
                print 'multi process pipeline error, please check in log'
                logging.error('pipe error!')
                logging.error(e)

        groups = CPU_CORES
        failed_ids = []
        failed_ids_queue = Queue()
        process_list = []

        for i in range(groups):
            p = Process(target=pipeline_store ,args=(i,failed_ids_queue))
            process_list.append(p)
        for p in process_list:
            p.start()
        for p in process_list:
            p.join()

        #Read from queue to get the failed ids
        failed_ids_queue.put(None)
        for ids in iter(failed_ids_queue.get, None):
            failed_ids.append(ids)
        return failed_ids

    @classmethod
    def deletes(cls, ids):
        result = cls.r.delete(*ids)
        print result,' datas was deleted!'

