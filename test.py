
import security as sec
import test_engine as zj_test_engine
from datetime import datetime

_AMOUNT = 50000
_COLUMNS = 200

def test_case():
    # In the morning, Li Lei run some linear regression analysis scripts for the 500K number of securities, and generate 200 attributes for each security.
    # the data was like: [{'ssm_id':'V2778286R7', 'duration':7.42, 'oas':8.23, 'pimco':1.33},{'ssm_id':'SD332X6R7', 'c1':7.42, 'b2':8.23}]
    source_data = pandas_generate_big_data()
    print 'starts testing Security API!!!'
    print '-----------------------------------------------------------'

    #Test case 1:
    #In the moring, Li Lei want to store all these 500K*200 data into redis
    time1 = datetime.now()
    print 'store begins: ',time1.time()
    sec.Security.store(source_data, protection=False)
    #protection=True means pre-processing will impliemented to protect old data
    #protection=False means data will be stored into redis without a pre-processing,
    #because all the 200 attributes have new value, we don't need to protect old data in this case ,so use protection=False can be faster
    time2 = datetime.now()
    print 'store finished, use: ',time2-time1

    #Test case 2:
    #Li lei want to update one attribute for one security, and dont want to affect other attributes
    checkpoint_ssm_id = 'test_V2_ssm_id9'
    sec_data1 = {'ssm_id':checkpoint_ssm_id, 'a1':9.99, 'z99':3.3}
    print 'checkpoint before update :',sec.Security.gets(checkpoint_ssm_id)
    #you don't want to lose other attributes in this case, don't use protection=False
    sec.Security.store(sec_data1)
    print '-----------------------------------------------------------'
    #sec.Security.store(sec_data1,protection=True)  is also OK
    #sec.Security.store(sec_data1,protection=False) is WRONG, this will overwrite other attributes
    print 'checkpoint after update :',sec.Security.gets(checkpoint_ssm_id)

    #Test case 3:
    #get a list of data from redis
    all_ids = sec.Security.getall()
    time1 = datetime.now()
    print 'gets begins: ',time1.time()
    sec_datas = sec.Security.gets(all_ids)
    #print sec_datas[0]
    time2 = datetime.now()
    print 'gets finished, use: ',time2-time1

def test_update():
    """Store one sec data into redis, and updata some attributes, check these attributes

    for the same sec_id, use sec_obj.store() twice:
    {'ssm_id':"singleV2_test1", 'a':9.32, 'b':1.23}
    {'ssm_id':'singleV2_test1', 'a':8.23, 'c':1.19, 'jw':1.23}

    check these attributes, excepted result should be:
    {'ssm_id':'singleV2_test1', 'a':8.23, 'c':1.19, 'jw':1.23, 'b':1.23}
    """
    sec_data1 = {'ssm_id':"singleV2_test2", 'a':9.32, 'b':1.23}
    sec_data2 = {'ssm_id':'singleV2_test2', 'a':8.23, 'c':1.19, 'jw':1.23}
    sec_data3 = {'ssm_id':'singleV2_test2', 'c':9.99}

    failed_list = sec.Security.store(sec_data1)
    if not failed_list:
        print sec.Security.gets('singleV2_test2')
    else:
        print 'store failed!'

    sec.Security.store(sec_data2)
    print sec.Security.gets('singleV2_test2')

    sec.Security.store(sec_data3)
    print sec.Security.gets('singleV2_test2')

    sec_datas = [{'ssm_id':'V2batch8286R7', 'a1':7.42, 'b2':8.23, 'pimco':1.33},{'ssm_id':'V2batch2X6R7', 'c1':7.42, 'b2':8.23}]
    failed_list = sec.Security.store(sec_datas)
    secs = sec.Security.gets(['V2batch8286R7', 'V2batch2X6R7', 'notexsitid'])
    print secs
    sec_datas = [{'ssm_id':'V2batch8286R7', 'a1':1.2, 'jw':2.33},{'ssm_id':'V2batch2X6R7', 'c1':7.42, 'b5':2.23}]
    failed_list = sec.Security.store(sec_datas)
    secs = sec.Security.gets(['V2batch8286R7', 'V2batch2X6R7', 'notexsitid'])
    print secs

def pandas_generate_big_data():
    # get source data
    print '-------------------------------------------------------------------'
    print '-------------------------------------------------------------------'
    print 'pandas starts generate a big amount of test data!!!!!!!!'
    print 'please wait!!!!'
    sec_pd = zj_test_engine.gen_sec_data_frame(_AMOUNT, _COLUMNS)
    print 'the test data: ',sec_pd
    print 'transforming source data, please wait...'
    source_data = zj_test_engine.gen_source_data(sec_pd) # returns a list of dict
    print 'test engine has generated ',len(source_data),' number of source data',\
        ' with ',_COLUMNS,' attributes!'
    return source_data

def deletes():
    all_ids = sec.Security.getall()
    sec.Security.deletes(all_ids)


if __name__ == '__main__':
    try:
        test_case()
        test_update()
        deletes()

    except Exception as e:
        print e


