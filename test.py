
import security as sec
import test_engine as zj_test_engine

_AMOUNT = 50000
_COLUMNS = 200

def single_store():
    """Store one sec data into redis, and updata some attributes, check these attributes

    for the same sec_id, use sec_obj.store() twice:
    {'ssm_id':"single_test1", 'a':9.32, 'b':1.23}
    {'ssm_id':'single_test1', 'a':8.23, 'c':1.19, 'jw':1.23}

    check these attributes, excepted result should be:
    {'ssm_id':'single_test1', 'a':8.23, 'c':1.19, 'jw':1.23, 'b':1.23}
    """
    sec_data1 = {'ssm_id':"single_test2", 'a':9.32, 'b':1.23}
    sec_data2 = {'ssm_id':'single_test2', 'a':8.23, 'c':1.19, 'jw':1.23}
    sec_data3 = {'ssm_id':'single_test2', 'c':9.99}

    failed_list = sec.Security.store(sec_data1)
    if not failed_list:
        print sec.Security.gets('single_test2')
    else:
        print 'store failed!'

    sec.Security.store(sec_data2)
    print sec.Security.gets('single_test2')

    sec.Security.store(sec_data3)
    print sec.Security.gets('single_test2')

    sec_datas = [{'ssm_id':'batch8286R7', 'a1':7.42, 'b2':8.23, 'pimco':1.33},{'ssm_id':'batch2X6R7', 'c1':7.42, 'b2':8.23}]
    failed_list = sec.Security.store(sec_datas)
    secs = sec.Security.gets(['batch8286R7', 'batch2X6R7', 'notexsitid'])
    print secs
    sec_datas = [{'ssm_id':'batch8286R7', 'a1':1.2, 'jw':2.33},{'ssm_id':'batch2X6R7', 'c1':7.42, 'b5':2.23}]
    failed_list = sec.Security.store(sec_datas)
    secs = sec.Security.gets(['batch8286R7', 'batch2X6R7', 'notexsitid'])
    print secs


def test_case():

    # In the morning, Li Lei run some linear regression analysis scripts for the 500K number of securities, and generate 200 attributes for each security.
    # the data was like: [{'ssm_id':'778286R7', 'duration':7.42, 'oas':8.23, 'pimco':1.33},{'ssm_id':'SD332X6R7', 'c1':7.42, 'b2':8.23}]
    source_data = pandas_generate_big_data()

    # Li Lei want to store all these 500K*200 data into redis
    sec.Security.store(source_data)


    # Li lei want to update one attribute for one security, and dont want to affect other attributes
    sec_data1 = {'ssm_id':'single_test1', 'duration':1.23}
    print 'before update :',sec.Security.get('single_test1')
    sec.Security.store(sec_data1)
    print 'after update :',sec.Security.get('single_test1')


def very_long_attribute():
    sec_pd = zj_test_engine.gen_sec_data_frame(1, 20000)# generate one sec data with 20000 attributes
    source_data = zj_test_engine.gen_source_data(sec_pd)
    print 'this data have ',len(source_data[0]),' attributes!'
    sec.Security.store(source_data[0])
    sec0 = sec.Security.gets(source_data[0]['ssm_id'])
    print 'check from redis returns data with length: ',len(sec0[0])



def pandas_generate_big_data():
    # get source data
    print '-------------------------------------------------------------------'
    print '-------------------------------------------------------------------'
    print 'pandas starts generate a big amount of test data!!!!!!!!'
    print 'please wait!!!!'
    sec_pd = zj_test_engine.gen_sec_data_frame(_AMOUNT, _COLUMNS)
    print 'the test data: ',sec_pd
    source_data = zj_test_engine.gen_source_data(sec_pd) # returns a list of dict
    print 'pandas has generated ',len(source_data),' number of source data',\
        ' with ',_COLUMNS,' attributes!'
    return source_data


def show_running_time(func):
    # a decorate to count how long a function executed
    import datetime
    def warp(*args):
        start = datetime.datetime.now()
        print 'fuction ',func.__name__, ' starts at ',start
        func(*args)
        end = datetime.datetime.now()
        print 'fuction ',func.__name__, ' ends at ',end
        print 'function running ',(end-start).seconds,' seconds'
    return warp


@show_running_time
def performance_test(source_data):

    print '-------------------------------------------------------------------'
    print '-------------------------------------------------------------------'
    print '-------------------------------------------------------------------'
    print '!!!!!!!!!!!performance test starts now!!!!!!!!!!!!!!!!!!!'

    check_point = source_data[3]['ssm_id']
    try:
        before = sec.Security.gets(check_point)[0]['ssm_id'], sec.Security.gets(check_point)[0]['a1'], sec.Security.gets(check_point)[0]['a3']
    except:
        before = None
    print 'before update, check point is: ',before

    print 'the check point in source data is: ',(source_data[3]['ssm_id'], source_data[3]['a1'], source_data[3]['a3'])

    sec.Security.store(source_data)
    print 'data update finish!'

    after = sec.Security.gets(check_point)[0]['ssm_id'], sec.Security.gets(check_point)[0]['a1'], sec.Security.gets(check_point)[0]['a3']
    print 'after update, check point is: ',after

    all_ids = sec.Security.getall()
    datas = sec.Security.gets(all_ids)





if __name__ == '__main__':

    try:
        #single_store()
        #normal_store_get()

        #very_long_attribute()

        # get source data from pandas
        source_data = pandas_generate_big_data()
        performance_test(source_data)

    except Exception as e:
        print e
