import pandas as pd
import numpy as np


_AMOUNT = 1000 # the number of sec data
_COLUMNS = 200 # the number of fields of each sec


def gen_sec_data_frame(amount, columns):
    """
    Args:
        amount: the number of sec data
        columns: the number of fields of each data

    Returns:
        pandas data_frame

    """
    # use pandas data frame to generate test data
    # return a pandas data frame
    sec_data = np.random.randn(amount,columns)
    fields = ['a'+str(i) for i in xrange(columns)]
    ssm_ids = ['test_V2_ssm_id'+str(i) for i in xrange(amount)]
    ssm_ids_np = pd.Series(ssm_ids)
    sec_data_pd = pd.DataFrame(sec_data, index=ssm_ids_np, columns=fields)
    sec_data_pd['ssm_id']=sec_data_pd.index#this can make to_dict() easy
    return sec_data_pd

def gen_source_data(data_frame):
    """
        Inputs: pandas data_frame
        Outputs: a list of python dict like:
        [{'a1': 1.7480122992999434, 'a0': 0.10490438557485701, 'ssm_id': '13R465798'},
        {'a1': 0.883099663140354, 'a0': -0.8665530090612172, 'ssm_id': '973R60524'}]
    """
    # return a list of source data [{'ssm_id':69540R173,'a1':1.2},{}]
    data_dict = data_frame.T.to_dict().values()
    return data_dict



if __name__ == '__main__':
    sec_data_pd = gen_sec_data_frame(_AMOUNT, _COLUMNS)
    print 'the head of data frame: '
    print sec_data_pd.head()
    print 'the tail of data frame: '
    print sec_data_pd.tail()
    #sec_data_pd.to_csv('sec_data.csv')

    source_data = gen_source_data(sec_data_pd)
    print 'here is ',len(source_data),' source data, the first is: ', source_data[0]


