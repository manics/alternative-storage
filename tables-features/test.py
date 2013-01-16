from TableConnection import *

def open():
    user = 'test1'
    passwd = 'test1'
    tableName = '/test.h5'

    tc = FeatureTableConnection(tableName, user, passwd)
    import atexit
    atexit.register(tc.close)
    return tc
