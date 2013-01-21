from TableConnection import *
import unittest

class TestFeatureTableConnection(unittest.TestCase):

    def setUp(self):
        user = 'test1'
        passwd = 'test1'
        tableName = '/test.h5'
        self.tc = FeatureTableConnection(user, passwd, tableName = tableName)

    def tearDown(self):
        try:
            #self.tc.table.delete()
            pass
        finally:
            self.tc.close()

    def createNewTable(self):
        idcolName = 'id'
        colDescriptions = [('da1', 2), ('da2', 3), ('da3', 4)]
        self.tc.createNewTable(idcolName, colDescriptions)

    def populateTable(self):
        cols = self.tc.getHeaders()
        self.assertEquals(len(cols), 4)

        cols[0].values = [1, 2]
        cols[1].values = [[10., 20.], [30., 40.]]
        cols[2].values = [[], [400., 500., 600.]]
        cols[3].values = [[0.5, 0.25, 0.125, 0.0625], []]
        self.tc.addData(cols)


    def testIsValid(self):
        self.createNewTable()
        self.populateTable()
        cols = self.tc.isValid(range(4), 0, 2)

        self.assertEquals(cols[0].values, [True, True])
        self.assertEquals(cols[1].values, [True, True])
        self.assertEquals(cols[2].values, [False, True])
        self.assertEquals(cols[3].values, [True, False])


    def testAddData(self):
        self.createNewTable()
        self.populateTable()
        cols = self.tc.readArray(range(4), 0, 2)

        self.assertEquals(cols[0].values, [1, 2])
        self.assertEquals(cols[1].values, [[10., 20.], [30., 40.]])
        self.assertEquals(cols[2].values, [[], [400., 500., 600.]])
        self.assertEquals(cols[3].values, [[0.5, 0.25, 0.125, 0.0625], []])

    def testAddPartialData(self):
        self.createNewTable()
        self.populateTable()
        cols = self.tc.getHeaders()
        cols = [cols[0], cols[2]]

        cols[0].values = [10, 11, 12]
        cols[1].values = [[-1., -2., -3.], [], [-7., -8., -9.]]
        self.tc.addPartialData(cols)

        cols = self.tc.readArray(range(4), 0, 5)

        self.assertEquals(cols[0].values, [1, 2, 10, 11, 12])
        self.assertEquals(cols[1].values, [[10., 20.], [30., 40.], [], [], []])
        self.assertEquals(
            cols[2].values,
            [[], [400., 500., 600.], [-1., -2., -3.], [], [-7., -8., -9.]])
        self.assertEquals(
            cols[3].values,
            [[0.5, 0.25, 0.125, 0.0625], [], [], [], []])


    def testGetRowId(self):
        self.createNewTable()
        self.populateTable()
        idx = self.tc.getRowId(2)
        #cols = self.tc.readArray(range(4), 0, 2)
        #print cols

        self.assertEquals(idx, [1])


    def testReadSubArray(self):
        self.createNewTable()
        self.populateTable()

        colArrayNumbers = {3:[0, 3], 0:[], 1:[1]}
        cols = self.tc.readSubArray(colArrayNumbers, 0, 2)

        # Dictionary: order of keys is unknown
        colMap = dict([(c.name, c) for c in cols])

        self.assertEquals(sorted(colMap.keys()), ['da1', 'da3', 'id'])
        self.assertEquals(colMap['id'].values, [1, 2])
        self.assertEquals(colMap['da1'].values, [[20.], [40.]])
        self.assertEquals(colMap['da3'].values, [[0.5, 0.0625], []])



def open():
    user = 'test1'
    passwd = 'test1'
    tableName = '/test.h5'

    tc = FeatureTableConnection(user, passwd, tableName = tableName)
    import atexit
    atexit.register(tc.close)
    return tc
