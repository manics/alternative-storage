#
#
from itertools import izip
import omero
from copy import deepcopy
from omero.gateway import BlitzGateway
from omero.grid import LongColumn, DoubleArrayColumn

class TableConnection(object):

    def __init__(self, tableName, user = None, passwd = None,
                 host = 'localhost', client = None):
        """
        Create a new table handler, either by specifying user and passwd or by
        providing a client object (for scripts)
        @param tableName The name of the table file top be used
        @param user Username
        @param passwd Password
        @param host The server hostname
        @param client Client object
        """
        if client is None:
            host = 'localhost'
            client = omero.client(host)
            sess = client.createSession(user, passwd)
            client.enableKeepAlive(60)
        else:
             sess = client.getSession()

        self.conn = BlitzGateway(client_obj = client)

        self.res = sess.sharedResources()
        if (not self.res.areTablesEnabled()):
            raise Exception('OMERO.Tables not enabled')

        repos = self.res.repositories()
        self.rid = repos.descriptions[0].id.val

        self.tableName = tableName

    def __enter__(self):
        print 'Entering Connection'
        return self

    def __exit__(self, type, value, traceback):
        print 'Exiting Connection'
        self.close()

    def close(self):
        print 'Closing Connection'
        self.conn._closeSession()


    def getTable(self):
        """
        Retrieve a handle to an existing table with a given name
        """
        if self.table:
            self.table.close()

        self.table = self.openTable(tableName = self.tableName)
        if not self.table:
            print "No table with name '%s'" % self.tableName
            return None

        print "Opened table with %d rows %d columns" % \
            (self.table.getNumberOfRows(), len(self.table.getHeaders()))
        return self.table


    def openTable(self, tableId = None, tableName = None):
        """
        Opens an existing table by ID or name.
        If there are multiple tables with the same name this just takes the
        first one (should really use an annotation to keep track of this).
        """
        if tableId is None:
            attrs = {'name': tableName}
            ofiles = self.conn.getObjects("OriginalFile", attributes = attrs)
            ofile = None
            for f in ofiles:
                if ofile is None:
                    ofile = f
                else:
                    print 'Multiple tables with name:%s found, using id:%d' % \
                        (tableName, ofile.getId())
                    break
        else:
            attrs = {'id': long(tableId)}
            if tableName is not None:
                attrs['name'] = tableName
            ofile = self.conn.getObject("OriginalFile", attributes = attrs)

        if ofile is None:
            print 'No table found with name:%s id:%s' % (tableName, tableId)
            return None

        table = self.res.openTable(ofile._obj)
        print 'Opened table name:%s id:%s' % (tableName, tableId)
        return table


    def randomData(self, table, nrows):
        """
        Add some randomly generated data to a table
        @param table table handle
        @param nrows Number of random rows to add
        """
        from random import random
        cols = table.getHeaders()
        n = table.getNumberOfRows()

        for row in xrange(nrows):
            cols[0].values = [n + row]
            for col in cols[1:]:
                col.values = [random()]
            table.addData(cols)


    def addRow(self, table, id, data):
        """
        Add a row to the table
        @param table table handle
        @param id Object id
        @param data the values to store in the table
        """
        cols = table.getHeaders()
        assert len(data) == len(cols) - 1
        cols[0].values = [id]
        for c, d in map(None, cols[1:], data):
            c.values = [float(d)]
        table.addData(cols)


    def deleteAllTables(self):
        """
        Delete all tables with <tableName>
        Will fail if there are any annotation links
        """
        ofiles = self.conn.getObjects("OriginalFile", \
            attributes = {'name': self.tableName})
        ids = [f.getId() for f in ofiles]
        print 'Deleting ids:%s' % ids
        self.conn.deleteObjects('OriginalFile', ids)


    def dumpTable(self, table):
        """
        Print out the table
        """
        headers = table.getHeaders()
        print ', '.join([t.name for t in headers])
        nrows = table.getNumberOfRows()
        #data = table.readCoordinates(xrange(table.getNumberOfRows))

        for r in xrange(nrows):
            data = table.read(range(len(headers)), r, r + 1)
            print ', '.join(['%.2f' % c.values[0] for c in data.columns])


    def newTable(self):
        """
        Create a new uninitialised table
        @return A handle to the table
        """
        self.table = self.res.newTable(self.rid, self.tableName)
        ofile = table.getOriginalFile()
        id = ofile.getId().getValue()
        return table


class FeatureTableConnection(TableConnection):

    def __init__(self, tableName, user, passwd, host = 'localhost'):
        super(FeatureTableConnection, self).__init__(
            tableName, user, passwd, host)
        self.table = None
        self.tableid = None

    def createNewTable(self, idcolName, colDescriptions):
        """
        Create a new table with an id column followed by DoubleArrayColumns
        which can be set to empty
        @param colDescriptions A list of 2-tuples describing each column as
        (name, size)
        """

        # Create an identical number of bool columns indicating whether
        # columns are valid or not. To make things easier this includes
        # a bool column for the id column even though it should always
        # be valid.

        if self.table:
            self.table.close()

        self.table = self.res.newTable(self.rid, self.tableName)
        ofile = table.getOriginalFile()
        self.id = ofile.getId().getValue()

        try:
            cols = [LongColumn(idcolName)] + \
                [DoubleArrayColumn(name, '', size) \
                     for (name, size) in colDescriptions] + \
                [BoolColumn('_b_' + idcolName)] + \
                [BoolColumn('_b_' + name) \
                     for (name, size) in colDescriptions]
            self.table.initialize(cols)
            print "Initialised '%s' (%d)" % (self.tableName, id)
        except Exception as e:
            print "Failed to create table: %s" % e
            try:
                self.table = None
                self.conn.deleteObjects('OriginalFile', [id])
            except Exception as e:
                print "Failed to delete table: %s" % e

        self.table.close()


    def isValid(colNumbers, start, stop):
        """
        Check whether the requested arrays are valid
        @param colNumbers Column numbers
        @param start The first row to be read
        @param stop The last + 1 row to be read
        @return A Data object containing a list of BoolColumn indicating
        whether the corresponding row-column element is valid (true) or null
        (false).
        """
        self._checkColNumbers(colNumbers)
        bcolNumbers = map(lambda x: x + nCols, colNumbers)
        data = self.table.read(bcolNumbers, start, stop)


    def readSubArray(self, colArrayNumbers, start, stop):
        """
        Read the requested array columns and indices from the table
        @param colArrayNumbers A dictionary mapping column numbers to
        an array of subindices
        @param start The first row to be read
        @param stop The last + 1 row to be read
        @return A dictionary mapping column numbers to the requested
        array elements. Empty array columns will result in an empty array.
        If the id columns is requested this will not be an array.

        # @param colNumbers A list of column indices to be read
        # @param arrayIndices A list of lists, corresponding to the subarray
        # indices of each of the columns listed in colNumbers
        """

        colNumbers = colArrayNumbers.keys()
        subIndices = colArrayNumbers.values()
        self._checkColNumbers(colNumbers)

        headers = self.table.getHeaders()
        nCols = len(headers) / 2
        nWanted = len(colNumbers)

        bcolNumbers = map(lambda x: x + nCols, colNumbers)
        data = self.table.read(colNumbers + bcolNumbers, start, stop)
        columns = data.columns

        for (c, b, s) in izip(columns[:nWanted], columns[nWanted:], subIndices):
            #indexer = opertor.itemgetter(*s)
            c.values = [[x[i] for i in s] if y else []
                        for (x, y) in izip(c.values, b.values)]

        return columns[:nWanted]


    def addData(cols, copy=True):
        """
        """
        columns = self.table.getHeaders()
        nCols = len(columns) / 2
        if len(cols) != nCols:
            raise Exception("Expected %d columns, got %d" % (nCols, len(cols)))

        if not isinstance(cols[0], LongColumn) or not \
                all(map(lambda x: isinstance(x, DoubleArrayColumn), cols[1:])):
            raise Exception("Expected 1 LongColumn and %d DoubleArrayColumn" %
                            (nCols - 1))

        if copy:
            columns[:nCols] = deepcopy(cols)
        else:
            columns[:nCols] = cols

        # Handle first ID column separately, it is not a DoubleArray
        columns[nCols].values = [True] * len(cols[0].values)
        for (c, b) in izip(columns[1:nCols], columns[(nCols + 1):]):
            emptyval = [0.0] * c.size
            # bool([])==false
            b.values = [bool(x) for x in c.values]
            c.values = [x if x else emptyval for x in c.values]

        self.table.addData(columns)
        return columns


    def _checkColNumbers(self, colNumbers):
        nCols = len(self.table.getHeaders()) / 2
        invalid = filter(lambda x: x >= nCols, colNumbers)
        if len(invalid) > 0:
            raise Exception("Invalid column index: %s" % invalid)



def open():
    user = 'test1'
    passwd = 'test1'
    tableName = '/test.h5'

    tc = FeatureTableConnection(tableName, user, passwd)
    import atexit
    atexit.register(tc.close)
    return tc
