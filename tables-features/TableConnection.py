#
#
from itertools import izip
import omero
from copy import deepcopy
from omero.gateway import BlitzGateway
from omero.grid import LongColumn, BoolColumn, \
    LongArrayColumn, DoubleArrayColumn


class TableConnectionError(Exception):
    """
    Errors occuring in the TableConnection class
    """
    pass


class TableConnection(object):

    def __init__(self, user = None, passwd = None, host = 'localhost',
                 client = None, tableName = None, tableId = None):
        """
        Create a new table handler, either by specifying user and passwd or by
        providing a client object (for scripts)
        @param tableName The name of the table file top be used
        @param user Username
        @param passwd Password
        @param host The server hostname
        @param client Client object
        """
        if not client:
            client = omero.client(host)
            sess = client.createSession(user, passwd)
            client.enableKeepAlive(60)
        else:
             sess = client.getSession()

        self.conn = BlitzGateway(client_obj = client)

        self.res = sess.sharedResources()
        if (not self.res.areTablesEnabled()):
            raise TableConnectionError('OMERO.Tables not enabled')

        repos = self.res.repositories()
        self.rid = repos.descriptions[0].id.val

        self.tableName = tableName
        self.tableId = tableId
        self.table = None

    def __enter__(self):
        print 'Entering Connection'
        return self

    def __exit__(self, type, value, traceback):
        print 'Exiting Connection'
        self.close()

    def close(self):
        print 'Closing Connection'
        if self.table:
            self.table.close()
        self.conn._closeSession()


    def openTable(self, tableId = None, tableName = None):
        """
        Opens an existing table by ID or name.
        If there are multiple tables with the same name this throws an error
        (should really use an annotation to keep track of this).
        """
        if not tableId and not tableName:
            tableId = self.tableId
            tableName = self.tableName

        if not tableId:
            if not tableName:
                tableName = self.tableName
            attrs = {'name': tableName}
            ofiles = self.conn.getObjects("OriginalFile", attributes = attrs)
            if len(ofiles) > 1:
                raise TableConnectionError(
                    'Multiple tables with name:%s found' % tableName)
            if not ofiles:
                raise TableConnectionError(
                    'No table found with name:%s' % tableName)
            ofile = ofiles[0]
        else:
            attrs = {'id': long(tableId)}
            if tableName:
                attrs['name'] = tableName
            ofile = self.conn.getObject("OriginalFile", attributes = attrs)

        if not ofile:
            raise TableConnectionError('No table found with name:%s id:%s' %
                                       (tableName, tableId))

        self.table = self.res.openTable(ofile._obj)
        try:
            print 'Opened table name:%s id:%s with %d rows %d columns' % \
                (tableName, tableId,
                 self.table.getNumberOfRows(), len(self.table.getHeaders()))
        except omero.ApiUsageException:
            print 'Opened table name:%s id:%s' % (tableName, tableId)

        self.tableId = tableId
        return self.table


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
        ofile = self.table.getOriginalFile()
        id = ofile.getId().getValue()
        return self.table


class FeatureTableConnection(TableConnection):

    def __init__(self, user, passwd, host = 'localhost', tableName = None):
        super(FeatureTableConnection, self).__init__(
            user, passwd, host, tableName = tableName)
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
        ofile = self.table.getOriginalFile()
        self.tableId = ofile.getId().getValue()

        try:
            cols = [LongColumn(idcolName)] + \
                [DoubleArrayColumn(name, '', size) \
                     for (name, size) in colDescriptions] + \
                [BoolColumn('_b_' + idcolName)] + \
                [BoolColumn('_b_' + name) \
                     for (name, size) in colDescriptions]
            self.table.initialize(cols)
            print "Initialised '%s' (%d)" % (self.tableName, self.tableId)
        except Exception as e:
            print "Failed to create table: %s" % e
            try:
                self.table = None
                self.conn.deleteObjects('OriginalFile', [id])
            except Exception as e:
                print "Failed to delete table: %s" % e


    def isValid(self, colNumbers, start, stop):
        """
        Check whether the requested arrays are valid
        @param colNumbers Column numbers
        @param start The first row to be read
        @param stop The last + 1 row to be read
        @return A Data object containing a list of BoolColumn indicating
        whether the corresponding row-column element is valid (true) or null
        (false).
        """
        nCols = self._checkColNumbers(colNumbers)
        bcolNumbers = map(lambda x: x + nCols, colNumbers)
        data = self.table.read(bcolNumbers, start, stop)
        return data.columns


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
        nCols = self._checkColNumbers(colNumbers)
        nWanted = len(colNumbers)

        bcolNumbers = map(lambda x: x + nCols, colNumbers)
        data = self.table.read(colNumbers + bcolNumbers, start, stop)
        columns = data.columns

        for (c, b, s) in izip(columns[:nWanted], columns[nWanted:], subIndices):
            #indexer = opertor.itemgetter(*s)
            if isinstance(c, (LongArrayColumn, DoubleArrayColumn)):
                #hasattr(c, '__getitem__'):
                c.values = [[x[i] for i in s] if y else []
                            for (x, y) in izip(c.values, b.values)]
            else:
                self._nullEmptyColumns(c, b)

        return columns[:nWanted]


    def readArray(self, colNumbers, start, stop):
        """
        Read the requested array columns which may include null entries
        @param colNumbers Column numbers
        @param start The first row to be read
        @param stop The last + 1 row to be read
        """

        nCols = self._checkColNumbers(colNumbers)
        nWanted = len(colNumbers)

        bcolNumbers = map(lambda x: x + nCols, colNumbers)
        data = self.table.read(colNumbers + bcolNumbers, start, stop)
        columns = data.columns

        for (c, b) in izip(columns[:nWanted], columns[nWanted:]):
            self._nullEmptyColumns(c, b)

        return columns[:nWanted]


    def getHeaders(self):
        """
        Get a set of columns to be used for populating the table with data
        """
        columns = self.table.getHeaders()
        return columns[:(len(columns) / 2)]


    def getNumberOfRows(self):
        """
        Get the number of rows
        """
        return self.table.getNumberOfRows()


    def addData(self, cols, copy=True):
        """
        Add a new row of data where DoubleArrays may be null
        """
        columns = self.table.getHeaders()
        nCols = len(columns) / 2
        if len(cols) != nCols:
            raise TableConnectionError(
                "Expected %d columns, got %d" % (nCols, len(cols)))

        if not isinstance(cols[0], LongColumn) or not \
                all(map(lambda x: isinstance(x, DoubleArrayColumn), cols[1:])):
            raise TableConnectionError(
                "Expected 1 LongColumn and %d DoubleArrayColumn" % (nCols - 1))

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


    def addPartialData(self, cols, copy=True):
        """
        Add a new row of data where some DoubleArray columns may be omitted
        (automatically set to null)
        """
        columns = self.table.getHeaders()
        nCols = len(columns) / 2

        if copy:
            cols = deepcopy(cols)
        columnMap = dict([(c.name, c) for c in cols])

        # Check the first id column is present
        idColName = columns[0].name
        try:
            columns[0] = columnMap.pop(idColName)
        except KeyError:
            raise TableConnectionError(
                "First column (%s) must be provided" % idCol.name)

        nRows = len(columns[0].values)
        columns[nCols].values = [True] * nRows

        for n in xrange(1, nCols):
            try:
                columns[n] = columnMap.pop(columns[n].name)
                self._zeroEmptyColumns(columns[n], columns[nCols + n])
            except KeyError:
                columns[n].values = [[0.0] * columns[n].size] * nRows
                columns[nCols + n].values = [False] * nRows

            if not isinstance(columns[n], DoubleArrayColumn):
                raise TableConnectionError(
                    "Expected DoubleArrayColumn (%s)" % columns[n].name)

        if columnMap.keys():
            raise TableConnectionError(
                "Unexpected columns: %s" % columnMap.keys())

        self.table.addData(columns)
        return columns


    def _zeroEmptyColumns(self, col, bcol):
        """
        Internal helper method, sets empty elements to zeros and the
        corresponding boolean indicator column entry to False
        """
        #for (c, b) in izip(columns[1:nCols], columns[(nCols + 1):]):
        emptyval = [0.0] * col.size
        bcol.values = [bool(x) for x in col.values]
        col.values = [x if x else emptyval for x in col.values]

    def _nullEmptyColumns(self, col, bcol):
        """
        Internal helper method, sets column elements which are indicated by
        the boolean indicator as empty to [] if they are array-columns, or
        None for scalar column types
        """
        if isinstance(col, (LongArrayColumn, DoubleArrayColumn)):
            col.values = [x if y else []
                          for (x, y) in izip(col.values, bcol.values)]
        else:
            col.values = [x if y else None
                          for (x, y) in izip(col.values, bcol.values)]


    def _checkColNumbers(self, colNumbers):
        """
        Checks the requested column numbers refer to the id or
        double-array-columns, and not the boolean indicator columns
        @return The number of id/data columns excluding the boolean indicators
        """
        nCols = len(self.table.getHeaders()) / 2
        invalid = filter(lambda x: x >= nCols, colNumbers)
        if len(invalid) > 0:
            raise TableConnectionError("Invalid column index: %s" % invalid)

        return nCols
