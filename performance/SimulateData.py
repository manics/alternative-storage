# Generate some data for performance testing
from random import normalvariate, random
from datetime import datetime



sep = '_'
sigma = 4
mus = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
ns = [10, 20, 30, 40]
#ns = [1,1,1,1]

def randN(mu, n):
    return [normalvariate(mu, sigma) for i in xrange(n)]

def createF(d, mu):
    e = {}
    for prefix in d:
        e[prefix + 'f0'] = randN(mu, ns[0])
        e[prefix + 'f1'] = randN(mu, ns[1])
        e[prefix + 'f2'] = randN(mu, ns[2])
        e[prefix + 'f3'] = randN(mu, ns[3])
    return e

def createT1(d, mu):
    e = {}
    for prefix in d:
        for t in ['t0', 't1', 't2', 't3']:
            e[prefix + t + sep] = []
    e = createF(e, mu)
    return e

def createT2(d, mu):
    e = {}
    for prefix in d:
        for t in ['t0', 't1', 't2', 't3']:
            e[prefix + t + sep] = []
    e = createT1(e, mu)
    return e

def randomDeleteField(d, p):
    """
    Delete a field with probability p
    """
    for k in d.keys():
        if random() < p:
            del d[k]
    return d

def simulate(id, mu, delField = 0.0):
    d = {'':[]}
    # 1 set of 4 feature groups (sum(ns) features in total)
    f = createF(d, mu)
    # 4 sets
    t1 = createT1(d, mu)
    # 16 sets
    t2 = createT2(d, mu)

    d = dict(f.items() + t1.items() + t2.items())
    d = randomDeleteField(d, delField)

    r = {'id': id, 'timestamp': datetime.utcnow(), 'features': d}
    return r






def dict2description(d):
    """
    Create a set of table description arguments suitable for the second
    argument of FeatureTableConnection.createNewTable
    """
    colNames = sorted(d.keys())
    desc = [(k, len(d[k])) for k in colNames]
    return desc

def dict2columns(id, d):
    """
    Convert a dictionary of features to a set of columns
    Columns are not guaranteed to be in a particular order
    """
    from omero.grid import LongColumn, DoubleArrayColumn
    cols = [LongColumn('id', '', [id])] + \
        [DoubleArrayColumn(k, '', len(v), [v]) for (k, v) in d.iteritems()]
    return cols

def multDict2columns(sims, missing = []):
    """
    Concatenate the values of multiple simulated data objects so that
    dict2columns will give a single set of columns.
    Keys which are missing from some dicts will automatically be assigned
    the value missing.
    """

    def concatDictValues(ds, missing = []):
        cd = {}
        allKeys = set()
        for d in ds:
            allKeys.update(d.keys())
        for k in allKeys:
            cd[k] = [d.get(k, missing) for d in ds]
        return cd

    ids = [s['id'] for s in sims]
    features = [s['features'] for s in sims]
    features = concatDictValues(features, missing)

    from omero.grid import LongColumn, DoubleArrayColumn
    cols = [LongColumn('id', '', ids)] + \
        [DoubleArrayColumn(k, '', len(v[0]), v)
         for (k, v) in features.iteritems()]
    return cols

def columns2dict(cols, nrows = 1):
    """
    Convert a set of columns into a dictionary of features
    If multiple rows then this will be a list of dicts
    """
    d = [dict([(c.name, c.values[n]) for c in cols]) for n in xrange(nrows)]
    return d


def setup(user = 'test1', passwd = 'test1', host = 'localhost',
          tableName = '/test.h5', new = False):
    from table_features.TableConnection import FeatureTableConnection
    tc = FeatureTableConnection(user, passwd, host, tableName)

    if new:
        dummy = simulate(0, 0)
        desc = dict2description(dummy['features'])
        tc.createNewTable('id', dict2description(dummy['features']))
    else:
        tc.openTable()
    return tc


def assertColsEqualsDict(cols, d):
    #cold = columns2dict(col)[0]
    assert(sorted([c.name for c in cols]) == sorted(d.keys()))
    assert(all([c.values[0] == d[c.name] for c in cols]))


def insert(tc, n, check, keep):
    k = []
    for i in xrange(n):
        print i,
        a = simulate(i, i % len(mus))
        tc.addPartialData(dict2columns(a['id'], a['features']))

        if check or keep:
            a2 = a['features']
            a2['id'] = a['id']

        if keep:
            k.append(a2)

        if check:
            nr = tc.getNumberOfRows()
            aRead = tc.readArray(range(len(a2)), nr - 1, nr);
            assertColsEqualsDict(aRead, a2)

    if keep:
        return k


def insertBulkRepeat(tc, nr, n, check, keep):
    """
    Bulk insert multiple rows
    Repeatedly insert the same set of rows
    """
    a = []
    for i in xrange(nr):
        a.append(simulate(i, i % len(mus)))
    acols = multDict2columns(a)

    print "Created %d data points" % nr

    for i in xrange(n):
        print i,
        tc.addPartialData(acols)

        #if check or keep:
        #    a2 = a['features']
        #    a2['id'] = a['id']

        #if keep:
        #    k.append(a2)

        #if check:
        #    nr = tc.getNumberOfRows()
        #    aRead = tc.readArray(range(len(a2)), nr - 1, nr);
        #    assertColsEqualsDict(aRead, a2)

    if keep:
        return a


def readBulk(tc, nr, skip = None):
    """
    Do a bulk read of the whole table, ignore data
    """

    if not skip:
        skip = nr

    start = datetime.now()
    stopwatch = []
    colNumbers = range(len(tc.getHeaders()))
    for i in xrange(0, tc.getNumberOfRows(), skip):
        tc.readArray(colNumbers, i, i + nr)
        stopwatch.append((datetime.now() - start).total_seconds())
        print stopwatch[-1]
    return stopwatch

#keep = performance.SimulateData.insert(tc, 100, True, True)
def compareLastKeep(tc, keep):
    n0 = tc.getNumberOfRows() - len(keep)
    colNumbers = range(len(tc.getHeaders()))
    for n in xrange(len(keep)):
        print n,
        r = tc.readArray(colNumbers, n0 + n, n0 + n + 1)
        assertColsEqualsDict(r, keep[n])

