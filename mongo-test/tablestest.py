#PyTables instead of MongoDB
import tables
from random import normalvariate, random
import sys
from datetime import datetime

default_filename = "tablestest.h5"

sigma = 4
mus = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
ns = [10, 20, 30, 40]
#ns = [2, 2, 2, 2]

def newDb(filename = default_filename, mode = "tables"):
    h = tables.openFile(filename, "w", title="Tables Test")
    d = simulate(0, 0)
    if mode == "tables":
        createSchema(h, d)
    elif mode == "earray":
        createSchemaAsEArray(h, d)
    else:
        raise Exception("Invalid mode")
    return h

def openDb(filename = default_filename):
    h = tables.openFile(filename, "a", title="Tables Test")
    return h

def createSchema(h, fulld):
    """
    Create tables from a flat dict in which descendants are indicated by "/"
    in the original single value per row/column format
    """
    def createTable(name, vs):
        desc = {}
        for n in xrange(len(vs)):
            desc['x%04d' % n] = tables.Float32Col()
        p = name.rfind('/')
        h.createTable(name[:p], name[p + 1:], desc, createparents=True)

    for k, v in fulld.iteritems():
        if type(v) == list:
            createTable(k, v)
        else:
            print "Ignoring field %s value %s" % (k, str(v))

def createSchemaAsEArray(h, fulld):
    """
    Create EArray tables from a flat dict in which descendants are indicated
    by "/"
    """
    def createTable(name, vs):
        p = name.rfind('/')
        h.createEArray(name[:p], name[p + 1:], tables.Float32Atom(),
                       (0, len(vs)), createparents=True)#,
                       #chunkshape=(1, len(vs)))

    for k, v in fulld.iteritems():
        if type(v) == list:
            createTable(k, v)
        else:
            print "Ignoring field %s value %s" % (k, str(v))

def createSchemaWithArrays(h, fulld):
    """
    Create a single table from a flat dict in which descendants are indicated
    by "/", with each column holding an array of values

    Incomplete, addRow() won't work at the moment
    """
    desc = {}
    for k, v in fulld.iteritems():
        if type(v) == list:
            k = k.strip("/").replace("/", "_")
            desc[k] = tables.Float32Col(shape=(len(v),))
        else:
            print "Ignoring field %s value %s" % (k, str(v))

    h.createTable("/", "all", desc)

def createSchemaFromNestedDict(h, fulld):
    """
    Create groups and tables from a nested dict
    """
    def createTable(group, k, vs):
        desc = {}
        for n in xrange(len(vs)):
            desc['x%04d' % n] = tables.Float32Col()
        h.createTable(group, k, desc)

    def createGroup(currentGroup, k, vs):
        if currentGroup:
            group = h.createGroup(currentGroup, k, k)
            currentGroup = currentGroup + "/" + k
        else:
            currentGroup = "/"

        for k, v in vs.iteritems():
            if type(v) == list:
                createTable(currentGroup, k, v)
            elif type(v) == dict:
                createGroup(currentGroup, k, v)
            else:
                print "Ignoring field %s value %s" % (k, str(v))

    createGroup(None, None, fulld)

def removeAll(h):
    for x in h.iterNodes('/'):
        h.removeNode('/', x._v_name, recursive=True)

def randN(mu, n):
    return [normalvariate(mu, sigma) for i in xrange(n)]

def createF(d, pre, mu):
    d[pre + "/f1"] = randN(mu, ns[0])
    d[pre + "/f2"] = randN(mu, ns[1])
    d[pre + "/f3"] = randN(mu, ns[2])
    d[pre + "/f4"] = randN(mu, ns[3])

def createT1(d, prefix, mu): 
    for g in ["t1", "t2", "t3", "t4"]:
        pre = prefix + "/" + g
        createF(d, pre, mu)

def createT2(d, prefix, mu):
    for g in ["t1", "t2", "t3", "t4"]:
        pre = prefix + "/" + g
        createT1(d, pre, mu)

def simulate(id, mu, delField = 0.0):
    d = {}
    # 1 set of 4 feature groups (sum(ns) features in total)
    createF(d, "", mu)
    # 4 sets
    createT1(d, "", mu)
    # 16 sets
    createT2(d, "", mu)
    #t2 = m.randomDeleteField(t2, delField)
    # 64 sets
    #t3 = createT3(mu)
    #mergeDicts(d, t3)

    d['id'] = id
    d['timestamp'] = datetime.utcnow()

    return d

def addRow(h, d):
    for k, v in d.iteritems():
        if type(v) == list:
            #print k
            table = h.getNode(k)
            table.append([v])
            table.flush()
        #else:
        #    print "Ignoring field %s value %s" % (k, str(v))



def addSimulated(h, ids):
    for id in ids:
        sys.stdout.write('%d ' % id)
        sys.stdout.flush()
        d = simulate(id + 1, mus[id % len(mus)])
        addRow(h, d)
    sys.stdout.write('\n')


def readAndSum(h):
    def sumTable(tab):
        total = 0.0
        for r in tab.iterrows():
            total = total + sum(r)
        return total

    def sumChild(c):
        total = 0.0
        if isinstance(c, tables.Array):
            print "Array:", c.name
            total = sumTable(c)
            print "\tRows: %d Cols:%d Table total:%e" % \
                (c.shape[0], c.shape[1], total)
        elif isinstance(c, tables.group.Group):
            print "Group:", c._v_name
            for k, v in c._v_children.iteritems():
                total = total + sumChild(v)
        else:
            raise Exception("Unexpected HDF5 object: " + str(c))
        return total

    grandTotal = 0.0;
    for child in h.listNodes("/"):
        grandTotal = grandTotal + sumChild(child)

    print "Grand total: %e" % grandTotal


#import tablestest as tt
#h.close(); reload(tt); h=tt.newDb()

#Flushing after every table row
#time tt.addSimulated(h,range(100))
#CPU times: user 45.89 s, sys: 0.34 s, total: 46.23 s
#Wall time: 46.24 s
#
#time tt.addSimulated(h,range(10000))
#CPU times: user 4483.58 s, sys: 34.85 s, total: 4518.43 s
#Wall time: 4519.40 s
#
#Without flushing
#time tt.addSimulated(h,range(100))
#CPU times: user 45.71 s, sys: 0.36 s, total: 46.07 s
#Wall time: 46.09 s


#Using EArray for storage
#h.close(); h=tt.newDb(mode="earray")

#Flushing after every table row
#time tt.addSimulated(h,range(10000))
#CPU times: user 321.75 s, sys: 22.19 s, total: 343.95 s
#Wall time: 344.30 s
#
#Reading back
#time tt.readAndSum(h)
#Grand total: 9.448957e+07
#CPU times: user 21.69 s, sys: 0.06 s, total: 21.74 s
#Wall time: 21.74 s
#
#time tt.addSimulated(h,range(100000))
#CPU times: user 3054.43 s, sys: 223.49 s, total: 3277.92 s
#Wall time: 3281.50 s
#File size of tablestest.h5 806M
#
#Reading back
#time tt.readAndSum(h)
#Grand total: 9.450075e+08
#CPU times: user 220.06 s, sys: 0.36 s, total: 220.42 s
#Wall time: 220.77 s

#Without flushing
#time tt.addSimulated(h,range(10000))
#CPU times: user 303.94 s, sys: 24.79 s, total: 328.74 s
#Wall time: 329.05 s
#
#time tt.addSimulated(h,range(100000))
#CPU times: user 3004.45 s, sys: 249.29 s, total: 3253.73 s
#Wall time: 3257.07 s


#Using EArray with chunksize (1, N) (i.e. one row)
#time tt.addSimulated(h,range(10000))
#CPU times: user 410.51 s, sys: 43.38 s, total: 453.89 s
#Wall time: 454.24 s
#
#Reading back
#time tt.readAndSum(h)
#Grand total: 9.450050e+07
#CPU times: user 27.81 s, sys: 3.60 s, total: 31.41 s
#Wall time: 43.58 s
#



