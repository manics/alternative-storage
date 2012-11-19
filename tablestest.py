#PyTables instead of MongoDB
import tables
import mongotest as m
import sys
from datetime import datetime

default_filename = "tablestest.h5"
h = None

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
        h.createEArray(name[:p], name[p + 1:], tables.Float32Atom(len(vs)), \
                           (0,), createparents=True)

    for k, v in fulld.iteritems():
        if type(v) == list:
            createTable(k, v)
        else:
            print "Ignoring field %s value %s" % (k, str(v))

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

def createF(d, pre, mu):
    d[pre + "/f1"] = m.randN(mu, m.ns[0])
    d[pre + "/f2"] = m.randN(mu, m.ns[1])
    d[pre + "/f3"] = m.randN(mu, m.ns[2])
    d[pre + "/f4"] = m.randN(mu, m.ns[3])

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
        d = simulate(id + 1, m.mus[id % len(m.mus)])
        addRow(h, d)
    sys.stdout.write('\n')


#import tablestest as tt
#tt.h.close(); reload(tt); tt.h=tt.newDb()

#Flushing after every table row
#time tt.addSimulated(tt.h,range(100))
#CPU times: user 45.89 s, sys: 0.34 s, total: 46.23 s
#Wall time: 46.24 s

#Without flushing
#time tt.addSimulated(tt.h,range(100))
#CPU times: user 45.71 s, sys: 0.36 s, total: 46.07 s
#Wall time: 46.09 s


#Using EArray for storage
#tt.h=tt.newDb(mode="earray")

#Flushing after every table row
#time tt.addSimulated(tt.h,range(10000))
#CPU times: user 321.08 s, sys: 22.26 s, total: 343.34 s
#Wall time: 343.68 s
