# Trying out Pycassa / Cassandra

import pycassa
from pycassa.types import *
from pycassa.index import *
from random import normalvariate, uniform
from itertools import izip, product
from collections import OrderedDict

ksp = 'demo'
pool = pycassa.pool.ConnectionPool(ksp)
sys = pycassa.system_manager.SystemManager()


sys.drop_column_family(ksp, 'c1')

col_name_type = IntegerType()
#col_name_type = UTF8Type()
#col_name_type = AsciiType()
row_key_type = IntegerType()
# Holds a maximum of 4 integers (allows fewer, truncates if more)
#col_value_type = CompositeType(* [IntegerType()] * 4)
col_value_type = DoubleType()
# Could also be left as a byterarray (i.e. can hold anything serialisable by
# python, e.g. using numpy.array.dumps and numpy.loads)
# Force ordering of rows by key
#partitioner = ...

sys.create_column_family(ksp, 'c1',
                         comparator_type = col_name_type,
                         key_validation_class = row_key_type,
                         default_validation_class = col_value_type)
# Use comparator_type not comparator
                         #validation_class = col_value_type)

cf = pycassa.ColumnFamily(pool, 'c1')

for r in xrange(10):
    for c in xrange(r):
        cf.insert(r, { c : c})

# Retrieves rows with at least one of the specified columns
list(cf.get_range(columns=[2, 5, 7]))

# Retrieves columns from the requested rows (ignores empty rows by default)
list(cf.multiget([2, 4, 6], columns=[2, 5, 7]))




sys.drop_column_family(ksp, 'c2')

#col_name_type = CompositeType(LongType(reversed=True), AsciiType())
col_name_type = CompositeType(AsciiType(), IntegerType())
row_key_type = IntegerType()
col_value_type = DoubleType()

sys.create_column_family(ksp, 'c2',
                         comparator_type = col_name_type,
                         key_validation_class = row_key_type,
                         default_validation_class = col_value_type)
                         #validation_class = col_value_type)

cf = pycassa.ColumnFamily(pool, 'c2')

def insert_data(cf, n):
    """ Insert individual rows and columns """
    for r in xrange(n):
        print r
        for c1 in xrange(ord('a'), ord('z') + 1):
            for c2 in xrange(0, 100):
                cf.insert(r, { (chr(c1), c2) : normalvariate(0, 1)})


def insert_bulk(cf, n):
    """ Insert whole rows """
    for r in xrange(n):
        print r
        keys = product(xrange(ord('a'), ord('z') + 1), xrange(100))
        d = OrderedDict(itertools.imap(
                lambda k: ((chr(k[0]), k[1]), normalvariate(0, 1)), keys))
        cf.insert(r, d)

# time insert_bulk(cf, 10000)
# CPU times: user 794.06 s, sys: 5.73 s, total: 799.79 s
# Wall time: 892.39 s

# du -h /usr/local/var/lib/cassandra/data/demo/c2
#  24K    /usr/local/var/lib/cassandra/data/demo/c2/snapshots/1357580472293-c2
#  24K    /usr/local/var/lib/cassandra/data/demo/c2/snapshots/1357586182699-c2
#  32K    /usr/local/var/lib/cassandra/data/demo/c2/snapshots/1357654673869-c2
#  80K    /usr/local/var/lib/cassandra/data/demo/c2/snapshots
# 778M    /usr/local/var/lib/cassandra/data/demo/c2

# Without cf.insert:
# CPU times: user 129.60 s, sys: 0.27 s, total: 129.87 s
# Wall time: 129.86 s


# Retrieve one column all rows
# Times out with default buffer size (1024)
cf.buffer_size = 128
a = cf.get_range(columns=[('b',31), ('z',67)])

time b = list(a)
# CPU times: user 0.48 s, sys: 0.01 s, total: 0.49 s
# Wall time: 13.80 s


# Attempt an index expression (on a column)
# At least one of the expression needs to be an EQ with a secondary indexed
# column. So just create a new dummy column ('index', 0) populated with random
# numbers.

# Note that int(uniform(0, 10)) leads to timeouts- we need to restrict to a
# smaller number of columns

kvs = cf.get_range(columns = [], filter_empty = False)
kvs = map(lambda kv: (kv[0], {('index', 0) : int(uniform(0, 100))}), kvs)
kvs = OrderedDict(kvs)
cf.batch_insert(kvs)
sys.create_index(ksp, 'c2', ('index', 0), pycassa.system_manager.DOUBLE_TYPE)

# The required indexed column expression
expr0 = create_index_expression(('index', 0), 5, EQ)
# The other queries
expr1 = create_index_expression(('f', 54), 1, GTE)
expr2 = create_index_expression(('g', 40), -1, LTE)
ic = create_index_clause([expr0, expr1, expr2], count = 1e6)

r = cf.get_indexed_slices(ic, columns = [('index', 0), ('f', 54), ('g', 40)])
r = list(r)


# Alternatively reduce the count parameter in create_index_clause... except it
# needs to be reduced quite a lot
kvs = cf.get_range(columns = [], filter_empty = False)
kvs = map(lambda kv: (kv[0], {('index', 0) : 0}), kvs)
kvs = OrderedDict(kvs)
cf.batch_insert(kvs)

expr0 = create_index_expression(('index', 0), 0, EQ)
ic = create_index_clause([expr0, expr1, expr2], count = 20)
r = cf.get_indexed_slices(ic, columns = [('index', 0), ('f', 54), ('g', 40)])
r = list(r)

