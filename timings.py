# Using ipython to get timings
import numpy
import mongotest as m

# without db insertion
time m.addSimulated(xrange(0,10000))
#CPU times: user 28.27 s, sys: 0.05 s, total: 28.32 s
#Wall time: 28.32 s

# with coll.save(d)
time m.addSimulated(xrange(0,10000))
#CPU times: user 33.09 s, sys: 0.19 s, total: 33.28 s
#Wall time: 33.28 s

# with coll.save(d, safe=True)
time m.addSimulated(xrange(0,10000))
#CPU times: user 33.61 s, sys: 0.33 s, total: 33.94 s
#Wall time: 38.68 s

# 100,000 with coll.save(d, safe=True)
time m.addSimulated(xrange(0,100000))
#CPU times: user 339.53 s, sys: 3.44 s, total: 342.97 s
#Wall time: 427.34 s

# Disk space
#du -sh /usr/local/var/mongodb/
#6.0G    /usr/local/var/mongodb/



coll = m.coll

# Retrieve a single nested field if it exists (without the exist check an empty field will be returned for objects without that field)
time a=list(coll.find({'t1.t1.f1':{'$exists':True}},{'t1.t1.f1':1}))
#CPU times: user 0.66 s, sys: 0.08 s, total: 0.74 s
#Wall time: 1.38 s

# Retrieve a nested field containing an array and find the mean with numpy
time a=[numpy.mean(x['t1']['t1']['f1']) for x in coll.find({'t1.t1.f1':{'$exists':True}},{'t1.t1.f1':1})]
#CPU times: user 3.48 s, sys: 0.07 s, total: 3.55 s
#Wall time: 4.21 s

# Same, but with f4 (40 elements)
time a=[numpy.mean(x['t1']['t1']['f4']) for x in coll.find({'t1.t1.f4':{'$exists':True}},{'t1.t1.f4':1})]
#CPU times: user 3.71 s, sys: 0.08 s, total: 3.79 s
#Wall time: 4.43 s

# Count the number of objects in which a field has all values>1
time coll.find({"$where":"return this.t2.t3.f4.every(function(v){return (v>1);})"} ).count()
#CPU times: user 0.00 s, sys: 0.00 s, total: 0.00 s
#Wall time: 6.33 s
#: 6811

# Doing the above calculation in python instead
time a=[x['t2']['t3']['f4'] for x in coll.find({'t2.t3.f4':{'$exists':True}},{'t2.t3.f4':1})]
#CPU times: user 1.44 s, sys: 0.12 s, total: 1.56 s
#Wall time: 2.19 s
time b=numpy.array(a)
#CPU times: user 0.64 s, sys: 0.00 s, total: 0.65 s
#Wall time: 0.65 s
time sum(numpy.all(b>1,1))
#CPU times: user 0.33 s, sys: 0.00 s, total: 0.33 s
#Wall time: 0.33 s
#: 6811

# Map-reduce example: Find the average of values in t2.t3.f4,
# round to nearest integer, count up number of objects
mr_map="function(){var res={s:0,n:0}; this.t2.t3.f4.forEach(function(v){res.s+=v;++res.n}); emit(Math.round(res.s/res.n),1);}"
mr_reduce="function(k,v){var res=0; v.forEach(function(v){res+=v}); return res}"
time a=coll.inline_map_reduce(mr_map,mr_reduce,full_response=True)
#CPU times: user 0.00 s, sys: 0.00 s, total: 0.00 s
#Wall time: 7.23 s

#See http://api.mongodb.org/python/1.11/examples/map_reduce.html for better
#formatting of JavaScript string in python

#See http://blog.pythonisito.com/search/label/pymongo for tutorial
#Note BSON object keys are ordered
#Batch inserts should be quicker
#16MB max document size, but GridFS API (on top of MongoDB, not native) handles chunking
#Also Ming object-document-mapper to give some relational semantics
#Indexes should improve performance


