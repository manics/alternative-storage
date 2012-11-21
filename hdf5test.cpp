/**
 * Testing HDF5 with large nested datasets
 */

#include <cassert>
#include <cmath>
#include <cstdlib>
#include <ctime>
#include <map>
#include <string>
#include <sstream>
#include <vector>

#include <H5Cpp.h>

#include <iostream>


using namespace H5;

class HDF5
{
public:
    typedef double Type;
    typedef std::vector<Type> Vector;
    static const int RANK = 2;

    HDF5(std::string filename, bool overwrite = false):
	_file(filename.c_str(), overwrite ? H5F_ACC_TRUNC : 0),
	_storedType(PredType::NATIVE_DOUBLE) {
    }

    Group createGroup(std::string name) {
	Group g = _file.createGroup(name.c_str());
	return g;
    }

    bool groupExists(std::string name) {
	return false;
    }

    DataSet createDataSet(std::string group, std::string name, int ncols) {
	std::string fullname = group + "/" + name;

	// Initial dimensions, allow extension to unlimited rows
	hsize_t dims[2] = { 0, ncols };
	hsize_t maxdims[2] = { H5S_UNLIMITED, ncols };
	DataSpace dspace(RANK, dims, maxdims);

	// Allow chunking (required for extension)
	DSetCreatPropList cparams;
	hsize_t chunkDims[2] = { 1, ncols };
	cparams.setChunk(RANK, chunkDims);

	DataSet dset = _file.createDataSet(
	    fullname.c_str(), _storedType, dspace, cparams);
	return dset;
    }

    DataSet getDataSet(std::string name) {
	return _file.openDataSet(name.c_str());
    }

    Group getGroup(std::string name) {
	return _file.openGroup(name.c_str());
    }

    struct RC {
	int r;
	int c;
    };

    RC numberOfRowsCols(const DataSet& ds) const {
	DataSpace space = ds.getSpace();
	int rank = space.getSimpleExtentNdims();
	assert(rank == RANK);

	hsize_t dims[2];
	space.getSimpleExtentDims(dims, NULL);

	RC rc;
	rc.r = dims[0];
	rc.c = dims[1];
	return rc;
    }

    void addRow(const Vector& values, DataSet& ds) {
	//printDataSetSize(ds);

	DataSpace wspace = ds.getSpace();
	int rank = wspace.getSimpleExtentNdims();
	assert(rank == RANK);
	hsize_t dims[2];
	wspace.getSimpleExtentDims(dims, NULL);
	hsize_t& nrows = dims[0];
	hsize_t& ncols = dims[1];
	assert(ncols == values.size());

	++nrows;
	ds.extend(dims);

	//printDataSetSize(ds);

	wspace = ds.getSpace();
	setRowHyperslab(wspace, nrows - 1, ncols);
	DataSpace mspace(rank, dims);
	setRowHyperslab(mspace, 0, ncols);

	ds.write(&values.front(), _storedType, mspace, wspace);
    }

    hsize_t getRow(DataSet& ds, Vector& values, int row) {
	// Dataset space
	DataSpace dspace = ds.getSpace();
	int rank = dspace.getSimpleExtentNdims();
	assert(rank == RANK);
	hsize_t dims[2];
	dspace.getSimpleExtentDims(dims, NULL);
	assert(row >= 0 && dims[0] > static_cast<unsigned int>(row));
	values.resize(dims[1]);

	setRowHyperslab(dspace, row, dims[1]);

	// Memory space
	DataSpace mspace(rank, dims);
	setRowHyperslab(mspace, 0, dims[1]);

	ds.read(&values.front(), _storedType, mspace, dspace);
	return dims[1];
    }

    void createGroupIfNotExist(std::string name) {
	using std::string;

	size_t p = 0;
	while (p != string::npos) {
	    p = name.find_first_of('/', p + 1);

	    string g = name.substr(0, p);

	    // This should throw a GroupIException exception if the group
	    // doesn't exist, but it seems to throw a FileIException instead
	    try {
		Exception::dontPrint();
		//std::cout << "Looking for group " << g << std::endl;
		//H5G_stat_t stat;
		//_file.getObjinfo(g.c_str(), true, stat);
		Group group = _file.openGroup(g.c_str());
	    }
	    catch (GroupIException notFoundError) {
		std::cout << "Caught GroupIException, Creating group \""
			  << g << "\"" << std::endl;
		createGroup(g);
	    }
	    catch (FileIException notFoundError) {
		std::cout << "Caught FileIException, Creating group \""
			  << g << "\"" << std::endl;
		createGroup(g);
	    }
	}
    }

    const H5File& getFile() const {
	return _file;
    }
private:
    void printDataSetSize(const DataSet& ds) const {
	DataSpace space = ds.getSpace();
	int rank = space.getSimpleExtentNdims();
	assert(rank == RANK);
	hsize_t dims[2];
	space.getSimpleExtentDims(dims, NULL);

	std::cout << "Dimensions: " << dims[0] << "x" << dims[1] << std::endl;
    }

    void setRowHyperslab(DataSpace& space, int row, int ncols) const {
	hsize_t offset[2] = { row, 0 };
	hsize_t dims[2] = { 1, ncols };
	space.selectHyperslab(H5S_SELECT_SET, dims, offset);
    }

    H5File _file;
    const PredType _storedType;
};


class SimulatedData
{
public:
    typedef std::map<std::string, HDF5::Vector*> MapType;

    SimulatedData(int id, double mu, double delField = 0.0):
	_id(id), _timestamp(0), _sigma(4) {
	_ns[0] = 10;
	_ns[1] = 20;
	_ns[2] = 30;
	_ns[3] = 40;

	// 1 set of 4 feature groups (sum(ns) features in total)
	createF("", mu);
	// 4 sets
	createT1("", mu);
	// 16 sets
	createT2("", mu);
	// TODO: randomDeleteField
    }

    ~SimulatedData() {
	for (MapType::iterator it = _values.begin(); it != _values.end(); ++it)
	{
	    delete it->second;
	}
    }

    const MapType& getData() const {
	return _values;
    }

private:
    class RandomGaussian {
    public:
	RandomGaussian():
	    _empty(true) {
	}

	double next(double mu, double sigma) {
	    if (!_empty) {
		_empty = true;
		return _y * sigma + mu;
	    }

	    // http://en.wikipedia.org/wiki/Marsaglia_polar_method
	    while (true) {
		double u = static_cast<double>(std::rand()) / RAND_MAX * 2 - 1;
		double v = static_cast<double>(std::rand()) / RAND_MAX * 2 - 1;
		double s = u * u + v * v;
		if (s < 1) {
		    double m = std::sqrt(-2.0 * std::log(s) / s);
		    double x = u * m;
		    _y = v * m;
		    _empty = false;
		    return x * sigma + mu;
		}
	    }
	}

    private:
	bool _empty;
	double _y;
    };

    void randN(HDF5::Vector& vs, double mu) {
	RandomGaussian r;
	for (HDF5::Vector::iterator it = vs.begin(); it != vs.end(); ++it) {
	    *it = r.next(mu, _sigma);
	}
    }

    void createF(std::string group, double mu) {
	std::string names[] = { "/f1", "/f2", "/f3", "/f4" };
	for (int i = 0; i < 4; ++i)
	{
	    HDF5::Vector* vs = new HDF5::Vector(_ns[i]);
	    randN(*vs, mu);
	    _values[group + names[i]] = vs;
	}
    }

    void createT1(std::string group, double mu) {
	std::string names[] = { "/t1", "/t2", "/t3", "/t4" };
	for (int i = 0; i < 4; ++i) {
	    createF(group + names[i], mu);
	}
    }

    void createT2(std::string group, double mu) {
	std::string names[] = { "/t1", "/t2", "/t3", "/t4" };
	for (int i = 0; i < 4; ++i) {
	    createT1(group + names[i], mu);
	}
    }

    std::map<std::string, HDF5::Vector*> _values;
    int _id;
    int _timestamp;
    int _ns[4];
    double _sigma;
};

void generateData(HDF5& hdf, int n)
{
    SimulatedData sd(0, 0.0);
    SimulatedData::MapType data = sd.getData();
    for (SimulatedData::MapType::const_iterator it = data.begin();
	 it != data.end(); ++it)
    {
	std::string g = it->first;
	size_t p = g.find_last_of('/');
	if (p == std::string::npos)
	{
	    std::cout << "Ignoring \"" << g << "\"" << std::endl;
	    continue;
	}

	std::string group = g.substr(0, p + 1);
	std::string table = g.substr(p + 1);

	hdf.createGroupIfNotExist(group);
	hdf.createDataSet(group, table, it->second->size());
    }

    for (int i = 0; i < n; ++i)
    {
	double mus[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

	std::cout << i << " ";
	SimulatedData sd(i, mus[i % 10]);
	SimulatedData::MapType data = sd.getData();
	for (SimulatedData::MapType::const_iterator it = data.begin();
	     it != data.end(); ++it)
	{
	    std::string table = it->first;
	    //std::cout << i << ": Adding table " << table << std::endl;
	    DataSet ds = hdf.getDataSet(table);
	    hdf.addRow(*it->second, ds);
	}
    }
    std::cout << std::endl;
}


struct DSAdder
{
    HDF5* hdf;
    double total;
    std::string path;
};

// An operator for use with H5Giterate
herr_t datasetAdder(hid_t gid, const char* name, void* data)
{
    DSAdder* d = static_cast<DSAdder*>(data);
    std::string prevPath = d->path;
    d->path = d->path + "/" + name;

    herr_t r = 0;
    H5G_stat_t stat;
    d->hdf->getFile().getObjinfo(d->path.c_str(), true, stat);

    switch (stat.type)
    {
    case H5G_GROUP:
    {
	std::cout << "Group: " << d->path << std::endl;
	Group group = d->hdf->getGroup(d->path);
	r = H5Giterate(group.getId(), d->path.c_str(), NULL,
		       datasetAdder, data);
	break;
    }

    case H5G_DATASET:
    {
	std::cout << "DataSet: " << d->path << std::endl;

	DataSet ds = d->hdf->getDataSet(d->path);
	HDF5::Vector values;

	HDF5::RC rc = d->hdf->numberOfRowsCols(ds);

	double total = 0;

	for (int r = 0; r < rc.r; ++r)
	{
	    d->hdf->getRow(ds, values, r);
	    for (HDF5::Vector::const_iterator it = values.begin();
		 it != values.end(); ++it)
	    {
		total += *it;
	    }
	}

	std::cout << "\tRows: " << rc.r << " Cols:" << rc.c
		  << " Table total:" << total << std::endl;
	d->total += total;
	break;
    }

    case H5G_LINK:
	std::cout << "Link: " << d->path << " (ignoring)" << std::endl;
	break;

    case H5G_TYPE:
	std::cout << "Type: " << d->path << " (ignoring)" << std::endl;
	break;

    default:
	std::cerr << "Unexpected type: " << d->path << std::endl;
	return -1;
    }

    d->path = prevPath;
    return 0;
}

void readData(HDF5& hdf)
{
    DSAdder data;
    data.hdf = &hdf;
    data.total = 0;
    data.path = "";

    herr_t r = datasetAdder(hdf.getFile().getId(), "",
			    static_cast<void*>(&data));
    if (r < 0)
    {
	std::cout << "Error occurred: " << r << std::endl;
    }
    std::cout << "Grand total: " << data.total << std::endl;
}

void writeHdf5(HDF5& hdf, int nrows, int ncols)
{
    using std::rand;
    using std::srand;
    using std::time;

    hdf.createGroup("/foo");
    hdf.createGroup("/foo/bar");
    DataSet ds = hdf.createDataSet("/foo/bar", "baz", ncols);

    HDF5::Vector values(ncols);
    //int n = 0;

    for (int r = 0; r < nrows; ++r)
    {
	for (int c = 0; c < ncols; ++c)
	{
	    //values[c] = n++;
	    values[c] = static_cast<double>(rand()) / RAND_MAX;
	}
	//std::cout << "Writing row " << r << std::endl;
	hdf.addRow(values, ds);
    }
}


void readHdf5(HDF5& hdf, int nrows, int ncols)
{
    DataSet rds = hdf.getDataSet("/foo/bar/baz");

    HDF5::Vector rvalues;
    double grandTotal = 0;
    for (int r = 0; r < nrows; ++r)
    {
	double total = 0;
	hdf.getRow(rds, rvalues, r);
	for (HDF5::Vector::const_iterator it = rvalues.begin();
	     it != rvalues.end(); ++it)
	{
	    //std::cout << *it << ", ";
	    total += *it;
	}
	//std::cout << "Row " << r << " total: " << total << std::endl;
	grandTotal += total;
    }
    std::cout << "Total: " << grandTotal << std::endl;
}


void usage(std::string progname)
{
    size_t p = progname.find_last_of("/\\");
    p = (p == std::string::npos)? 0 : p + 1;
    std::cerr << "Usage: " << progname.substr(p) << " 0|1|N [filename]\n"
	      << "0 Read objects\n"
	      << "1 Write a 20,000 row by 2,000 column table\n"
	      << "N Write N multi-feature multi-table objects (N:integer)\n"
	      << "filename Default: hdf5-cpp-test.h5\n"
	      << std::endl;
}


int main(int argc, char* argv[])
{
    using std::cout;
    using std::cerr;
    using std::endl;
    using std::srand;

    srand(time(NULL));

    if (argc < 2 || argc > 3)
    {
	usage(argv[0]);
	return 2;
    }

    std::istringstream iss(argv[1]);
    int mode;

    iss >> mode;
    if (!iss || mode < 0)
    {
	cerr << "Invalid argument" <<endl;
	usage(argv[0]);
	return 2;
    }

    std::string fname("hdf5-cpp-test.h5");
    if (argc == 3)
    {
	fname = argv[2];
    }
    cerr << "Using file \"" << fname << "\"" << endl;

    std::clock_t start = clock();
    cerr << "Timing started" << endl;

    if (mode == 0)
    {
	bool overwrite = false;
	HDF5 hdf(fname, overwrite);

	readData(hdf);
	cerr << "Reading time: "
	     << static_cast<double>(clock() - start) / CLOCKS_PER_SEC
	     << "s" << endl;
    }
    else if (mode == 1)
    {
	int nrows = 20000;
	int ncols = 2000;
	bool overwrite = true;

	HDF5 hdf(fname, overwrite);

	writeHdf5(hdf, nrows, ncols);
	cerr << "Writing time: "
	     << static_cast<double>(clock() - start) / CLOCKS_PER_SEC 
	     << "s" << endl;

	/*start = clock();

	readHdf5(hdf, nrows, ncols);
	cerr << "Reading: "
	     << static_cast<double>(clock() - start) / CLOCKS_PER_SEC
	     << "s" << endl;*/
    }
    else
    {
	int ndata = mode;
	bool overwrite = true;

	HDF5 hdf(fname, overwrite);

	generateData(hdf, ndata);
	cerr << "Writing " << ndata << " objects, time: "
	     << static_cast<double>(clock() - start) / CLOCKS_PER_SEC
	     << "s" << endl;
    }

    return 0;
}


/***************************************************************************
 * g++ -Wall hdf5test.cpp -lhdf5_cpp -lhdf5 -O3
 * time ./a.out
 * Saved 10000: 26.5098s
 * 
 * real    0m27.010s
 * user    0m23.283s
 * sys     0m3.251s
 * hdf5-cpp-test.h5 file size 198M
 ***************************************************************************/

/***************************************************************************
 * g++ -Wall hdf5test.cpp -lhdf5_cpp -lhdf5 -O3
 * time ./a.out
 * Saved 100000: 262.898s
 *
 * real    4m28.952s
 * user    3m50.361s
 * sys     0m32.577s
 * hdf5-cpp-test.h5 file size 1.9G
 ***************************************************************************/
