/**
 * Testing HDF5 with large nested datasets
 */

#include <cassert>
#include <cstdlib>
#include <ctime>
#include <map>
#include <string>
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

    HDF5(std::string filename):
	_file(filename.c_str(), H5F_ACC_TRUNC),
	_storedType(PredType::NATIVE_DOUBLE) {
    }

    Group createGroup(std::string name) {
	Group g = _file.createGroup(name.c_str());
	return g;
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
	//_dsmap[fullname] =  dset;
	return dset;
    }

    DataSet getDataSet(std::string name) {
	return _file.openDataSet(name.c_str());
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

private:
    void printDataSetSize(const DataSet& ds) {
	DataSpace space = ds.getSpace();
	int rank = space.getSimpleExtentNdims();
	assert(rank == RANK);
	hsize_t dims[2];
	space.getSimpleExtentDims(dims, NULL);

	std::cout << "Dimensions: " << dims[0] << "x" << dims[1] << std::endl;
    }

    void setRowHyperslab(DataSpace& space, int row, int ncols) {
	hsize_t offset[2] = { row, 0 };
	hsize_t dims[2] = { 1, ncols };
	space.selectHyperslab(H5S_SELECT_SET, dims, offset);
    }

    H5File _file;
    std::map<std::string, DataSet> _dsmap;
    const PredType _storedType;
};


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


int main(int c, char* argv[])
{
    using std::cout;
    using std::cerr;
    using std::endl;
    using std::srand;

    int nrows = 20000;
    int ncols = 2000;

    srand(time(NULL));

    std::clock_t start = clock();

    HDF5 hdf("hdf5-cpp-test.h5");
    writeHdf5(hdf, nrows, ncols);
    cerr << "Writing: "
	 << static_cast<double>(clock() - start) / CLOCKS_PER_SEC 
	 << "s" << endl;

    start = clock();

    readHdf5(hdf, nrows, ncols);
    cerr << "Reading: "
	 << static_cast<double>(clock() - start) / CLOCKS_PER_SEC
	 << "s" << endl;

    return 0;
}
