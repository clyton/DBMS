#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        PagedFileManager *pfm;
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
    
    FileHandle fileHandle;

};

class Key{
public:
	virtual Key* setKeyData(char* entry, int offset) = 0;
	virtual r_slot getKeySize() = 0;
	virtual ~Key();
};

class StringKey : public Key{
private:
	string data;
	r_slot keySize = 0;
public:
	virtual Key* setKeyData(char* entry, int offset);
	virtual r_slot getKeySize() ;
};

class IntKey : public Key{
private:
	int data;
public:
	virtual Key* setKeyData(char* entry, int offset);
	virtual r_slot getKeySize() ;
};

class FloatKey : public Key{
private:
	float data;
public:
	virtual Key* setKeyData(char* entry, int offset);
	virtual r_slot getKeySize() ;
};

class Entry{
public:
	Entry(char* entry, AttrType aType);
	Key* getKey();
	RID getRID();
	virtual int getKeyOffset();
	virtual int getRIDOffset();
	virtual ~Entry();
protected:
	char* entry;
	AttrType aType;
	Key *key;
	RID rid;
};

class IntermediateEntry : public Entry{
public:
	PageNum getLeftPtr();
	PageNum getRightPtr();
	IntermediateEntry(char* entry, AttrType aType);
	virtual int getKeyOffset();
	virtual int getRIDOffset();
private:
	PageNum leftPtr = 0;
	PageNum rightPtr = 0;


};
#endif

