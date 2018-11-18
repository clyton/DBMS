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
        const int success = 0;
        const int failure = 1;

    private:
      RC setUpIndexFile(IXFileHandle &ixfileHandle, const Attribute &attribute);
      PageNum getRootPageID(IXFileHandle &ixfileHandle);
};


class IX_ScanIterator {
    public:

        IXFileHandle *ixfileHandle;
        Attribute attribute;
        const void* lowKey;
        const void* highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;

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
	virtual int compare(Key& other) = 0;
};

class StringKey : public Key{
private:
	string data;
	r_slot keySize = 0;
public:
	Key* setKeyData(char* entry, int offset);
	r_slot getKeySize() ;
	int compare(Key& other);
	string getData();
};

class IntKey : public Key{
private:
	int data;
public:
	Key* setKeyData(char* entry, int offset);
	r_slot getKeySize() ;
	int compare(Key& other);
	int getData();
};

class FloatKey : public Key{
private:
	float data;
public:
	 Key* setKeyData(char* entry, int offset);
	r_slot getKeySize() ;
	 int compare(Key& other);
	float getData();
};

class Entry{
public:
	Entry(char* entry, AttrType aType);
	virtual Key* getKey();
	virtual RID getRID();
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
	int getKeyOffset();
	int getRIDOffset();
private:
	PageNum leftPtr = 0;
	PageNum rightPtr = 0;


};

class LeafEntry : public Entry{
public:
	RID getSiblingPtr();
	LeafEntry(char* entry, AttrType aType);
	int getKeyOffset();
	int getRIDOffset();
private:
	RID siblingPtr;
};

class EntryComparator{
public:
	virtual int compare(Entry a , Entry b) = 0;
	virtual ~EntryComparator();

};

class IntermediateComparator : public EntryComparator{
public:
	int compare(Entry a, Entry b);
};

class LeafComparator : public EntryComparator{
public:
	int compare(Entry a, Entry b);
};


enum BTPageType { INTERMEDIATE = 1, LEAF = 0 };

/**
 * Page Format for BTPage :
 * | L/I | SP |    |    |    |    |        |    |
 * |-----+----+----+----+----+----+--------+----|
 * |     |    |    |    |    |    |        |    |
 * |     |    |    |    |    |    |        |    |
 * |     |    |    |    |    |    |        |    |
 * |     |    | L2 | O2 | L1 | O1 | FS = 5 | NS |
 *
 * Legends:
 * L/I = Leaf/Intermediate
 * SP = Sibling Pointer: Will be used only by leaf nodes
 * NS = Number of slots
 * FS = Free space pointer
 *
 * Provides interface to insert entries into a btpage.
 * It's the user's responsibilities to interpret the entries
 * and sort them while inserting
 */
class BTPage {
 public:
  BTPage(const char *page, const Attribute &attribute);
  ~BTPage();
  BTPageType getPageType();
  r_slot getFreeSpaceAvailable();
  bool isSpaceAvailableToInsertEntryOfSize(r_slot size);

  /**
   * Inserts entry buf in BTPage at freespace pointer and stores its
   * offset at the {@code slotNumber}. Pushes other slots ahead by 1
   *
   * @param entry
   * @param slotNumber : index at which entry offset, length are stored.
   * @param length : length of the char buffer.
   * @return
   */
  RC insertEntry(char *entry, int slotNumber, int length);
  RC getEntry(r_slot slotNum, char *buf);  // same as readEntry()
  RC removeEntry(int slotNumber, char* entryBuf);
  RC readEntry(r_slot slotNum, char *buf);
  char *getPage();
  int getNumberOfSlots();


 private:
  // The read methods will read the pageBuffer into data members
  void readPageType();
  void readAttribute(const Attribute &attribute);
  void readPageRecordInfo();
  void readSlotDirectory();
  void readSiblingNode();
  // r_slot getSmallestEntryGreaterThan(char* key, KeyComparator const* key);
  void copySlotsToPage(vector<SlotDirectory> slots, char *pageBuffer);

  // The set methods will update the pageBuffer*
  void setPageType(); // copy pType to pageBuffer
  void setAttribute(const Attribute &attribute);
  void setPageRecordInfo(); // copy pri to page buffer
  void setSlotDirectory(); //copy the slots vector to page buffer
  void setSiblingNode();  // copy sibling pointer to page buffer
  void printPage();

  RC shiftRecord(char *pageData, r_slot slotToShiftOffset, int byBytesToShift);

 private:
  BTPageType pageType;
  PageRecordInfo pri;
  vector<SlotDirectory> slots;
  PageNum siblingPage;  // for leaf pages
  char *pageBuffer;
  Attribute attribute;
  const int success = 0;
  const int failure = 1;
};

#endif
