#include "ix.h"

#include <float.h>
#include <stdlib.h>
#include <cassert>
#include <climits>
#include <cmath>
#include <cstring>
#include <iostream>
#include <iterator>
#include <stack>

#include "../rbf/pfm.h"

IndexManager* IndexManager::_index_manager = 0;

RC IndexManager::setRootPage(IXFileHandle &ixfileHandle, PageNum rootPgID) {
	char* headerPage = (char*) malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(0, headerPage);

	memcpy(headerPage, &rootPgID, sizeof(rootPgID));

	ixfileHandle.fileHandle.writePage(0, headerPage);

	return success;
}

r_slot prepareLeafEntry(char *leafEntryBuf, const void* key, RID rid,
		const Attribute &attribute) {

	int keySize = 0;
	if (attribute.type == TypeVarChar) {
		int length = 0;
		memcpy(&length, key, sizeof(length));
		keySize = sizeof(length) + length;

	} else {
		keySize = sizeof(int);
	}
	memcpy(leafEntryBuf, key, keySize);
	memcpy(leafEntryBuf + keySize, &rid, sizeof(rid));

	return keySize + sizeof(rid);
}

r_slot prepareIntermediateEntry(char* intermediateEntryBuffer,
		char* leafEntryBuf, int leafEntrySize, PageNum lPg, PageNum rPg) {
	int offset = 0;
	// left page pointer
	memcpy(intermediateEntryBuffer, &lPg, sizeof(lPg));
	offset += sizeof(lPg);

	// data
	memcpy(intermediateEntryBuffer + offset, leafEntryBuf, leafEntrySize);
	offset += leafEntrySize;

	// right page pointer
	memcpy(intermediateEntryBuffer + offset, &rPg, sizeof(rPg));
	offset += sizeof(rPg);

	return offset;

}
IndexManager* IndexManager::instance() {
	if (!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

void BTPage::prepareEmptyBTPageBuffer(char* rootPage, BTPageType pageType) {
	// set up root page data
	// set leaf/intermediate indicator
//	BTPageType pageType = BTPageType::LEAF;
	int offset = 0;
	memcpy(rootPage, &pageType, sizeof(pageType));
	offset += sizeof(pageType);
	// set sibling pointer. will be used only in leaf node
	PageNum siblingPage = BTPage::NULL_PAGE;
	memcpy(rootPage + offset, &siblingPage, sizeof(siblingPage));
	offset += sizeof(siblingPage);
	PageRecordInfo pri;
	pri.freeSpacePos = offset;
	pri.numberOfSlots = 0;
	updatePageRecordInfo(pri, rootPage);
}

/**
 * This method will be called for the very first insert in the index file
 * It will create a header page with attribute information and root node info
 * It will create a root page as a leaf node
 * By the end of the method the index file will have 2 pages:0-header, 1-root
 * @param ixfileHandle
 * @param attribute
 * @return status code
 */
RC IndexManager::setUpIndexFile(IXFileHandle &ixfileHandle,
		const Attribute &attribute) {
	//	PageNum HEADER_PAGE_NUM = 0;
	PageNum ROOT_PAGE_NUM = 1;
	if (ixfileHandle.fileHandle.getNumberOfPages() != 0) {
		cout << "setupIndexFile called on an already setup index file" << endl;
		return failure;
	}
	// setup header page data
	AttrType keyAttributeType = attribute.type;
	AttrLength keyAttributeLength = attribute.length;

	// set up header page buffer and write to file
	char *headerPage = (char *) malloc(PAGE_SIZE);
	memset(headerPage, 0, PAGE_SIZE);
	int offset = 0;
	memcpy(headerPage, &ROOT_PAGE_NUM, sizeof(ROOT_PAGE_NUM));
	offset += sizeof(ROOT_PAGE_NUM);
	memcpy(headerPage + offset, &keyAttributeType, sizeof(keyAttributeType));
	offset += sizeof(keyAttributeType);
	memcpy(headerPage + offset, &keyAttributeLength,
			sizeof(keyAttributeLength));
	offset += sizeof(keyAttributeLength);
	ixfileHandle.fileHandle.appendPage(headerPage);  // header page id is 0
	free(headerPage);
	headerPage = NULL;

	// set up root page data
	char* rootPage = (char*) (malloc(PAGE_SIZE));
	BTPage::prepareEmptyBTPageBuffer(rootPage, BTPageType::LEAF);

	ixfileHandle.fileHandle.appendPage(rootPage);  // root page id is 1
	free(rootPage);
	rootPage = NULL;

	return success;
}

IndexManager::IndexManager() {
}

IndexManager::~IndexManager() {
}

RC IndexManager::createFile(const string &fileName) {
	return pfm->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName) {
	return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle) {
	return pfm->openFile(fileName, ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle) {
	return pfm->closeFile(ixfileHandle.fileHandle);
}

PageNum IndexManager::getRootPageID(IXFileHandle &ixfileHandle) const{
	char *headerPage = (char *) malloc(PAGE_SIZE);
	memset(headerPage, 0, PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(0, headerPage);
	unsigned rootID = 0;
	memcpy(&rootID, headerPage, sizeof(rootID));
	free(headerPage);
	headerPage = NULL;
	return rootID;

}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute, const void *key, const RID &rid) {
	if (ixfileHandle.fileHandle.getNumberOfPages() == 0) {
		setUpIndexFile(ixfileHandle, attribute);
	}

	char* leafEntryBuf = (char*) malloc(PAGE_SIZE);
	memset(leafEntryBuf, 0, PAGE_SIZE);
	r_slot entrySize = prepareLeafEntry(leafEntryBuf, key, rid, attribute);
	Entry leafEntry(leafEntryBuf, attribute.type);

	PageNum rootPageId = getRootPageID(ixfileHandle);
	char* pageData = (char*) malloc(PAGE_SIZE);
	memset(pageData, 0, PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(rootPageId, pageData);

	BTPage *btPg = new BTPage(pageData, attribute);
	stack<PageNum> traversal;
	PageNum currentPageNum = rootPageId;
	char* entry = (char*) malloc(PAGE_SIZE);
	while (btPg->getPageType() != BTPageType::LEAF) {
		traversal.push(currentPageNum);
		r_slot islot;
		IntermediateEntry *ptrIEntry = NULL;
		for (islot = 0; islot < btPg->getNumberOfSlots(); islot++) {
			memset(entry, 0, PAGE_SIZE);
			btPg->readEntry(islot, entry);
			ptrIEntry = new IntermediateEntry(entry, attribute.type);
			IntermediateComparator iComp;
			if (iComp.compare(*ptrIEntry, leafEntry) > 0) { // if entry in node greater than leaf entry
				break;

			}
			delete ptrIEntry;
		}
		if (islot < btPg->getNumberOfSlots()) { // islot in range
			currentPageNum = ptrIEntry->getLeftPtr();
		} else { //islot is out of range
				 // this means all entries in the file were smaller than
				 // ientry and that caused all slots to be read
				 // But it may also mean that there were no entries in the file
				 // In the first case, we have to traverse to the rightPtr of iEntry
				 // Case 2 can never happen. We will never get an empty intermediate node
				 // at this point in the code
			currentPageNum = ptrIEntry->getRightPtr();

		}
		if (btPg)
			delete[] btPg;
		memset(pageData, 0, PAGE_SIZE);
		ixfileHandle.fileHandle.readPage(currentPageNum, pageData);
		btPg = new BTPage(pageData, attribute);
		delete ptrIEntry;
	}
	// we have reached the leaf page
	// and traversal stack stores all our ancestor pages
	// condition to check if there is no ancestor : traversal.size() == 0

	if (btPg->isSpaceAvailableToInsertEntryOfSize(entrySize)) {
//		r_slot islot = 0;
//		r_slot numberOfSlots = btPg->getNumberOfSlots();
//		IntermediateComparator icomp; // for insert cmp both key,rid in leaf
//		bool slotFound = false;
//		while(islot < numberOfSlots){
//			btPg->readEntry(islot, entry);
//			islot++;
//			Entry pageLeafEntry(entry, attribute.type);
//			if(icomp.compare(leafEntry, pageLeafEntry) > 0){
//				slotFound = true;
//				break;
//			}
//		}
//		btPg->insertEntry(leafEntryBuf, islot, entrySize);
		btPg->insertEntryInOrder(leafEntry);
	} else {
		// space not available then split
		// and pop out the pageID from traversalStack and repeat
		// traversalStack won't contain the leaf node.
		// it will contain ancestors of leaf node

		// we have to prepare two split leaf nodes
		// then insert the middle one in the ancestor page num stored in traversal stack
		// repeat above two steps until step 2 does not cause a split
		traversal.push(currentPageNum);
		Entry* splitEntry; // entry whose insertion causes a split
		splitEntry = &leafEntry;
		bool exitWithoutSplit = false;

		while (!traversal.empty()) {
			PageNum pageNum = traversal.top();
			traversal.pop();
			char* pageData = (char*) malloc(PAGE_SIZE);
			ixfileHandle.fileHandle.readPage(pageNum, pageData);
			btPg = new BTPage(pageData, attribute);

			if (btPg->isSpaceAvailableToInsertEntryOfSize(
					splitEntry->getEntrySize())) {
				btPg->insertEntryInOrder(*splitEntry);
				exitWithoutSplit = true;
				break;
			}
			IntermediateComparator icomp;
			SplitInfo *split = btPg->splitNodes(*splitEntry, icomp);

			// First write the right hand page. It does not need any further changes
			// the old left page's sibling pointer gets copied into the right page sibling
			// pointer in the split nodes method. So directly write this page to disk
			// don't move these lines. they need to be together
			ixfileHandle.fileHandle.appendPage(split->rightChild->getPage());
			PageNum rPg = ixfileHandle.fileHandle.getNumberOfPages();

			// store left's sibling as right page and write it to disk at its
			// old page number
			PageNum lPg = pageNum;
			split->leftChild->setSiblingNode(rPg);
			ixfileHandle.fileHandle.writePage(lPg, split->leftChild->getPage());
			// Now make //the proper connections from parent to left and right page
			split->iEntryParent->setLeftPtr(lPg);
			split->iEntryParent->setRightPtr(rPg);

			splitEntry = split->iEntryParent;
			delete[] split;

			// recursively push up the iEntryParent if needed
//		pushUp(ixfileHandle, attribute, split->iEntryParent, traversal);
		}
		if (!exitWithoutSplit) { // that means split was done till root level
			char* intermediateRootPageBuffer = (char*) malloc(PAGE_SIZE);
			BTPage::prepareEmptyBTPageBuffer(intermediateRootPageBuffer, INTERMEDIATE);
			btPg = new BTPage(intermediateRootPageBuffer, attribute);
			btPg->insertEntryInOrder(*splitEntry);

			ixfileHandle.fileHandle.appendPage(splitEntry->getEntryBuffer());
			PageNum pNum = ixfileHandle.fileHandle.getNumberOfPages();
			setRootPage(ixfileHandle, pNum);
		}

	}

	return success;
}
///**
// *
// * @param ixfileHandle
// * @param iEntry
// * @param pathToLeaf
// * @return ifRootChangeRequired
// */
//bool IndexManager::pushUp(IXFileHandle &ixfileHandle, const Attribute& attr,
//		IntermediateEntry &iEntry, stack<PageNum>& pathToLeaf) {
//
//	IntermediateComparator icomp;
//	SplitInfo *split = btPg->splitNodes(leafEntry, icomp);
//
//	// First write the right hand page. It does not need any further changes
//	// the old left page's sibling pointer gets copied into the right page sibling
//	// pointer in the split nodes method. So directly write this page to disk
//	// don't move these lines. they need to be together
//	ixfileHandle.fileHandle.appendPage(split->rightChild->getPage());
//	PageNum rPg = ixfileHandle.fileHandle.getNumberOfPages();
//
//	// store left's sibling as right page and write it to disk at its
//	// old page number
//	PageNum lPg = currentPageNum;
//	split->leftChild->setSiblingNode(rPg);
//	ixfileHandle.fileHandle.writePage(lPg, split->leftChild->getPage());
//
//	// Now make the proper connections from parent to left and right page
//	split->iEntryParent->setLeftPtr(lPg);
//	split->iEntryParent->setRightPtr(rPg);
//
//}

//}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle,
	const Attribute &attribute, const void *key, const RID &rid) {
return -1;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
	const void *lowKey, const void *highKey, bool lowKeyInclusive,
	bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {
return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle,
	const Attribute &attribute) const {

	PageNum rootPageID = getRootPageID(ixfileHandle);
	char* pageData = (char*) malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(rootPageID, pageData);
	BTPage btPage(pageData, attribute);

	printBtree(&btPage);
	/*
	 * {
"keys":["P"],
"children":[
    {"keys":["C","G","M"],
     "children": [
        {"keys": ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"]},
        {"keys": ["D:[(3,1),(3,2)]","E:[(4,1)]","F:[(5,1)]"]},
        {"keys": ["J:[(5,1),(5,2)]","K:[(6,1),(6,2)]","L:[(7,1)]"]},
        {"keys": ["N:[(8,1)]","O:[(9,1)]"]}
    ]},
    {"keys":["T","X"],
     "children": [
        {"keys": ["Q:[(10,1)]","R:[(11,1)]","S:[(12,1)]"]},
        {"keys": ["U:[(13,1)]","V:[(14,1)]"]},
        {"keys": ["Y:[(15,1)]","Z:[(16,1)]"]}
    ]}
]
}
	 */


}

IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
return -1;
}

RC IX_ScanIterator::close() {
return -1;
}

IXFileHandle::IXFileHandle() {
ixReadPageCounter = 0;
ixWritePageCounter = 0;
ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount,
	unsigned &writePageCount, unsigned &appendPageCount) {
if (fileHandle.collectCounterValues(readPageCount, writePageCount,
		appendPageCount) != -1)
	return 0;
return -1;
}

Entry::Entry(char* entry, AttrType atype) {
this->aType = atype;
this->entry = entry;
switch (aType) {
case TypeReal:
	key = new FloatKey();
	break;
case TypeInt:
	key = new IntKey();
	break;
case TypeVarChar:
	key = new StringKey();
	break;
}
}

Entry* Entry::getEntry(char* entry, AttrType aType, BTPageType pageType) {
Entry *ptrEntry = NULL;

switch (pageType) {
case INTERMEDIATE:
	ptrEntry = new IntermediateEntry(entry, aType);
	break;
case LEAF:
	ptrEntry = new Entry(entry, aType);
	break;
default:
	cerr << "Error: Invalid page type";
}
return ptrEntry;
}

Entry::~Entry() {
if (key) {
	delete key;
}
}
Key* Entry::getKey() {
int offset = getKeyOffset();
return key->setKeyData(entry, offset);
}
RID Entry::getRID() {
int offset = getRIDOffset();
memcpy(&rid, entry + offset, sizeof(rid));
return rid;
}

int Entry::getKeyOffset() {
return 0;
}

int Entry::getRIDOffset() {
return getKeyOffset() + key->getKeySize();
}

Key::~Key() {

}

Key* StringKey::setKeyData(char* entry, int offset) {
int length;
memcpy(&length, entry + offset, sizeof(length));
offset += sizeof(length);
data.assign(entry + offset, entry + offset + length);
keySize = sizeof(length) + length;

return this;
}

r_slot StringKey::getKeySize() {
if (keySize == 0) {
	cerr << "Before asking for size, set key data using setKeyData" << endl;
}
return keySize;
}

string StringKey::getData() {
return data;
}

int StringKey::compare(Key& other) {
string otherString = static_cast<StringKey&>(other).getData();
return this->data.compare(otherString);
}

Key* IntKey::setKeyData(char* entry, int offset) {
memcpy(&data, entry + offset, sizeof(int));
return this;
}

r_slot IntKey::getKeySize() {
return sizeof(int);
}

int IntKey::getData() {
return data;
}

int IntKey::compare(Key& other) {

int thisData = this->getData();
int otherData = static_cast<IntKey&>(other).getData();

return thisData - otherData;
}

Key* FloatKey::setKeyData(char* entry, int offset) {
memcpy(&data, entry + offset, sizeof(float));
return this;
}

r_slot FloatKey::getKeySize() {
return sizeof(float);
}

float FloatKey::getData() {
return this->data;
}
int FloatKey::compare(Key& other) {

float thisData = this->getData();
float otherData = static_cast<FloatKey&>(other).getData();

if (fabs(thisData - otherData) < FLT_EPSILON)
	return 0;

else if (thisData - otherData < 0)
	return -1;
else
	return 1;
}

IntermediateEntry::IntermediateEntry(char* entry, AttrType aType) :
	Entry(entry, aType) {
}

PageNum IntermediateEntry::getLeftPtr() {
memcpy(&leftPtr, this->entry, sizeof(leftPtr));
return leftPtr;
}

PageNum IntermediateEntry::getRightPtr() {
memcpy(&rightPtr,
		this->entry + sizeof(leftPtr) + key->getKeySize() + sizeof(rid),
		sizeof(rightPtr));
return rightPtr;
}

int IntermediateEntry::getKeyOffset() {
return sizeof(leftPtr);
}

int IntermediateEntry::getRIDOffset() {
return sizeof(leftPtr) + key->getKeySize();
}

EntryComparator::~EntryComparator() {

}

int IntermediateComparator::compare(Entry a, Entry b) {
if (a.getKey()->compare(*(b.getKey())) != 0)
	return a.getKey()->compare(*(b.getKey()));
else {
	RID aRid = a.getRID();
	RID bRid = b.getRID();

	if (aRid.pageNum != bRid.pageNum)
		return aRid.pageNum - bRid.pageNum;
	else
		return aRid.slotNum - bRid.slotNum;
}
}

int LeafComparator::compare(Entry a, Entry b) {
Key *aKey = a.getKey();
Key *bKey = b.getKey();

return aKey->compare(*bKey);
}
BTPage::BTPage(const char *pageData, const Attribute &attribute) {
//	pageBuffer = pageData;
pageBuffer = (char *) malloc(PAGE_SIZE);
memcpy(pageBuffer, pageData, PAGE_SIZE);
readAttribute(attribute);
readPageType();
readSiblingNode();
readPageRecordInfo();
readSlotDirectory();
}

void BTPage::readAttribute(const Attribute &attribute) {
this->attribute = attribute;
}

void BTPage::readSiblingNode() {
memcpy(&siblingPage, pageBuffer + sizeof(pageType), sizeof(siblingPage));
}
void BTPage::readPageType() {
memcpy(&pageType, pageBuffer, sizeof(pageType));
}

void BTPage::readPageRecordInfo() {
memcpy(&pri, pageBuffer + PAGE_SIZE - sizeof(pri), sizeof(pri));
}

/**
 * should be called after initializing {@link #pri}
 */
void BTPage::readSlotDirectory() {
slots.clear();
for (r_slot sloti = 0; sloti < pri.numberOfSlots; sloti++) {
	SlotDirectory currentSlot;
	// page number not needed to get slot. hence hardcoded to 0
	getSlotForRID(pageBuffer, RID { 0, sloti }, currentSlot);
	slots.push_back(currentSlot);
}
}

bool BTPage::isSpaceAvailableToInsertEntryOfSize(r_slot length) {
unsigned freeSpaceAvailable = PAGE_SIZE - pri.freeSpacePos;
if (freeSpaceAvailable >= length + sizeof(struct SlotDirectory)) {
	return true;
} else
	return false;
}

RC BTPage::insertEntry(const char * const entry, int slotNumber, int length) {
if (!isSpaceAvailableToInsertEntryOfSize(length)) {
	// I need to split the leaf page and send information about
	// split node to callee so that an intermediate node entry can be
	// created
	return failure;
}
SlotDirectory slot;
slot.offset = pri.freeSpacePos;
slot.length = length;
memcpy(pageBuffer + slot.offset, entry, slot.length);

slots.insert(slots.begin() + slotNumber, slot);
copySlotsToPage(slots, pageBuffer);
pri.freeSpacePos += length;
pri.numberOfSlots += 1;
updatePageRecordInfo(pri, pageBuffer);

return success;
}

void BTPage::copySlotsToPage(vector<SlotDirectory> slots, char *pageBuffer) {
memcpy(
		pageBuffer + PAGE_SIZE - sizeof(pri)
				- sizeof(struct SlotDirectory) * slots.size(), slots.data(),
		slots.size() * sizeof(struct SlotDirectory));
}
/**
 * soft deletes the entry from the page and returns the
 * entry in form of a char buffer
 * @param slotNumber
 * @return
 */

RC BTPage::removeEntry(int slotNumber, char * const entryBuf) {

readEntry(slotNumber, entryBuf);
SlotDirectory slotToDelete = slots[slotNumber];
r_slot nextSlotOffset = slotToDelete.offset + slotToDelete.length;

shiftRecord(pageBuffer, nextSlotOffset, -slotToDelete.length);

pri.numberOfSlots--;
pri.freeSpacePos -= slotToDelete.length;
setPageRecordInfo();

slots.erase(slots.begin() + slotNumber);
setSlotDirectory();

return success;

}

void BTPage::setPageType(BTPageType pgType) {
memcpy(pageBuffer, &pgType, sizeof(pgType));
this->pageType = pgType;
}

void BTPage::setPageRecordInfo() {
if (pri.numberOfSlots != slots.size()) {
	cout << "BTPage::setPageRecordInfo() : Number of slots in page record info"
			<< pri.numberOfSlots;
	cout << "BTPage::setPageRecordInfo() : Number of slots in slots vector "
			<< slots.size();
}

updatePageRecordInfo(pri, pageBuffer);
}

void BTPage::setSlotDirectory() {
//	for (r_slot sloti = 0; sloti < pri.numberOfSlots; sloti++) {
//		SlotDirectory currentSlot;
//		// page number not needed to get slot. hence hardcoded to 0
//		getSlotForRID(pageBuffer, RID{0, sloti}, currentSlot);
//		slots.push_back(currentSlot);
//	}
if (pri.numberOfSlots != slots.size()) {
	cout << "BTPage::setSlotDirectory() : Number of slots in page record info"
			<< pri.numberOfSlots;
	cout << "BTPage::setSlotDirectory() : Number of slots in slots vector "
			<< slots.size();
}
memcpy(
		pageBuffer + PAGE_SIZE - sizeof(pri)
				- sizeof(struct SlotDirectory) * slots.size(), slots.data(),
		slots.size() * sizeof(struct SlotDirectory));

}

void BTPage::setSiblingNode(PageNum rightSiblingPgNum) {
int offset = sizeof(pageType);
memcpy(pageBuffer + offset, &(rightSiblingPgNum), sizeof(rightSiblingPgNum));
}

void BTPage::printPage() {
cout << "L/I : " << this->pageType;
cout << "SP  : " << this->siblingPage;
cout << "FS  : " << this->pri.freeSpacePos;
cout << "NS  : " << this->pri.numberOfSlots;
int i = 0;
for (SlotDirectory slot : slots) {
	cout << "Slot" << i << "  :  " << slot.offset << "," << slot.length << endl;
	i++;
}
}

RC BTPage::shiftRecord(char *pageData, r_slot slotToShiftOffset,
	int byBytesToShift) {

PageRecordInfo pri;
getPageRecordInfo(pri, pageData);

if (slotToShiftOffset + byBytesToShift < 0||
byBytesToShift + pri.freeSpacePos + getRecordDirectorySize(pri) > PAGE_SIZE) {
	return failure;
}

memmove(pageData + slotToShiftOffset + byBytesToShift,
		pageData + slotToShiftOffset, pri.freeSpacePos - slotToShiftOffset);

for (r_slot islot = 0; islot < pri.numberOfSlots; islot++) {
	RID ridOfRecordToShift;
//       ridOfRecordToShift.pageNum = rid.pageNum; page number not needed
	ridOfRecordToShift.slotNum = islot;

	SlotDirectory islotDir;
	getSlotForRID(pageData, ridOfRecordToShift, islotDir);

	if (islotDir.offset == USHRT_MAX) {
		continue;
	}

	if (islotDir.offset >= slotToShiftOffset) {
		islotDir.offset += byBytesToShift;
		updateSlotDirectory(ridOfRecordToShift, pageData, islotDir);
	}
}

return success;
}
BTPageType BTPage::getPageType() {
return pageType;
}

BTPage::~BTPage() {
if (pageBuffer) {
	free(pageBuffer);
}
}

int BTPage::getNumberOfSlots() {
return slots.size();
}

RC BTPage::readEntry(r_slot slotNum, char* const entryBuf) {
assert(slotNum < slots.size() && "Slot index out of bounds");

memcpy(entryBuf, pageBuffer + slots[slotNum].offset, slots[slotNum].length);

return success;
}

/**
 * TODO: Link the left page with the right page in the callee.
 *	use the setSiblingNode method to set the siblentrying page data
 *
 * @param insertEntry : entry to insert in BTPage. This is the entry that will cause split
 * @param comparator : Comparator to compare the entries
 * @return SplitInfo object
 *
 */
SplitInfo* BTPage::splitNodes(Entry &insertEntry, EntryComparator& comparator) {

SplitInfo* split = new SplitInfo();

// prepare left child page
char* leftChildBuf = (char*) malloc(PAGE_SIZE);
memset(leftChildBuf, 0, PAGE_SIZE);
BTPage::prepareEmptyBTPageBuffer(leftChildBuf, this->pageType);
split->leftChild = new BTPage(leftChildBuf, this->attribute);

// prepare right child page
char* rightChildBuf = (char*) malloc(PAGE_SIZE);
memset(rightChildBuf, 0, PAGE_SIZE);
BTPage::prepareEmptyBTPageBuffer(rightChildBuf, this->pageType);
split->rightChild = new BTPage(rightChildBuf, this->attribute);

// link the right child to left's sibling
// it is the  caller's responsibility to link the left page's sibling
// to right child's page
// num when the right child will be inserted in the page
split->rightChild->setSiblingNode(this->siblingPage);

Entry *slotEntry = NULL;
//bool haveIReachedMidWay = false;
BTPage *pageToLoad = split->leftChild;
for (uint islot = 0; islot < slots.size(); islot++) {
	if (islot * 2 >= slots.size() + 1) {
//		haveIReachedMidWay = true;
		//prepare intermediate buffer
		char* intermediateEntryBuffer = (char*) malloc(PAGE_SIZE);
		memset(intermediateEntryBuffer, 0, PAGE_SIZE);

		if (this->pageType == LEAF) {
			char* leafEntryBuf = (char*) malloc(PAGE_SIZE);
			memset(leafEntryBuf, 0, PAGE_SIZE);
			readEntry(islot, leafEntryBuf);

			int leafEntrySize = slots[islot].length;
			prepareIntermediateEntry(intermediateEntryBuffer, leafEntryBuf,
					leafEntrySize, NULL_PAGE, NULL_PAGE);
		} else {
			readEntry(islot, intermediateEntryBuffer);
		}
		split->iEntryParent = new IntermediateEntry(intermediateEntryBuffer,
				this->attribute.type);
		pageToLoad = split->rightChild;

	}
	char* entryBuf = (char*) malloc(PAGE_SIZE);
	memset(entryBuf, 0, PAGE_SIZE);

	readEntry(islot, entryBuf);
	slotEntry = Entry::getEntry(entryBuf, attribute.type, pageType);

	int difference = comparator.compare(*slotEntry, insertEntry);

//	if (difference == 0)// both are same
//	if (difference > 0) // pageSlotEntry > insertEntry
	if (difference > 0) // pageSlotEntry < insertEntry
	{
		pageToLoad->appendEntry(insertEntry.getEntryBuffer(),
				insertEntry.getEntrySize());
	}

	pageToLoad->appendEntry(slotEntry->getEntryBuffer(), slots[islot].length);

	delete[] slotEntry;
	free(entryBuf);
}
return split;
}

RC BTPage::appendEntry(const char * const entry, int length) {
if (!isSpaceAvailableToInsertEntryOfSize(length))
	return failure;
SlotDirectory slot;
slot.offset = pri.freeSpacePos;
slot.length = length;

memcpy(pageBuffer + slot.offset, entry, length);

pri.freeSpacePos += length;
pri.numberOfSlots += 1;

slots.push_back(slot);
copySlotsToPage(slots, pageBuffer);

updatePageRecordInfo(pri, pageBuffer);

}

r_slot Entry::getEntrySize() {

return key->getKeySize() + sizeof(rid);
}

r_slot IntermediateEntry::getEntrySize() {
return sizeof(leftPtr) + key->getKeySize() + sizeof(rid) + sizeof(rightPtr);
}

char* Entry::getEntryBuffer() {
return this->entry;
}

void IntermediateEntry::setLeftPtr(PageNum lPg) {
leftPtr = lPg;
memcpy(entry, &lPg, sizeof(lPg));
}
void IntermediateEntry::setRightPtr(PageNum rPg) {
rightPtr = rPg;
int rightPointerOffset = sizeof(leftPtr) + key->getKeySize() + sizeof(rid);
memcpy(entry + rightPointerOffset, &rPg, sizeof(rPg));
}

RC BTPage::insertEntryInOrder(Entry& entry) {
if (!isSpaceAvailableToInsertEntryOfSize(entry.getEntrySize())) {
	return failure;
}

r_slot islot = 0;
r_slot numberOfSlots = this->getNumberOfSlots();
IntermediateComparator icomp; // for insert cmp both key,rid in leaf
char* entryBuf = (char*) malloc(PAGE_SIZE);
bool slotFound = false;
while (islot < numberOfSlots) {
	this->readEntry(islot, entryBuf);
	islot++;
	Entry pageLeafEntry(entryBuf, attribute.type);
	if (icomp.compare(entry, pageLeafEntry) > 0) {
		slotFound = true;
		break;
	}
}
this->insertEntry(entry.getEntryBuffer(), islot, entry.getEntrySize());
return success;
}

char* BTPage::getPage(){
	return this->pageBuffer;
}

void IndexManager::printBtree(BTPage* root) const {

//	printf(R"( { "keys" : ["%s"], )", root->ge)
}
