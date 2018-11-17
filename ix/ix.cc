
#include "ix.h"

#include <float.h>
#include <stdlib.h>
#include <climits>
#include <cmath>
#include <cstring>
#include <iostream>
#include <iterator>
#include <stack>
#include <cassert>

#include "../rbf/pfm.h"

IndexManager* IndexManager::_index_manager = 0;

void prepareLeafEntry(char *leafEntryBuf,const void* key, RID rid, const Attribute &attribute){

	int keySize = 0;
	if (attribute.type == TypeVarChar){
		int length =0;
		memcpy(&length, key, sizeof(length));
		keySize = sizeof(length) + length;

	}
	else{
		keySize = sizeof(int);
	}
	memcpy(leafEntryBuf, key, keySize);
	memcpy(leafEntryBuf+keySize, &rid, sizeof(rid));
}
IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
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
  char *headerPage = (char *)malloc(PAGE_SIZE);
  int offset = 0;
  memcpy(headerPage, &ROOT_PAGE_NUM, sizeof(ROOT_PAGE_NUM));
  offset += sizeof(ROOT_PAGE_NUM);
  memcpy(headerPage + offset, &keyAttributeType, sizeof(keyAttributeType));
  offset += sizeof(keyAttributeType);
  memcpy(headerPage + offset, &keyAttributeLength, sizeof(keyAttributeLength));
  offset += sizeof(keyAttributeLength);
  ixfileHandle.fileHandle.appendPage(headerPage);  // header page id is 0
  free(headerPage);
  headerPage = NULL;

  // set up root page data
  char *rootPage = (char *)malloc(PAGE_SIZE);
  // set leaf/intermediate indicator
  BTPageType pageType = BTPageType::LEAF;
  offset = 0;
  memcpy(rootPage, &pageType, sizeof(pageType));
  offset += sizeof(pageType);
  // set sibling pointer. will be used only in leaf node
  PageNum siblingPage = UINT_MAX;
  memcpy(rootPage + offset, &siblingPage, sizeof(siblingPage));
  offset += sizeof(siblingPage);

  PageRecordInfo pri;
  pri.freeSpacePos = offset;
  pri.numberOfSlots = 0;
  updatePageRecordInfo(pri, rootPage);

  ixfileHandle.fileHandle.appendPage(rootPage);  // root page id is 1
  free(rootPage);
  rootPage = NULL;

  return success;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return pfm->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
    return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return pfm->openFile(fileName, ixfileHandle.fileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return pfm->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	if (ixfileHandle.fileHandle.getNumberOfPages() == 0){
		setUpIndexFile(ixfileHandle, attribute);
	}

	char* leafEntryBuf = (char*) malloc(PAGE_SIZE);
	prepareLeafEntry(leafEntryBuf,key, rid, attribute);
	Entry leafEntry(leafEntryBuf, attribute.type);


	PageNum rootPageId = getRootPageID(ixfileHandle);
	char* pageData = (char*) malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(rootPageId, pageData);

	BTPage *btPg = new BTPage(pageData, attribute);
	stack<PageNum> traversal;
	PageNum pgToTraverse = rootPageId;
	while(btPg->getPageType() != BTPageType::LEAF){
		traversal.push(pgToTraverse);
		char* entry = (char*) malloc(PAGE_SIZE);
		r_slot islot;
		IntermediateEntry *ptrIEntry = NULL;
		for ( islot = 0; islot < btPg->getNumberOfSlots(); islot++){
			btPg->readEntry(islot, entry);
			ptrIEntry = new IntermediateEntry (entry, attribute.type);
			IntermediateComparator iComp;
			if(iComp.compare(*ptrIEntry, leafEntry) > 0){ // if entry in node greater than leaf entry
				break;

			}
		}
		if (islot < btPg->getNumberOfSlots()){ // islot in range
			pgToTraverse = ptrIEntry->getLeftPtr();
			if(btPg) delete[] btPg;
			memset(pageData,0, PAGE_SIZE);
			ixfileHandle.fileHandle.readPage(pgToTraverse, pageData);
			btPg = new BTPage(pageData, attribute);
		}
		else{ //islot is out of range
// this means all entries in the file were smaller than
// ientry and that caused all slots to be read
// But it may also mean that there were no entries in the file
// In the first case, we have to traverse to the rightPtr of the
		}

	}
    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    if(fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount) != -1)
		return 0;
    return -1;
}


Entry::Entry(char* entry, AttrType atype){
	this->aType = aType;
	this->entry = entry;
	switch(aType){
	case TypeReal : key = new FloatKey(); break;
	case TypeInt : key = new IntKey(); break;
	case TypeVarChar : key = new StringKey(); break;
	}
}
Entry::~Entry(){
	if (key){
		delete[] key;
	}
}
Key* Entry::getKey(){
	int offset = getKeyOffset();
	return key->setKeyData(entry, offset);
}
RID Entry::getRID(){
	int offset = getRIDOffset();
	memcpy(&rid, entry+offset, sizeof(rid));
	return rid;
}

int Entry::getKeyOffset(){
	return 0;
}

int Entry::getRIDOffset(){
	return getKeyOffset() + key->getKeySize();
}

Key::~Key(){

}

Key* StringKey::setKeyData(char* entry, int offset){
 int length;
 memcpy(&length, entry+ offset, sizeof(length));
 offset+=sizeof(length);
 data.assign(entry+offset, entry+offset+length);
 keySize = sizeof(length) + length;

 return this;
}

r_slot StringKey::getKeySize(){
	if (keySize == 0){
		cerr << "Before asking for size, set key data using setKeyData" <<endl;
	}
	return keySize;
}

string StringKey::getData(){
return data;
}

int StringKey::compare(Key& other){
	string otherString = static_cast<StringKey&>(other).getData();
	return this->data.compare(otherString);
}

Key* IntKey::setKeyData(char* entry, int offset){
	memcpy(&data, entry+offset, sizeof(int));
	return this;
}

r_slot IntKey::getKeySize(){
	return sizeof(int);
}

int IntKey::getData(){
	return data;
}

int IntKey::compare(Key& other){

	int thisData = this->getData();
	int otherData = static_cast<IntKey&>(other).getData();

	return thisData - otherData;
}

Key* FloatKey::setKeyData(char* entry, int offset){
	memcpy(&data, entry+offset, sizeof(float));
	return this;
}

r_slot FloatKey::getKeySize(){
	return sizeof(float);
}

float FloatKey::getData(){
	return this->data;
}
int FloatKey::compare(Key& other){

	float thisData = this->getData();
	float otherData = static_cast<FloatKey&>(other).getData();

	if (fabs(thisData - otherData) < FLT_EPSILON)
	return 0;

	else if (thisData - otherData < 0)
		return -1;
	else
		return 1;
}

IntermediateEntry::IntermediateEntry(char* entry, AttrType aType): Entry(entry, aType){
}

PageNum IntermediateEntry::getLeftPtr(){
	memcpy(&leftPtr, this->entry, sizeof(leftPtr));
	return leftPtr;
}

PageNum IntermediateEntry::getRightPtr(){
	memcpy(&rightPtr, this->entry + sizeof(leftPtr) + key->getKeySize() + sizeof(rid), sizeof(rightPtr));
	return rightPtr;
}

int IntermediateEntry::getKeyOffset(){
	return sizeof(leftPtr);
}

int IntermediateEntry::getRIDOffset(){
	return sizeof(leftPtr) + key->getKeySize();
}

EntryComparator::~EntryComparator(){

}

int IntermediateComparator::compare(Entry a, Entry b){
	if (a.getKey()->compare(*(b.getKey())) != 0)
		return a.getKey()->compare(*(b.getKey()));
	else
	{
		RID aRid = a.getRID();
		RID bRid = b.getRID();

		if (aRid.pageNum != bRid.pageNum)
			return aRid.pageNum - bRid.pageNum;
		else
			return aRid.slotNum - bRid.slotNum;
	}
}

int LeafComparator::compare(Entry a, Entry b){
	Key *aKey = a.getKey();
	Key *bKey = b.getKey();

	return aKey->compare(*bKey);
}
BTPage::BTPage(char *pageData, const Attribute &attribute) {
  //	pageBuffer = pageData;
  pageBuffer = (char *)malloc(PAGE_SIZE);
  memcpy(pageBuffer, pageData, PAGE_SIZE);
  setAttribute(attribute);
  setPageType();
  setSiblingNode();
  setPageRecordInfo();
  setSlotDirectory();
}

void BTPage::setAttribute(const Attribute &attribute) {
  this->attribute = attribute;
}

void BTPage::setSiblingNode() {
  memcpy(&siblingPage, pageBuffer + 1, sizeof(siblingPage));
}
void BTPage::setPageType() { memcpy(&pageType, pageBuffer, 1); }

void BTPage::setPageRecordInfo() {
  memcpy(&pri, pageBuffer, PAGE_SIZE - sizeof(pri));
}

/**
 * should be called after initializing {@link #pri}
 */
void BTPage::setSlotDirectory() {
  for (r_slot sloti = 0; sloti < pri.numberOfSlots; sloti++) {
    SlotDirectory currentSlot;
    // page number not needed to get slot. hence hardcoded to 0
    getSlotForRID(pageBuffer, RID{0, sloti}, currentSlot);
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

RC BTPage::insertEntry(char *entry, int slotNumber, int length) {
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
  memcpy(pageBuffer + PAGE_SIZE - sizeof(pri) -
             sizeof(struct SlotDirectory) * slots.size(),
         slots.data(), slots.size() * sizeof(struct SlotDirectory));
}
/**
 * soft deletes the entry from the page and returns the
 * entry in form of a char buffer
 * @param slotNumber
 * @return
 */
char *BTPage::removeEntry(int slotNumber) { return NULL; }

BTPageType BTPage::getPageType() { return pageType; }

BTPage::~BTPage() {
  if (pageBuffer) {
    free(pageBuffer);
  }
}

int BTPage::getNumberOfSlots(){
	return slots.size();
}

RC BTPage::readEntry(r_slot slotNum, char* entryBuf){
	assert(slotNum < slots.size() && "Slot index out of bounds");

	memcpy(entryBuf, pageBuffer + slots[slotNum].offset, slots[slotNum].length);

	return success;
}
