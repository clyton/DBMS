
#include "ix.h"

#include <stdlib.h>
#include <cstring>
#include <iostream>

#include "../rbf/pfm.h"

IndexManager* IndexManager::_index_manager = 0;

PageNum getRootPageID(IXFileHandle& ixfileHandle){
	char* data = (char*) malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(0, data);

	PageNum rootPageID;
	memcpy(&rootPageID, data, sizeof(rootPageID));

	return rootPageID;

}
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
	char* leafEntryBuf = (char*) malloc(PAGE_SIZE);
	prepareLeafEntry(leafEntryBuf,key, rid, attribute);

	PageNum rootPageId = getRootPageID(ixfileHandle);
	char* pageData = (char*) malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(rootPageId, pageData);

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

Key* IntKey::setKeyData(char* entry, int offset){
	memcpy(&data, entry+offset, sizeof(int));
	return this;
}

r_slot IntKey::getKeySize(){
	return sizeof(int);
}

Key* FloatKey::setKeyData(char* entry, int offset){
	memcpy(&data, entry+offset, sizeof(float));
	return this;
}

r_slot FloatKey::getKeySize(){
	return sizeof(float);
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
