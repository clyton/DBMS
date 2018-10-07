#include <rbf/rbfm.h>
#include <stddef.h>
#include <stdlib.h>
#include <cmath>
#include <cstdint>
#include <cstring>

struct Rinf{
	unsigned int startPos = 0;
	unsigned int offset = 0;
};
/**
 * Assuming that record size ranges from 100 to 1000 bytes.
 * We assume that in 80% of the cases, our record size is 100
 * So the weighted average is 0.8*100 + 0.2*1000 = 280 ~ 256
 * We consider the average record size to be 256 bytes.
 * Therefore, a page of size 4096 bytes will hold 16 (4096/256) records.
 */
const unsigned RECORDS_PER_PAGE = 16;

struct RecordDictionary{
	unsigned char numberOfRecords = 0;
	/**
	 * @{code freeSpacePointer} points to the first free space position available in the page
	 *
	 */
	unsigned int freeSpacePos = sizeof(struct RecordDictionary);
	Rinf recordInfo[RECORDS_PER_PAGE];
};
/**
 * This method gets the best first page found where a record of size specified
 * by @{code sizeInBytes} will fit.
 * If sizeInBytes is greater than @{code PAGE_SIZE - sizeof(RecordDictionary)}
 * then return PageNum.max
 * If none of the pages in the file can occupy
 * the record, a new page is appended to the file and a RecordDictionary will be
 * created for it. That page number will be returned
 * @param fileHandle
 * @param sizeInBytes
 * @return
 */
PageNum getPageForRecordOfSize(FileHandle &fileHandle, size_t sizeInBytes){
	if (sizeInBytes > (PAGE_SIZE - sizeof(struct RecordDictionary))){
		return UINT_MAX;// sentinel value
	}
	PageNum pageNum;
	void *data = malloc(PAGE_SIZE);
	RecordDictionary *dict;
	for(pageNum=0; pageNum < fileHandle.getNumberOfPages(); pageNum++){
         fileHandle.readPage(pageNum, data);
         dict = reinterpret_cast<RecordDictionary *> (data);
         unsigned freeSpaceAvailable = PAGE_SIZE - dict->freeSpacePos;
         if (freeSpaceAvailable > sizeInBytes || dict->numberOfRecords < RECORDS_PER_PAGE){
        	 free(data);
        	 return pageNum;
         }
	}
	// none of the existing pages can fit the record
	if (pageNum == fileHandle.getNumberOfPages()){
		memset(data,0,PAGE_SIZE);
		memcpy(data,dict,sizeof(struct RecordDictionary));
		fileHandle.appendPage(data);
	}
	free(data);
	return fileHandle.getNumberOfPages() - 1;
}
RC createRecordDictionary(FileHandle &fileHandle){
	void *data = malloc(PAGE_SIZE);
	memset(data, 0, PAGE_SIZE);
	RecordDictionary *dict = new RecordDictionary;
	memcpy(data, dict, sizeof(struct RecordDictionary));
	fileHandle.appendPage(data);
	free(data);
	return 0;
}

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	if (recordDescriptor.size() == 0){
		return -1;
	}
	size_t recordSizeInBytes = 0;
	// find the memory needed to store a record
	for (size_t attri=0; attri < recordDescriptor.size(); attri++){
		Attribute recordAttribute = recordDescriptor.at(attri);
		if (recordAttribute.type == TypeInt || recordAttribute.type == TypeReal){
			recordSizeInBytes += 4;
		}
		else{
			recordSizeInBytes = recordSizeInBytes + 4 + recordAttribute.length;
		}
	}
	// Add the space needed for the null bytes indicator
	int numberOfFields = recordDescriptor.size();
	int nullFieldsIndicatorLength = ceil(numberOfFields/8.0);
	recordSizeInBytes += nullFieldsIndicatorLength;


	// prepare the null indicator array
	char* nullIndicatorArray = (char*) malloc(nullFieldsIndicatorLength);
	memset(nullIndicatorArray,0,nullFieldsIndicatorLength);
	memcpy(nullIndicatorArray, data, nullFieldsIndicatorLength);

	// Allocate memory for record data to write to page
	char *record = (char *)malloc(recordSizeInBytes);

	// copy the nullIndicator bytes into record
	memset(record, 0, recordSizeInBytes);
	memcpy(record, data, nullFieldsIndicatorLength);
	int offset = nullFieldsIndicatorLength;

	// copy the field values into the record
	for(int attri=0; attri<numberOfFields; attri++){
		bool isNull =  nullIndicatorArray[attri] & (1<<(numberOfFields - attri) );
		if (isNull){
			continue;
		}

		Attribute currAttr = recordDescriptor[attri];
		if (currAttr.type == TypeVarChar){
			memcpy(record+offset, (char *)data+offset, currAttr.length + 4);
			offset = offset +  currAttr.length + 4;
		}
		else {
			memcpy(record+offset, (char *)data+offset, currAttr.length);
			offset += 4;
		}
	}

	// search for a page with free space greater than the record size
	PageNum pageNum = getPageForRecordOfSize(fileHandle, recordSizeInBytes);
	if (pageNum == UINT_MAX){
		free(nullIndicatorArray);
		free(record);
		return -1; // no insert possible for records greater than page size
	}

	// Now read the record dictionary and find the first empty slot for insertion
	char *pageRecordData = (char *)malloc(PAGE_SIZE);
	RecordDictionary *dict;
	fileHandle.readPage(pageNum, pageRecordData);
	dict = reinterpret_cast<RecordDictionary*>(pageRecordData);
	unsigned int newRecordStart = dict->freeSpacePos;
	unsigned char newRecordNum = dict->numberOfRecords;

	// Then update the dictionary and insert record at freeSpacePointer
	dict->recordInfo[newRecordNum].startPos = newRecordStart;
	dict->recordInfo[newRecordNum].offset = recordSizeInBytes;
	dict->freeSpacePos = newRecordStart + (unsigned int)recordSizeInBytes;

	memcpy(pageRecordData+newRecordStart, record, recordSizeInBytes);
	fileHandle.writePage(pageNum, pageRecordData);

	// update the new rid for the record
	rid.pageNum=pageNum;
	rid.slotNum=newRecordNum;
	// return 0

	free(record);
	free(nullIndicatorArray);
	free(pageRecordData);

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	void *pageData = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);

	RecordDictionary dict;
	memcpy(&dict, pageData, sizeof(struct RecordDictionary));

	Rinf readRecordInfo = dict.recordInfo[rid.slotNum];
	memcpy(data, (char *)pageData + readRecordInfo.startPos, readRecordInfo.offset);

	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}

