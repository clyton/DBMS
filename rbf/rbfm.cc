#include <rbf/rbfm.h>
#include <stddef.h>
#include <stdlib.h>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <ostream>

#define SLOT_SIZE sizeof(struct SlotDirectory)

typedef unsigned short int r_slot;
struct SlotDirectory {
	r_slot offset = 0;
	r_slot length = 0;
};

struct PageRecordInfo {
	r_slot numberOfRecords = 0;
	/**
	 * @{code freeSpacePointer} points to the first free space position available in the page
	 *
	 */
	r_slot freeSpacePos = 0;
};

/**
 * Start location of this struct in a page
 */
const r_slot PAGE_RECORD_INFO_OFFSET = PAGE_SIZE
		- sizeof(struct PageRecordInfo);

/**
 *
 * @param pri
 * @return the size taken by array of SlotDirectories and the
 * 		   PageRecordInfo structure
 */
r_slot getRecordDirectorySize(PageRecordInfo pri) {
	r_slot numberOfRecords = pri.numberOfRecords;
	return (numberOfRecords * sizeof(struct SlotDirectory) + sizeof(struct PageRecordInfo));
	cout << "Entire Record Directory size is calculated as"<< numberOfRecords * sizeof(struct SlotDirectory) + sizeof(struct PageRecordInfo)<< endl;
}

void getPageRecordInfo(PageRecordInfo& pageRecordInfo, char const* pageData) {
	memcpy(&pageRecordInfo,  (pageData) + PAGE_RECORD_INFO_OFFSET,
			sizeof(struct PageRecordInfo));
	cout << "Page Record Info is getting read from"<< 0 + PAGE_RECORD_INFO_OFFSET<< endl;
}

void putPageRecordInfo(PageRecordInfo &pri, char* pageData) {
	memcpy( (pageData) + PAGE_RECORD_INFO_OFFSET, &pri,
			sizeof(struct PageRecordInfo));
	cout << "Page Record Info is getting added at"<< 0 + PAGE_RECORD_INFO_OFFSET<< endl;
}

void addSlotDirectory(PageRecordInfo &pri, SlotDirectory &slot, char* pageData) {
	memcpy(
			 (pageData) - sizeof(struct SlotDirectory) + PAGE_SIZE
					- getRecordDirectorySize(pri), &slot,
			sizeof(struct SlotDirectory));
	cout << "Slot directory is getting added at"<< 0 - sizeof(struct SlotDirectory) + PAGE_SIZE - getRecordDirectorySize(pri) << endl;
}

SlotDirectory getSlotForRID(char const* pageData, RID rid, SlotDirectory &slot){
unsigned int slotNumber = rid.slotNum;

r_slot slotStartPos = PAGE_SIZE - sizeof(struct PageRecordInfo) - sizeof(struct SlotDirectory)*(slotNumber + 1);
memcpy(&slot, pageData + slotStartPos, sizeof(struct SlotDirectory));
cout << "Slot for RID slot: " << rid.slotNum << " is : "<< 0 + slotStartPos << endl;
return slot;

}

/**
 * This method gets the best first page found where a record of size specified
 * by @{code sizeInBytes} will fit.
 * If none of the pages in the file can occupy
 * the record, a new page is appended to the file and its recordDirectory is
 * updated for insert
 *
 * @param fileHandle
 * @param sizeInBytes
 * @param pageData : pointer pointing to a memory buffer of size PAGE_SIZE
 * @return
 */
PageNum getPageForRecordOfSize(FileHandle &fileHandle, r_slot sizeInBytes,
		char* pageData,r_slot &recordNo, r_slot &startPos) {
//	if (sizeInBytes > (PAGE_SIZE - sizeof(struct SlotDirectory)- sizeof(struct PageRecordInfo))) {
//		return UINT_MAX; // sentinel value to indicate impossible to add record
//	}
	PageNum pageNum;
	for (pageNum = 0; pageNum < fileHandle.getNumberOfPages(); pageNum++) {
		fileHandle.readPage(pageNum, pageData);
		PageRecordInfo pageRecordInfo;
		getPageRecordInfo(pageRecordInfo, pageData);
		r_slot freeSpaceAvailable = PAGE_SIZE
				- getRecordDirectorySize(pageRecordInfo)//slots + pageRecordInfo
				- (pageRecordInfo.freeSpacePos); // pg occupied from top
		if (freeSpaceAvailable > sizeInBytes) {
			SlotDirectory newSlot;
			newSlot.offset = pageRecordInfo.freeSpacePos;
			newSlot.length = sizeInBytes;
			addSlotDirectory(pageRecordInfo, newSlot, pageData);
			pageRecordInfo.numberOfRecords++;
			pageRecordInfo.freeSpacePos+=sizeInBytes;
			putPageRecordInfo(pageRecordInfo, pageData);
			recordNo = pageRecordInfo.numberOfRecords - 1;
			startPos = newSlot.offset;
			fileHandle.writePage(pageNum, pageData);
			return pageNum;
		}
	}
	// none of the existing pages can fit the record
	if (pageNum == fileHandle.getNumberOfPages()) {
		SlotDirectory slot;
		PageRecordInfo pri;
		slot.offset = 0;
		slot.length = sizeInBytes;
		addSlotDirectory(pri, slot, pageData);
		pri.freeSpacePos = sizeInBytes;
		pri.numberOfRecords = 1;
		putPageRecordInfo(pri, pageData);
		fileHandle.appendPage(pageData);
		recordNo = pri.numberOfRecords -1 ;
		startPos = slot.offset;
		cout << "Appending record in page " << pageNum << "at offset " << startPos << "The length of slot is" << slot.length << endl;
		cout << "Record Directory Entry: Number Of Records : " << pri.numberOfRecords << endl;
		cout << "Record Directory Entry: Free Space Pos: " << pri.freeSpacePos << endl;
//		cout << "Record Directory Entry: Free Space Pos: " << pri.freeSpacePos << endl;

	}
	return fileHandle.getNumberOfPages() - 1;
}

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance() {
	if (!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() {
	pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager() {
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName,
		FileHandle &fileHandle) {
	return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	if (recordDescriptor.size() == 0) {
		return -1;
	}
	size_t recordSizeInBytes = 0;
	// find the memory needed to store a record
	for (size_t attri = 0; attri < recordDescriptor.size(); attri++) {
		Attribute recordAttribute = recordDescriptor.at(attri);
		if (recordAttribute.type == TypeInt
				|| recordAttribute.type == TypeReal) {
			recordSizeInBytes += 4;
		} else {
			recordSizeInBytes = recordSizeInBytes + 4 + recordAttribute.length;
		}
	}
//	// Add the space needed for the null bytes indicator
	int numberOfFields = recordDescriptor.size();
	int nullFieldsIndicatorLength = ceil(numberOfFields / 8.0);
	recordSizeInBytes += nullFieldsIndicatorLength;
//
//	// prepare the null indicator array
//	unsigned char* nullIndicatorArray = (unsigned char*) malloc(
//			nullFieldsIndicatorLength);
//	memset(nullIndicatorArray, 0, nullFieldsIndicatorLength);
//	memcpy(nullIndicatorArray, data, nullFieldsIndicatorLength);
//
	// Allocate memory for record data to write to page
	void *record =  malloc(recordSizeInBytes);

	// copy the nullIndicator bytes into record
	memset(record, 0, recordSizeInBytes);
	memcpy(record, data, recordSizeInBytes);
//	memcpy(record, data, nullFieldsIndicatorLength);
//	int offset = nullFieldsIndicatorLength;

	// copy the field values into the record
//	for (int attri = 0; attri < numberOfFields; attri++) {
//		bool isNull = nullIndicatorArray[attri]
//				& (1 << (nullFieldsIndicatorLength * 8 - 1 - attri));
//		if (isNull) {
//			continue;
//		}
//
//		Attribute currAttr = recordDescriptor[attri];
//		if (currAttr.type == TypeVarChar) {
//			memcpy(record + offset, (char *) data + offset,
//					currAttr.length + 4);
//			offset = offset + currAttr.length + 4;
//		} else {
//			memcpy(record + offset, (char *) data + offset, currAttr.length);
//			offset += 4;
//		}
//	}

	// search for a page with free space greater than the record size

	char *pageRecordData = (char *) malloc(PAGE_SIZE);
	r_slot recordNo;
	r_slot startPos;
	PageNum pageNum = getPageForRecordOfSize(fileHandle, recordSizeInBytes, pageRecordData, recordNo, startPos);
	fileHandle.readPage(pageNum, pageRecordData);
	memcpy(pageRecordData + startPos, record, recordSizeInBytes);
	fileHandle.writePage(pageNum, pageRecordData);

	// Now read the record dictionary and find the first empty slot for insertion
//	fileHandle.readPage(pageNum, pageRecordData);
//	dict = reinterpret_cast<RecordDictionary*>(pageRecordData);
//	memcpy(&dict, pageRecordData, sizeof(struct RecordDictionary));
//	unsigned int newRecordStart = dict.freeSpacePos;
//	unsigned char newRecordNum = dict.numberOfRecords;
//
//	// Then update the dictionary and insert record at freeSpacePointer
//	dict.recordInfo[newRecordNum].startPos = newRecordStart;
//	dict.recordInfo[newRecordNum].offset = recordSizeInBytes;
//	dict.freeSpacePos = newRecordStart + (unsigned int) recordSizeInBytes;
//	dict.numberOfRecords++;
//
//	memcpy(pageRecordData, &dict, sizeof(struct RecordDictionary));
//	memcpy(pageRecordData + newRecordStart, record, recordSizeInBytes);
//	fileHandle.writePage(pageNum, pageRecordData);
//
	// update the new rid for the record
	rid.pageNum = pageNum;
	rid.slotNum = recordNo;
	// return 0

	free(record);
	record = NULL;
//	free(nullIndicatorArray);
	free(pageRecordData);
	pageRecordData = NULL;

	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	char *pageData = (char *)malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, pageData);

	SlotDirectory slot;
	getSlotForRID(pageData, rid, slot);
	cout << "Reading Data" << endl;
	cout << "For slot " << rid.slotNum << " : Slot offset from file is "<<  slot.offset << ". And slot length is " << slot.length << endl;
	PageRecordInfo pri;
	getPageRecordInfo(pri, (char *)pageData);
	cout << "Record Directory : Number of records : " << pri.numberOfRecords <<". Free Space : " << pri.freeSpacePos << endl;
	memcpy(data,  pageData + slot.offset, slot.length);
	free(pageData);
	pageData = NULL;
	return 0;
}

RC RecordBasedFileManager::printRecord(
		const vector<Attribute> &recordDescriptor, const void *data) {
	size_t nullIndicatorSize = ceil(recordDescriptor.size() / 8.0);
	unsigned char* nullIndicator = (unsigned char *) malloc(nullIndicatorSize);
	memcpy(nullIndicator, data, nullIndicatorSize);
	size_t offset = nullIndicatorSize;
	for (unsigned int attri = 0; attri < recordDescriptor.size(); attri++) {
		Attribute attribute = recordDescriptor[attri];
		bool isNull = nullIndicator[0]
				& (1 << (nullIndicatorSize * 8 - 1 - attri));
		if (isNull) {
			printf("%s : NULL ", attribute.name.c_str());
			continue;
		}
		if (attribute.type == TypeVarChar) {
			int length = 0;
			memcpy(&length, (char *) data + offset, sizeof(int));
			char *content = (char *) malloc(length);
			memcpy(content, (char *) data + offset + sizeof(int), length);
			offset = offset + sizeof(int) + length;
			printf("%s : %s ", attribute.name.c_str(), content);
			free(content);
		} else if (attribute.type == TypeInt) {
			int integerData = 0;
			memcpy(&integerData, (char *) data + offset, sizeof(int));
			offset = offset + sizeof(int);
			printf("%s : %d ", attribute.name.c_str(), integerData);
		} else {
			float realData = 0;
			memcpy(&realData, (char *) data + offset, sizeof(int));
			offset = offset + sizeof(float);
			printf("%s : %f ", attribute.name.c_str(), realData);
		}

	}
	return 0;
}

